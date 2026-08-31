#!/usr/bin/env python3
"""Aster cross-venue algo scanner v5.

Purpose
-------
Read PUBLIC market data only. No exchange API keys and no trading.

The scanner has two stages:
1) Fast cross-market prefilter across Aster perpetuals vs Bitget/MEXC/Bybit perpetuals.
2) A short confirmation window that measures:
   - Aster spread and cross-venue PERP deviation
   - conservative executable edge from Aster bid/ask vs external PERP fair price
   - actual Aster trade excursions away from PERP fair value
   - how often those excursions revert
   - how fast they revert
   - whether the external PERP reference stayed stable while Aster reverted
3) Spot prices are collected separately as diagnostics only, so normal perp/spot basis cannot create a LAG signal.

This is designed for scheduled GitHub Actions runs. It is a candidate detector for
manual inspection in MetaScalp, not a trading bot and not a sub-second HFT engine.
"""

from __future__ import annotations

import argparse
import bisect
import json
import math
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests


# Correct current Aster futures REST host. All hosts are overrideable from Actions env.
ASTER_BASE = os.getenv("ASTER_BASE_URL", "https://fapi.asterdex.com").rstrip("/")
BYBIT_BASE = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com").rstrip("/")
BITGET_BASE = os.getenv("BITGET_BASE_URL", "https://api.bitget.com").rstrip("/")
MEXC_BASE = os.getenv("MEXC_BASE_URL", "https://api.mexc.com").rstrip("/")

USER_AGENT = "aster-algo-scanner/5.0"
STATE_PATH = Path(os.getenv("STATE_PATH", "state/state.json"))
HTTP_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
    "Cache-Control": "no-cache",
}


@dataclass(frozen=True)
class Quote:
    bid: float
    ask: float

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def spread_pct(self) -> float:
        m = self.mid
        return ((self.ask - self.bid) / m * 100.0) if m > 0 else math.inf


@dataclass
class MarketSample:
    ts: float
    aster: Quote
    fair: float
    refs: Dict[str, Quote]
    ref_disagreement_pct: float
    deviation_pct: float
    short_edge_pct: float
    long_edge_pct: float
    # In v5, ``fair`` / ``refs`` are always external PERPETUAL references.
    # Spot is diagnostic only and never participates in LAG/MM confirmation.
    spot_fair: Optional[float] = None
    spot_refs: Dict[str, Quote] = field(default_factory=dict)
    spot_ref_disagreement_pct: Optional[float] = None

    @property
    def best_executable_edge_pct(self) -> float:
        return max(self.short_edge_pct, self.long_edge_pct, 0.0)

    @property
    def edge_side(self) -> str:
        if self.short_edge_pct > 0 and self.short_edge_pct >= self.long_edge_pct:
            return "SHORT"
        if self.long_edge_pct > 0:
            return "LONG"
        return "NONE"


@dataclass(frozen=True)
class TradePoint:
    ts: float
    price: float
    qty: float
    fair: float
    deviation_pct: float


@dataclass(frozen=True)
class Excursion:
    direction: str
    start_ts: float
    end_ts: Optional[float]
    peak_abs_deviation_pct: float
    start_fair: float
    end_fair: Optional[float]
    reverted: bool
    clean_reversion: bool

    @property
    def reversion_seconds(self) -> Optional[float]:
        if not self.reverted or self.end_ts is None:
            return None
        return max(0.0, self.end_ts - self.start_ts)


@dataclass
class Candidate:
    symbol: str
    move24h_pct: float
    quote_volume24h: float
    pre_score: float = 0.0
    samples: List[MarketSample] = field(default_factory=list)
    trades: List[TradePoint] = field(default_factory=list)
    excursions: List[Excursion] = field(default_factory=list)

    def metrics(self, cfg: "Config") -> dict:
        if not self.samples:
            return {}

        spreads = [s.aster.spread_pct for s in self.samples]
        deviations = [s.deviation_pct for s in self.samples]
        disagreements = [s.ref_disagreement_pct for s in self.samples]
        executable_edges = [s.best_executable_edge_pct for s in self.samples]
        long_edges = [s.long_edge_pct for s in self.samples]
        short_edges = [s.short_edge_pct for s in self.samples]

        med_spread = statistics.median(spreads)
        max_spread = max(spreads)
        max_abs_dev = max(abs(x) for x in deviations)
        med_abs_dev = statistics.median(abs(x) for x in deviations)
        current = self.samples[-1]
        current_dev = current.deviation_pct
        current_spot_fair = current.spot_fair
        current_spot_refs = sorted(current.spot_refs.keys())
        current_spot_disagreement = current.spot_ref_disagreement_pct
        if current_spot_fair and current_spot_fair > 0:
            external_perp_spot_basis_pct = (current.fair - current_spot_fair) / current_spot_fair * 100.0
            aster_spot_basis_pct = (current.aster.mid - current_spot_fair) / current_spot_fair * 100.0
        else:
            external_perp_spot_basis_pct = None
            aster_spot_basis_pct = None
        current_exec_edge = current.best_executable_edge_pct
        max_exec_edge = max(executable_edges)
        med_exec_edge = statistics.median(executable_edges)

        spread_hit_ratio = sum(x >= cfg.min_aster_spread_pct for x in spreads) / len(spreads)
        dev_hit_ratio = sum(abs(x) >= cfg.min_deviation_pct for x in deviations) / len(deviations)
        ref_good_ratio = sum(x <= cfg.max_reference_disagreement_pct for x in disagreements) / len(disagreements)

        # IMPORTANT: lag confirmation is based on executable Aster prices, not the Aster midpoint.
        # LONG means Aster ask is below fair; SHORT means Aster bid is above fair.
        long_hit_ratio = sum(x >= cfg.min_executable_edge_pct for x in long_edges) / len(long_edges)
        short_hit_ratio = sum(x >= cfg.min_executable_edge_pct for x in short_edges) / len(short_edges)
        if long_hit_ratio > short_hit_ratio:
            lag_side = "LONG"
            side_edges = long_edges
            persistent_exec_hit_ratio = long_hit_ratio
            current_side_edge = current.long_edge_pct
        elif short_hit_ratio > long_hit_ratio:
            lag_side = "SHORT"
            side_edges = short_edges
            persistent_exec_hit_ratio = short_hit_ratio
            current_side_edge = current.short_edge_pct
        else:
            # Tie-break by the median executable edge on each side.
            med_long = statistics.median(long_edges)
            med_short = statistics.median(short_edges)
            if max(med_long, med_short) > 0:
                lag_side = "LONG" if med_long >= med_short else "SHORT"
                side_edges = long_edges if lag_side == "LONG" else short_edges
                persistent_exec_hit_ratio = long_hit_ratio if lag_side == "LONG" else short_hit_ratio
                current_side_edge = current.long_edge_pct if lag_side == "LONG" else current.short_edge_pct
            else:
                lag_side = "NONE"
                side_edges = [0.0 for _ in self.samples]
                persistent_exec_hit_ratio = 0.0
                current_side_edge = 0.0

        persistent_median_edge = statistics.median(side_edges) if side_edges else 0.0
        persistent_max_edge = max(side_edges) if side_edges else 0.0
        current_side_edge_positive = max(0.0, current_side_edge)

        # Midpoint direction is retained only as diagnostics; it cannot confirm a lag by itself.
        positive = sum(x >= cfg.min_deviation_pct for x in deviations)
        negative = sum(x <= -cfg.min_deviation_pct for x in deviations)
        directional_hit_ratio = max(positive, negative) / len(deviations)

        events = self.excursions
        resolved = [e for e in events if e.reverted]
        clean = [e for e in events if e.clean_reversion]
        unresolved = [e for e in events if not e.reverted]
        reversion_times = [e.reversion_seconds for e in resolved if e.reversion_seconds is not None]
        clean_reversion_times = [e.reversion_seconds for e in clean if e.reversion_seconds is not None]
        peaks = [e.peak_abs_deviation_pct for e in events]

        excursion_count = len(events)
        reversion_rate = len(resolved) / excursion_count if excursion_count else 0.0
        clean_reversion_rate = len(clean) / excursion_count if excursion_count else 0.0
        median_reversion_seconds = statistics.median(reversion_times) if reversion_times else None
        median_clean_reversion_seconds = statistics.median(clean_reversion_times) if clean_reversion_times else None
        median_peak_excursion_pct = statistics.median(peaks) if peaks else 0.0
        max_peak_excursion_pct = max(peaks) if peaks else 0.0
        trades_analyzed = len(self.trades)

        # Two separate scores. The final score is the stronger setup score.
        # This prevents a giant bid/ask spread from artificially creating a high LAG score.
        lag_spread_quality = 0.0
        if med_spread <= cfg.max_lag_median_spread_pct:
            if cfg.max_lag_median_spread_pct <= cfg.min_aster_spread_pct:
                lag_spread_quality = 1.0
            elif med_spread <= cfg.min_aster_spread_pct:
                lag_spread_quality = 1.0
            else:
                lag_spread_quality = max(
                    0.0,
                    min(
                        1.0,
                        (cfg.max_lag_median_spread_pct - med_spread)
                        / (cfg.max_lag_median_spread_pct - cfg.min_aster_spread_pct),
                    ),
                )

        lag_score = min(
            100.0,
            25.0 * min(1.0, current_side_edge_positive / max(cfg.min_current_executable_edge_pct, 1e-9))
            + 25.0 * persistent_exec_hit_ratio
            + 15.0 * min(1.0, max(0.0, persistent_median_edge) / max(cfg.min_executable_edge_pct, 1e-9))
            + 10.0 * ref_good_ratio
            + 10.0 * lag_spread_quality
            + 10.0 * min(1.0, trades_analyzed / max(cfg.min_aster_trades_for_lag, 1))
            + 5.0 * min(1.0, abs(self.move24h_pct) / max(cfg.min_24h_move_pct, 1e-9)),
        )

        if median_clean_reversion_seconds is None:
            mm_speed_quality = 0.0
        else:
            mm_speed_quality = min(
                1.0,
                cfg.max_median_reversion_seconds / max(median_clean_reversion_seconds, 1e-9),
            )

        mm_score = min(
            100.0,
            15.0 * min(1.0, med_spread / max(cfg.min_aster_spread_pct, 1e-9))
            + 20.0 * min(1.0, excursion_count / max(cfg.min_excursions, 1))
            + 30.0 * clean_reversion_rate
            + 15.0 * mm_speed_quality
            + 10.0 * min(1.0, median_peak_excursion_pct / max(cfg.excursion_threshold_pct, 1e-9))
            + 10.0 * ref_good_ratio,
        )

        # CONFIRMED-LAG now requires a genuinely executable edge on ONE persistent side,
        # a sane spread, and actual Aster trading activity during the observation window.
        lag_confirmed = (
            ref_good_ratio >= cfg.min_ref_good_ratio
            and lag_side in {"LONG", "SHORT"}
            and med_spread <= cfg.max_lag_median_spread_pct
            and trades_analyzed >= cfg.min_aster_trades_for_lag
            and persistent_exec_hit_ratio >= cfg.min_exec_edge_hit_ratio
            and current_side_edge_positive >= cfg.min_current_executable_edge_pct
            and persistent_median_edge >= cfg.min_executable_edge_pct
        )

        mm_confirmed = (
            ref_good_ratio >= cfg.min_ref_good_ratio
            and med_spread >= cfg.min_aster_spread_pct
            and trades_analyzed >= cfg.min_aster_trades_for_mm
            and excursion_count >= cfg.min_excursions
            and clean_reversion_rate >= cfg.min_clean_reversion_rate
            and median_peak_excursion_pct >= cfg.excursion_threshold_pct
            and median_clean_reversion_seconds is not None
            and median_clean_reversion_seconds <= cfg.max_median_reversion_seconds
        )

        if mm_confirmed and lag_confirmed:
            setup = "CONFIRMED-BOTH"
            score = max(lag_score, mm_score)
        elif mm_confirmed:
            setup = "CONFIRMED-MM"
            score = mm_score
        elif lag_confirmed:
            setup = "CONFIRMED-LAG"
            score = lag_score
        else:
            setup = "NONE"
            score = max(lag_score, mm_score)

        # WATCH alerts are intentionally removed in v4.
        level = "CONFIRMED" if setup.startswith("CONFIRMED") and score >= cfg.confirmed_score else "NONE"

        if lag_side == "SHORT":
            direction = "Persistent executable edge is on SHORT side (Aster bid above external PERP fair)"
        elif lag_side == "LONG":
            direction = "Persistent executable edge is on LONG side (Aster ask below external PERP fair)"
        elif current_dev > 0:
            direction = "Aster midpoint above external PERP fair, but no confirmed executable edge"
        elif current_dev < 0:
            direction = "Aster midpoint below external PERP fair, but no confirmed executable edge"
        else:
            direction = "Aster near external PERP fair"

        return {
            "level": level,
            "setup": setup,
            "score": score,
            "lag_score": lag_score,
            "mm_score": mm_score,
            "median_spread_pct": med_spread,
            "max_spread_pct": max_spread,
            "max_abs_deviation_pct": max_abs_dev,
            "median_abs_deviation_pct": med_abs_dev,
            "current_deviation_pct": current_dev,
            "spread_hit_ratio": spread_hit_ratio,
            "deviation_hit_ratio": dev_hit_ratio,
            "directional_hit_ratio": directional_hit_ratio,
            "long_exec_hit_ratio": long_hit_ratio,
            "short_exec_hit_ratio": short_hit_ratio,
            "persistent_exec_hit_ratio": persistent_exec_hit_ratio,
            "persistent_edge_side": lag_side,
            "persistent_median_executable_edge_pct": persistent_median_edge,
            "persistent_max_executable_edge_pct": persistent_max_edge,
            "ref_good_ratio": ref_good_ratio,
            "current_fair": current.fair,
            "current_aster_bid": current.aster.bid,
            "current_aster_ask": current.aster.ask,
            "current_refs": sorted(current.refs.keys()),
            "current_ref_disagreement_pct": current.ref_disagreement_pct,
            "current_spot_fair": current_spot_fair,
            "current_spot_refs": current_spot_refs,
            "current_spot_ref_disagreement_pct": current_spot_disagreement,
            "external_perp_spot_basis_pct": external_perp_spot_basis_pct,
            "aster_spot_basis_pct": aster_spot_basis_pct,
            "current_executable_edge_pct": current_exec_edge,
            "current_edge_side": current.edge_side,
            "max_executable_edge_pct": max_exec_edge,
            "median_executable_edge_pct": med_exec_edge,
            "excursion_count": excursion_count,
            "reverted_count": len(resolved),
            "clean_reverted_count": len(clean),
            "unresolved_count": len(unresolved),
            "reversion_rate": reversion_rate,
            "clean_reversion_rate": clean_reversion_rate,
            "median_reversion_seconds": median_reversion_seconds,
            "median_clean_reversion_seconds": median_clean_reversion_seconds,
            "median_peak_excursion_pct": median_peak_excursion_pct,
            "max_peak_excursion_pct": max_peak_excursion_pct,
            "trades_analyzed": trades_analyzed,
            "direction": direction,
            "samples": len(self.samples),
        }


@dataclass(frozen=True)
class Config:
    # Stage 1 filters
    min_aster_spread_pct: float = float(os.getenv("MIN_ASTER_SPREAD_PCT", "0.15"))
    min_deviation_pct: float = float(os.getenv("MIN_DEVIATION_PCT", "0.20"))
    min_executable_edge_pct: float = float(os.getenv("MIN_EXECUTABLE_EDGE_PCT", "0.15"))
    min_current_executable_edge_pct: float = float(os.getenv("MIN_CURRENT_EXECUTABLE_EDGE_PCT", "0.15"))
    min_24h_move_pct: float = float(os.getenv("MIN_24H_MOVE_PCT", "8.0"))
    min_quote_volume24h: float = float(os.getenv("MIN_ASTER_QUOTE_VOLUME_24H", "50000"))
    min_reference_exchanges: int = int(os.getenv("MIN_PERP_REFERENCE_EXCHANGES", os.getenv("MIN_REFERENCE_EXCHANGES", "2")))
    max_reference_disagreement_pct: float = float(os.getenv("MAX_PERP_REFERENCE_DISAGREEMENT_PCT", os.getenv("MAX_REFERENCE_DISAGREEMENT_PCT", "0.20")))
    min_spot_reference_exchanges: int = int(os.getenv("MIN_SPOT_REFERENCE_EXCHANGES", "2"))
    max_spot_reference_disagreement_pct: float = float(os.getenv("MAX_SPOT_REFERENCE_DISAGREEMENT_PCT", "0.30"))
    max_candidates: int = int(os.getenv("MAX_CANDIDATES", "25"))

    # Stage 2 confirmation sampling
    confirm_duration_seconds: float = float(os.getenv("CONFIRM_DURATION_SECONDS", "45"))
    confirm_interval_seconds: float = float(os.getenv("CONFIRM_INTERVAL_SECONDS", "1.5"))
    max_trade_analysis_candidates: int = int(os.getenv("MAX_TRADE_ANALYSIS_CANDIDATES", "25"))
    aster_trade_limit: int = int(os.getenv("ASTER_TRADE_LIMIT", "500"))
    trade_sample_match_tolerance_seconds: float = float(os.getenv("TRADE_SAMPLE_MATCH_TOLERANCE_SECONDS", "3.0"))

    # Excursion / reversion definition
    excursion_threshold_pct: float = float(os.getenv("EXCURSION_THRESHOLD_PCT", "0.20"))
    reversion_band_pct: float = float(os.getenv("REVERSION_BAND_PCT", "0.08"))
    min_excursions: int = int(os.getenv("MIN_EXCURSIONS", "3"))
    min_clean_reversion_rate: float = float(os.getenv("MIN_CLEAN_REVERSION_RATE", "0.70"))
    max_median_reversion_seconds: float = float(os.getenv("MAX_MEDIAN_REVERSION_SECONDS", "8.0"))
    max_reference_move_during_reversion_pct: float = float(os.getenv("MAX_REFERENCE_MOVE_DURING_REVERSION_PCT", "0.12"))
    min_aster_trades_for_mm: int = int(os.getenv("MIN_ASTER_TRADES_FOR_MM", "6"))

    # Persistent executable LAG confirmation. Midpoint deviation alone cannot confirm LAG.
    min_exec_edge_hit_ratio: float = float(os.getenv("MIN_EXEC_EDGE_HIT_RATIO", "0.60"))
    max_lag_median_spread_pct: float = float(os.getenv("MAX_LAG_MEDIAN_SPREAD_PCT", "0.60"))
    min_aster_trades_for_lag: int = int(os.getenv("MIN_ASTER_TRADES_FOR_LAG", "3"))
    min_ref_good_ratio: float = float(os.getenv("MIN_REF_GOOD_RATIO", "0.80"))

    # Alerts: v4 sends CONFIRMED only; WATCH is intentionally disabled.
    confirmed_score: float = float(os.getenv("CONFIRMED_SCORE", "70"))
    alert_levels: Tuple[str, ...] = tuple(
        x.strip().upper() for x in os.getenv("ALERT_LEVELS", "CONFIRMED").split(",") if x.strip()
    )
    cooldown_minutes: int = int(os.getenv("ALERT_COOLDOWN_MINUTES", "60"))
    chart_alert_levels: Tuple[str, ...] = tuple(
        x.strip().upper() for x in os.getenv("CHART_ALERT_LEVELS", "CONFIRMED").split(",") if x.strip()
    )
    charts_dir: str = os.getenv("CHARTS_DIR", "charts")
    request_timeout_seconds: float = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "12"))


class ApiError(RuntimeError):
    pass


def fnum(value, default: float = 0.0) -> float:
    try:
        x = float(value)
        if math.isfinite(x):
            return x
    except (TypeError, ValueError):
        pass
    return default


def valid_quote(bid, ask) -> Optional[Quote]:
    b, a = fnum(bid), fnum(ask)
    if b > 0 and a > 0 and a >= b:
        return Quote(b, a)
    return None


def get_json(url: str, timeout: float, params: Optional[dict] = None) -> object:
    r = requests.get(url, params=params, timeout=timeout, headers=HTTP_HEADERS)
    r.raise_for_status()
    return r.json()


def fetch_aster_book(cfg: Config) -> Dict[str, Quote]:
    data = get_json(f"{ASTER_BASE}/fapi/v3/ticker/bookTicker", cfg.request_timeout_seconds)
    if isinstance(data, dict):
        data = [data]
    out: Dict[str, Quote] = {}
    for row in data if isinstance(data, list) else []:
        symbol = str(row.get("symbol", "")).upper()
        q = valid_quote(row.get("bidPrice"), row.get("askPrice"))
        if symbol.endswith("USDT") and q:
            out[symbol] = q
    return out


def fetch_aster_24h(cfg: Config) -> Dict[str, dict]:
    data = get_json(f"{ASTER_BASE}/fapi/v3/ticker/24hr", cfg.request_timeout_seconds)
    if isinstance(data, dict):
        data = [data]
    out: Dict[str, dict] = {}
    for row in data if isinstance(data, list) else []:
        symbol = str(row.get("symbol", "")).upper()
        if not symbol.endswith("USDT"):
            continue
        out[symbol] = {
            "move24h_pct": fnum(row.get("priceChangePercent")),
            "quote_volume24h": fnum(row.get("quoteVolume")),
        }
    return out


def fetch_aster_recent_trades(symbol: str, cfg: Config) -> List[dict]:
    limit = max(1, min(cfg.aster_trade_limit, 1000))
    data = get_json(
        f"{ASTER_BASE}/fapi/v3/trades",
        cfg.request_timeout_seconds,
        params={"symbol": symbol, "limit": limit},
    )
    return data if isinstance(data, list) else []


def fetch_bybit_spot(cfg: Config) -> Dict[str, Quote]:
    data = get_json(f"{BYBIT_BASE}/v5/market/tickers", cfg.request_timeout_seconds, params={"category": "spot"})
    rows = (((data or {}).get("result") or {}).get("list") or []) if isinstance(data, dict) else []
    out: Dict[str, Quote] = {}
    for row in rows:
        symbol = str(row.get("symbol", "")).upper()
        q = valid_quote(row.get("bid1Price"), row.get("ask1Price"))
        if symbol.endswith("USDT") and q:
            out[symbol] = q
    return out


def fetch_bybit_perp(cfg: Config) -> Dict[str, Quote]:
    """USDT linear perpetual/futures best bid/ask from Bybit."""
    data = get_json(f"{BYBIT_BASE}/v5/market/tickers", cfg.request_timeout_seconds, params={"category": "linear"})
    rows = (((data or {}).get("result") or {}).get("list") or []) if isinstance(data, dict) else []
    out: Dict[str, Quote] = {}
    for row in rows:
        symbol = str(row.get("symbol", "")).upper()
        q = valid_quote(row.get("bid1Price"), row.get("ask1Price"))
        if symbol.endswith("USDT") and q:
            out[symbol] = q
    return out


def fetch_bitget_spot(cfg: Config) -> Dict[str, Quote]:
    data = get_json(f"{BITGET_BASE}/api/v2/spot/market/tickers", cfg.request_timeout_seconds)
    rows = (data or {}).get("data", []) if isinstance(data, dict) else []
    out: Dict[str, Quote] = {}
    for row in rows:
        symbol = str(row.get("symbol", "")).upper()
        q = valid_quote(row.get("bidPr"), row.get("askPr"))
        if symbol.endswith("USDT") and q:
            out[symbol] = q
    return out


def fetch_bitget_perp(cfg: Config) -> Dict[str, Quote]:
    """USDT-M futures/perpetual best bid/ask from Bitget."""
    data = get_json(
        f"{BITGET_BASE}/api/v2/mix/market/tickers",
        cfg.request_timeout_seconds,
        params={"productType": "USDT-FUTURES"},
    )
    rows = (data or {}).get("data", []) if isinstance(data, dict) else []
    out: Dict[str, Quote] = {}
    for row in rows:
        symbol = str(row.get("symbol", "")).upper()
        q = valid_quote(row.get("bidPr"), row.get("askPr"))
        if symbol.endswith("USDT") and q:
            out[symbol] = q
    return out


def fetch_mexc_spot(cfg: Config) -> Dict[str, Quote]:
    data = get_json(f"{MEXC_BASE}/api/v3/ticker/bookTicker", cfg.request_timeout_seconds)
    if isinstance(data, dict):
        data = [data]
    out: Dict[str, Quote] = {}
    for row in data if isinstance(data, list) else []:
        symbol = str(row.get("symbol", "")).upper()
        q = valid_quote(row.get("bidPrice"), row.get("askPrice"))
        if symbol.endswith("USDT") and q:
            out[symbol] = q
    return out


def fetch_mexc_perp(cfg: Config) -> Dict[str, Quote]:
    """MEXC USDT perpetual tickers.

    MEXC futures symbols are commonly returned as BASE_USDT, so normalize them
    to BASEUSDT to match Aster/Bitget/Bybit.
    """
    data = get_json(f"{MEXC_BASE}/api/v1/contract/ticker", cfg.request_timeout_seconds)
    rows = (data or {}).get("data", []) if isinstance(data, dict) else []
    if isinstance(rows, dict):
        rows = [rows]
    out: Dict[str, Quote] = {}
    for row in rows if isinstance(rows, list) else []:
        raw_symbol = str(row.get("symbol", "")).upper()
        symbol = raw_symbol.replace("_", "")
        q = valid_quote(row.get("bid1"), row.get("ask1"))
        if symbol.endswith("USDT") and q:
            out[symbol] = q
    return out


def fetch_snapshot(
    cfg: Config,
) -> Tuple[Dict[str, Quote], Dict[str, Dict[str, Quote]], Dict[str, Dict[str, Quote]], List[str]]:
    """Fetch one cross-venue snapshot.

    Returns:
      Aster perpetual book,
      external perpetual references,
      external spot references,
      non-fatal errors.

    Aster is mandatory. Individual reference venues are optional; a symbol is
    only analyzed when enough PERP references agree.
    """
    funcs = {
        "aster": fetch_aster_book,
        "bybit-perp": fetch_bybit_perp,
        "bitget-perp": fetch_bitget_perp,
        "mexc-perp": fetch_mexc_perp,
        "bybit-spot": fetch_bybit_spot,
        "bitget-spot": fetch_bitget_spot,
        "mexc-spot": fetch_mexc_spot,
    }
    results: Dict[str, Dict[str, Quote]] = {}
    errors: List[str] = []
    with ThreadPoolExecutor(max_workers=7) as pool:
        futures = {pool.submit(fn, cfg): name for name, fn in funcs.items()}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                results[name] = fut.result()
            except Exception as e:
                errors.append(f"{name}: {type(e).__name__}: {e}")
                results[name] = {}

    if not results.get("aster"):
        raise ApiError("Aster market data is unavailable; cannot scan.")

    perp_refs = {k: v for k, v in results.items() if k.endswith("-perp") and v}
    spot_refs = {k: v for k, v in results.items() if k.endswith("-spot") and v}
    return results["aster"], perp_refs, spot_refs, errors


def reference_for_symbol(
    symbol: str,
    refs: Dict[str, Dict[str, Quote]],
    min_exchanges: int,
    max_disagreement_pct: float,
) -> Optional[Tuple[float, Dict[str, Quote], float]]:
    selected: Dict[str, Quote] = {}
    mids: List[float] = []
    for name, market in refs.items():
        q = market.get(symbol)
        if q:
            selected[name] = q
            mids.append(q.mid)

    if len(mids) < min_exchanges:
        return None

    fair = statistics.median(mids)
    disagreement = ((max(mids) - min(mids)) / fair * 100.0) if fair > 0 else math.inf
    if disagreement > max_disagreement_pct:
        return None
    return fair, selected, disagreement


def build_sample(
    symbol: str,
    aster_book: Dict[str, Quote],
    perp_refs: Dict[str, Dict[str, Quote]],
    spot_refs: Dict[str, Dict[str, Quote]],
    cfg: Config,
) -> Optional[MarketSample]:
    aq = aster_book.get(symbol)
    if not aq:
        return None

    # V5: the ONLY fair price used by LAG/MM confirmation is external PERP fair.
    perp_ref = reference_for_symbol(
        symbol,
        perp_refs,
        cfg.min_reference_exchanges,
        cfg.max_reference_disagreement_pct,
    )
    if not perp_ref:
        return None

    fair, selected_perps, perp_disagreement = perp_ref

    # Spot is best-effort diagnostic context. Missing or divergent spot data must
    # never block a valid perp-vs-perp signal.
    spot_ref = reference_for_symbol(
        symbol,
        spot_refs,
        cfg.min_spot_reference_exchanges,
        cfg.max_spot_reference_disagreement_pct,
    )
    if spot_ref:
        spot_fair, selected_spots, spot_disagreement = spot_ref
    else:
        spot_fair, selected_spots, spot_disagreement = None, {}, None

    deviation = (aq.mid - fair) / fair * 100.0

    # Conservative executable edge against external PERP fair.
    # SHORT: immediately sell Aster bid when Aster is rich vs other perps.
    # LONG: immediately buy Aster ask when Aster is cheap vs other perps.
    short_edge = (aq.bid - fair) / fair * 100.0
    long_edge = (fair - aq.ask) / fair * 100.0

    return MarketSample(
        ts=time.time(),
        aster=aq,
        fair=fair,
        refs=selected_perps,
        ref_disagreement_pct=perp_disagreement,
        deviation_pct=deviation,
        short_edge_pct=short_edge,
        long_edge_pct=long_edge,
        spot_fair=spot_fair,
        spot_refs=selected_spots,
        spot_ref_disagreement_pct=spot_disagreement,
    )


def prefilter_candidates(
    aster_book: Dict[str, Quote],
    aster_24h: Dict[str, dict],
    perp_refs: Dict[str, Dict[str, Quote]],
    spot_refs: Dict[str, Dict[str, Quote]],
    cfg: Config,
) -> List[Candidate]:
    ranked: List[Tuple[float, Candidate]] = []
    for symbol, aq in aster_book.items():
        stats = aster_24h.get(symbol, {})
        move = fnum(stats.get("move24h_pct"))
        qvol = fnum(stats.get("quote_volume24h"))
        if abs(move) < cfg.min_24h_move_pct or qvol < cfg.min_quote_volume24h:
            continue

        sample = build_sample(symbol, aster_book, perp_refs, spot_refs, cfg)
        if not sample:
            continue

        spread = aq.spread_pct
        dev = abs(sample.deviation_pct)
        edge = sample.best_executable_edge_pct
        if (
            spread < cfg.min_aster_spread_pct
            and dev < cfg.min_deviation_pct
            and edge < cfg.min_executable_edge_pct
        ):
            continue

        pre_score = (
            spread / max(cfg.min_aster_spread_pct, 1e-9)
            + dev / max(cfg.min_deviation_pct, 1e-9)
            + edge / max(cfg.min_executable_edge_pct, 1e-9)
            + min(abs(move) / max(cfg.min_24h_move_pct, 1e-9), 3.0) * 0.20
        )
        ranked.append((pre_score, Candidate(symbol, move, qvol, pre_score=pre_score, samples=[sample])))

    ranked.sort(key=lambda x: x[0], reverse=True)
    return [c for _, c in ranked[: cfg.max_candidates]]


def collect_confirmation_samples(
    candidates: List[Candidate],
    cfg: Config,
    errors: List[str],
) -> None:
    if not candidates:
        return

    by_symbol = {c.symbol: c for c in candidates}
    start = time.time()
    target_end = start + max(0.0, cfg.confirm_duration_seconds)
    sample_no = 1  # the prefilter snapshot is sample #1

    while time.time() < target_end:
        time.sleep(max(0.1, cfg.confirm_interval_seconds))
        sample_no += 1
        try:
            aster_book, perp_refs, spot_refs, sample_errors = fetch_snapshot(cfg)
            errors.extend(sample_errors)
        except Exception as e:
            errors.append(f"confirmation sample {sample_no}: {type(e).__name__}: {e}")
            continue

        for symbol, candidate in by_symbol.items():
            s = build_sample(symbol, aster_book, perp_refs, spot_refs, cfg)
            if s:
                candidate.samples.append(s)


def nearest_sample(samples: List[MarketSample], ts: float, tolerance: float) -> Optional[MarketSample]:
    if not samples:
        return None
    times = [s.ts for s in samples]
    i = bisect.bisect_left(times, ts)
    choices = []
    if i < len(samples):
        choices.append(samples[i])
    if i > 0:
        choices.append(samples[i - 1])
    if not choices:
        return None
    best = min(choices, key=lambda s: abs(s.ts - ts))
    return best if abs(best.ts - ts) <= tolerance else None


def map_trades_to_fair(candidate: Candidate, raw_trades: List[dict], cfg: Config) -> List[TradePoint]:
    if not candidate.samples:
        return []
    first_ts = candidate.samples[0].ts - cfg.trade_sample_match_tolerance_seconds
    last_ts = candidate.samples[-1].ts + cfg.trade_sample_match_tolerance_seconds
    out: List[TradePoint] = []

    for row in raw_trades:
        ts_ms = fnum(row.get("time"))
        price = fnum(row.get("price"))
        qty = fnum(row.get("qty"))
        if ts_ms <= 0 or price <= 0:
            continue
        ts = ts_ms / 1000.0
        if ts < first_ts or ts > last_ts:
            continue
        sample = nearest_sample(candidate.samples, ts, cfg.trade_sample_match_tolerance_seconds)
        if not sample or sample.fair <= 0:
            continue
        dev = (price - sample.fair) / sample.fair * 100.0
        out.append(TradePoint(ts=ts, price=price, qty=qty, fair=sample.fair, deviation_pct=dev))

    out.sort(key=lambda t: t.ts)
    return out


def detect_excursions(trades: List[TradePoint], cfg: Config) -> List[Excursion]:
    events: List[Excursion] = []
    active: Optional[dict] = None

    def sign_of(dev: float) -> int:
        return 1 if dev > 0 else -1

    for t in trades:
        dev = t.deviation_pct

        if active is None:
            if abs(dev) >= cfg.excursion_threshold_pct:
                active = {
                    "sign": sign_of(dev),
                    "direction": "ABOVE" if dev > 0 else "BELOW",
                    "start_ts": t.ts,
                    "start_fair": t.fair,
                    "peak": abs(dev),
                }
            continue

        active["peak"] = max(active["peak"], abs(dev))

        # A reversion is observed if the trade returns near fair OR crosses through fair.
        reverted = abs(dev) <= cfg.reversion_band_pct or sign_of(dev) != active["sign"]
        if reverted:
            ref_move = abs((t.fair - active["start_fair"]) / active["start_fair"] * 100.0)
            clean = ref_move <= cfg.max_reference_move_during_reversion_pct
            events.append(
                Excursion(
                    direction=active["direction"],
                    start_ts=active["start_ts"],
                    end_ts=t.ts,
                    peak_abs_deviation_pct=active["peak"],
                    start_fair=active["start_fair"],
                    end_fair=t.fair,
                    reverted=True,
                    clean_reversion=clean,
                )
            )
            active = None

            # If the same trade crossed all the way into a new opposite excursion, start it immediately.
            if abs(dev) >= cfg.excursion_threshold_pct:
                active = {
                    "sign": sign_of(dev),
                    "direction": "ABOVE" if dev > 0 else "BELOW",
                    "start_ts": t.ts,
                    "start_fair": t.fair,
                    "peak": abs(dev),
                }

    if active is not None:
        events.append(
            Excursion(
                direction=active["direction"],
                start_ts=active["start_ts"],
                end_ts=None,
                peak_abs_deviation_pct=active["peak"],
                start_fair=active["start_fair"],
                end_fair=None,
                reverted=False,
                clean_reversion=False,
            )
        )

    return events


def add_trade_analysis(candidates: List[Candidate], cfg: Config, errors: List[str]) -> None:
    if not candidates:
        return

    # Rank provisionally after confirmation samples so the extra Aster trade calls are spent on the best names.
    provisional = []
    for c in candidates:
        m = c.metrics(cfg)
        provisional.append((m.get("score", 0.0), c))
    provisional.sort(key=lambda x: x[0], reverse=True)
    selected = [c for _, c in provisional[: max(1, cfg.max_trade_analysis_candidates)]]

    def worker(c: Candidate) -> Tuple[str, List[dict]]:
        return c.symbol, fetch_aster_recent_trades(c.symbol, cfg)

    with ThreadPoolExecutor(max_workers=min(5, len(selected))) as pool:
        futures = {pool.submit(worker, c): c for c in selected}
        for fut in as_completed(futures):
            c = futures[fut]
            try:
                _, raw = fut.result()
                c.trades = map_trades_to_fair(c, raw, cfg)
                c.excursions = detect_excursions(c.trades, cfg)
            except Exception as e:
                errors.append(f"aster trades {c.symbol}: {type(e).__name__}: {e}")


def scan(cfg: Config) -> Tuple[List[Tuple[Candidate, dict]], List[str]]:
    try:
        aster_24h = fetch_aster_24h(cfg)
    except Exception as e:
        raise ApiError(f"Failed to fetch Aster 24h stats: {type(e).__name__}: {e}") from e

    aster_book, perp_refs, spot_refs, errors = fetch_snapshot(cfg)
    candidates = prefilter_candidates(aster_book, aster_24h, perp_refs, spot_refs, cfg)

    if not candidates:
        return [], errors

    print(f"Prefilter candidates ({len(candidates)}): {', '.join(c.symbol for c in candidates)}")
    print(
        f"Confirming for ~{cfg.confirm_duration_seconds:.0f}s every "
        f"{cfg.confirm_interval_seconds:.1f}s..."
    )
    collect_confirmation_samples(candidates, cfg, errors)
    add_trade_analysis(candidates, cfg, errors)

    ranked: List[Tuple[Candidate, dict]] = []
    for candidate in candidates:
        m = candidate.metrics(cfg)
        if m:
            ranked.append((candidate, m))
    ranked.sort(key=lambda x: x[1]["score"], reverse=True)
    return ranked, errors


def load_state() -> dict:
    try:
        if STATE_PATH.exists():
            data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                data.setdefault("last_alerts", {})
                return data
    except Exception as e:
        print(f"WARN: cannot read state: {e}", file=sys.stderr)
    return {"last_alerts": {}}


def save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def should_alert(symbol: str, level: str, setup: str, state: dict, cfg: Config) -> bool:
    prev = state.get("last_alerts", {}).get(symbol)
    if not prev:
        return True
    prev_level = str(prev.get("level", "NONE")).upper()
    prev_setup = str(prev.get("setup", "NONE")).upper()
    prev_ts = fnum(prev.get("ts"))

    # A materially different confirmed setup may alert immediately; otherwise honor cooldown.
    if level == "CONFIRMED" and prev_level != "CONFIRMED":
        return True
    if level == "CONFIRMED" and setup != prev_setup:
        return True
    return time.time() - prev_ts >= cfg.cooldown_minutes * 60


def fmt_price(x: float) -> str:
    if x >= 1000:
        return f"{x:,.2f}"
    if x >= 1:
        return f"{x:.6f}".rstrip("0").rstrip(".")
    return f"{x:.10f}".rstrip("0").rstrip(".")


def fmt_seconds(x: Optional[float]) -> str:
    return "n/a" if x is None else f"{x:.2f}s"


def create_price_chart(candidate: Candidate, m: dict, cfg: Config) -> Optional[Path]:
    """Create a Telegram-friendly seconds-scale PNG.

    V5 separates benchmarks:
      - External PERP fair (Bitget/MEXC/Bybit perps): PRIMARY benchmark used for signals.
      - External SPOT fair: DIAGNOSTIC line only, never used to confirm LAG/MM.
      - Aster bid/ask: executable Aster prices.
      - Crosses: actual Aster trades far from matched PERP fair.
    """
    if len(candidate.samples) < 2:
        return None

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.ticker import FuncFormatter
    except Exception as e:
        print(f"WARN: chart library unavailable: {type(e).__name__}: {e}", file=sys.stderr)
        return None

    samples = candidate.samples
    t0 = samples[0].ts
    xs = [max(0.0, s.ts - t0) for s in samples]
    perp_fairs = [s.fair for s in samples]
    spot_fairs = [s.spot_fair if s.spot_fair and s.spot_fair > 0 else math.nan for s in samples]
    bids = [s.aster.bid for s in samples]
    asks = [s.aster.ask for s in samples]

    out_dir = Path(cfg.charts_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{candidate.symbol}_{m['setup']}_{int(time.time())}.png"

    fig, ax = plt.subplots(figsize=(10, 5.2), dpi=150)
    ax.plot(xs, perp_fairs, label="External PERP fair", linewidth=2.2)
    if sum(math.isfinite(x) for x in spot_fairs) >= 2:
        ax.plot(xs, spot_fairs, label="External SPOT fair (diagnostic)", linewidth=1.5, linestyle="--")
    ax.plot(xs, bids, label="Aster bid", linewidth=1.5)
    ax.plot(xs, asks, label="Aster ask", linewidth=1.5)
    ax.fill_between(xs, bids, asks, alpha=0.10, label="Aster spread")

    excursion_trades = [
        t for t in candidate.trades
        if abs(t.deviation_pct) >= cfg.excursion_threshold_pct and -1.0 <= t.ts - t0 <= xs[-1] + 1.0
    ]
    if excursion_trades:
        tx = [t.ts - t0 for t in excursion_trades]
        ty = [t.price for t in excursion_trades]
        ax.scatter(tx, ty, marker="x", s=38, label="Aster excursion trades vs PERP fair", zorder=5)

    side = m.get("current_edge_side", "NONE")
    edge = m.get("current_executable_edge_pct", 0.0)
    persistent_side = m.get("persistent_edge_side", "NONE")
    persistent_hit = m.get("persistent_exec_hit_ratio", 0.0) * 100.0
    ax.set_title(
        f"{candidate.symbol} | {m['setup']} | ~{xs[-1]:.0f}s\n"
        f"PERP executable edge {edge:.3f}% {side} | persistent {persistent_side} hit {persistent_hit:.0f}%"
    )
    ax.set_xlabel("Seconds from start of confirmation window")
    ax.set_ylabel("Price")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _pos: fmt_price(float(y))))
    ax.grid(True, alpha=0.20)
    ax.legend(loc="best")
    fig.tight_layout()
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    return out


def send_telegram_photo(path: Path, candidate: Candidate, m: dict) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        print(f"Telegram secrets are not set; chart saved only: {path}")
        return False

    duration = 0.0
    if len(candidate.samples) >= 2:
        duration = candidate.samples[-1].ts - candidate.samples[0].ts

    basis = m.get("external_perp_spot_basis_pct")
    basis_line = ""
    if basis is not None:
        basis_line = f"\nExternal PERP/SPOT basis: <b>{basis:+.3f}%</b> (diagnostic)"

    caption = (
        f"📈 <b>{candidate.symbol} — {m['setup']}</b>\n"
        f"~{duration:.0f}s: external PERP fair vs Aster bid/ask"
        f"{basis_line}\n"
        f"PERP edge now: <b>{m['current_executable_edge_pct']:.3f}% {m['current_edge_side']}</b> | "
        f"persistent {m['persistent_edge_side']} hit {m['persistent_exec_hit_ratio'] * 100:.0f}%\n"
        "Solid PERP fair is the signal benchmark. Dashed SPOT fair is context only and cannot create a signal."
    )

    try:
        with path.open("rb") as fh:
            r = requests.post(
                f"https://api.telegram.org/bot{token}/sendPhoto",
                data={
                    "chat_id": chat_id,
                    "caption": caption,
                    "parse_mode": "HTML",
                },
                files={"photo": (path.name, fh, "image/png")},
                timeout=20,
                headers=HTTP_HEADERS,
            )
        r.raise_for_status()
        payload = r.json()
        if not payload.get("ok"):
            raise ApiError(f"Telegram sendPhoto returned ok=false: {payload.get('description', 'unknown error')}")
        return True
    except Exception as e:
        print(f"ERROR sending Telegram chart: {type(e).__name__}: {e}", file=sys.stderr)
        return False


def format_alert(candidate: Candidate, m: dict) -> str:
    perp_refs = ", ".join(m["current_refs"])
    spot_refs = ", ".join(m.get("current_spot_refs", []))
    side = m["current_edge_side"]
    side_text = "none" if side == "NONE" else side
    persistent_side = m["persistent_edge_side"]

    spot_block = ""
    spot_fair = m.get("current_spot_fair")
    if spot_fair is not None:
        spot_dis = m.get("current_spot_ref_disagreement_pct")
        perp_spot_basis = m.get("external_perp_spot_basis_pct")
        aster_spot_basis = m.get("aster_spot_basis_pct")
        spot_block = (
            f"SPOT fair (diagnostic): {fmt_price(spot_fair)} [{spot_refs}]\n"
            f"External PERP vs SPOT basis: {perp_spot_basis:+.3f}%\n"
            f"Aster midpoint vs SPOT basis: {aster_spot_basis:+.3f}%\n"
        )
        if spot_dis is not None:
            spot_block += f"SPOT reference disagreement: {spot_dis:.3f}%\n"
    else:
        spot_block = "SPOT fair (diagnostic): unavailable / insufficient agreeing venues\n"

    return (
        f"🔥 <b>{m['level']} / {m['setup']} — {candidate.symbol}</b>\n"
        f"Score: <b>{m['score']:.0f}/100</b> | LAG {m['lag_score']:.0f} | MM {m['mm_score']:.0f}\n"
        f"Aster PERP: {fmt_price(m['current_aster_bid'])} / {fmt_price(m['current_aster_ask'])}\n"
        f"External PERP fair: <b>{fmt_price(m['current_fair'])}</b> [{perp_refs}]\n"
        f"Executable edge vs PERP fair now: <b>{m['current_executable_edge_pct']:.3f}% {side_text}</b>\n"
        f"Persistent executable side: <b>{persistent_side}</b> | hit ratio "
        f"<b>{m['persistent_exec_hit_ratio'] * 100:.0f}%</b> | median edge "
        f"{m['persistent_median_executable_edge_pct']:.3f}%\n"
        f"Aster midpoint deviation vs PERP fair: {m['current_deviation_pct']:+.3f}% (diagnostic)\n"
        f"PERP reference disagreement: {m['current_ref_disagreement_pct']:.3f}%\n"
        f"{spot_block}"
        f"Aster spread: median <b>{m['median_spread_pct']:.3f}%</b> | max {m['max_spread_pct']:.3f}%\n"
        f"Aster trades analyzed: <b>{m['trades_analyzed']}</b>\n"
        f"Excursions vs PERP fair ≥ threshold: <b>{m['excursion_count']}</b> | unresolved {m['unresolved_count']}\n"
        f"Clean reverted: <b>{m['clean_reverted_count']}/{m['excursion_count']} "
        f"({m['clean_reversion_rate'] * 100:.0f}%)</b>\n"
        f"Median clean reversion: <b>{fmt_seconds(m['median_clean_reversion_seconds'])}</b>\n"
        f"Median peak excursion: {m['median_peak_excursion_pct']:.3f}% | max {m['max_peak_excursion_pct']:.3f}%\n"
        f"Aster 24h: {candidate.move24h_pct:+.1f}% | quote vol {candidate.quote_volume24h:,.0f}\n"
        f"State: <b>{m['direction']}</b>\n\n"
        "V5 confirmation uses Aster PERP vs external PERP fair only. SPOT is shown only as context, so normal perp/spot basis cannot create CONFIRMED-LAG or CONFIRMED-MM. Manual inspection only."
    )


def send_telegram(text: str) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        print("Telegram secrets are not set; alert printed only.")
        print(text.replace("<b>", "").replace("</b>", ""))
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": "true",
            },
            timeout=12,
            headers=HTTP_HEADERS,
        )
        r.raise_for_status()
        payload = r.json()
        if not payload.get("ok"):
            raise ApiError(f"Telegram returned ok=false: {payload.get('description', 'unknown error')}")
        return True
    except Exception as e:
        print(f"ERROR sending Telegram alert: {type(e).__name__}: {e}", file=sys.stderr)
        return False


def print_table(ranked: List[Tuple[Candidate, dict]]) -> None:
    if not ranked:
        print("No candidates passed the prefilter.")
        return
    print("\nTop results:")
    print(
        f"{'SYMBOL':<16} {'LEVEL':<10} {'SETUP':<15} {'SCORE':>5} {'SPR%':>7} "
        f"{'EDGE%':>7} {'EXC':>4} {'C.REV%':>7} {'REVsec':>7} {'24H%':>8}"
    )
    for c, m in ranked[:20]:
        revsec = m["median_clean_reversion_seconds"]
        revtxt = "-" if revsec is None else f"{revsec:.2f}"
        print(
            f"{c.symbol:<16} {m['level']:<10} {m['setup']:<15} {m['score']:>5.0f} "
            f"{m['median_spread_pct']:>7.3f} {m['max_executable_edge_pct']:>7.3f} "
            f"{m['excursion_count']:>4} {m['clean_reversion_rate'] * 100:>7.0f} {revtxt:>7} "
            f"{c.move24h_pct:>+8.1f}"
        )


def self_test() -> None:
    cfg = Config(
        min_aster_spread_pct=0.15,
        min_deviation_pct=0.20,
        min_executable_edge_pct=0.15,
        min_current_executable_edge_pct=0.15,
        min_24h_move_pct=8.0,
        min_quote_volume24h=50000,
        min_reference_exchanges=2,
        max_reference_disagreement_pct=0.20,
        max_candidates=25,
        confirm_duration_seconds=0,
        confirm_interval_seconds=0.1,
        max_trade_analysis_candidates=25,
        aster_trade_limit=500,
        trade_sample_match_tolerance_seconds=3,
        excursion_threshold_pct=0.20,
        reversion_band_pct=0.08,
        min_excursions=3,
        min_clean_reversion_rate=0.70,
        max_median_reversion_seconds=8,
        max_reference_move_during_reversion_pct=0.12,
        min_aster_trades_for_mm=6,
        min_exec_edge_hit_ratio=0.60,
        max_lag_median_spread_pct=0.60,
        min_aster_trades_for_lag=3,
        min_ref_good_ratio=0.80,
        confirmed_score=70,
        alert_levels=("CONFIRMED",),
        cooldown_minutes=60,
        chart_alert_levels=("CONFIRMED",),
        charts_dir="charts",
        request_timeout_seconds=1,
    )

    now = time.time()
    refs = {"bitget": Quote(9.999, 10.001), "mexc": Quote(9.998, 10.002)}

    # 1) Repeated clean trade excursions should confirm the MM setup.
    mm = Candidate("MMTESTUSDT", 22.0, 1_500_000)
    devs = [0.05, 0.26, 0.04, -0.28, -0.03, 0.31, 0.02, 0.22, 0.01]
    for i, dev in enumerate(devs):
        fair = 10.0
        mid = fair * (1 + dev / 100)
        width = mid * 0.22 / 100
        aq = Quote(mid - width / 2, mid + width / 2)
        short_edge = (aq.bid - fair) / fair * 100
        long_edge = (fair - aq.ask) / fair * 100
        mm.samples.append(MarketSample(now + i, aq, fair, refs, 0.0, dev, short_edge, long_edge))
    trade_devs = [0.02, 0.27, 0.29, 0.04, -0.25, -0.31, -0.02, 0.24, 0.03]
    mm.trades = [TradePoint(now + i, 10 * (1 + d / 100), 100, 10.0, d) for i, d in enumerate(trade_devs)]
    mm.excursions = detect_excursions(mm.trades, cfg)
    mm_m = mm.metrics(cfg)
    assert len(mm.excursions) >= 3, mm.excursions
    assert mm_m["clean_reversion_rate"] >= 0.70, mm_m
    assert mm_m["setup"] in {"CONFIRMED-MM", "CONFIRMED-BOTH"}, mm_m
    assert mm_m["level"] == "CONFIRMED", mm_m

    # 2) A genuine persistent executable SHORT edge with a sane spread and real trades should confirm LAG.
    lag = Candidate("LAGTESTUSDT", 25.0, 2_000_000)
    for i in range(12):
        fair = 10.0
        # bid 0.40% above fair; ~0.20% spread. Both bid/ask remain well above fair.
        aq = Quote(10.040, 10.060)
        dev = (aq.mid - fair) / fair * 100
        lag.samples.append(
            MarketSample(now + i, aq, fair, refs, 0.0, dev,
                         (aq.bid - fair) / fair * 100,
                         (fair - aq.ask) / fair * 100)
        )
    lag.trades = [TradePoint(now + i, 10.05, 10, 10.0, 0.50) for i in range(5)]
    lag.excursions = detect_excursions(lag.trades, cfg)
    lag_m = lag.metrics(cfg)
    assert lag_m["setup"] in {"CONFIRMED-LAG", "CONFIRMED-BOTH"}, lag_m
    assert lag_m["persistent_edge_side"] == "SHORT", lag_m
    assert lag_m["persistent_exec_hit_ratio"] >= 0.99, lag_m
    assert lag_m["level"] == "CONFIRMED", lag_m

    # 3) LA-like false positive: huge spread, midpoint far from fair, but ask almost at fair and zero trades.
    # This MUST NOT be a confirmed lag anymore.
    bad = Candidate("WIDETESTUSDT", 10.0, 100_000)
    for i in range(12):
        fair = 10.0
        aq = Quote(9.80, 9.99)  # huge spread; LONG executable edge only 0.10%
        dev = (aq.mid - fair) / fair * 100
        bad.samples.append(
            MarketSample(now + i, aq, fair, refs, 0.0, dev,
                         (aq.bid - fair) / fair * 100,
                         (fair - aq.ask) / fair * 100)
        )
    bad_m = bad.metrics(cfg)
    assert bad_m["setup"] == "NONE", bad_m
    assert bad_m["level"] == "NONE", bad_m

    # 4) Large PERP/SPOT basis must NOT create a signal when Aster agrees with external PERPs.
    basis = Candidate("BASISTESTUSDT", 50.0, 3_000_000)
    spot_refs = {"bitget-spot": Quote(10.399, 10.401), "mexc-spot": Quote(10.398, 10.402)}
    perp_refs = {"bitget-perp": Quote(9.999, 10.001), "mexc-perp": Quote(9.998, 10.002)}
    for i in range(12):
        perp_fair = 10.0
        spot_fair = 10.4  # ~4% spot/perp basis
        aq = Quote(9.995, 10.005)  # Aster PERP agrees with external PERPs
        dev = (aq.mid - perp_fair) / perp_fair * 100
        basis.samples.append(
            MarketSample(
                now + i, aq, perp_fair, perp_refs, 0.0, dev,
                (aq.bid - perp_fair) / perp_fair * 100,
                (perp_fair - aq.ask) / perp_fair * 100,
                spot_fair=spot_fair,
                spot_refs=spot_refs,
                spot_ref_disagreement_pct=0.0,
            )
        )
    basis.trades = [TradePoint(now + i, 10.0, 10, 10.0, 0.0) for i in range(5)]
    basis_m = basis.metrics(cfg)
    assert basis_m["setup"] == "NONE", basis_m
    assert basis_m["level"] == "NONE", basis_m
    assert basis_m["external_perp_spot_basis_pct"] < -3.0, basis_m

    assert valid_quote("1", "1.01") is not None
    assert valid_quote("1.01", "1") is None
    print("Self-test OK (PERP MM + executable PERP LAG + wide-spread + perp/spot-basis rejection)")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Never send Telegram; print candidates only")
    parser.add_argument("--self-test", action="store_true", help="Run deterministic local self-test")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return 0

    cfg = Config()
    print("Config:", cfg)

    try:
        ranked, errors = scan(cfg)
    except ApiError as e:
        print(f"FATAL: {e}", file=sys.stderr)
        return 2

    print_table(ranked)
    if errors:
        unique_errors = list(dict.fromkeys(errors))
        print("\nWarnings:")
        for e in unique_errors[:20]:
            print(" -", e)

    state = load_state()
    state_changed = False
    for candidate, m in ranked:
        level = m["level"]
        if level not in cfg.alert_levels:
            continue
        if not should_alert(candidate.symbol, level, m["setup"], state, cfg):
            continue

        text = format_alert(candidate, m)
        if args.dry_run:
            print("\nDRY ALERT:\n", text.replace("<b>", "").replace("</b>", ""))
            continue

        if send_telegram(text):
            if level in cfg.chart_alert_levels:
                chart_path = create_price_chart(candidate, m, cfg)
                if chart_path is not None:
                    if send_telegram_photo(chart_path, candidate, m):
                        print(f"Telegram chart sent: {chart_path}")
                    else:
                        print(f"WARN: Telegram text sent, but chart delivery failed: {chart_path}", file=sys.stderr)

            state.setdefault("last_alerts", {})[candidate.symbol] = {
                "ts": int(time.time()),
                "level": level,
                "setup": m["setup"],
                "score": round(m["score"], 2),
            }
            state_changed = True

    if state_changed:
        save_state(state)
        print(f"State updated: {STATE_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
