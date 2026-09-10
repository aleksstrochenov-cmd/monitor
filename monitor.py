#!/usr/bin/env python3
"""Multi-target perp inefficiency scanner v13.

Purpose
-------
Read PUBLIC market data only. No exchange API keys and no trading.

The scanner uses a staged pipeline:
1) Fast cross-market prefilter across Aster perpetuals vs Bitget/MEXC/Bybit perpetuals.
2) A short confirmation window that measures:
   - Aster spread and cross-venue PERP deviation
   - robust hedgeable edge using Aster bid/ask vs median external PERP bid/ask
   - estimated net convergence edge after a configurable round-trip fee reserve
   - actual Aster trade excursions away from PERP fair value
   - how often those excursions revert
   - how fast they revert
   - whether the external PERP reference stayed stable while Aster reverted
3) If a real CONFIRMED-MM appears, an extended MM verification stage keeps observing the best few MM candidates for several more minutes.
   This stage measures whether the regime persists for dozens of excursions rather than only a short burst.
4) If an initial CONFIRMED-LAG appears, the same extended stage keeps observing the best few LAG candidates and measures actual convergence: how far the gap shrank, time to 50%/80% convergence and repeated convergence cycles.
5) A compact LAG baseline is persisted between GitHub Actions runs. It records recent per-run median hedgeable gaps so a structural, always-present cross-venue premium can be rejected.
6) V9 ACTIVE-LAG emits a new live-gap signal only after prior verified convergence history exists for the same symbol/side.
7) V9 ACTIVE-MM-EXCURSION watches newly detected excursions after an MM regime is confirmed; alerts are batched but exact event counts are preserved.
8) Spot prices are collected separately as diagnostics only, so normal perp/spot basis cannot create a LAG signal.

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
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests


# Correct current Aster futures REST host. All hosts are overrideable from Actions env.
ASTER_BASE = os.getenv("ASTER_BASE_URL", "https://fapi.asterdex.com").rstrip("/")
BYBIT_BASE = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com").rstrip("/")
BITGET_BASE = os.getenv("BITGET_BASE_URL", "https://api.bitget.com").rstrip("/")
MEXC_BASE = os.getenv("MEXC_BASE_URL", "https://api.mexc.com").rstrip("/")
MEXC_CONTRACT_BASE = os.getenv("MEXC_CONTRACT_BASE_URL", "https://contract.mexc.com").rstrip("/")

USER_AGENT = "perp-inefficiency-scanner/13.0"
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
    # Best-level quantities when the venue exposes them. Zero means unknown,
    # not zero liquidity. V11 uses these only for conservative BBO-capacity
    # diagnostics; they do not change signal confirmation by themselves.
    bid_size: float = 0.0
    ask_size: float = 0.0

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def spread_pct(self) -> float:
        m = self.mid
        return ((self.ask - self.bid) / m * 100.0) if m > 0 else math.inf

    @property
    def bid_notional(self) -> float:
        return self.bid * self.bid_size if self.bid > 0 and self.bid_size > 0 else 0.0

    @property
    def ask_notional(self) -> float:
        return self.ask * self.ask_size if self.ask > 0 and self.ask_size > 0 else 0.0


@dataclass
class MarketSample:
    ts: float
    aster: Quote
    fair: float
    refs: Dict[str, Quote]
    ref_disagreement_pct: float
    deviation_pct: float
    # Relative Aster-vs-mid-fair edges are diagnostic only in v6.
    short_edge_pct: float
    long_edge_pct: float

    # Robust executable external PERP band. We use MEDIAN bid/ask across the
    # agreeing external perpetual venues so one bad quote cannot create LAG.
    external_bid: float = 0.0
    external_ask: float = 0.0

    # Best visible external hedge quote is shown for manual inspection only.
    # Confirmation still uses the robust median BBO above.
    best_external_bid: float = 0.0
    best_external_ask: float = 0.0
    best_external_bid_venue: str = ""
    best_external_ask_venue: str = ""

    # Hedgeable gross edges using robust external BBO:
    # SHORT target -> sell target bid, buy external median ask.
    # LONG target  -> buy target ask, sell external median bid.
    hedge_short_edge_pct: float = 0.0
    hedge_long_edge_pct: float = 0.0

    # V11 top-of-book capacity diagnostics. These are conservative BBO-only
    # notionals, not full-book slippage simulations. Zero means unknown.
    target_bid_notional: float = 0.0
    target_ask_notional: float = 0.0
    external_bid_notional: float = 0.0
    external_ask_notional: float = 0.0

    # Best-case visible edges using the single best external venue.
    best_hedge_short_edge_pct: float = 0.0
    best_hedge_long_edge_pct: float = 0.0

    # Spot is diagnostic only and never participates in LAG/MM confirmation.
    spot_fair: Optional[float] = None
    spot_refs: Dict[str, Quote] = field(default_factory=dict)
    spot_ref_disagreement_pct: Optional[float] = None

    @property
    def best_relative_edge_pct(self) -> float:
        return max(self.short_edge_pct, self.long_edge_pct, 0.0)

    @property
    def relative_edge_side(self) -> str:
        if self.short_edge_pct > 0 and self.short_edge_pct >= self.long_edge_pct:
            return "SHORT"
        if self.long_edge_pct > 0:
            return "LONG"
        return "NONE"

    @property
    def best_hedgeable_edge_pct(self) -> float:
        return max(self.hedge_short_edge_pct, self.hedge_long_edge_pct, 0.0)

    @property
    def hedge_edge_side(self) -> str:
        if self.hedge_short_edge_pct > 0 and self.hedge_short_edge_pct >= self.hedge_long_edge_pct:
            return "SHORT"
        if self.hedge_long_edge_pct > 0:
            return "LONG"
        return "NONE"

    # Backward-compatible aliases used by a few generic reporting paths.
    @property
    def best_executable_edge_pct(self) -> float:
        return self.best_hedgeable_edge_pct

    @property
    def edge_side(self) -> str:
        return self.hedge_edge_side


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

    # V9 LAG verification state. Initial Stage-2 LAG detection is not sent to
    # Telegram by itself; selected LAG candidates must survive extended
    # observation and demonstrate real convergence.
    lag_verification_status: str = "pending"  # pending / not_selected / selected / done
    lag_detection_sample_index: Optional[int] = None
    lag_detection_ts: Optional[float] = None
    lag_detection_side: str = "NONE"
    lag_initial_edge_pct: Optional[float] = None
    lag_initial_score: float = 0.0
    lag_initial_confirmed: bool = False
    lag_baseline_edges_pct: List[float] = field(default_factory=list)
    run_baseline_side: str = "NONE"
    run_baseline_edge_pct: float = 0.0

    # V9 active-signal diagnostics. These do NOT place orders. They only record
    # when a currently actionable-looking condition appeared.
    active_lag_detected: bool = False
    active_lag_detection_ts: Optional[float] = None
    active_lag_side: str = "NONE"
    active_lag_edge_pct: float = 0.0
    active_lag_net_edge_pct: float = 0.0
    active_lag_verified_episodes: int = 0

    active_mm_excursion_count: int = 0
    active_mm_long_count: int = 0
    active_mm_short_count: int = 0
    active_mm_peak_values_pct: List[float] = field(default_factory=list)
    active_mm_last_event_ts: Optional[float] = None

    def metrics(self, cfg: "Config") -> dict:
        if not self.samples:
            return {}

        spreads = [s.aster.spread_pct for s in self.samples]
        deviations = [s.deviation_pct for s in self.samples]
        disagreements = [s.ref_disagreement_pct for s in self.samples]

        relative_edges = [s.best_relative_edge_pct for s in self.samples]
        relative_long_edges = [s.long_edge_pct for s in self.samples]
        relative_short_edges = [s.short_edge_pct for s in self.samples]

        hedge_edges = [s.best_hedgeable_edge_pct for s in self.samples]
        hedge_long_edges = [s.hedge_long_edge_pct for s in self.samples]
        hedge_short_edges = [s.hedge_short_edge_pct for s in self.samples]

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

        current_relative_edge = current.best_relative_edge_pct
        max_relative_edge = max(relative_edges)
        med_relative_edge = statistics.median(relative_edges)

        current_hedge_edge = current.best_hedgeable_edge_pct
        max_hedge_edge = max(hedge_edges)
        med_hedge_edge = statistics.median(hedge_edges)

        spread_hit_ratio = sum(x >= cfg.min_aster_spread_pct for x in spreads) / len(spreads)
        dev_hit_ratio = sum(abs(x) >= cfg.min_deviation_pct for x in deviations) / len(deviations)
        ref_good_ratio = sum(x <= cfg.max_reference_disagreement_pct for x in disagreements) / len(disagreements)

        # V11 executable LAG logic. A missing external BBO is INVALID DATA.
        # It must not count as a negative edge and must never manufacture a
        # convergence event. Hit ratios use only valid executable snapshots.
        valid_long_edges = [x for x in hedge_long_edges if math.isfinite(x)]
        valid_short_edges = [x for x in hedge_short_edges if math.isfinite(x)]
        long_hit_ratio = (sum(
            x >= cfg.min_hedgeable_edge_pct
            and (x - cfg.estimated_roundtrip_fees_pct) >= cfg.min_net_hedgeable_edge_pct
            for x in valid_long_edges
        ) / len(valid_long_edges)) if valid_long_edges else 0.0
        short_hit_ratio = (sum(
            x >= cfg.min_hedgeable_edge_pct
            and (x - cfg.estimated_roundtrip_fees_pct) >= cfg.min_net_hedgeable_edge_pct
            for x in valid_short_edges
        ) / len(valid_short_edges)) if valid_short_edges else 0.0

        if long_hit_ratio > short_hit_ratio:
            lag_side = "LONG"
            side_edges = valid_long_edges
            persistent_exec_hit_ratio = long_hit_ratio
            current_side_edge = current.hedge_long_edge_pct if math.isfinite(current.hedge_long_edge_pct) else 0.0
            current_best_case_edge = current.best_hedge_long_edge_pct if math.isfinite(current.best_hedge_long_edge_pct) else 0.0
            best_hedge_venue = current.best_external_bid_venue
            best_hedge_price = current.best_external_bid
        elif short_hit_ratio > long_hit_ratio:
            lag_side = "SHORT"
            side_edges = valid_short_edges
            persistent_exec_hit_ratio = short_hit_ratio
            current_side_edge = current.hedge_short_edge_pct if math.isfinite(current.hedge_short_edge_pct) else 0.0
            current_best_case_edge = current.best_hedge_short_edge_pct if math.isfinite(current.best_hedge_short_edge_pct) else 0.0
            best_hedge_venue = current.best_external_ask_venue
            best_hedge_price = current.best_external_ask
        else:
            med_long = statistics.median(valid_long_edges) if valid_long_edges else 0.0
            med_short = statistics.median(valid_short_edges) if valid_short_edges else 0.0
            if max(med_long, med_short) > 0:
                lag_side = "LONG" if med_long >= med_short else "SHORT"
                side_edges = valid_long_edges if lag_side == "LONG" else valid_short_edges
                persistent_exec_hit_ratio = long_hit_ratio if lag_side == "LONG" else short_hit_ratio
                if lag_side == "LONG":
                    current_side_edge = current.hedge_long_edge_pct if math.isfinite(current.hedge_long_edge_pct) else 0.0
                    current_best_case_edge = current.best_hedge_long_edge_pct if math.isfinite(current.best_hedge_long_edge_pct) else 0.0
                    best_hedge_venue = current.best_external_bid_venue
                    best_hedge_price = current.best_external_bid
                else:
                    current_side_edge = current.hedge_short_edge_pct if math.isfinite(current.hedge_short_edge_pct) else 0.0
                    current_best_case_edge = current.best_hedge_short_edge_pct if math.isfinite(current.best_hedge_short_edge_pct) else 0.0
                    best_hedge_venue = current.best_external_ask_venue
                    best_hedge_price = current.best_external_ask
            else:
                lag_side = "NONE"
                side_edges = []
                persistent_exec_hit_ratio = 0.0
                current_side_edge = 0.0
                current_best_case_edge = 0.0
                best_hedge_venue = ""
                best_hedge_price = 0.0

        persistent_median_edge = statistics.median(side_edges) if side_edges else 0.0
        persistent_max_edge = max(side_edges) if side_edges else 0.0
        current_side_edge_positive = max(0.0, current_side_edge)

        persistent_median_net_edge = persistent_median_edge - cfg.estimated_roundtrip_fees_pct
        current_net_edge = current_side_edge_positive - cfg.estimated_roundtrip_fees_pct
        max_net_edge = persistent_max_edge - cfg.estimated_roundtrip_fees_pct

        # Midpoint direction is retained only as diagnostics.
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

        # Extended MM regime statistics. These are meaningful even during the
        # initial window, but become much more useful after Stage 3.
        observed_seconds = max(0.0, self.samples[-1].ts - self.samples[0].ts) if len(self.samples) >= 2 else 0.0
        observed_minutes = observed_seconds / 60.0 if observed_seconds > 0 else 0.0
        excursion_rate_per_minute = excursion_count / observed_minutes if observed_minutes > 0 else 0.0
        above_excursions = sum(e.direction == "ABOVE" for e in events)
        below_excursions = sum(e.direction == "BELOW" for e in events)
        excursion_starts = sorted(e.start_ts for e in events)
        if excursion_starts:
            first_excursion_offset_seconds = max(0.0, excursion_starts[0] - self.samples[0].ts)
            last_excursion_offset_seconds = max(0.0, excursion_starts[-1] - self.samples[0].ts)
            active_span_seconds = max(0.0, excursion_starts[-1] - excursion_starts[0])
            last_excursion_age_seconds = max(0.0, self.samples[-1].ts - excursion_starts[-1])
            boundaries = [self.samples[0].ts] + excursion_starts + [self.samples[-1].ts]
            longest_quiet_seconds = max((b - a for a, b in zip(boundaries, boundaries[1:])), default=observed_seconds)
        else:
            first_excursion_offset_seconds = None
            last_excursion_offset_seconds = None
            active_span_seconds = 0.0
            last_excursion_age_seconds = observed_seconds
            longest_quiet_seconds = observed_seconds

        p90_peak_excursion_pct = percentile(peaks, 0.90) or 0.0
        p90_clean_reversion_seconds = percentile(clean_reversion_times, 0.90)

        # V9 extended LAG convergence statistics. These are evaluated only for
        # Stage-2 LAG candidates selected for extended observation.
        lag_extended_observed_seconds = 0.0
        lag_initial_gap_pct = self.lag_initial_edge_pct or 0.0
        lag_min_gap_pct = lag_initial_gap_pct
        lag_current_gap_pct = current_side_edge
        lag_max_gap_pct = lag_initial_gap_pct
        lag_max_convergence_fraction = 0.0
        lag_time_to_50_seconds = None
        lag_time_to_80_seconds = None
        lag_convergence_events = 0
        lag_full_convergence_cycles = 0
        lag_reexpanded_after_convergence = False
        lag_valid_bbo_points = 0
        lag_total_bbo_points = 0
        lag_bbo_coverage_ratio = 0.0

        def lag_edge_for_sample(sample: MarketSample, side: str) -> float:
            if side == "LONG":
                return sample.hedge_long_edge_pct
            if side == "SHORT":
                return sample.hedge_short_edge_pct
            return math.nan

        if (
            self.lag_detection_sample_index is not None
            and self.lag_detection_side in {"LONG", "SHORT"}
            and 0 <= self.lag_detection_sample_index < len(self.samples)
        ):
            lag_samples = self.samples[self.lag_detection_sample_index :]
            lag_total_bbo_points = len(lag_samples)
            valid_lag_points = [
                (sample, lag_edge_for_sample(sample, self.lag_detection_side))
                for sample in lag_samples
                if math.isfinite(lag_edge_for_sample(sample, self.lag_detection_side))
                and sample.external_bid > 0 and sample.external_ask > 0
            ]
            lag_valid_bbo_points = len(valid_lag_points)
            lag_bbo_coverage_ratio = lag_valid_bbo_points / lag_total_bbo_points if lag_total_bbo_points else 0.0
            if valid_lag_points:
                valid_samples = [p[0] for p in valid_lag_points]
                lag_edges = [p[1] for p in valid_lag_points]
                lag_initial_gap_pct = self.lag_initial_edge_pct if self.lag_initial_edge_pct is not None else lag_edges[0]
                lag_initial_gap_pct = max(0.0, lag_initial_gap_pct)
                lag_min_gap_pct = min(lag_edges)
                lag_current_gap_pct = lag_edges[-1]
                lag_max_gap_pct = max(lag_edges)
                lag_extended_observed_seconds = max(0.0, lag_samples[-1].ts - lag_samples[0].ts)
                if lag_initial_gap_pct > 0:
                    lag_max_convergence_fraction = max(
                        0.0, min(1.0, (lag_initial_gap_pct - lag_min_gap_pct) / lag_initial_gap_pct)
                    )
                    threshold_50 = lag_initial_gap_pct * 0.50
                    threshold_80 = lag_initial_gap_pct * 0.20
                    t0 = lag_samples[0].ts
                    for sample, edge in valid_lag_points:
                        elapsed = max(0.0, sample.ts - t0)
                        if lag_time_to_50_seconds is None and edge <= threshold_50:
                            lag_time_to_50_seconds = elapsed
                        if lag_time_to_80_seconds is None and edge <= threshold_80:
                            lag_time_to_80_seconds = elapsed

                    convergence_threshold = lag_initial_gap_pct * (1.0 - cfg.lag_min_convergence_fraction)
                    rearm_threshold = lag_initial_gap_pct * cfg.lag_reexpansion_fraction
                    armed = True
                    full_armed = True
                    had_convergence = False
                    for edge in lag_edges:
                        if armed and edge <= convergence_threshold:
                            lag_convergence_events += 1
                            armed = False
                            had_convergence = True
                        elif not armed and edge >= rearm_threshold:
                            armed = True
                            if had_convergence:
                                lag_reexpanded_after_convergence = True

                        if full_armed and edge <= cfg.lag_full_convergence_edge_pct:
                            lag_full_convergence_cycles += 1
                            full_armed = False
                        elif not full_armed and edge >= rearm_threshold:
                            full_armed = True
                            lag_reexpanded_after_convergence = True

        baseline_points = len(self.lag_baseline_edges_pct)
        baseline_median_gap_pct = statistics.median(self.lag_baseline_edges_pct) if self.lag_baseline_edges_pct else None
        baseline_p90_gap_pct = percentile(self.lag_baseline_edges_pct, 0.90) if self.lag_baseline_edges_pct else None
        baseline_required_gap_pct = None
        baseline_ratio = None
        baseline_excess_pct = None
        if baseline_median_gap_pct is not None:
            baseline_required_gap_pct = max(
                baseline_median_gap_pct + cfg.lag_baseline_min_excess_pct,
                baseline_median_gap_pct * cfg.lag_baseline_min_ratio,
            )
            baseline_excess_pct = lag_initial_gap_pct - baseline_median_gap_pct
            baseline_ratio = lag_initial_gap_pct / max(baseline_median_gap_pct, 1e-9)

        baseline_ready = baseline_points >= cfg.lag_baseline_min_points
        baseline_anomalous = (
            not baseline_ready
            or baseline_required_gap_pct is None
            or lag_initial_gap_pct >= baseline_required_gap_pct
        )
        lag_convergence_confirmed = (
            self.lag_verification_status == "done"
            and self.lag_initial_confirmed
            and lag_extended_observed_seconds >= cfg.lag_min_extended_observed_seconds
            and lag_max_convergence_fraction >= cfg.lag_min_convergence_fraction
            and lag_convergence_events >= cfg.lag_min_convergence_events
            and lag_bbo_coverage_ratio >= cfg.lag_min_valid_bbo_coverage
            and lag_valid_bbo_points >= cfg.lag_min_valid_bbo_points
            and baseline_anomalous
        )

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

        # LAG score now rewards only hedgeable economics. A large midpoint gap
        # cannot create a high LAG score by itself.
        lag_score = min(
            100.0,
            20.0 * min(1.0, current_side_edge_positive / max(cfg.min_current_hedgeable_edge_pct, 1e-9))
            + 20.0 * persistent_exec_hit_ratio
            + 15.0 * min(1.0, max(0.0, persistent_median_edge) / max(cfg.min_hedgeable_edge_pct, 1e-9))
            + 15.0 * min(1.0, max(0.0, current_net_edge) / max(cfg.min_net_hedgeable_edge_pct, 1e-9))
            + 10.0 * ref_good_ratio
            + 10.0 * lag_spread_quality
            + 5.0 * min(1.0, trades_analyzed / max(cfg.min_aster_trades_for_lag, 1))
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

        raw_lag_confirmed = (
            ref_good_ratio >= cfg.min_ref_good_ratio
            and lag_side in {"LONG", "SHORT"}
            and med_spread <= cfg.max_lag_median_spread_pct
            and trades_analyzed >= cfg.min_aster_trades_for_lag
            and persistent_exec_hit_ratio >= cfg.min_hedgeable_edge_hit_ratio
            and current_side_edge_positive >= cfg.min_current_hedgeable_edge_pct
            and persistent_median_edge >= cfg.min_hedgeable_edge_pct
            and current_net_edge >= cfg.min_net_hedgeable_edge_pct
            and persistent_median_net_edge >= cfg.min_net_hedgeable_edge_pct
        )

        # Before Stage 3, raw_lag_confirmed is used only to select candidates.
        # After Stage 3, Telegram LAG alerts require demonstrated convergence.
        if self.lag_verification_status == "done":
            lag_confirmed = lag_convergence_confirmed
        elif self.lag_verification_status == "not_selected":
            lag_confirmed = False
        else:
            lag_confirmed = raw_lag_confirmed

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

        mature_mm = (
            mm_confirmed
            and observed_seconds >= cfg.mature_mm_min_observed_seconds
            and excursion_count >= cfg.mature_mm_min_excursions
            and clean_reversion_rate >= cfg.mature_mm_min_clean_reversion_rate
            and median_clean_reversion_seconds is not None
            and median_clean_reversion_seconds <= cfg.mature_mm_max_median_reversion_seconds
            and excursion_rate_per_minute >= cfg.mature_mm_min_excursions_per_minute
            and longest_quiet_seconds <= cfg.mature_mm_max_quiet_seconds
            and last_excursion_age_seconds <= cfg.mature_mm_max_quiet_seconds
        )

        verified_lag_score = max(lag_score, self.lag_initial_score) if lag_convergence_confirmed else lag_score
        if mature_mm and lag_convergence_confirmed:
            setup = "MATURE-MM+CONVERGING-LAG"
            score = max(95.0, verified_lag_score, mm_score)
        elif mature_mm:
            setup = "MATURE-MM"
            score = max(95.0, mm_score)
        elif mm_confirmed and lag_convergence_confirmed:
            setup = "CONFIRMED-MM+CONVERGING-LAG"
            score = max(verified_lag_score, mm_score)
        elif mm_confirmed and raw_lag_confirmed and self.lag_verification_status in {"pending", "selected"}:
            # Internal pre-extended combined state used for Stage-3 selection.
            setup = "CONFIRMED-BOTH"
            score = max(lag_score, mm_score)
        elif mm_confirmed:
            setup = "CONFIRMED-MM"
            score = mm_score
        elif lag_convergence_confirmed:
            setup = "CONVERGING-LAG"
            score = verified_lag_score
        elif raw_lag_confirmed and self.lag_verification_status in {"pending", "selected"}:
            # Internal pre-extended state used only for Stage-3 selection.
            setup = "CONFIRMED-LAG"
            score = lag_score
        else:
            setup = "NONE"
            score = max(lag_score, mm_score)

        level = "CONFIRMED" if (
            (setup.startswith("CONFIRMED") or setup.startswith("MATURE-MM") or setup == "CONVERGING-LAG")
            and score >= cfg.confirmed_score
        ) else "NONE"

        effective_lag_side = self.lag_detection_side if self.lag_verification_status == "done" and self.lag_detection_side in {"LONG", "SHORT"} else lag_side
        if lag_convergence_confirmed and effective_lag_side == "SHORT":
            direction = "CONVERGING SHORT gap: Aster premium over external PERP shrank during extended observation"
        elif lag_convergence_confirmed and effective_lag_side == "LONG":
            direction = "CONVERGING LONG gap: Aster discount to external PERP shrank during extended observation"
        elif lag_side == "SHORT" and current_hedge_edge > 0:
            direction = "Hedgeable SHORT: sell Aster bid; external median ask is lower"
        elif lag_side == "LONG" and current_hedge_edge > 0:
            direction = "Hedgeable LONG: buy Aster ask; external median bid is higher"
        elif current_dev > 0:
            direction = "Aster midpoint above external PERP fair, but no confirmed hedgeable edge"
        elif current_dev < 0:
            direction = "Aster midpoint below external PERP fair, but no confirmed hedgeable edge"
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
            "persistent_median_net_edge_pct": persistent_median_net_edge,
            "max_net_edge_pct": max_net_edge,
            "ref_good_ratio": ref_good_ratio,
            "current_fair": current.fair,
            "current_external_bid": current.external_bid,
            "current_external_ask": current.external_ask,
            "current_best_external_bid": current.best_external_bid,
            "current_best_external_ask": current.best_external_ask,
            "best_hedge_venue": best_hedge_venue,
            "best_hedge_price": best_hedge_price,
            "current_best_case_hedge_edge_pct": current_best_case_edge,
            "current_aster_bid": current.aster.bid,
            "current_aster_ask": current.aster.ask,
            "current_refs": sorted(current.refs.keys()),
            "current_ref_disagreement_pct": current.ref_disagreement_pct,
            "current_spot_fair": current_spot_fair,
            "current_spot_refs": current_spot_refs,
            "current_spot_ref_disagreement_pct": current_spot_disagreement,
            "external_perp_spot_basis_pct": external_perp_spot_basis_pct,
            "aster_spot_basis_pct": aster_spot_basis_pct,
            "current_relative_edge_pct": current_relative_edge,
            "current_relative_edge_side": current.relative_edge_side,
            "max_relative_edge_pct": max_relative_edge,
            "median_relative_edge_pct": med_relative_edge,
            "current_executable_edge_pct": current_hedge_edge,
            "current_edge_side": current.hedge_edge_side,
            "max_executable_edge_pct": max_hedge_edge,
            "median_executable_edge_pct": med_hedge_edge,
            "current_net_edge_pct": current_net_edge,
            "estimated_roundtrip_fees_pct": cfg.estimated_roundtrip_fees_pct,
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
            "observed_seconds": observed_seconds,
            "excursion_rate_per_minute": excursion_rate_per_minute,
            "above_excursions": above_excursions,
            "below_excursions": below_excursions,
            "first_excursion_offset_seconds": first_excursion_offset_seconds,
            "last_excursion_offset_seconds": last_excursion_offset_seconds,
            "active_span_seconds": active_span_seconds,
            "last_excursion_age_seconds": last_excursion_age_seconds,
            "longest_quiet_seconds": longest_quiet_seconds,
            "p90_peak_excursion_pct": p90_peak_excursion_pct,
            "p90_clean_reversion_seconds": p90_clean_reversion_seconds,
            "mature_mm": mature_mm,
            "raw_lag_confirmed": raw_lag_confirmed,
            "lag_verification_status": self.lag_verification_status,
            "lag_detection_side": self.lag_detection_side,
            "lag_extended_observed_seconds": lag_extended_observed_seconds,
            "lag_initial_gap_pct": lag_initial_gap_pct,
            "lag_min_gap_pct": lag_min_gap_pct,
            "lag_current_gap_pct": lag_current_gap_pct,
            "lag_max_gap_pct": lag_max_gap_pct,
            "lag_max_convergence_fraction": lag_max_convergence_fraction,
            "lag_time_to_50_seconds": lag_time_to_50_seconds,
            "lag_time_to_80_seconds": lag_time_to_80_seconds,
            "lag_convergence_events": lag_convergence_events,
            "lag_full_convergence_cycles": lag_full_convergence_cycles,
            "lag_reexpanded_after_convergence": lag_reexpanded_after_convergence,
            "lag_valid_bbo_points": lag_valid_bbo_points,
            "lag_total_bbo_points": lag_total_bbo_points,
            "lag_bbo_coverage_ratio": lag_bbo_coverage_ratio,
            "lag_convergence_confirmed": lag_convergence_confirmed,
            "lag_baseline_points": baseline_points,
            "lag_baseline_median_gap_pct": baseline_median_gap_pct,
            "lag_baseline_p90_gap_pct": baseline_p90_gap_pct,
            "lag_baseline_required_gap_pct": baseline_required_gap_pct,
            "lag_baseline_excess_pct": baseline_excess_pct,
            "lag_baseline_ratio": baseline_ratio,
            "lag_baseline_ready": baseline_ready,
            "lag_baseline_anomalous": baseline_anomalous,
            "active_lag_detected": self.active_lag_detected,
            "active_lag_detection_ts": self.active_lag_detection_ts,
            "active_lag_side": self.active_lag_side,
            "active_lag_edge_pct": self.active_lag_edge_pct,
            "active_lag_net_edge_pct": self.active_lag_net_edge_pct,
            "active_lag_verified_episodes": self.active_lag_verified_episodes,
            "active_mm_excursion_count": self.active_mm_excursion_count,
            "active_mm_long_count": self.active_mm_long_count,
            "active_mm_short_count": self.active_mm_short_count,
            "active_mm_median_peak_pct": statistics.median(self.active_mm_peak_values_pct) if self.active_mm_peak_values_pct else 0.0,
            "active_mm_max_peak_pct": max(self.active_mm_peak_values_pct) if self.active_mm_peak_values_pct else 0.0,
            "active_mm_last_event_age_seconds": (
                max(0.0, self.samples[-1].ts - self.active_mm_last_event_ts)
                if self.active_mm_last_event_ts is not None and self.samples else None
            ),
            "direction": direction,
            "samples": len(self.samples),
        }


@dataclass(frozen=True)
class Config:
    # Stage 1 filters / MM diagnostics
    min_aster_spread_pct: float = float(os.getenv("MIN_ASTER_SPREAD_PCT", "0.15"))
    min_deviation_pct: float = float(os.getenv("MIN_DEVIATION_PCT", "0.20"))

    # Legacy relative-to-midpoint thresholds are kept only for prefiltering and diagnostics.
    min_executable_edge_pct: float = float(os.getenv("MIN_EXECUTABLE_EDGE_PCT", "0.15"))
    min_current_executable_edge_pct: float = float(os.getenv("MIN_CURRENT_EXECUTABLE_EDGE_PCT", "0.15"))

    # V6 LAG economics: robust median external BBO, not midpoint fair.
    min_hedgeable_edge_pct: float = float(
        os.getenv("MIN_HEDGEABLE_EDGE_PCT", os.getenv("MIN_EXECUTABLE_EDGE_PCT", "0.20"))
    )
    min_current_hedgeable_edge_pct: float = float(
        os.getenv("MIN_CURRENT_HEDGEABLE_EDGE_PCT", os.getenv("MIN_CURRENT_EXECUTABLE_EDGE_PCT", "0.20"))
    )
    min_net_hedgeable_edge_pct: float = float(os.getenv("MIN_NET_HEDGEABLE_EDGE_PCT", "0.05"))
    min_hedgeable_edge_hit_ratio: float = float(
        os.getenv("MIN_HEDGEABLE_EDGE_HIT_RATIO", os.getenv("MIN_EXEC_EDGE_HIT_RATIO", "0.60"))
    )

    # Estimated TOTAL round-trip trading fees for a hedged convergence trade:
    # open Aster + open reference + close Aster + close reference.
    # This is intentionally configurable because fee tiers / maker-taker choices differ.
    estimated_roundtrip_fees_pct: float = float(os.getenv("ESTIMATED_ROUNDTRIP_FEES_PCT", "0.20"))

    min_24h_move_pct: float = float(os.getenv("MIN_24H_MOVE_PCT", "8.0"))
    min_quote_volume24h: float = float(os.getenv("MIN_ASTER_QUOTE_VOLUME_24H", "50000"))
    min_reference_exchanges: int = int(os.getenv("MIN_PERP_REFERENCE_EXCHANGES", os.getenv("MIN_REFERENCE_EXCHANGES", "2")))
    max_reference_disagreement_pct: float = float(os.getenv("MAX_PERP_REFERENCE_DISAGREEMENT_PCT", os.getenv("MAX_REFERENCE_DISAGREEMENT_PCT", "0.20")))
    min_spot_reference_exchanges: int = int(os.getenv("MIN_SPOT_REFERENCE_EXCHANGES", "2"))
    max_spot_reference_disagreement_pct: float = float(os.getenv("MAX_SPOT_REFERENCE_DISAGREEMENT_PCT", "0.30"))
    max_candidates: int = int(os.getenv("MAX_CANDIDATES", "50"))

    # Stage 2 confirmation sampling
    confirm_duration_seconds: float = float(os.getenv("CONFIRM_DURATION_SECONDS", "45"))
    confirm_interval_seconds: float = float(os.getenv("CONFIRM_INTERVAL_SECONDS", "1.5"))
    max_trade_analysis_candidates: int = int(os.getenv("MAX_TRADE_ANALYSIS_CANDIDATES", "50"))
    aster_trade_limit: int = int(os.getenv("ASTER_TRADE_LIMIT", "500"))
    trade_sample_match_tolerance_seconds: float = float(os.getenv("TRADE_SAMPLE_MATCH_TOLERANCE_SECONDS", "3.0"))

    # Stage 3: combined extended verification for the strongest initial MM and LAG candidates.
    # This is intentionally NOT run for every candidate.
    extended_mm_enabled: bool = os.getenv("EXTENDED_MM_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
    extended_mm_max_candidates: int = int(os.getenv("EXTENDED_MM_MAX_CANDIDATES", "3"))
    extended_mm_duration_seconds: float = float(os.getenv("EXTENDED_MM_DURATION_SECONDS", "180"))
    extended_mm_interval_seconds: float = float(os.getenv("EXTENDED_MM_INTERVAL_SECONDS", "1.5"))
    extended_mm_trades_poll_seconds: float = float(os.getenv("EXTENDED_MM_TRADES_POLL_SECONDS", "5"))

    # V9 extended LAG verification. Initial LAG detection is only a candidate;
    # final Telegram LAG alerts require actual gap convergence.
    extended_lag_enabled: bool = os.getenv("EXTENDED_LAG_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
    extended_lag_max_candidates: int = int(os.getenv("EXTENDED_LAG_MAX_CANDIDATES", "3"))
    extended_lag_duration_seconds: float = float(os.getenv("EXTENDED_LAG_DURATION_SECONDS", "180"))
    extended_lag_interval_seconds: float = float(os.getenv("EXTENDED_LAG_INTERVAL_SECONDS", "1.5"))
    lag_min_extended_observed_seconds: float = float(os.getenv("LAG_MIN_EXTENDED_OBSERVED_SECONDS", "120"))
    lag_min_convergence_fraction: float = float(os.getenv("LAG_MIN_CONVERGENCE_FRACTION", "0.50"))
    lag_min_convergence_events: int = int(os.getenv("LAG_MIN_CONVERGENCE_EVENTS", "1"))
    lag_full_convergence_edge_pct: float = float(os.getenv("LAG_FULL_CONVERGENCE_EDGE_PCT", "0.05"))
    lag_reexpansion_fraction: float = float(os.getenv("LAG_REEXPANSION_FRACTION", "0.75"))
    # V11: missing executable BBO is INVALID DATA, never convergence.
    lag_min_valid_bbo_coverage: float = float(os.getenv("LAG_MIN_VALID_BBO_COVERAGE", "0.80"))
    lag_min_valid_bbo_points: int = int(os.getenv("LAG_MIN_VALID_BBO_POINTS", "20"))

    # Cross-run structural-gap baseline. Stored compactly in state/state.json.
    lag_baseline_lookback_minutes: int = int(os.getenv("LAG_BASELINE_LOOKBACK_MINUTES", "60"))
    lag_baseline_min_points: int = int(os.getenv("LAG_BASELINE_MIN_POINTS", "3"))
    lag_baseline_max_points: int = int(os.getenv("LAG_BASELINE_MAX_POINTS", "12"))
    lag_baseline_min_excess_pct: float = float(os.getenv("LAG_BASELINE_MIN_EXCESS_PCT", "0.10"))
    lag_baseline_min_ratio: float = float(os.getenv("LAG_BASELINE_MIN_RATIO", "1.25"))

    # V9 ACTIVE-LAG. This is a signal only: no orders are ever placed.
    # It requires at least one previously VERIFIED convergence episode for the same symbol/side,
    # plus a new currently hedgeable gap that is still economically meaningful.
    active_lag_enabled: bool = os.getenv("ACTIVE_LAG_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
    active_lag_min_gross_edge_pct: float = float(os.getenv("ACTIVE_LAG_MIN_GROSS_EDGE_PCT", "0.30"))
    active_lag_min_net_edge_pct: float = float(os.getenv("ACTIVE_LAG_MIN_NET_EDGE_PCT", "0.10"))
    active_lag_min_hit_ratio: float = float(os.getenv("ACTIVE_LAG_MIN_HIT_RATIO", "0.60"))
    active_lag_max_reference_disagreement_pct: float = float(os.getenv("ACTIVE_LAG_MAX_REFERENCE_DISAGREEMENT_PCT", "0.20"))
    active_lag_min_verified_episodes: int = int(os.getenv("ACTIVE_LAG_MIN_VERIFIED_EPISODES", "1"))
    active_lag_profile_lookback_hours: int = int(os.getenv("ACTIVE_LAG_PROFILE_LOOKBACK_HOURS", "24"))
    active_lag_profile_max_points: int = int(os.getenv("ACTIVE_LAG_PROFILE_MAX_POINTS", "50"))

    # V9 ACTIVE-MM-EXCURSION. Selected Stage-3 MM regimes are watched for NEW excursion
    # events after confirmation. Alerts are batched so a 20-30 excursions/minute market
    # does not flood Telegram; the batch still reports the exact event count.
    active_mm_excursion_enabled: bool = os.getenv("ACTIVE_MM_EXCURSION_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
    active_mm_min_excursion_pct: float = float(os.getenv("ACTIVE_MM_MIN_EXCURSION_PCT", "0.30"))
    active_mm_max_reference_disagreement_pct: float = float(os.getenv("ACTIVE_MM_MAX_REFERENCE_DISAGREEMENT_PCT", "0.20"))
    active_mm_min_prior_excursions: int = int(os.getenv("ACTIVE_MM_MIN_PRIOR_EXCURSIONS", "3"))
    active_mm_min_clean_reversion_rate: float = float(os.getenv("ACTIVE_MM_MIN_CLEAN_REVERSION_RATE", "0.70"))
    active_mm_initial_lookback_seconds: float = float(os.getenv("ACTIVE_MM_INITIAL_LOOKBACK_SECONDS", "10"))
    active_mm_alert_batch_seconds: float = float(os.getenv("ACTIVE_MM_ALERT_BATCH_SECONDS", "15"))

    # V11 forward outcome tracking. Open ACTIVE signals are followed after the
    # signal timestamp so we can measure forward success rather than hindsight.
    # V13: LAG outcomes are now followed for up to 24h (configurable) instead of
    # declaring a 30-minute timeout. Checkpoints let us see how quickly each gap
    # converges on 5m / 30m / 2h / 6h / 24h horizons.
    active_lag_outcome_max_age_minutes: float = float(os.getenv("ACTIVE_LAG_OUTCOME_MAX_AGE_MINUTES", "1440"))
    active_lag_outcome_horizons_minutes: Tuple[float, ...] = tuple(
        sorted(set(float(x.strip()) for x in os.getenv("ACTIVE_LAG_OUTCOME_HORIZONS_MINUTES", "5,30,120,360,1440").split(",") if x.strip()))
    )
    active_lag_horizon_alerts_enabled: bool = os.getenv("ACTIVE_LAG_HORIZON_ALERTS_ENABLED", "true").strip().lower() in {"1","true","yes","on"}
    active_mm_outcome_max_age_seconds: float = float(os.getenv("ACTIVE_MM_OUTCOME_MAX_AGE_SECONDS", "120"))
    active_outcome_alerts_enabled: bool = os.getenv("ACTIVE_OUTCOME_ALERTS_ENABLED", "true").strip().lower() in {"1","true","yes","on"}

    # Hypothetical funding carry for ACTIVE-LAG pair trades. We use PUBLIC,
    # already-settled funding rates where available. Positive result means the
    # pair would have RECEIVED funding; negative means it would have PAID it.
    funding_tracking_enabled: bool = os.getenv("FUNDING_TRACKING_ENABLED", "true").strip().lower() in {"1","true","yes","on"}
    funding_notional_usd: float = float(os.getenv("FUNDING_NOTIONAL_USD", "100"))

    bbo_notional_checks: Tuple[float, ...] = tuple(
        float(x.strip()) for x in os.getenv("BBO_NOTIONAL_CHECKS_USD", "100,500,1000").split(",") if x.strip()
    )

    # V12 execution-readiness probe (signal only, never places orders).
    execution_readiness_enabled: bool = os.getenv("EXECUTION_READINESS_ENABLED", "true").strip().lower() in {"1","true","yes","on"}
    execution_probe_notionals: Tuple[float, ...] = tuple(
        float(x.strip()) for x in os.getenv("EXECUTION_PROBE_NOTIONALS_USD", "100,500,1000").split(",") if x.strip()
    )
    execution_min_notional_usd: float = float(os.getenv("EXECUTION_MIN_NOTIONAL_USD", "100"))
    execution_min_net_edge_pct: float = float(os.getenv("EXECUTION_MIN_NET_EDGE_PCT", "0.10"))
    execution_book_levels: int = int(os.getenv("EXECUTION_BOOK_LEVELS", "20"))
    execution_max_probe_seconds: float = float(os.getenv("EXECUTION_MAX_PROBE_SECONDS", "6.0"))
    execution_max_book_age_seconds: float = float(os.getenv("EXECUTION_MAX_BOOK_AGE_SECONDS", "2.0"))
    lighter_book_warmup_seconds: float = float(os.getenv("LIGHTER_BOOK_WARMUP_SECONDS", "0.50"))

    # Reject obviously incomparable same-symbol contracts / multipliers (e.g. 100x/1000x).
    max_cross_venue_price_ratio: float = float(os.getenv("MAX_CROSS_VENUE_PRICE_RATIO", "2.0"))

    # MATURE-MM means the MM regime survived extended observation instead of
    # being only a short burst during the first ~45 seconds.
    mature_mm_min_observed_seconds: float = float(os.getenv("MATURE_MM_MIN_OBSERVED_SECONDS", "180"))
    mature_mm_min_excursions: int = int(os.getenv("MATURE_MM_MIN_EXCURSIONS", "20"))
    mature_mm_min_clean_reversion_rate: float = float(os.getenv("MATURE_MM_MIN_CLEAN_REVERSION_RATE", "0.70"))
    mature_mm_max_median_reversion_seconds: float = float(os.getenv("MATURE_MM_MAX_MEDIAN_REVERSION_SECONDS", "5.0"))
    mature_mm_min_excursions_per_minute: float = float(os.getenv("MATURE_MM_MIN_EXCURSIONS_PER_MINUTE", "3.0"))
    mature_mm_max_quiet_seconds: float = float(os.getenv("MATURE_MM_MAX_QUIET_SECONDS", "45"))

    # Excursion / reversion definition
    excursion_threshold_pct: float = float(os.getenv("EXCURSION_THRESHOLD_PCT", "0.20"))
    reversion_band_pct: float = float(os.getenv("REVERSION_BAND_PCT", "0.08"))
    min_excursions: int = int(os.getenv("MIN_EXCURSIONS", "3"))
    min_clean_reversion_rate: float = float(os.getenv("MIN_CLEAN_REVERSION_RATE", "0.70"))
    max_median_reversion_seconds: float = float(os.getenv("MAX_MEDIAN_REVERSION_SECONDS", "8.0"))
    max_reference_move_during_reversion_pct: float = float(os.getenv("MAX_REFERENCE_MOVE_DURING_REVERSION_PCT", "0.12"))
    min_aster_trades_for_mm: int = int(os.getenv("MIN_ASTER_TRADES_FOR_MM", "6"))

    # Persistent LAG confirmation.
    max_lag_median_spread_pct: float = float(os.getenv("MAX_LAG_MEDIAN_SPREAD_PCT", "0.60"))
    min_aster_trades_for_lag: int = int(os.getenv("MIN_ASTER_TRADES_FOR_LAG", "3"))
    min_ref_good_ratio: float = float(os.getenv("MIN_REF_GOOD_RATIO", "0.80"))

    # Alerts: CONFIRMED only; WATCH is intentionally disabled.
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


def percentile(values: List[float], q: float) -> Optional[float]:
    """Simple linear-interpolated percentile for small in-memory samples."""
    if not values:
        return None
    xs = sorted(float(x) for x in values)
    if len(xs) == 1:
        return xs[0]
    q = max(0.0, min(1.0, q))
    pos = (len(xs) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return xs[lo]
    frac = pos - lo
    return xs[lo] * (1.0 - frac) + xs[hi] * frac


def valid_quote(bid, ask, bid_size=0.0, ask_size=0.0) -> Optional[Quote]:
    b, a = fnum(bid), fnum(ask)
    bs, ass = fnum(bid_size), fnum(ask_size)
    if b > 0 and a > 0 and a >= b:
        return Quote(b, a, max(0.0, bs), max(0.0, ass))
    return None

def first_positive(*values) -> float:
    for value in values:
        x = fnum(value)
        if x > 0:
            return x
    return 0.0


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
        q = valid_quote(row.get("bidPrice"), row.get("askPrice"), row.get("bidQty"), row.get("askQty"))
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
        q = valid_quote(row.get("bid1Price"), row.get("ask1Price"), row.get("bid1Size"), row.get("ask1Size"))
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
        q = valid_quote(row.get("bid1Price"), row.get("ask1Price"), row.get("bid1Size"), row.get("ask1Size"))
        if symbol.endswith("USDT") and q:
            out[symbol] = q
    return out


def fetch_bitget_spot(cfg: Config) -> Dict[str, Quote]:
    data = get_json(f"{BITGET_BASE}/api/v2/spot/market/tickers", cfg.request_timeout_seconds)
    rows = (data or {}).get("data", []) if isinstance(data, dict) else []
    out: Dict[str, Quote] = {}
    for row in rows:
        symbol = str(row.get("symbol", "")).upper()
        q = valid_quote(row.get("bidPr"), row.get("askPr"), first_positive(row.get("bidSz"), row.get("bidSize")), first_positive(row.get("askSz"), row.get("askSize")))
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
        q = valid_quote(row.get("bidPr"), row.get("askPr"), first_positive(row.get("bidSz"), row.get("bidSize")), first_positive(row.get("askSz"), row.get("askSize")))
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
        q = valid_quote(row.get("bidPrice"), row.get("askPrice"), first_positive(row.get("bidQty"), row.get("bidSize")), first_positive(row.get("askQty"), row.get("askSize")))
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
        q = valid_quote(row.get("bid1"), row.get("ask1"), first_positive(row.get("bid1Size"), row.get("bid1Qty"), row.get("bidSize")), first_positive(row.get("ask1Size"), row.get("ask1Qty"), row.get("askSize")))
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

    # V6: PERP fair remains the benchmark for excursion/reversion detection.
    perp_ref = reference_for_symbol(
        symbol,
        perp_refs,
        cfg.min_reference_exchanges,
        cfg.max_reference_disagreement_pct,
    )
    if not perp_ref:
        return None

    fair, selected_perps, perp_disagreement = perp_ref

    # V9 sanity guard: identical ticker strings can still represent contracts
    # with different multipliers/underlyings across venues. Huge price ratios
    # are treated as incomparable instead of "18,000% arbitrage".
    if fair <= 0 or aq.mid <= 0:
        return None
    cross_ratio = aq.mid / fair
    max_ratio = max(1.01, cfg.max_cross_venue_price_ratio)
    if cross_ratio > max_ratio or cross_ratio < 1.0 / max_ratio:
        return None

    # Robust external executable band: median bid and median ask across agreeing
    # reference perpetual venues. Signal confirmation uses this band.
    ext_bids = [q.bid for q in selected_perps.values()]
    ext_asks = [q.ask for q in selected_perps.values()]
    external_bid = statistics.median(ext_bids)
    external_ask = statistics.median(ext_asks)

    best_bid_venue, best_bid_quote = max(selected_perps.items(), key=lambda kv: kv[1].bid)
    best_ask_venue, best_ask_quote = min(selected_perps.items(), key=lambda kv: kv[1].ask)
    best_external_bid = best_bid_quote.bid
    best_external_ask = best_ask_quote.ask

    # Spot is best-effort diagnostic context. Missing or divergent spot data
    # never blocks a valid perp-vs-perp signal.
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

    # Relative Aster-vs-midpoint edge: diagnostics only.
    short_edge = (aq.bid - fair) / fair * 100.0
    long_edge = (fair - aq.ask) / fair * 100.0

    # Robust hedgeable edge:
    # SHORT Aster: sell Aster bid and buy/long external at median ask.
    # LONG Aster: buy Aster ask and sell/short external at median bid.
    hedge_short_edge = (aq.bid - external_ask) / external_ask * 100.0 if external_ask > 0 else -math.inf
    hedge_long_edge = (external_bid - aq.ask) / external_bid * 100.0 if external_bid > 0 else -math.inf

    # Best-case visible external venue, shown only as a manual-execution hint.
    best_hedge_short_edge = (
        (aq.bid - best_external_ask) / best_external_ask * 100.0 if best_external_ask > 0 else -math.inf
    )
    best_hedge_long_edge = (
        (best_external_bid - aq.ask) / best_external_bid * 100.0 if best_external_bid > 0 else -math.inf
    )

    return MarketSample(
        ts=time.time(),
        aster=aq,
        fair=fair,
        refs=selected_perps,
        ref_disagreement_pct=perp_disagreement,
        deviation_pct=deviation,
        short_edge_pct=short_edge,
        long_edge_pct=long_edge,
        external_bid=external_bid,
        external_ask=external_ask,
        best_external_bid=best_external_bid,
        best_external_ask=best_external_ask,
        best_external_bid_venue=best_bid_venue,
        best_external_ask_venue=best_ask_venue,
        hedge_short_edge_pct=hedge_short_edge,
        hedge_long_edge_pct=hedge_long_edge,
        best_hedge_short_edge_pct=best_hedge_short_edge,
        best_hedge_long_edge_pct=best_hedge_long_edge,
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
        relative_edge = sample.best_relative_edge_pct
        hedge_edge = sample.best_hedgeable_edge_pct

        # Keep MM candidates (spread/deviation) as well as economically
        # hedgeable LAG candidates. WATCH alerts remain disabled later.
        if (
            spread < cfg.min_aster_spread_pct
            and dev < cfg.min_deviation_pct
            and relative_edge < cfg.min_executable_edge_pct
            and hedge_edge < cfg.min_hedgeable_edge_pct
        ):
            continue

        pre_score = (
            spread / max(cfg.min_aster_spread_pct, 1e-9)
            + dev / max(cfg.min_deviation_pct, 1e-9)
            + relative_edge / max(cfg.min_executable_edge_pct, 1e-9)
            + hedge_edge / max(cfg.min_hedgeable_edge_pct, 1e-9)
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
        return False

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


def raw_trade_key(row: dict) -> tuple:
    """Stable-enough dedupe key for repeated polling of Aster recent trades."""
    if row.get("id") is not None:
        return ("id", str(row.get("id")))
    return (
        "fallback",
        str(row.get("time", "")),
        str(row.get("price", "")),
        str(row.get("qty", "")),
        str(row.get("isBuyerMaker", "")),
    )


def lag_baseline_values(state: dict, symbol: str, side: str, cfg: Config, now: Optional[float] = None) -> List[float]:
    """Return recent historical per-run median hedgeable gaps for one symbol/side."""
    now = time.time() if now is None else now
    cutoff = now - max(1, cfg.lag_baseline_lookback_minutes) * 60
    rows = state.get("lag_baseline", {}).get(symbol, [])
    values: List[float] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("side", "")).upper() != side:
            continue
        ts = fnum(row.get("ts"))
        edge = fnum(row.get("edge_pct"), default=-1.0)
        if ts >= cutoff and edge >= 0:
            values.append(edge)
    return values[-cfg.lag_baseline_max_points :]


def lag_verified_profile_rows(
    state: dict, symbol: str, side: str, cfg: Config, now: Optional[float] = None
) -> List[dict]:
    """Recent VERIFIED convergence episodes for one symbol/side."""
    now = time.time() if now is None else now
    cutoff = now - max(1, cfg.active_lag_profile_lookback_hours) * 3600
    rows = state.get("lag_verified_profiles", {}).get(symbol, [])
    out: List[dict] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        if str(row.get("side", "")).upper() != side:
            continue
        if fnum(row.get("ts")) < cutoff:
            continue
        if fnum(row.get("max_convergence_fraction")) < cfg.lag_min_convergence_fraction:
            continue
        out.append(row)
    return out[-cfg.active_lag_profile_max_points :]


def summarize_lag_profile(rows: List[dict]) -> dict:
    t50s = [fnum(r.get("time_to_50_seconds"), default=-1.0) for r in rows]
    t50s = [x for x in t50s if x >= 0]
    convs = [fnum(r.get("max_convergence_fraction")) for r in rows]
    initial = [fnum(r.get("initial_gap_pct")) for r in rows]
    return {
        "verified_episodes": len(rows),
        "median_max_convergence_fraction": statistics.median(convs) if convs else 0.0,
        "median_initial_gap_pct": statistics.median(initial) if initial else 0.0,
        "median_time_to_50_seconds": statistics.median(t50s) if t50s else None,
        "total_convergence_events": sum(int(fnum(r.get("convergence_events"))) for r in rows),
        "total_full_cycles": sum(int(fnum(r.get("full_cycles"))) for r in rows),
    }


def record_signal_stats(state: dict, event_type: str, symbol: str, count: int = 1, side_counts: Optional[dict] = None) -> None:
    stats = state.setdefault("signal_stats", {})
    bucket = stats.setdefault(event_type, {"total": 0, "symbols": {}, "sides": {}, "last_ts": 0})
    bucket["total"] = int(fnum(bucket.get("total"))) + max(0, int(count))
    symbols = bucket.setdefault("symbols", {})
    symbols[symbol] = int(fnum(symbols.get(symbol))) + max(0, int(count))
    sides = bucket.setdefault("sides", {})
    for side, n in (side_counts or {}).items():
        sides[side] = int(fnum(sides.get(side))) + max(0, int(n))
    bucket["last_ts"] = int(time.time())


def detect_active_lag_events(candidates: List[Candidate], cfg: Config, state: dict) -> Tuple[List[dict], bool]:
    """Detect NEW ACTIVE-LAG activations after Stage 2. No orders are placed.

    ACTIVE-LAG requires a prior verified convergence profile for the same symbol/side.
    A transition latch prevents a persistent gap from being counted as a brand-new event every run.
    """
    if not cfg.active_lag_enabled:
        return [], False

    open_state = state.setdefault("active_signal_open", {})
    prev_open_rows = open_state.get("ACTIVE-LAG", [])
    prev_open = set(str(x) for x in prev_open_rows) if isinstance(prev_open_rows, list) else set()
    current_open: set[str] = set()
    events: List[dict] = []

    for c in candidates:
        m = c.metrics(cfg)
        side = str(m.get("persistent_edge_side", "NONE"))
        if side not in {"LONG", "SHORT"}:
            continue

        profile_rows = lag_verified_profile_rows(state, c.symbol, side, cfg)
        profile = summarize_lag_profile(profile_rows)
        baseline_values = lag_baseline_values(state, c.symbol, side, cfg)
        baseline_median = statistics.median(baseline_values) if baseline_values else None
        baseline_ready = len(baseline_values) >= cfg.lag_baseline_min_points
        baseline_required = None
        if baseline_median is not None:
            baseline_required = max(
                baseline_median + cfg.lag_baseline_min_excess_pct,
                baseline_median * cfg.lag_baseline_min_ratio,
            )
        baseline_ok = (not baseline_ready) or baseline_required is None or (
            float(m.get("current_executable_edge_pct", 0.0)) >= baseline_required
        )

        active = (
            bool(m.get("raw_lag_confirmed"))
            and profile["verified_episodes"] >= cfg.active_lag_min_verified_episodes
            and float(m.get("current_executable_edge_pct", 0.0)) >= cfg.active_lag_min_gross_edge_pct
            and float(m.get("current_net_edge_pct", -999.0)) >= cfg.active_lag_min_net_edge_pct
            and float(m.get("persistent_exec_hit_ratio", 0.0)) >= cfg.active_lag_min_hit_ratio
            and float(m.get("current_ref_disagreement_pct", 999.0)) <= cfg.active_lag_max_reference_disagreement_pct
            and baseline_ok
        )
        if not active:
            continue

        key = f"{c.symbol}:{side}"
        current_open.add(key)
        c.active_lag_detected = True
        c.active_lag_detection_ts = c.samples[-1].ts if c.samples else time.time()
        c.active_lag_side = side
        c.active_lag_edge_pct = float(m.get("current_executable_edge_pct", 0.0))
        c.active_lag_net_edge_pct = float(m.get("current_net_edge_pct", 0.0))
        c.active_lag_verified_episodes = int(profile["verified_episodes"])

        if key in prev_open:
            continue

        event = {
            "type": "ACTIVE-LAG",
            "symbol": c.symbol,
            "ts": c.active_lag_detection_ts,
            "side": side,
            "gross_edge_pct": c.active_lag_edge_pct,
            "net_edge_pct": c.active_lag_net_edge_pct,
            "aster_bid": float(m.get("current_aster_bid", 0.0)),
            "aster_ask": float(m.get("current_aster_ask", 0.0)),
            "external_bid": float(m.get("current_external_bid", 0.0)),
            "external_ask": float(m.get("current_external_ask", 0.0)),
            "persistent_hit_ratio": float(m.get("persistent_exec_hit_ratio", 0.0)),
            "persistent_median_edge_pct": float(m.get("persistent_median_executable_edge_pct", 0.0)),
            "reference_disagreement_pct": float(m.get("current_ref_disagreement_pct", 0.0)),
            "baseline_points": len(baseline_values),
            "baseline_min_points": cfg.lag_baseline_min_points,
            "baseline_median_gap_pct": baseline_median,
            "baseline_required_gap_pct": baseline_required,
            "trigger": "historical-profile",
            "bbo_coverage_ratio": float(m.get("lag_bbo_coverage_ratio", 1.0)),
            "target_bbo_capacity_usd": float(m.get("target_bbo_capacity_usd", 0.0)),
            "external_bbo_capacity_usd": float(m.get("external_bbo_capacity_usd", 0.0)),
            **profile,
        }
        events.append(event)
        record_signal_stats(state, "ACTIVE-LAG", c.symbol, 1, {side: 1})

    new_open = sorted(current_open)
    changed = set(prev_open) != set(new_open) or bool(events)
    open_state["ACTIVE-LAG"] = new_open
    return events, changed


def extended_regime_observation(
    candidates: List[Candidate],
    cfg: Config,
    errors: List[str],
    state: dict,
    active_signal_callback: Optional[Callable[[dict], None]] = None,
) -> bool:
    """Observe strongest initial MM and LAG candidates in one shared Stage-3 window.

    MM: accumulate actual Aster trades and recompute multi-minute excursion/reversion statistics.
    LAG: remember the initial hedgeable gap, then measure whether that SAME directional gap
    actually contracts during the next few minutes. Historical per-run gap baselines are attached
    from state so a structural always-present premium/discount can be rejected.
    """
    if not candidates:
        return

    initial_mm: List[Tuple[float, Candidate, dict]] = []
    initial_lag: List[Tuple[float, Candidate, dict]] = []

    for c in candidates:
        m = c.metrics(cfg)
        # Record the Stage-2 typical gap for cross-run baseline persistence before
        # extended samples can dilute it.
        c.run_baseline_side = str(m.get("persistent_edge_side", "NONE"))
        c.run_baseline_edge_pct = max(0.0, float(m.get("persistent_median_executable_edge_pct", 0.0)))

        if m.get("setup") in {"CONFIRMED-MM", "CONFIRMED-BOTH"}:
            rank = (
                float(m.get("mm_score", 0.0))
                + min(20.0, float(m.get("excursion_count", 0)) * 1.5)
                + float(m.get("clean_reversion_rate", 0.0)) * 10.0
            )
            initial_mm.append((rank, c, m))

        if bool(m.get("raw_lag_confirmed")):
            c.lag_verification_status = "not_selected"
            rank = (
                float(m.get("lag_score", 0.0))
                + float(m.get("persistent_exec_hit_ratio", 0.0)) * 15.0
                + min(15.0, max(0.0, float(m.get("persistent_median_net_edge_pct", 0.0))) * 30.0)
            )
            initial_lag.append((rank, c, m))

    initial_mm.sort(key=lambda x: x[0], reverse=True)
    initial_lag.sort(key=lambda x: x[0], reverse=True)

    selected_mm = [c for _, c, _ in initial_mm[: max(0, cfg.extended_mm_max_candidates)]] if cfg.extended_mm_enabled else []
    selected_lag_rows = initial_lag[: max(0, cfg.extended_lag_max_candidates)] if cfg.extended_lag_enabled else []
    selected_lag = [c for _, c, _ in selected_lag_rows]

    for _, c, m in selected_lag_rows:
        c.lag_verification_status = "selected"
        c.lag_initial_confirmed = True
        c.lag_detection_sample_index = max(0, len(c.samples) - 1)
        c.lag_detection_ts = c.samples[c.lag_detection_sample_index].ts if c.samples else time.time()
        c.lag_detection_side = str(m.get("persistent_edge_side", "NONE"))
        c.lag_initial_edge_pct = max(0.0, float(m.get("current_executable_edge_pct", 0.0)))
        c.lag_initial_score = float(m.get("lag_score", 0.0))
        c.lag_baseline_edges_pct = lag_baseline_values(state, c.symbol, c.lag_detection_side, cfg)

    # Union while preserving ranking-ish order.
    selected: List[Candidate] = []
    seen = set()
    for c in selected_mm + selected_lag:
        if c.symbol not in seen:
            seen.add(c.symbol)
            selected.append(c)
    if not selected:
        return False

    duration = max(
        cfg.extended_mm_duration_seconds if selected_mm else 0.0,
        cfg.extended_lag_duration_seconds if selected_lag else 0.0,
    )
    interval = min(
        cfg.extended_mm_interval_seconds if selected_mm else 999.0,
        cfg.extended_lag_interval_seconds if selected_lag else 999.0,
    )
    interval = max(0.25, interval if interval < 999 else 1.5)

    labels = []
    if selected_mm:
        labels.append("MM=" + ",".join(c.symbol for c in selected_mm))
    if selected_lag:
        labels.append("LAG=" + ",".join(c.symbol for c in selected_lag))
    print(f"Extended regime verification ({'; '.join(labels)}) for ~{duration:.0f}s every {interval:.1f}s")

    raw_by_symbol: Dict[str, Dict[tuple, dict]] = {c.symbol: {} for c in selected_mm}
    initial_mm_context: Dict[str, dict] = {c.symbol: m for _, c, m in initial_mm}
    active_mm_seen: Dict[str, set] = {c.symbol: set() for c in selected_mm}
    active_mm_pending: Dict[str, List[dict]] = {c.symbol: [] for c in selected_mm}
    active_mm_last_emit: Dict[str, float] = {c.symbol: 0.0 for c in selected_mm}
    state_changed = False

    def poll_mm_trades() -> None:
        if not selected_mm:
            return
        def worker(c: Candidate) -> Tuple[str, List[dict]]:
            return c.symbol, fetch_aster_recent_trades(c.symbol, cfg)
        with ThreadPoolExecutor(max_workers=min(5, len(selected_mm))) as pool:
            futures = {pool.submit(worker, c): c for c in selected_mm}
            for fut in as_completed(futures):
                c = futures[fut]
                try:
                    symbol, rows = fut.result()
                    store = raw_by_symbol[symbol]
                    for row in rows:
                        if isinstance(row, dict):
                            store[raw_trade_key(row)] = row
                except Exception as e:
                    errors.append(f"extended trades {c.symbol}: {type(e).__name__}: {e}")

    poll_mm_trades()
    start = time.time()
    end = start + max(0.0, duration)

    def process_active_mm_events(force_emit: bool = False) -> None:
        nonlocal state_changed
        if not (cfg.active_mm_excursion_enabled and selected_mm):
            return
        now_ts = time.time()
        for c in selected_mm:
            raw_rows = list(raw_by_symbol[c.symbol].values())
            c.trades = map_trades_to_fair(c, raw_rows, cfg)
            c.excursions = detect_excursions(c.trades, cfg)
            context = initial_mm_context.get(c.symbol, {})
            if (
                int(context.get("excursion_count", 0)) < cfg.active_mm_min_prior_excursions
                or float(context.get("clean_reversion_rate", 0.0)) < cfg.active_mm_min_clean_reversion_rate
            ):
                continue

            window_start = start - max(0.0, cfg.active_mm_initial_lookback_seconds)
            new_events: List[dict] = []
            for exc in c.excursions:
                if exc.start_ts < window_start:
                    continue
                if exc.peak_abs_deviation_pct < cfg.active_mm_min_excursion_pct:
                    continue
                key = (round(exc.start_ts, 3), exc.direction)
                if key in active_mm_seen[c.symbol]:
                    continue
                sample = nearest_sample(c.samples, exc.start_ts, cfg.trade_sample_match_tolerance_seconds)
                if sample is None or sample.ref_disagreement_pct > cfg.active_mm_max_reference_disagreement_pct:
                    continue
                active_mm_seen[c.symbol].add(key)
                action = "SHORT" if exc.direction == "ABOVE" else "LONG"
                event = {
                    "start_ts": exc.start_ts,
                    "direction": exc.direction,
                    "action": action,
                    "peak_pct": exc.peak_abs_deviation_pct,
                    "reverted": exc.reverted,
                    "clean_reversion": exc.clean_reversion,
                    "reversion_seconds": exc.reversion_seconds,
                    "reference_disagreement_pct": sample.ref_disagreement_pct,
                }
                new_events.append(event)
                c.active_mm_excursion_count += 1
                if action == "LONG":
                    c.active_mm_long_count += 1
                else:
                    c.active_mm_short_count += 1
                c.active_mm_peak_values_pct.append(exc.peak_abs_deviation_pct)
                c.active_mm_last_event_ts = max(c.active_mm_last_event_ts or exc.start_ts, exc.start_ts)

            if new_events:
                active_mm_pending[c.symbol].extend(new_events)
                side_counts = {
                    "LONG": sum(1 for e in new_events if e["action"] == "LONG"),
                    "SHORT": sum(1 for e in new_events if e["action"] == "SHORT"),
                }
                record_signal_stats(state, "ACTIVE-MM-EXCURSION", c.symbol, len(new_events), side_counts)
                state_changed = True

            pending = active_mm_pending[c.symbol]
            elapsed_since_emit = now_ts - active_mm_last_emit[c.symbol] if active_mm_last_emit[c.symbol] > 0 else math.inf
            if pending and (force_emit or elapsed_since_emit >= cfg.active_mm_alert_batch_seconds):
                peaks = [float(e["peak_pct"]) for e in pending]
                latest = max(pending, key=lambda e: float(e["start_ts"]))
                payload = {
                    "type": "ACTIVE-MM-EXCURSION",
                    "symbol": c.symbol,
                    "ts": now_ts,
                    "events": list(pending),
                    "event_count": len(pending),
                    "long_count": sum(1 for e in pending if e["action"] == "LONG"),
                    "short_count": sum(1 for e in pending if e["action"] == "SHORT"),
                    "median_peak_pct": statistics.median(peaks) if peaks else 0.0,
                    "max_peak_pct": max(peaks) if peaks else 0.0,
                    "latest_action": latest["action"],
                    "latest_direction": latest["direction"],
                    "latest_event_age_seconds": max(0.0, now_ts - float(latest["start_ts"])),
                    "latest_reverted": bool(latest["reverted"]),
                    "latest_clean_reversion": bool(latest.get("clean_reversion")),
                    "latest_taker_edge_pct": latest.get("taker_edge_pct"),
                    "latest_maker_quote_edge_pct": latest.get("maker_quote_edge_pct"),
                    "latest_target_bbo_capacity_usd": float(latest.get("target_bbo_capacity_usd",0) or 0),
                    "latest_external_bbo_capacity_usd": float(latest.get("external_bbo_capacity_usd",0) or 0),
                    "regime_excursions_before_extended": int(context.get("excursion_count", 0)),
                    "regime_clean_reversion_rate": float(context.get("clean_reversion_rate", 0.0)),
                    "regime_median_reversion_seconds": context.get("median_clean_reversion_seconds"),
                }
                if active_signal_callback is not None:
                    active_signal_callback(payload)
                pending.clear()
                active_mm_last_emit[c.symbol] = now_ts

    next_snapshot = start
    next_trade_poll = start + max(0.5, cfg.extended_mm_trades_poll_seconds)
    sample_no = 0

    # The first raw-trade poll happened just before start; process a short lookback
    # immediately so an excursion already active at the Stage-2 -> Stage-3 boundary
    # is not silently missed.
    process_active_mm_events(force_emit=False)

    while time.time() < end:
        now = time.time()
        did_work = False
        if now >= next_snapshot:
            sample_no += 1
            try:
                aster_book, perp_refs, spot_refs, sample_errors = fetch_snapshot(cfg)
                errors.extend(sample_errors)
                elapsed = time.time() - start
                for c in selected:
                    wants_mm = c in selected_mm and elapsed <= cfg.extended_mm_duration_seconds + interval
                    wants_lag = c in selected_lag and elapsed <= cfg.extended_lag_duration_seconds + interval
                    if not (wants_mm or wants_lag):
                        continue
                    sample = build_sample(c.symbol, aster_book, perp_refs, spot_refs, cfg)
                    if sample:
                        c.samples.append(sample)
            except Exception as e:
                errors.append(f"extended snapshot {sample_no}: {type(e).__name__}: {e}")
            next_snapshot = time.time() + interval
            did_work = True

        now = time.time()
        if selected_mm and now >= next_trade_poll:
            poll_mm_trades()
            process_active_mm_events(force_emit=False)
            next_trade_poll = time.time() + max(1.0, cfg.extended_mm_trades_poll_seconds)
            did_work = True

        if not did_work:
            targets = [next_snapshot, end]
            if selected_mm:
                targets.append(next_trade_poll)
            sleep_for = min(targets) - time.time()
            if sleep_for > 0:
                time.sleep(min(0.25, sleep_for))

    poll_mm_trades()
    process_active_mm_events(force_emit=True)

    for c in selected_mm:
        raw_rows = list(raw_by_symbol[c.symbol].values())
        c.trades = map_trades_to_fair(c, raw_rows, cfg)
        c.excursions = detect_excursions(c.trades, cfg)

    for c in selected_lag:
        c.lag_verification_status = "done"

    for c in selected:
        m = c.metrics(cfg)
        if c in selected_mm:
            print(
                f"Extended MM {c.symbol}: observed={m.get('observed_seconds', 0):.0f}s, "
                f"excursions={m.get('excursion_count', 0)}, clean={m.get('clean_reversion_rate', 0) * 100:.0f}%, "
                f"rate={m.get('excursion_rate_per_minute', 0):.1f}/min, setup={m.get('setup')}"
            )
        if c in selected_lag:
            base = m.get("lag_baseline_median_gap_pct")
            base_txt = "bootstrap" if base is None else f"{base:.3f}%/{m.get('lag_baseline_points', 0)}pts"
            print(
                f"Extended LAG {c.symbol}: side={m.get('lag_detection_side')}, "
                f"initial={m.get('lag_initial_gap_pct', 0):.3f}%, min={m.get('lag_min_gap_pct', 0):.3f}%, "
                f"convergence={m.get('lag_max_convergence_fraction', 0) * 100:.0f}%, "
                f"events={m.get('lag_convergence_events', 0)}, baseline={base_txt}, setup={m.get('setup')}"
            )

    return state_changed


def scan(
    cfg: Config,
    state: dict,
    active_signal_callback: Optional[Callable[[dict], None]] = None,
) -> Tuple[List[Tuple[Candidate, dict]], List[str], bool]:
    try:
        aster_24h = fetch_aster_24h(cfg)
    except Exception as e:
        raise ApiError(f"Failed to fetch Aster 24h stats: {type(e).__name__}: {e}") from e

    aster_book, perp_refs, spot_refs, errors = fetch_snapshot(cfg)
    candidates = prefilter_candidates(aster_book, aster_24h, perp_refs, spot_refs, cfg)

    if not candidates:
        return [], errors, False

    print(f"Prefilter candidates ({len(candidates)}): {', '.join(c.symbol for c in candidates)}")
    print(
        f"Confirming for ~{cfg.confirm_duration_seconds:.0f}s every "
        f"{cfg.confirm_interval_seconds:.1f}s..."
    )
    collect_confirmation_samples(candidates, cfg, errors)
    add_trade_analysis(candidates, cfg, errors)

    # V9: ACTIVE-LAG is evaluated immediately after Stage 2 using PRIOR verified
    # convergence history. It is an informational signal only; no orders are placed.
    active_lag_events, active_state_changed = detect_active_lag_events(candidates, cfg, state)
    if active_signal_callback is not None:
        for event in active_lag_events:
            active_signal_callback(event)

    # Shared extended observation still verifies MM persistence and LAG convergence.
    # During this stage, new MM excursions can emit batched ACTIVE-MM-EXCURSION signals.
    extended_state_changed = extended_regime_observation(
        candidates, cfg, errors, state, active_signal_callback=active_signal_callback
    )

    ranked: List[Tuple[Candidate, dict]] = []
    for candidate in candidates:
        m = candidate.metrics(cfg)
        if m:
            ranked.append((candidate, m))
    ranked.sort(key=lambda x: x[1]["score"], reverse=True)
    return ranked, errors, bool(active_state_changed or extended_state_changed)


def load_state() -> dict:
    try:
        if STATE_PATH.exists():
            data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                data.setdefault("last_alerts", {})
                data.setdefault("lag_baseline", {})
                data.setdefault("lag_verified_profiles", {})
                data.setdefault("active_signal_open", {})
                data.setdefault("signal_stats", {})
                data.setdefault("active_lag_outcomes", {})
                data.setdefault("active_mm_outcomes", {})
                data.setdefault("outcome_stats", {})
                data.setdefault("reference_health", {})
                data.setdefault("state_schema_version", 0)
                return data
    except Exception as e:
        print(f"WARN: cannot read state: {e}", file=sys.stderr)
    return {
        "last_alerts": {},
        "lag_baseline": {},
        "lag_verified_profiles": {},
        "active_signal_open": {},
        "signal_stats": {},
        "active_lag_outcomes": {},
        "active_mm_outcomes": {},
        "outcome_stats": {},
        "reference_health": {},
        "state_schema_version": 0,
    }


def update_lag_baseline_state(state: dict, candidates: List[Candidate], cfg: Config) -> bool:
    """Persist a compact per-run LAG gap baseline for structural-gap detection."""
    now = int(time.time())
    baseline = state.setdefault("lag_baseline", {})
    changed = False
    cutoff = now - max(1, cfg.lag_baseline_lookback_minutes) * 60 * 3

    for c in candidates:
        side = c.run_baseline_side
        edge = c.run_baseline_edge_pct
        if side not in {"LONG", "SHORT"} or edge < 0:
            continue
        rows = baseline.setdefault(c.symbol, [])
        if not isinstance(rows, list):
            rows = []
            baseline[c.symbol] = rows
        rows.append({"ts": now, "side": side, "edge_pct": round(edge, 6)})
        # Keep enough history for the configured lookback while strictly bounding file size.
        rows[:] = [r for r in rows if isinstance(r, dict) and fnum(r.get("ts")) >= cutoff]
        if len(rows) > cfg.lag_baseline_max_points * 3:
            del rows[: len(rows) - cfg.lag_baseline_max_points * 3]
        changed = True

    # Remove empty/stale symbols.
    for symbol in list(baseline.keys()):
        rows = baseline.get(symbol, [])
        if not isinstance(rows, list) or not rows:
            baseline.pop(symbol, None)
            changed = True
    return changed


def update_lag_verified_profile_state(state: dict, candidates: List[Candidate], cfg: Config) -> bool:
    """Persist only LAG episodes that actually passed V9 convergence verification."""
    now = int(time.time())
    profiles = state.setdefault("lag_verified_profiles", {})
    cutoff = now - max(1, cfg.active_lag_profile_lookback_hours) * 3600 * 2
    changed = False

    for c in candidates:
        m = c.metrics(cfg)
        if not bool(m.get("lag_convergence_confirmed")):
            continue
        side = str(m.get("lag_detection_side", "NONE"))
        if side not in {"LONG", "SHORT"}:
            continue
        event_ts = int(c.lag_detection_ts or now)
        rows = profiles.setdefault(c.symbol, [])
        if not isinstance(rows, list):
            rows = []
            profiles[c.symbol] = rows
        # One verified profile row per detected episode/run.
        if any(isinstance(r, dict) and int(fnum(r.get("detection_ts"))) == event_ts and str(r.get("side", "")).upper() == side for r in rows):
            continue
        rows.append({
            "ts": now,
            "detection_ts": event_ts,
            "side": side,
            "initial_gap_pct": round(float(m.get("lag_initial_gap_pct", 0.0)), 6),
            "max_convergence_fraction": round(float(m.get("lag_max_convergence_fraction", 0.0)), 6),
            "convergence_events": int(m.get("lag_convergence_events", 0)),
            "full_cycles": int(m.get("lag_full_convergence_cycles", 0)),
            "time_to_50_seconds": m.get("lag_time_to_50_seconds"),
            "time_to_80_seconds": m.get("lag_time_to_80_seconds"),
        })
        rows[:] = [r for r in rows if isinstance(r, dict) and fnum(r.get("ts")) >= cutoff]
        if len(rows) > cfg.active_lag_profile_max_points * 2:
            del rows[: len(rows) - cfg.active_lag_profile_max_points * 2]
        changed = True

    for symbol in list(profiles.keys()):
        rows = profiles.get(symbol, [])
        if not isinstance(rows, list) or not rows:
            profiles.pop(symbol, None)
            changed = True
    return changed


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

    V6 chart:
      - External PERP fair: midpoint benchmark for MM excursion/reversion analysis.
      - External PERP median bid/ask band: robust executable hedge benchmark for LAG.
      - External SPOT fair: diagnostic only.
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
    external_bids = [s.external_bid for s in samples]
    external_asks = [s.external_ask for s in samples]
    spot_fairs = [s.spot_fair if s.spot_fair and s.spot_fair > 0 else math.nan for s in samples]
    bids = [s.aster.bid for s in samples]
    asks = [s.aster.ask for s in samples]

    out_dir = Path(cfg.charts_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{candidate.symbol}_{m['setup']}_{int(time.time())}.png"

    fig, ax = plt.subplots(figsize=(10, 5.2), dpi=150)
    ax.plot(xs, perp_fairs, label="External PERP fair", linewidth=2.1)
    ax.fill_between(xs, external_bids, external_asks, alpha=0.10, label="External PERP median BBO")
    if sum(math.isfinite(x) for x in spot_fairs) >= 2:
        ax.plot(xs, spot_fairs, label="External SPOT fair (diagnostic)", linewidth=1.4, linestyle="--")
    ax.plot(xs, bids, label="Aster bid", linewidth=1.6)
    ax.plot(xs, asks, label="Aster ask", linewidth=1.6)
    ax.fill_between(xs, bids, asks, alpha=0.08, label="Aster spread")

    excursion_trades = [
        t for t in candidate.trades
        if abs(t.deviation_pct) >= cfg.excursion_threshold_pct and -1.0 <= t.ts - t0 <= xs[-1] + 1.0
    ]
    if excursion_trades:
        tx = [t.ts - t0 for t in excursion_trades]
        ty = [t.price for t in excursion_trades]
        ax.scatter(tx, ty, marker="x", s=38, label="Aster excursion trades vs PERP fair", zorder=5)

    side = m.get("current_edge_side", "NONE")
    gross = m.get("current_executable_edge_pct", 0.0)
    net = m.get("current_net_edge_pct", 0.0)
    persistent_side = m.get("persistent_edge_side", "NONE")
    persistent_hit = m.get("persistent_exec_hit_ratio", 0.0) * 100.0
    if "MM" in m.get("setup", ""):
        ax.set_title(
            f"{candidate.symbol} | {m['setup']} | ~{xs[-1]:.0f}s\n"
            f"{m.get('excursion_count', 0)} excursions | clean {m.get('clean_reversion_rate', 0) * 100:.0f}% | "
            f"{m.get('excursion_rate_per_minute', 0):.1f}/min | "
            f"median reversion {fmt_seconds(m.get('median_clean_reversion_seconds'))}"
        )
    else:
        ax.set_title(
            f"{candidate.symbol} | {m['setup']} | ~{xs[-1]:.0f}s\n"
            f"Hedgeable gross {gross:.3f}% {side} | est. net {net:+.3f}% | "
            f"persistent {persistent_side} hit {persistent_hit:.0f}%"
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

    if "MM" in m.get("setup", ""):
        caption = (
            f"📈 <b>{candidate.symbol} — {m['setup']}</b>\n"
            f"Observed <b>{duration:.0f}s</b> | excursions <b>{m['excursion_count']}</b> | "
            f"clean <b>{m['clean_reverted_count']}/{m['excursion_count']} "
            f"({m['clean_reversion_rate'] * 100:.0f}%)</b>\n"
            f"Rate <b>{m.get('excursion_rate_per_minute', 0):.1f}/min</b> | "
            f"median peak {m['median_peak_excursion_pct']:.3f}% | "
            f"median reversion {fmt_seconds(m['median_clean_reversion_seconds'])}\n"
            f"ABOVE {m.get('above_excursions', 0)} / BELOW {m.get('below_excursions', 0)} | "
            f"longest quiet {m.get('longest_quiet_seconds', 0):.1f}s | "
            f"last excursion {m.get('last_excursion_age_seconds', 0):.1f}s ago"
            f"{basis_line}\n"
            "MM statistics use actual Aster trades matched to time-local external PERP fair. "
            "MATURE-MM means the regime survived the extended observation criteria."
        )
    else:
        caption = (
            f"📈 <b>{candidate.symbol} — {m['setup']}</b>\n"
            f"~{duration:.0f}s: external PERP BBO vs Aster bid/ask"
            f"{basis_line}\n"
            f"Gross hedgeable edge: <b>{m['current_executable_edge_pct']:.3f}% {m['current_edge_side']}</b>\n"
            f"Est. round-trip fees: {m['estimated_roundtrip_fees_pct']:.3f}% | "
            f"est. net: <b>{m['current_net_edge_pct']:+.3f}%</b>\n"
            f"Initial gap {m.get('lag_initial_gap_pct', m['current_executable_edge_pct']):.3f}% {m.get('lag_detection_side', m['current_edge_side'])} | "
            f"max convergence {m.get('lag_max_convergence_fraction', 0) * 100:.0f}% | events {m.get('lag_convergence_events', 0)}\n"
            f"Time to 50% {fmt_seconds(m.get('lag_time_to_50_seconds'))} | time to 80% {fmt_seconds(m.get('lag_time_to_80_seconds'))}\n"
            "V9 verified LAG alerts require observed convergence; ACTIVE-LAG reports a new live gap only after prior verified history. Spot and midpoint fair remain diagnostics."
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

    mm_regime_block = ""
    if m.get("setup") in {
        "CONFIRMED-MM", "CONFIRMED-BOTH", "MATURE-MM",
        "CONFIRMED-MM+CONVERGING-LAG", "MATURE-MM+CONVERGING-LAG",
    }:
        p90_rev = m.get("p90_clean_reversion_seconds")
        p90_rev_text = "n/a" if p90_rev is None else f"{p90_rev:.2f}s"
        active_mm_summary = ""
        if int(m.get("active_mm_excursion_count", 0)) > 0:
            age = m.get("active_mm_last_event_age_seconds")
            age_text = "n/a" if age is None else f"{age:.1f}s"
            active_mm_summary = (
                f"ACTIVE-MM opportunities observed after confirmation: <b>{m.get('active_mm_excursion_count', 0)}</b> | "
                f"LONG {m.get('active_mm_long_count', 0)} / SHORT {m.get('active_mm_short_count', 0)} | "
                f"median peak {m.get('active_mm_median_peak_pct', 0):.3f}% | max {m.get('active_mm_max_peak_pct', 0):.3f}% | "
                f"last {age_text} ago\n"
            )
        mm_regime_block = (
            f"MM observed: <b>{m.get('observed_seconds', 0):.0f}s</b> | excursions "
            f"<b>{m['excursion_count']}</b> | clean {m['clean_reverted_count']}/{m['excursion_count']} "
            f"(<b>{m['clean_reversion_rate'] * 100:.0f}%</b>)\n"
            f"Excursion rate: <b>{m.get('excursion_rate_per_minute', 0):.1f}/min</b> | "
            f"ABOVE {m.get('above_excursions', 0)} / BELOW {m.get('below_excursions', 0)}\n"
            f"Median peak: {m['median_peak_excursion_pct']:.3f}% | P90 peak: "
            f"{m.get('p90_peak_excursion_pct', 0):.3f}% | max {m['max_peak_excursion_pct']:.3f}%\n"
            f"Median clean reversion: {fmt_seconds(m['median_clean_reversion_seconds'])} | "
            f"P90 clean reversion: {p90_rev_text}\n"
            f"Active span: {m.get('active_span_seconds', 0):.0f}s | longest quiet: "
            f"{m.get('longest_quiet_seconds', 0):.1f}s | last excursion: "
            f"{m.get('last_excursion_age_seconds', 0):.1f}s ago\n"
            f"{active_mm_summary}"
        )

    lag_convergence_block = ""
    if "CONVERGING-LAG" in m.get("setup", ""):
        t50 = fmt_seconds(m.get("lag_time_to_50_seconds"))
        t80 = fmt_seconds(m.get("lag_time_to_80_seconds"))
        baseline_median = m.get("lag_baseline_median_gap_pct")
        if m.get("lag_baseline_ready") and baseline_median is not None:
            baseline_text = (
                f"Baseline recent runs: median {baseline_median:.3f}% "
                f"({m.get('lag_baseline_points', 0)} pts) | excess {m.get('lag_baseline_excess_pct', 0):+.3f}% | "
                f"ratio {m.get('lag_baseline_ratio', 0):.2f}x\n"
            )
        else:
            baseline_text = f"Baseline: bootstrap / insufficient prior points ({m.get('lag_baseline_points', 0)})\n"
        lag_convergence_block = (
            f"LAG extended observation: <b>{m.get('lag_extended_observed_seconds', 0):.0f}s</b> | side <b>{m.get('lag_detection_side', 'NONE')}</b>\n"
            f"Initial gross gap: <b>{m.get('lag_initial_gap_pct', 0):.3f}%</b> | minimum gap: "
            f"{m.get('lag_min_gap_pct', 0):.3f}% | current same-side gap: {m.get('lag_current_gap_pct', 0):.3f}%\n"
            f"Max convergence: <b>{m.get('lag_max_convergence_fraction', 0) * 100:.0f}%</b> | "
            f"time to 50%: {t50} | time to 80%: {t80}\n"
            f"Convergence events: <b>{m.get('lag_convergence_events', 0)}</b> | full cycles: "
            f"{m.get('lag_full_convergence_cycles', 0)}\n"
            f"{baseline_text}"
        )

    hedge_hint = ""
    if m.get("best_hedge_venue"):
        hedge_action = "BUY/LONG" if persistent_side == "SHORT" else "SELL/SHORT"
        hedge_hint = (
            f"Best visible external hedge: <b>{hedge_action} {m['best_hedge_venue']}</b> @ "
            f"{fmt_price(m['best_hedge_price'])} | best-case gross "
            f"{m['current_best_case_hedge_edge_pct']:.3f}%\n"
        )

    return (
        f"🔥 <b>{m['level']} / {m['setup']} — {candidate.symbol}</b>\n"
        f"Signal strength: <b>{m['score']:.0f}/100</b> | LAG {m['lag_score']:.0f} | MM {m['mm_score']:.0f}\n"
        f"Aster PERP: {fmt_price(m['current_aster_bid'])} / {fmt_price(m['current_aster_ask'])}\n"
        f"External PERP fair (mid diagnostic): {fmt_price(m['current_fair'])} [{perp_refs}]\n"
        f"External PERP robust BBO: <b>{fmt_price(m['current_external_bid'])} / {fmt_price(m['current_external_ask'])}</b>\n"
        f"Gross hedgeable edge now: <b>{m['current_executable_edge_pct']:.3f}% {side_text}</b>\n"
        f"Estimated round-trip fees: {m['estimated_roundtrip_fees_pct']:.3f}%\n"
        f"Estimated net edge if convergence: <b>{m['current_net_edge_pct']:+.3f}%</b>\n"
        f"Persistent hedgeable side: <b>{persistent_side}</b> | hit ratio "
        f"<b>{m['persistent_exec_hit_ratio'] * 100:.0f}%</b> | median gross "
        f"{m['persistent_median_executable_edge_pct']:.3f}% | median est. net "
        f"{m['persistent_median_net_edge_pct']:+.3f}%\n"
        f"{hedge_hint}"
        f"{lag_convergence_block}"
        f"{mm_regime_block}"
        f"Relative Aster-vs-PERP-mid edge (diagnostic): "
        f"{m['current_relative_edge_pct']:.3f}% {m['current_relative_edge_side']}\n"
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
        "V12 keeps verified convergence and ACTIVE signals, and adds execution-readiness depth checks; no orders are ever placed. "
        "The fee reserve is configurable and only an estimate; slippage, funding and execution risk are not included. "
        "CONFIRMED-MM requires repeated clean Aster trade reversions vs external PERP fair; MATURE-MM additionally requires the regime to persist through extended observation. Manual inspection only."
    )


def format_active_signal(event: dict, state: dict) -> str:
    event_type = str(event.get("type", ""))
    symbol = str(event.get("symbol", ""))
    stats = state.get("signal_stats", {}).get(event_type, {})
    total = int(fnum(stats.get("total"))) if isinstance(stats, dict) else 0
    symbol_total = 0
    if isinstance(stats, dict):
        symbol_total = int(fnum(stats.get("symbols", {}).get(symbol)))

    if event_type == "ACTIVE-LAG":
        side = str(event.get("side", "NONE"))
        pair = "LONG Aster / SHORT external PERP" if side == "LONG" else "SHORT Aster / LONG external PERP"
        baseline = event.get("baseline_median_gap_pct")
        baseline_text = "bootstrap / insufficient" if baseline is None else f"{float(baseline):.3f}%"
        t50 = fmt_seconds(event.get("median_time_to_50_seconds"))
        return (
            f"⚡ <b>ACTIVE-LAG — {symbol}</b>\n"
            f"Side: <b>{side}</b> | pair interpretation: {pair}\n"
            f"Current gross hedgeable gap: <b>{float(event.get('gross_edge_pct', 0)):.3f}%</b>\n"
            f"Estimated net after fee reserve: <b>{float(event.get('net_edge_pct', 0)):+.3f}%</b>\n"
            f"Aster bid/ask: {fmt_price(float(event.get('aster_bid', 0)))} / {fmt_price(float(event.get('aster_ask', 0)))}\n"
            f"External robust bid/ask: {fmt_price(float(event.get('external_bid', 0)))} / {fmt_price(float(event.get('external_ask', 0)))}\n"
            f"Persistent hit: {float(event.get('persistent_hit_ratio', 0)) * 100:.0f}% | "
            f"median gross {float(event.get('persistent_median_edge_pct', 0)):.3f}%\n"
            f"Reference disagreement: {float(event.get('reference_disagreement_pct', 0)):.3f}%\n"
            f"Prior VERIFIED convergence episodes: <b>{int(event.get('verified_episodes', 0))}</b> | "
            f"median max convergence {float(event.get('median_max_convergence_fraction', 0)) * 100:.0f}% | "
            f"full cycles {int(event.get('total_full_cycles', 0))}\n"
            f"Historical median time to 50% convergence: {t50}\n"
            f"Recent baseline median: {baseline_text}\n"
            f"V13 counter: {symbol} ACTIVE-LAG detections <b>{symbol_total}</b> | all symbols <b>{total}</b>\n\n"
            "Signal only. No API orders are sent."
        )

    if event_type == "ACTIVE-MM-EXCURSION":
        latest_action = str(event.get("latest_action", "NONE"))
        latest_status = "already reverted by poll time" if event.get("latest_reverted") else "still unresolved at poll time"
        return (
            f"⚡ <b>ACTIVE-MM-EXCURSION — {symbol}</b>\n"
            f"New qualifying excursions in this batch: <b>{int(event.get('event_count', 0))}</b>\n"
            f"LONG-side opportunities: {int(event.get('long_count', 0))} | SHORT-side opportunities: {int(event.get('short_count', 0))}\n"
            f"Median peak deviation: <b>{float(event.get('median_peak_pct', 0)):.3f}%</b> | max {float(event.get('max_peak_pct', 0)):.3f}%\n"
            f"Latest inferred side: <b>{latest_action}</b> | event age ~{float(event.get('latest_event_age_seconds', 0)):.1f}s | {latest_status}\n"
            f"Regime before extended stage: {int(event.get('regime_excursions_before_extended', 0))} excursions | "
            f"clean {float(event.get('regime_clean_reversion_rate', 0)) * 100:.0f}% | "
            f"median reversion {fmt_seconds(event.get('regime_median_reversion_seconds'))}\n"
            f"V13 counter: {symbol} ACTIVE-MM excursion events <b>{symbol_total}</b> | all symbols <b>{total}</b>\n\n"
            "Events are batched to avoid Telegram spam. The counter uses the actual qualifying excursion count, not the number of Telegram messages. No orders are sent."
        )

    return f"⚡ <b>{event_type} — {symbol}</b>\nSignal only. No orders are sent."


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
        f"{'HEDGE%':>8} {'NET%':>8} {'EXC':>4} {'C.REV%':>7} {'REVsec':>7} {'24H%':>8}"
    )
    for c, m in ranked[:20]:
        revsec = m["median_clean_reversion_seconds"]
        revtxt = "-" if revsec is None else f"{revsec:.2f}"
        print(
            f"{c.symbol:<16} {m['level']:<10} {m['setup']:<15} {m['score']:>5.0f} "
            f"{m['median_spread_pct']:>7.3f} {m['max_executable_edge_pct']:>8.3f} "
            f"{m['max_net_edge_pct']:>8.3f} {m['excursion_count']:>4} "
            f"{m['clean_reversion_rate'] * 100:>7.0f} {revtxt:>7} "
            f"{c.move24h_pct:>+8.1f}"
        )


# ---------------------------------------------------------------------------
# V10 multi-target / multi-reference layer
# ---------------------------------------------------------------------------
# Targets: Aster, Hyperliquid, Lighter.
# Robust PERP fair consensus: Bitget, MEXC, Bybit, Hyperliquid, Lighter,
# edgeX and dYdX. The target venue is excluded from its own consensus.
#
# Important distinction:
# - MM fair consensus may use midpoint/mark-like public prices from all sources.
# - LAG executable consensus only uses venues for which a real bid/ask is
#   available in the current snapshot. We never fabricate an executable BBO
#   around a midpoint.

import threading
from collections import deque

try:
    import websocket  # websocket-client; optional at import time for --self-test
except Exception:
    websocket = None

HYPERLIQUID_INFO_URL = os.getenv("HYPERLIQUID_INFO_URL", "https://api.hyperliquid.xyz/info").strip()
LIGHTER_BASE = os.getenv("LIGHTER_BASE_URL", "https://mainnet.zklighter.elliot.ai").rstrip("/")
LIGHTER_WS_URL = os.getenv(
    "LIGHTER_WS_URL", "wss://mainnet.zklighter.elliot.ai/stream?readonly=true"
).strip()
EDGEX_WS_URL = os.getenv(
    "EDGEX_WS_URL", "wss://quote.edgex.exchange/api/v1/public/ws"
).strip()
DYDX_BASE = os.getenv("DYDX_BASE_URL", "https://indexer.dydx.trade/v4").rstrip("/")

TARGET_LABELS = {
    "aster": "Aster",
    "hyperliquid": "Hyperliquid",
    "lighter": "Lighter",
}


def env_bool(name: str, default: str = "true") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def cfg_target_venues() -> Tuple[str, ...]:
    raw = os.getenv("TARGET_VENUES", "aster,hyperliquid,lighter")
    out = []
    for item in raw.split(","):
        x = item.strip().lower()
        if x in TARGET_LABELS and x not in out:
            out.append(x)
    return tuple(out or ["aster"])


def cfg_target_cap(target: str, cfg: Config) -> int:
    defaults = {"aster": "25", "hyperliquid": "5", "lighter": "10"}
    return max(0, int(os.getenv(f"MAX_CANDIDATES_{target.upper()}", defaults[target])))


def cfg_min_fair_refs(cfg: Config) -> int:
    return max(2, int(os.getenv("MIN_FAIR_REFERENCE_EXCHANGES", "3")))


def cfg_max_fair_disagreement(cfg: Config) -> float:
    return float(os.getenv("MAX_FAIR_REFERENCE_DISAGREEMENT_PCT", str(cfg.max_reference_disagreement_pct)))


def cfg_min_exec_refs(cfg: Config) -> int:
    return max(1, int(os.getenv("MIN_EXEC_REFERENCE_EXCHANGES", "2")))


def cfg_max_exec_disagreement(cfg: Config) -> float:
    return float(os.getenv("MAX_EXEC_REFERENCE_DISAGREEMENT_PCT", str(cfg.max_reference_disagreement_pct)))


def target_label(candidate_or_target: Any) -> str:
    if isinstance(candidate_or_target, str):
        target = candidate_or_target
    else:
        target = str(getattr(candidate_or_target, "target", "aster"))
    return TARGET_LABELS.get(target, target.title())


def candidate_state_key(c: Candidate) -> str:
    return f"{getattr(c, 'target', 'aster')}:{c.symbol}"


def candidate_display_name(c: Candidate) -> str:
    return f"{c.symbol} @ {target_label(c)}"


def normalize_perp_symbol(raw: Any) -> Optional[str]:
    """Normalize public venue symbols to BASEUSDT for cross-venue matching.

    This is intentionally conservative. HIP-3 names (DEX:COIN), index-style
    names and empty values are skipped. Existing price-ratio sanity checks still
    reject multiplier mismatches that happen to share a ticker string.
    """
    s = str(raw or "").strip().upper()
    if not s or s.startswith("@") or ":" in s:
        return None
    for suffix in ("-PERPETUAL", "_PERPETUAL", "PERPETUAL", "-PERP", "_PERP"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
            break
    s = s.replace("/", "-").replace("_", "-")
    parts = [p for p in s.split("-") if p]
    if len(parts) >= 2 and parts[-1] in {"USD", "USDC", "USDT"}:
        base = "".join(parts[:-1])
        return f"{base}USDT" if base else None
    flat = "".join(ch for ch in s if ch.isalnum())
    if not flat:
        return None
    if flat.endswith("USDT"):
        return flat
    if flat.endswith("USDC"):
        return flat[:-4] + "USDT"
    if flat.endswith("USD"):
        return flat[:-3] + "USDT"
    return flat + "USDT"


def post_json(url: str, timeout: float, payload: dict) -> object:
    r = requests.post(url, json=payload, timeout=timeout, headers=HTTP_HEADERS)
    r.raise_for_status()
    return r.json()


def robust_reference_for_symbol(
    symbol: str,
    refs: Dict[str, Dict[str, Quote]],
    min_exchanges: int,
    max_disagreement_pct: float,
    excluded: Optional[set[str]] = None,
) -> Optional[Tuple[float, Dict[str, Quote], float]]:
    """Median consensus with iterative outlier trimming.

    If seven venues are available and one is stale, the stale venue is removed
    instead of invalidating the whole symbol. Trimming stops once the remaining
    range is within max_disagreement_pct or removing another venue would violate
    min_exchanges.
    """
    excluded = excluded or set()
    selected: Dict[str, Quote] = {}
    for name, market in refs.items():
        if name in excluded:
            continue
        q = market.get(symbol)
        if q and q.mid > 0:
            selected[name] = q
    if len(selected) < min_exchanges:
        return None

    work = dict(selected)
    while len(work) >= min_exchanges:
        mids = [q.mid for q in work.values()]
        fair = statistics.median(mids)
        disagreement = ((max(mids) - min(mids)) / fair * 100.0) if fair > 0 else math.inf
        if disagreement <= max_disagreement_pct:
            return fair, work, disagreement
        if len(work) == min_exchanges:
            return None
        # Drop the venue furthest from the robust center.
        worst_name = max(work, key=lambda name: abs(work[name].mid - fair) / fair)
        work.pop(worst_name, None)
    return None


# Make Candidate.metrics target-aware without rewriting the thoroughly tested V9
# scoring/convergence engine. The numerical engine still uses historical field
# names such as current_aster_bid internally; the reporting layer exposes them as
# generic target bid/ask values.
_v9_candidate_metrics = Candidate.metrics


def _v10_candidate_metrics(self: Candidate, cfg: Config) -> dict:
    m = _v9_candidate_metrics(self, cfg)
    if not m:
        return m
    label = target_label(self)
    m["target"] = getattr(self, "target", "aster")
    m["target_label"] = label
    m["state_key"] = candidate_state_key(self)
    m["display_name"] = candidate_display_name(self)
    m["current_target_bid"] = m.get("current_aster_bid", 0.0)
    m["current_target_ask"] = m.get("current_aster_ask", 0.0)
    m["current_target_mid"] = (m.get("current_target_bid", 0.0) + m.get("current_target_ask", 0.0)) / 2.0
    if isinstance(m.get("direction"), str):
        m["direction"] = m["direction"].replace("Aster", label)
    if self.samples:
        cur = self.samples[-1]
        m["current_exec_refs"] = sorted(getattr(cur, "exec_refs", {}).keys())
        m["target_bid_notional"] = float(getattr(cur, "target_bid_notional", 0.0))
        m["target_ask_notional"] = float(getattr(cur, "target_ask_notional", 0.0))
        m["external_bid_notional"] = float(getattr(cur, "external_bid_notional", 0.0))
        m["external_ask_notional"] = float(getattr(cur, "external_ask_notional", 0.0))
        side = str(m.get("current_edge_side", "NONE"))
        target_capacity = m["target_ask_notional"] if side == "LONG" else m["target_bid_notional"] if side == "SHORT" else 0.0
        external_capacity = m["external_bid_notional"] if side == "LONG" else m["external_ask_notional"] if side == "SHORT" else 0.0
        m["target_bbo_capacity_usd"] = target_capacity
        m["external_bbo_capacity_usd"] = external_capacity
        checks = {}
        for usd in cfg.bbo_notional_checks:
            if target_capacity <= 0 or external_capacity <= 0:
                checks[str(int(usd) if float(usd).is_integer() else usd)] = "UNKNOWN"
            elif target_capacity >= usd and external_capacity >= usd:
                checks[str(int(usd) if float(usd).is_integer() else usd)] = "OK"
            else:
                checks[str(int(usd) if float(usd).is_integer() else usd)] = "INSUFFICIENT"
        m["bbo_notional_checks"] = checks
    else:
        m["current_exec_refs"] = []
        m["target_bid_notional"] = m["target_ask_notional"] = 0.0
        m["external_bid_notional"] = m["external_ask_notional"] = 0.0
        m["target_bbo_capacity_usd"] = m["external_bbo_capacity_usd"] = 0.0
        m["bbo_notional_checks"] = {}
    return m


Candidate.metrics = _v10_candidate_metrics


class LighterPublicStream:
    """Read-only Lighter market_stats + trade stream.

    market_stats/all gives broad BBO + stats for every market, which avoids the
    restrictive REST request budget. Trade subscriptions are added only for
    Lighter target candidates.
    """

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.quotes: Dict[str, Quote] = {}
        self.stats: Dict[str, dict] = {}
        self.market_id_by_symbol: Dict[str, int] = {}
        self.symbol_by_market_id: Dict[int, str] = {}
        self.trades: Dict[int, deque] = {}
        self.trade_subscriptions: set[int] = set()
        self.book_subscriptions: set[int] = set()
        self.books: Dict[int, Dict[str, Dict[float, float]]] = {}
        self.book_nonce: Dict[int, int] = {}
        self.book_valid: Dict[int, bool] = {}
        self.book_last_ts: Dict[int, float] = {}
        self.ws = None
        self.thread = None
        self.started = False
        self.last_error = ""
        self.last_message_ts = 0.0

    def start(self) -> None:
        if self.started:
            return
        self.started = True
        if websocket is None:
            self.last_error = "websocket-client is not installed"
            return
        self.thread = threading.Thread(target=self._run, name="lighter-public-ws", daemon=True)
        self.thread.start()

    def _send_subscriptions(self, ws) -> None:
        try:
            ws.send(json.dumps({"type": "subscribe", "channel": "market_stats/all"}))
            with self.lock:
                mids = list(self.trade_subscriptions)
                book_mids = list(self.book_subscriptions)
            for market_id in mids:
                ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{market_id}"}))
            for market_id in book_mids:
                ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{market_id}"}))
        except Exception as e:
            self.last_error = f"subscribe: {type(e).__name__}: {e}"

    def _run(self) -> None:
        while True:
            try:
                app = websocket.WebSocketApp(
                    LIGHTER_WS_URL,
                    on_open=lambda ws: self._send_subscriptions(ws),
                    on_message=self._on_message,
                    on_error=lambda _ws, err: setattr(self, "last_error", f"ws: {err}"),
                )
                self.ws = app
                app.run_forever(ping_interval=45, ping_timeout=15)
            except Exception as e:
                self.last_error = f"run: {type(e).__name__}: {e}"
            time.sleep(2.0)

    def _on_message(self, ws, message: str) -> None:
        try:
            obj = json.loads(message)
        except Exception:
            return
        self.last_message_ts = time.time()
        if isinstance(obj, dict) and obj.get("type") == "ping":
            try:
                ws.send(json.dumps({"type": "pong"}))
            except Exception:
                pass
            return

        stats_obj = obj.get("market_stats") if isinstance(obj, dict) else None
        stats_rows: List[dict] = []
        if isinstance(stats_obj, dict):
            # market_stats/all may be keyed by market id, while single market
            # updates use one object.
            if "market_id" in stats_obj or "symbol" in stats_obj:
                stats_rows = [stats_obj]
            else:
                stats_rows = [v for v in stats_obj.values() if isinstance(v, dict)]
        elif isinstance(stats_obj, list):
            stats_rows = [x for x in stats_obj if isinstance(x, dict)]
        if isinstance(obj, dict) and isinstance(obj.get("market_stats_list"), list):
            stats_rows.extend(x for x in obj["market_stats_list"] if isinstance(x, dict))

        if stats_rows:
            with self.lock:
                for row in stats_rows:
                    symbol = normalize_perp_symbol(row.get("symbol"))
                    market_id = int(fnum(row.get("market_id"), default=-1))
                    if not symbol or market_id < 0:
                        continue
                    q = valid_quote(row.get("best_bid_price"), row.get("best_ask_price"), first_positive(row.get("best_bid_size"), row.get("best_bid_amount"), row.get("bid_size")), first_positive(row.get("best_ask_size"), row.get("best_ask_amount"), row.get("ask_size")))
                    if q:
                        self.quotes[symbol] = q
                    self.market_id_by_symbol[symbol] = market_id
                    self.symbol_by_market_id[market_id] = symbol
                    self.stats[symbol] = {
                        "move24h_pct": fnum(row.get("daily_price_change")),
                        "quote_volume24h": fnum(row.get("daily_quote_token_volume")),
                        "market_id": market_id,
                        "native_symbol": str(row.get("symbol", "")),
                        "mark_price": fnum(row.get("mark_price")),
                        "mid_price": fnum(row.get("mid_price")),
                        "last_trade_price": fnum(row.get("last_trade_price")),
                        # Lighter docs: funding_rate is the LAST settled funding
                        # payment and funding_timestamp is its settlement time.
                        "funding_rate": fnum(row.get("funding_rate")),
                        "current_funding_rate": fnum(row.get("current_funding_rate")),
                        "funding_timestamp": fnum(row.get("funding_timestamp")),
                    }

        if isinstance(obj, dict) and isinstance(obj.get("order_book"), dict):
            channel = str(obj.get("channel", ""))
            market_id = -1
            if ":" in channel:
                market_id = int(fnum(channel.split(":")[-1], default=-1))
            elif "/" in channel:
                market_id = int(fnum(channel.split("/")[-1], default=-1))
            ob = obj.get("order_book") or {}
            if market_id >= 0:
                nonce = int(fnum(ob.get("nonce"), default=-1))
                begin_nonce = int(fnum(ob.get("begin_nonce"), default=-1))
                with self.lock:
                    exists = market_id in self.books and bool(self.books.get(market_id))
                    prev_nonce = self.book_nonce.get(market_id, -1)
                    # First payload after subscription is a full snapshot. Later payloads are
                    # absolute state changes. If continuity breaks, mark the book invalid and
                    # resubscribe so the next payload is a fresh snapshot.
                    if exists and prev_nonce >= 0 and begin_nonce >= 0 and begin_nonce != prev_nonce:
                        self.book_valid[market_id] = False
                        self.books.pop(market_id, None)
                        self.book_nonce.pop(market_id, None)
                        try:
                            if self.ws is not None:
                                self.ws.send(json.dumps({"type":"unsubscribe","channel":f"order_book/{market_id}"}))
                                self.ws.send(json.dumps({"type":"subscribe","channel":f"order_book/{market_id}"}))
                        except Exception:
                            pass
                    else:
                        if not exists:
                            self.books[market_id] = {"bids": {}, "asks": {}}
                        book = self.books[market_id]
                        for side_key in ("bids", "asks"):
                            rows = ob.get(side_key, [])
                            if isinstance(rows, list):
                                for row in rows:
                                    if not isinstance(row, dict):
                                        continue
                                    px = fnum(row.get("price")); sz = fnum(row.get("size"))
                                    if px <= 0:
                                        continue
                                    if sz <= 0:
                                        book[side_key].pop(px, None)
                                    else:
                                        book[side_key][px] = sz
                        self.book_nonce[market_id] = nonce
                        self.book_valid[market_id] = True
                        self.book_last_ts[market_id] = time.time()
                        sym = self.symbol_by_market_id.get(market_id)
                        if sym and book["bids"] and book["asks"]:
                            bp = max(book["bids"]); ap = min(book["asks"])
                            self.quotes[sym] = Quote(bp, ap, book["bids"][bp], book["asks"][ap])

        if isinstance(obj, dict):
            trade_rows = obj.get("trades")
            if isinstance(trade_rows, list):
                channel = str(obj.get("channel", ""))
                market_id = -1
                if ":" in channel:
                    market_id = int(fnum(channel.split(":")[-1], default=-1))
                elif "/" in channel:
                    market_id = int(fnum(channel.split("/")[-1], default=-1))
                if trade_rows and market_id < 0:
                    market_id = int(fnum(trade_rows[0].get("market_id"), default=-1))
                if market_id >= 0:
                    with self.lock:
                        buf = self.trades.setdefault(market_id, deque(maxlen=3000))
                        for row in trade_rows:
                            if not isinstance(row, dict):
                                continue
                            ts = fnum(row.get("timestamp"))
                            if ts > 0 and ts < 1e12:
                                ts *= 1000.0
                            buf.append({
                                "id": row.get("trade_id_str", row.get("trade_id")),
                                "time": ts,
                                "price": fnum(row.get("price")),
                                "qty": fnum(row.get("size")),
                                "isBuyerMaker": row.get("is_maker_ask"),
                            })

    def subscribe_trades(self, market_id: int) -> None:
        if market_id < 0:
            return
        with self.lock:
            is_new = market_id not in self.trade_subscriptions
            self.trade_subscriptions.add(market_id)
            self.trades.setdefault(market_id, deque(maxlen=3000))
        if is_new and self.ws is not None:
            try:
                self.ws.send(json.dumps({"type": "subscribe", "channel": f"trade/{market_id}"}))
            except Exception:
                pass

    def subscribe_order_book(self, market_id: int) -> None:
        if market_id < 0:
            return
        with self.lock:
            is_new = market_id not in self.book_subscriptions
            self.book_subscriptions.add(market_id)
        if is_new and self.ws is not None:
            try:
                self.ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{market_id}"}))
            except Exception:
                pass

    def depth_snapshot(self, market_id: int, levels: int = 20) -> Optional[dict]:
        with self.lock:
            if market_id < 0 or not self.book_valid.get(market_id, False):
                return None
            book = self.books.get(market_id) or {}
            bids_map = dict(book.get("bids", {})); asks_map = dict(book.get("asks", {}))
            ts = self.book_last_ts.get(market_id, 0.0)
        if not bids_map or not asks_map:
            return None
        n = max(1, int(levels))
        bids = [(px, bids_map[px]) for px in sorted(bids_map, reverse=True)[:n]]
        asks = [(px, asks_map[px]) for px in sorted(asks_map)[:n]]
        return {"bids": bids, "asks": asks, "ts": ts, "venue": "lighter-perp"}

    def snapshot(self) -> Tuple[Dict[str, Quote], Dict[str, dict]]:
        with self.lock:
            return dict(self.quotes), {k: dict(v) for k, v in self.stats.items()}

    def recent_trades(self, market_id: int) -> List[dict]:
        with self.lock:
            return list(self.trades.get(market_id, ()))


class EdgeXPublicStream:
    """Best-effort edgeX ticker.all reference stream.

    edgeX is a fair-price reference in V10, not an execution venue. If its
    public stream or metadata changes, the adapter simply drops out and the
    robust consensus continues with the remaining venues.
    """

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.id_to_symbol: Dict[str, str] = {}
        self.pending_last: Dict[str, float] = {}
        self.quotes: Dict[str, Quote] = {}
        self.started = False
        self.thread = None
        self.last_error = ""
        self.last_message_ts = 0.0

    def start(self) -> None:
        if self.started:
            return
        self.started = True
        if websocket is None:
            self.last_error = "websocket-client is not installed"
            return
        self.thread = threading.Thread(target=self._run, name="edgex-public-ws", daemon=True)
        self.thread.start()

    def _run(self) -> None:
        while True:
            try:
                app = websocket.WebSocketApp(
                    EDGEX_WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=lambda _ws, err: setattr(self, "last_error", f"ws: {err}"),
                )
                app.run_forever(ping_interval=45, ping_timeout=15)
            except Exception as e:
                self.last_error = f"run: {type(e).__name__}: {e}"
            time.sleep(2.0)

    def _on_open(self, ws) -> None:
        for ch in ("metadata", "ticker.all.1s"):
            try:
                ws.send(json.dumps({"type": "subscribe", "channel": ch}))
            except Exception:
                pass

    @staticmethod
    def _walk_dicts(obj: Any):
        if isinstance(obj, dict):
            yield obj
            for v in obj.values():
                yield from EdgeXPublicStream._walk_dicts(v)
        elif isinstance(obj, list):
            for v in obj:
                yield from EdgeXPublicStream._walk_dicts(v)

    def _on_message(self, ws, message: str) -> None:
        try:
            obj = json.loads(message)
        except Exception:
            return
        self.last_message_ts = time.time()
        if isinstance(obj, dict) and obj.get("type") == "ping":
            try:
                ws.send(json.dumps({"type": "pong", "time": obj.get("time")}))
            except Exception:
                pass
            return

        changed_mapping = False
        with self.lock:
            for row in self._walk_dicts(obj):
                cid = row.get("contractId", row.get("contract_id"))
                name = row.get("contractName", row.get("contract_name", row.get("symbol")))
                if cid is not None and name:
                    symbol = normalize_perp_symbol(name)
                    if symbol:
                        self.id_to_symbol[str(cid)] = symbol
                        changed_mapping = True

                if cid is not None:
                    last = fnum(row.get("lastPrice", row.get("last_price", row.get("close"))))
                    if last > 0:
                        self.pending_last[str(cid)] = last
                elif name:
                    symbol = normalize_perp_symbol(name)
                    last = fnum(row.get("lastPrice", row.get("last_price", row.get("close"))))
                    if symbol and last > 0:
                        self.quotes[symbol] = Quote(last, last)

            if changed_mapping or self.pending_last:
                for cid, px in list(self.pending_last.items()):
                    symbol = self.id_to_symbol.get(cid)
                    if symbol and px > 0:
                        self.quotes[symbol] = Quote(px, px)

    def snapshot(self) -> Dict[str, Quote]:
        with self.lock:
            return dict(self.quotes)


_LIGHTER_STREAM = LighterPublicStream()
_EDGEX_STREAM = EdgeXPublicStream()
_HYPE_STATS: Dict[str, dict] = {}
_HYPE_NATIVE: Dict[str, str] = {}
_HYPE_BOOK_CACHE: Dict[str, Tuple[float, Quote]] = {}
_HYPE_TRADE_CACHE: Dict[str, Tuple[float, List[dict]]] = {}
_DYDX_CACHE: Tuple[float, Dict[str, Quote]] = (0.0, {})
_LIGHTER_BOOTSTRAP_STATS: Dict[str, dict] = {}
_STREAMS_STARTED = False


def start_v10_streams() -> None:
    global _STREAMS_STARTED
    if _STREAMS_STARTED:
        return
    _STREAMS_STARTED = True
    if env_bool("LIGHTER_ENABLED", "true"):
        _LIGHTER_STREAM.start()
    if env_bool("EDGEX_ENABLED", "true"):
        _EDGEX_STREAM.start()


def fetch_hyperliquid_meta_stats(cfg: Config) -> Tuple[Dict[str, Quote], Dict[str, dict], Dict[str, str]]:
    data = post_json(HYPERLIQUID_INFO_URL, cfg.request_timeout_seconds, {"type": "metaAndAssetCtxs"})
    out: Dict[str, Quote] = {}
    stats: Dict[str, dict] = {}
    native: Dict[str, str] = {}
    if not (isinstance(data, list) and len(data) >= 2 and isinstance(data[0], dict) and isinstance(data[1], list)):
        return out, stats, native
    universe = data[0].get("universe", [])
    ctxs = data[1]
    for asset, ctx in zip(universe if isinstance(universe, list) else [], ctxs):
        if not isinstance(asset, dict) or not isinstance(ctx, dict):
            continue
        coin = str(asset.get("name", "")).strip()
        symbol = normalize_perp_symbol(coin)
        if not symbol:
            continue
        mid = fnum(ctx.get("midPx")) or fnum(ctx.get("markPx"))
        if mid <= 0:
            continue
        prev = fnum(ctx.get("prevDayPx"))
        move = ((mid - prev) / prev * 100.0) if prev > 0 else 0.0
        out[symbol] = Quote(mid, mid)
        stats[symbol] = {
            "move24h_pct": move,
            "quote_volume24h": fnum(ctx.get("dayNtlVlm")),
            "native_symbol": coin,
        }
        native[symbol] = coin
    return out, stats, native


def fetch_hyperliquid_mids(cfg: Config) -> Dict[str, Quote]:
    data = post_json(HYPERLIQUID_INFO_URL, cfg.request_timeout_seconds, {"type": "allMids"})
    out: Dict[str, Quote] = {}
    if isinstance(data, dict):
        for coin, value in data.items():
            symbol = normalize_perp_symbol(coin)
            px = fnum(value)
            if symbol and px > 0:
                out[symbol] = Quote(px, px)
    return out


def fetch_hyperliquid_book(coin: str, cfg: Config) -> Optional[Quote]:
    if not coin:
        return None
    now = time.time()
    ttl = max(0.5, float(os.getenv("HYPERLIQUID_BOOK_POLL_SECONDS", "3.0")))
    cached = _HYPE_BOOK_CACHE.get(coin)
    if cached and now - cached[0] < ttl:
        return cached[1]
    data = post_json(HYPERLIQUID_INFO_URL, cfg.request_timeout_seconds, {"type": "l2Book", "coin": coin})
    if not isinstance(data, dict):
        return None
    levels = data.get("levels")
    if not (isinstance(levels, list) and len(levels) >= 2):
        return None
    bids = levels[0] if isinstance(levels[0], list) else []
    asks = levels[1] if isinstance(levels[1], list) else []
    if not bids or not asks:
        return None
    q = valid_quote(
        bids[0].get("px"), asks[0].get("px"), bids[0].get("sz"), asks[0].get("sz")
    ) if isinstance(bids[0], dict) and isinstance(asks[0], dict) else None
    if q:
        _HYPE_BOOK_CACHE[coin] = (now, q)
    return q


def fetch_hyperliquid_recent_trades(coin: str, cfg: Config) -> List[dict]:
    now = time.time()
    ttl = max(1.0, float(os.getenv("HYPERLIQUID_TRADES_POLL_SECONDS", "10")))
    cached = _HYPE_TRADE_CACHE.get(coin)
    if cached and now - cached[0] < ttl:
        return list(cached[1])
    data = post_json(HYPERLIQUID_INFO_URL, cfg.request_timeout_seconds, {"type": "recentTrades", "coin": coin})
    out: List[dict] = []
    for row in data if isinstance(data, list) else []:
        if not isinstance(row, dict):
            continue
        out.append({
            "id": row.get("tid", row.get("hash")),
            "time": fnum(row.get("time")),
            "price": fnum(row.get("px")),
            "qty": fnum(row.get("sz")),
            "isBuyerMaker": str(row.get("side", "")).upper() == "A",
        })
    _HYPE_TRADE_CACHE[coin] = (now, out)
    return list(out)


def fetch_lighter_bootstrap(cfg: Config) -> Tuple[Dict[str, Quote], Dict[str, dict]]:
    """One low-frequency REST bootstrap if the websocket has not warmed up yet."""
    global _LIGHTER_BOOTSTRAP_STATS
    data = get_json(
        f"{LIGHTER_BASE}/api/v1/orderBookDetails",
        cfg.request_timeout_seconds,
        params={"market_id": 255, "filter": "perp"},
    )
    rows: Any = data
    if isinstance(data, dict):
        for key in ("order_book_details", "orderBookDetails", "order_books", "orderBooks", "data"):
            if isinstance(data.get(key), list):
                rows = data[key]
                break
    if isinstance(rows, dict):
        rows = list(rows.values())
    quotes: Dict[str, Quote] = {}
    stats: Dict[str, dict] = {}
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        raw_symbol = row.get("symbol", row.get("market", row.get("name")))
        symbol = normalize_perp_symbol(raw_symbol)
        market_id = int(fnum(row.get("market_id", row.get("market_index", row.get("id"))), default=-1))
        if not symbol or market_id < 0:
            continue
        q = valid_quote(
            row.get("best_bid_price", row.get("best_bid")),
            row.get("best_ask_price", row.get("best_ask")),
            first_positive(row.get("best_bid_size"), row.get("best_bid_amount"), row.get("bid_size")),
            first_positive(row.get("best_ask_size"), row.get("best_ask_amount"), row.get("ask_size")),
        )
        if not q:
            px = (
                fnum(row.get("mid_price"))
                or fnum(row.get("last_trade_price"))
                or fnum(row.get("mark_price"))
                or fnum(row.get("index_price"))
            )
            if px > 0:
                q = Quote(px, px)
        if q:
            quotes[symbol] = q
        stats[symbol] = {
            "move24h_pct": fnum(row.get("daily_price_change")),
            "quote_volume24h": fnum(row.get("daily_quote_token_volume")),
            "market_id": market_id,
            "native_symbol": str(raw_symbol or ""),
            "mark_price": fnum(row.get("mark_price")),
            "mid_price": fnum(row.get("mid_price")),
            "last_trade_price": fnum(row.get("last_trade_price")),
            "funding_rate": fnum(row.get("funding_rate")),
            "current_funding_rate": fnum(row.get("current_funding_rate")),
            "funding_timestamp": fnum(row.get("funding_timestamp")),
        }
    _LIGHTER_BOOTSTRAP_STATS = stats
    return quotes, stats


def fetch_lighter_recent_trades_rest(market_id: int, cfg: Config) -> List[dict]:
    if market_id < 0:
        return []
    data = get_json(
        f"{LIGHTER_BASE}/api/v1/recentTrades",
        cfg.request_timeout_seconds,
        params={"market_id": market_id, "limit": min(100, max(1, cfg.aster_trade_limit))},
    )
    rows: Any = data
    if isinstance(data, dict):
        rows = data.get("trades", data.get("data", data.get("recent_trades", [])))
    if isinstance(rows, dict):
        rows = rows.get("trades", [])
    out: List[dict] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        ts = fnum(row.get("timestamp", row.get("time")))
        if 0 < ts < 1e12:
            ts *= 1000.0
        out.append({
            "id": row.get("trade_id_str", row.get("trade_id", row.get("id"))),
            "time": ts,
            "price": fnum(row.get("price")),
            "qty": fnum(row.get("size", row.get("qty"))),
            "isBuyerMaker": row.get("is_maker_ask", row.get("isBuyerMaker")),
        })
    return out


def fetch_dydx_reference(cfg: Config) -> Dict[str, Quote]:
    global _DYDX_CACHE
    now = time.time()
    ttl = max(1.0, float(os.getenv("AUX_REFERENCE_REFRESH_SECONDS", "5")))
    if now - _DYDX_CACHE[0] < ttl and _DYDX_CACHE[1]:
        return dict(_DYDX_CACHE[1])
    data = get_json(f"{DYDX_BASE}/perpetualMarkets", cfg.request_timeout_seconds)
    markets: Any = data
    if isinstance(data, dict):
        markets = data.get("markets", data.get("perpetualMarkets", data))
    rows: List[Tuple[str, dict]] = []
    if isinstance(markets, dict):
        rows = [(str(k), v) for k, v in markets.items() if isinstance(v, dict)]
    elif isinstance(markets, list):
        rows = [(str(v.get("ticker", v.get("market", ""))), v) for v in markets if isinstance(v, dict)]
    out: Dict[str, Quote] = {}
    for key, row in rows:
        ticker = row.get("ticker", row.get("market", key))
        symbol = normalize_perp_symbol(ticker)
        px = (
            fnum(row.get("oraclePrice"))
            or fnum(row.get("indexPrice"))
            or fnum(row.get("price"))
            or fnum(row.get("lastPrice"))
        )
        if symbol and px > 0:
            out[symbol] = Quote(px, px)
    _DYDX_CACHE = (now, out)
    return dict(out)


@dataclass
class V10Snapshot:
    target_books: Dict[str, Dict[str, Quote]]
    fair_refs: Dict[str, Dict[str, Quote]]
    exec_refs: Dict[str, Dict[str, Quote]]
    spot_refs: Dict[str, Dict[str, Quote]]
    target_stats: Dict[str, Dict[str, dict]]
    errors: List[str]


def fetch_v10_snapshot(cfg: Config, candidates: Optional[List[Candidate]] = None, initial: bool = False) -> V10Snapshot:
    global _HYPE_STATS, _HYPE_NATIVE
    funcs: Dict[str, Callable[[Config], Dict[str, Quote]]] = {
        "aster": fetch_aster_book,
        "bitget-perp": fetch_bitget_perp,
        "mexc-perp": fetch_mexc_perp,
        "bybit-perp": fetch_bybit_perp,
        "bitget-spot": fetch_bitget_spot,
        "mexc-spot": fetch_mexc_spot,
        "bybit-spot": fetch_bybit_spot,
        "dydx-perp": fetch_dydx_reference,
    }
    results: Dict[str, Dict[str, Quote]] = {}
    errors: List[str] = []

    # Hyperliquid: expensive meta+ctx only on initial bootstrap; cheap allMids thereafter.
    if env_bool("HYPERLIQUID_ENABLED", "true"):
        if initial or not _HYPE_STATS:
            try:
                hq, hs, hn = fetch_hyperliquid_meta_stats(cfg)
                results["hyperliquid-perp"] = hq
                _HYPE_STATS = hs
                _HYPE_NATIVE = hn
            except Exception as e:
                errors.append(f"hyperliquid-perp: {type(e).__name__}: {e}")
                results["hyperliquid-perp"] = {}
        else:
            funcs["hyperliquid-perp"] = fetch_hyperliquid_mids

    with ThreadPoolExecutor(max_workers=min(10, len(funcs))) as pool:
        futures = {pool.submit(fn, cfg): name for name, fn in funcs.items()}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                results[name] = fut.result()
            except Exception as e:
                errors.append(f"{name}: {type(e).__name__}: {e}")
                results[name] = {}

    lighter_quotes, lighter_stats = _LIGHTER_STREAM.snapshot() if env_bool("LIGHTER_ENABLED", "true") else ({}, {})
    if env_bool("LIGHTER_ENABLED", "true") and initial and (not lighter_quotes or not lighter_stats):
        try:
            boot_q, boot_s = fetch_lighter_bootstrap(cfg)
            if not lighter_quotes:
                lighter_quotes = boot_q
            if not lighter_stats:
                lighter_stats = boot_s
        except Exception as e:
            errors.append(f"lighter bootstrap: {type(e).__name__}: {e}")
    if lighter_quotes:
        results["lighter-perp"] = lighter_quotes
    elif env_bool("LIGHTER_ENABLED", "true") and _LIGHTER_STREAM.last_error:
        errors.append(f"lighter-perp: {_LIGHTER_STREAM.last_error}")

    edgex_quotes = _EDGEX_STREAM.snapshot() if env_bool("EDGEX_ENABLED", "true") else {}
    if edgex_quotes:
        results["edgex-perp"] = edgex_quotes
    elif env_bool("EDGEX_ENABLED", "true") and _EDGEX_STREAM.last_error:
        errors.append(f"edgex-perp: {_EDGEX_STREAM.last_error}")

    target_books: Dict[str, Dict[str, Quote]] = {
        "aster": results.get("aster", {}),
        "hyperliquid": dict(results.get("hyperliquid-perp", {})),  # synthetic mid until exact BBO below
        "lighter": dict(lighter_quotes),
    }

    # Exact Hyperliquid target BBO only for selected candidates; cached so 1.5s
    # scanner snapshots do not imply 1.5s l2Book requests.
    hype_candidates = [c for c in (candidates or []) if getattr(c, "target", "aster") == "hyperliquid"]
    if hype_candidates:
        def hype_worker(c: Candidate) -> Tuple[str, Optional[Quote]]:
            coin = str(getattr(c, "native_symbol", _HYPE_NATIVE.get(c.symbol, "")))
            return c.symbol, fetch_hyperliquid_book(coin, cfg)
        with ThreadPoolExecutor(max_workers=min(5, len(hype_candidates))) as pool:
            futures = {pool.submit(hype_worker, c): c for c in hype_candidates}
            for fut in as_completed(futures):
                c = futures[fut]
                try:
                    sym, q = fut.result()
                    if q:
                        target_books["hyperliquid"][sym] = q
                except Exception as e:
                    errors.append(f"hyperliquid book {c.symbol}: {type(e).__name__}: {e}")

    fair_refs = {
        name: market for name, market in results.items()
        if name in {
            "bitget-perp", "mexc-perp", "bybit-perp", "hyperliquid-perp",
            "lighter-perp", "edgex-perp", "dydx-perp",
        } and market
    }
    # Only real BBO sources belong in the executable LAG consensus. CEX tickers
    # and Lighter market_stats carry actual bid/ask; midpoint-only DEX references do not.
    exec_refs = {
        name: market for name, market in results.items()
        if name in {"bitget-perp", "mexc-perp", "bybit-perp", "lighter-perp"} and market
    }
    spot_refs = {
        name: market for name, market in results.items()
        if name in {"bitget-spot", "mexc-spot", "bybit-spot"} and market
    }
    target_stats = {
        "hyperliquid": dict(_HYPE_STATS),
        "lighter": lighter_stats,
        "aster": {},
    }
    return V10Snapshot(target_books, fair_refs, exec_refs, spot_refs, target_stats, errors)


def build_target_sample(c: Candidate, snap: V10Snapshot, cfg: Config) -> Optional[MarketSample]:
    target = getattr(c, "target", "aster")
    aq = snap.target_books.get(target, {}).get(c.symbol)
    if not aq:
        return None
    excluded = {f"{target}-perp"} if target in {"hyperliquid", "lighter"} else set()
    fair_ref = robust_reference_for_symbol(
        c.symbol,
        snap.fair_refs,
        cfg_min_fair_refs(cfg),
        cfg_max_fair_disagreement(cfg),
        excluded=excluded,
    )
    if not fair_ref:
        return None
    fair, selected_fair, fair_disagreement = fair_ref
    if fair <= 0 or aq.mid <= 0:
        return None
    ratio = aq.mid / fair
    max_ratio = max(1.01, cfg.max_cross_venue_price_ratio)
    if ratio > max_ratio or ratio < 1.0 / max_ratio:
        return None

    exec_ref = robust_reference_for_symbol(
        c.symbol,
        snap.exec_refs,
        cfg_min_exec_refs(cfg),
        cfg_max_exec_disagreement(cfg),
        excluded=excluded,
    )
    if exec_ref:
        _, selected_exec, _exec_dis = exec_ref
        external_bid = statistics.median([q.bid for q in selected_exec.values()])
        external_ask = statistics.median([q.ask for q in selected_exec.values()])
        best_bid_venue, best_bid_quote = max(selected_exec.items(), key=lambda kv: kv[1].bid)
        best_ask_venue, best_ask_quote = min(selected_exec.items(), key=lambda kv: kv[1].ask)
        best_external_bid = best_bid_quote.bid
        best_external_ask = best_ask_quote.ask
        bid_notionals = [q.bid_notional for q in selected_exec.values() if q.bid_notional > 0]
        ask_notionals = [q.ask_notional for q in selected_exec.values() if q.ask_notional > 0]
        external_bid_notional = statistics.median(bid_notionals) if bid_notionals else 0.0
        external_ask_notional = statistics.median(ask_notionals) if ask_notionals else 0.0
    else:
        selected_exec = {}
        external_bid = external_ask = 0.0
        external_bid_notional = external_ask_notional = 0.0
        best_bid_venue = best_ask_venue = ""
        best_external_bid = best_external_ask = 0.0

    spot_ref = robust_reference_for_symbol(
        c.symbol,
        snap.spot_refs,
        cfg.min_spot_reference_exchanges,
        cfg.max_spot_reference_disagreement_pct,
    )
    if spot_ref:
        spot_fair, selected_spots, spot_disagreement = spot_ref
    else:
        spot_fair, selected_spots, spot_disagreement = None, {}, None

    deviation = (aq.mid - fair) / fair * 100.0
    short_edge = (aq.bid - fair) / fair * 100.0
    long_edge = (fair - aq.ask) / fair * 100.0
    hedge_short = (aq.bid - external_ask) / external_ask * 100.0 if external_ask > 0 else -math.inf
    hedge_long = (external_bid - aq.ask) / external_bid * 100.0 if external_bid > 0 else -math.inf
    best_short = (aq.bid - best_external_ask) / best_external_ask * 100.0 if best_external_ask > 0 else -math.inf
    best_long = (best_external_bid - aq.ask) / best_external_bid * 100.0 if best_external_bid > 0 else -math.inf

    sample = MarketSample(
        ts=time.time(), aster=aq, fair=fair, refs=selected_fair,
        ref_disagreement_pct=fair_disagreement, deviation_pct=deviation,
        short_edge_pct=short_edge, long_edge_pct=long_edge,
        external_bid=external_bid, external_ask=external_ask,
        best_external_bid=best_external_bid, best_external_ask=best_external_ask,
        best_external_bid_venue=best_bid_venue, best_external_ask_venue=best_ask_venue,
        hedge_short_edge_pct=hedge_short, hedge_long_edge_pct=hedge_long,
        target_bid_notional=aq.bid_notional, target_ask_notional=aq.ask_notional,
        external_bid_notional=external_bid_notional, external_ask_notional=external_ask_notional,
        best_hedge_short_edge_pct=best_short, best_hedge_long_edge_pct=best_long,
        spot_fair=spot_fair, spot_refs=selected_spots,
        spot_ref_disagreement_pct=spot_disagreement,
    )
    sample.exec_refs = selected_exec
    return sample


def _new_candidate(target: str, symbol: str, stats: dict, sample: MarketSample, pre_score: float) -> Candidate:
    c = Candidate(
        symbol=symbol,
        move24h_pct=fnum(stats.get("move24h_pct")),
        quote_volume24h=fnum(stats.get("quote_volume24h")),
        pre_score=pre_score,
        samples=[sample],
    )
    c.target = target
    c.native_symbol = stats.get("native_symbol", symbol)
    c.native_id = stats.get("market_id", symbol)
    return c


def prefilter_v10_candidates(snap: V10Snapshot, aster_stats: Dict[str, dict], cfg: Config) -> List[Candidate]:
    all_candidates: List[Candidate] = []
    for target in cfg_target_venues():
        books = snap.target_books.get(target, {})
        stats_map = aster_stats if target == "aster" else snap.target_stats.get(target, {})
        ranked: List[Tuple[float, Candidate]] = []
        for symbol, aq in books.items():
            stats = stats_map.get(symbol, {})
            move = fnum(stats.get("move24h_pct"))
            qvol = fnum(stats.get("quote_volume24h"))
            if abs(move) < cfg.min_24h_move_pct or qvol < cfg.min_quote_volume24h:
                continue
            temp = Candidate(symbol, move, qvol)
            temp.target = target
            temp.native_symbol = stats.get("native_symbol", _HYPE_NATIVE.get(symbol, symbol))
            temp.native_id = stats.get("market_id", symbol)
            sample = build_target_sample(temp, snap, cfg)
            if not sample:
                continue
            spread = aq.spread_pct
            dev = abs(sample.deviation_pct)
            relative_edge = sample.best_relative_edge_pct
            hedge_edge = sample.best_hedgeable_edge_pct
            has_current_signal = not (
                spread < cfg.min_aster_spread_pct
                and dev < cfg.min_deviation_pct
                and relative_edge < cfg.min_executable_edge_pct
                and hedge_edge < cfg.min_hedgeable_edge_pct
            )
            # Hyperliquid initial allMids is midpoint-only. Seed a few hot/high-volume
            # names even when the first midpoint snapshot is quiet, then Stage 2 uses
            # exact l2Book BBO and real trades.
            if not has_current_signal and target != "hyperliquid":
                continue
            pre_score = (
                spread / max(cfg.min_aster_spread_pct, 1e-9)
                + dev / max(cfg.min_deviation_pct, 1e-9)
                + relative_edge / max(cfg.min_executable_edge_pct, 1e-9)
                + hedge_edge / max(cfg.min_hedgeable_edge_pct, 1e-9)
                + min(abs(move) / max(cfg.min_24h_move_pct, 1e-9), 4.0) * 0.25
                + min(math.log10(max(qvol, 1.0)) / 10.0, 1.0) * 0.2
            )
            ranked.append((pre_score, _new_candidate(target, symbol, stats, sample, pre_score)))
        ranked.sort(key=lambda x: x[0], reverse=True)
        selected = [c for _, c in ranked[: cfg_target_cap(target, cfg)]]
        all_candidates.extend(selected)

    # Subscribe to Lighter trades as soon as Stage-1 decides which Lighter markets
    # are worth observing, so Stage-2 gets a full 45-second live trade window.
    for c in all_candidates:
        if getattr(c, "target", "aster") == "lighter":
            market_id = int(fnum(getattr(c, "native_id", -1), default=-1))
            _LIGHTER_STREAM.subscribe_trades(market_id)
            _LIGHTER_STREAM.subscribe_order_book(market_id)
    if any(getattr(c, "target", "aster") == "lighter" for c in all_candidates):
        time.sleep(max(0.0, cfg.lighter_book_warmup_seconds))
    # Global safety cap, while preserving each target's local ranking order.
    return all_candidates[: max(1, cfg.max_candidates)]


def collect_confirmation_samples_v10(candidates: List[Candidate], cfg: Config, errors: List[str]) -> None:
    if not candidates:
        return
    start = time.time()
    end = start + max(0.0, cfg.confirm_duration_seconds)
    n = 1
    while time.time() < end:
        time.sleep(max(0.1, cfg.confirm_interval_seconds))
        n += 1
        try:
            snap = fetch_v10_snapshot(cfg, candidates=candidates, initial=False)
            errors.extend(snap.errors)
            for c in candidates:
                sample = build_target_sample(c, snap, cfg)
                if sample:
                    c.samples.append(sample)
        except Exception as e:
            errors.append(f"confirmation sample {n}: {type(e).__name__}: {e}")


def fetch_target_recent_trades(c: Candidate, cfg: Config, include_rest_lighter: bool = False) -> List[dict]:
    target = getattr(c, "target", "aster")
    if target == "aster":
        return fetch_aster_recent_trades(c.symbol, cfg)
    if target == "hyperliquid":
        coin = str(getattr(c, "native_symbol", _HYPE_NATIVE.get(c.symbol, "")))
        return fetch_hyperliquid_recent_trades(coin, cfg)
    if target == "lighter":
        market_id = int(fnum(getattr(c, "native_id", -1), default=-1))
        rows = _LIGHTER_STREAM.recent_trades(market_id)
        if include_rest_lighter:
            try:
                rows = fetch_lighter_recent_trades_rest(market_id, cfg) + rows
            except Exception:
                pass
        # De-duplicate REST snapshot + WS buffer.
        dedup = {raw_trade_key(r): r for r in rows if isinstance(r, dict)}
        return list(dedup.values())
    return []


def add_trade_analysis_v10(candidates: List[Candidate], cfg: Config, errors: List[str]) -> None:
    provisional = []
    for c in candidates:
        provisional.append((c.metrics(cfg).get("score", 0.0), c))
    provisional.sort(key=lambda x: x[0], reverse=True)
    selected = [c for _, c in provisional[: max(1, cfg.max_trade_analysis_candidates)]]

    def worker(c: Candidate) -> Tuple[str, List[dict]]:
        return candidate_state_key(c), fetch_target_recent_trades(c, cfg, include_rest_lighter=True)

    with ThreadPoolExecutor(max_workers=min(8, len(selected))) as pool:
        futures = {pool.submit(worker, c): c for c in selected}
        for fut in as_completed(futures):
            c = futures[fut]
            try:
                _key, rows = fut.result()
                c.trades = map_trades_to_fair(c, rows, cfg)
                c.excursions = detect_excursions(c.trades, cfg)
            except Exception as e:
                errors.append(f"{target_label(c)} trades {c.symbol}: {type(e).__name__}: {e}")


def _state_rows(state: dict, bucket: str, key: str) -> List[dict]:
    rows = state.get(bucket, {}).get(key, [])
    if isinstance(rows, list) and rows:
        return rows
    # V9 -> V10 migration: old unqualified rows belong to Aster only.
    if key.startswith("aster:"):
        legacy = key.split(":", 1)[1]
        rows = state.get(bucket, {}).get(legacy, [])
        if isinstance(rows, list):
            return rows
    return []


def lag_baseline_values(state: dict, symbol: str, side: str, cfg: Config, now: Optional[float] = None) -> List[float]:
    now = time.time() if now is None else now
    cutoff = now - max(1, cfg.lag_baseline_lookback_minutes) * 60
    values: List[float] = []
    for row in _state_rows(state, "lag_baseline", symbol):
        if not isinstance(row, dict):
            continue
        if str(row.get("side", "")).upper() != side:
            continue
        if fnum(row.get("ts")) < cutoff:
            continue
        edge = fnum(row.get("edge_pct"), default=-1.0)
        if edge >= 0:
            values.append(edge)
    return values[-cfg.lag_baseline_max_points :]


def lag_verified_profile_rows(state: dict, symbol: str, side: str, cfg: Config, now: Optional[float] = None) -> List[dict]:
    now = time.time() if now is None else now
    cutoff = now - max(1, cfg.active_lag_profile_lookback_hours) * 3600
    out: List[dict] = []
    for row in _state_rows(state, "lag_verified_profiles", symbol):
        if not isinstance(row, dict):
            continue
        if str(row.get("side", "")).upper() != side:
            continue
        if fnum(row.get("ts")) < cutoff:
            continue
        if fnum(row.get("max_convergence_fraction")) < cfg.lag_min_convergence_fraction:
            continue
        # V12 does not trust legacy verified rows created before invalid-BBO
        # coverage was enforced; this prevents a V10 -inf artifact from seeding ACTIVE-LAG.
        if int(fnum(row.get("version"))) < 11:
            continue
        if fnum(row.get("bbo_coverage_ratio")) < cfg.lag_min_valid_bbo_coverage:
            continue
        out.append(row)
    return out[-cfg.active_lag_profile_max_points :]


def record_signal_stats(state: dict, event_type: str, symbol: str, count: int = 1, side_counts: Optional[dict] = None) -> None:
    stats = state.setdefault("signal_stats", {})
    bucket = stats.setdefault(event_type, {"total": 0, "symbols": {}, "sides": {}, "last_ts": 0})
    bucket["total"] = int(fnum(bucket.get("total"))) + max(0, int(count))
    symbols = bucket.setdefault("symbols", {})
    symbols[symbol] = int(fnum(symbols.get(symbol))) + max(0, int(count))
    sides = bucket.setdefault("sides", {})
    for side, n in (side_counts or {}).items():
        sides[side] = int(fnum(sides.get(side))) + max(0, int(n))
    bucket["last_ts"] = int(time.time())


def detect_active_lag_events(candidates: List[Candidate], cfg: Config, state: dict) -> Tuple[List[dict], bool]:
    if not cfg.active_lag_enabled:
        return [], False
    open_state = state.setdefault("active_signal_open", {})
    prev_rows = open_state.get("ACTIVE-LAG", [])
    prev_open = set(str(x) for x in prev_rows) if isinstance(prev_rows, list) else set()
    current_open: set[str] = set()
    events: List[dict] = []

    for c in candidates:
        m = c.metrics(cfg)
        side = str(m.get("persistent_edge_side", "NONE"))
        if side not in {"LONG", "SHORT"}:
            continue
        skey = candidate_state_key(c)
        profile_rows = lag_verified_profile_rows(state, skey, side, cfg)
        profile = summarize_lag_profile(profile_rows)
        baseline_values = lag_baseline_values(state, skey, side, cfg)
        baseline_median = statistics.median(baseline_values) if baseline_values else None
        baseline_ready = len(baseline_values) >= cfg.lag_baseline_min_points
        baseline_required = None
        if baseline_median is not None:
            baseline_required = max(
                baseline_median + cfg.lag_baseline_min_excess_pct,
                baseline_median * cfg.lag_baseline_min_ratio,
            )
        baseline_ok = (not baseline_ready) or baseline_required is None or (
            float(m.get("current_executable_edge_pct", 0.0)) >= baseline_required
        )
        active = (
            bool(m.get("raw_lag_confirmed"))
            and profile["verified_episodes"] >= cfg.active_lag_min_verified_episodes
            and float(m.get("current_executable_edge_pct", 0.0)) >= cfg.active_lag_min_gross_edge_pct
            and float(m.get("current_net_edge_pct", -999.0)) >= cfg.active_lag_min_net_edge_pct
            and float(m.get("persistent_exec_hit_ratio", 0.0)) >= cfg.active_lag_min_hit_ratio
            and float(m.get("current_ref_disagreement_pct", 999.0)) <= cfg.active_lag_max_reference_disagreement_pct
            and baseline_ok
        )
        if not active:
            continue
        latch = f"{skey}:{side}"
        current_open.add(latch)
        c.active_lag_detected = True
        c.active_lag_detection_ts = c.samples[-1].ts if c.samples else time.time()
        c.active_lag_side = side
        c.active_lag_edge_pct = float(m.get("current_executable_edge_pct", 0.0))
        c.active_lag_net_edge_pct = float(m.get("current_net_edge_pct", 0.0))
        c.active_lag_verified_episodes = int(profile["verified_episodes"])
        if latch in prev_open:
            continue
        event = {
            "type": "ACTIVE-LAG", "symbol": c.symbol, "state_key": skey,
            "target": getattr(c, "target", "aster"), "target_label": target_label(c),
            "ts": c.active_lag_detection_ts, "side": side,
            "gross_edge_pct": c.active_lag_edge_pct, "net_edge_pct": c.active_lag_net_edge_pct,
            "target_bid": float(m.get("current_target_bid", 0.0)),
            "target_ask": float(m.get("current_target_ask", 0.0)),
            "external_bid": float(m.get("current_external_bid", 0.0)),
            "external_ask": float(m.get("current_external_ask", 0.0)),
            "persistent_hit_ratio": float(m.get("persistent_exec_hit_ratio", 0.0)),
            "persistent_median_edge_pct": float(m.get("persistent_median_executable_edge_pct", 0.0)),
            "reference_disagreement_pct": float(m.get("current_ref_disagreement_pct", 0.0)),
            "baseline_points": len(baseline_values),
            "baseline_median_gap_pct": baseline_median,
            "baseline_required_gap_pct": baseline_required,
            **profile,
        }
        events.append(event)
        record_signal_stats(state, "ACTIVE-LAG", skey, 1, {side: 1})

    new_open = sorted(current_open)
    changed = set(prev_open) != set(new_open) or bool(events)
    open_state["ACTIVE-LAG"] = new_open
    return events, changed


def extended_regime_observation(
    candidates: List[Candidate], cfg: Config, errors: List[str], state: dict,
    active_signal_callback: Optional[Callable[[dict], None]] = None,
) -> bool:
    if not candidates:
        return False
    initial_mm: List[Tuple[float, Candidate, dict]] = []
    initial_lag: List[Tuple[float, Candidate, dict]] = []
    for c in candidates:
        m = c.metrics(cfg)
        c.run_baseline_side = str(m.get("persistent_edge_side", "NONE"))
        c.run_baseline_edge_pct = max(0.0, float(m.get("persistent_median_executable_edge_pct", 0.0)))
        if m.get("setup") in {"CONFIRMED-MM", "CONFIRMED-BOTH"}:
            rank = float(m.get("mm_score", 0)) + min(20.0, float(m.get("excursion_count", 0)) * 1.5) + float(m.get("clean_reversion_rate", 0)) * 10
            initial_mm.append((rank, c, m))
        if bool(m.get("raw_lag_confirmed")):
            c.lag_verification_status = "not_selected"
            rank = float(m.get("lag_score", 0)) + float(m.get("persistent_exec_hit_ratio", 0)) * 15 + min(15.0, max(0.0, float(m.get("persistent_median_net_edge_pct", 0))) * 30)
            initial_lag.append((rank, c, m))
    initial_mm.sort(key=lambda x: x[0], reverse=True)
    initial_lag.sort(key=lambda x: x[0], reverse=True)
    selected_mm = [c for _, c, _ in initial_mm[: max(0, cfg.extended_mm_max_candidates)]] if cfg.extended_mm_enabled else []
    lag_rows = initial_lag[: max(0, cfg.extended_lag_max_candidates)] if cfg.extended_lag_enabled else []
    selected_lag = [c for _, c, _ in lag_rows]
    for _, c, m in lag_rows:
        c.lag_verification_status = "selected"
        c.lag_initial_confirmed = True
        c.lag_detection_sample_index = max(0, len(c.samples) - 1)
        c.lag_detection_ts = c.samples[c.lag_detection_sample_index].ts if c.samples else time.time()
        c.lag_detection_side = str(m.get("persistent_edge_side", "NONE"))
        c.lag_initial_edge_pct = max(0.0, float(m.get("current_executable_edge_pct", 0.0)))
        c.lag_initial_score = float(m.get("lag_score", 0.0))
        c.lag_baseline_edges_pct = lag_baseline_values(state, candidate_state_key(c), c.lag_detection_side, cfg)

    selected: List[Candidate] = []
    seen: set[str] = set()
    for c in selected_mm + selected_lag:
        key = candidate_state_key(c)
        if key not in seen:
            seen.add(key)
            selected.append(c)
    if not selected:
        return False
    duration = max(cfg.extended_mm_duration_seconds if selected_mm else 0.0, cfg.extended_lag_duration_seconds if selected_lag else 0.0)
    interval = min(cfg.extended_mm_interval_seconds if selected_mm else 999.0, cfg.extended_lag_interval_seconds if selected_lag else 999.0)
    interval = max(0.25, interval if interval < 999 else 1.5)
    labels = []
    if selected_mm:
        labels.append("MM=" + ",".join(candidate_display_name(c) for c in selected_mm))
    if selected_lag:
        labels.append("LAG=" + ",".join(candidate_display_name(c) for c in selected_lag))
    print(f"Extended regime verification ({'; '.join(labels)}) for ~{duration:.0f}s every {interval:.1f}s")

    raw_by_key: Dict[str, Dict[tuple, dict]] = {candidate_state_key(c): {} for c in selected_mm}
    initial_mm_context = {candidate_state_key(c): m for _, c, m in initial_mm}
    active_seen = {candidate_state_key(c): set() for c in selected_mm}
    active_pending = {candidate_state_key(c): [] for c in selected_mm}
    active_last_emit = {candidate_state_key(c): 0.0 for c in selected_mm}
    state_changed = False

    def poll_mm_trades() -> None:
        if not selected_mm:
            return
        def worker(c: Candidate) -> Tuple[str, List[dict]]:
            return candidate_state_key(c), fetch_target_recent_trades(c, cfg, include_rest_lighter=False)
        with ThreadPoolExecutor(max_workers=min(6, len(selected_mm))) as pool:
            futures = {pool.submit(worker, c): c for c in selected_mm}
            for fut in as_completed(futures):
                c = futures[fut]
                try:
                    key, rows = fut.result()
                    store = raw_by_key[key]
                    for row in rows:
                        if isinstance(row, dict):
                            store[raw_trade_key(row)] = row
                except Exception as e:
                    errors.append(f"extended trades {candidate_display_name(c)}: {type(e).__name__}: {e}")

    poll_mm_trades()
    start = time.time()
    end = start + max(0.0, duration)

    def process_active_mm_events(force_emit: bool = False) -> None:
        nonlocal state_changed
        if not (cfg.active_mm_excursion_enabled and selected_mm):
            return
        now_ts = time.time()
        for c in selected_mm:
            skey = candidate_state_key(c)
            c.trades = map_trades_to_fair(c, list(raw_by_key[skey].values()), cfg)
            c.excursions = detect_excursions(c.trades, cfg)
            context = initial_mm_context.get(skey, {})
            if int(context.get("excursion_count", 0)) < cfg.active_mm_min_prior_excursions or float(context.get("clean_reversion_rate", 0)) < cfg.active_mm_min_clean_reversion_rate:
                continue
            window_start = start - max(0.0, cfg.active_mm_initial_lookback_seconds)
            new_events = []
            for exc in c.excursions:
                if exc.start_ts < window_start or exc.peak_abs_deviation_pct < cfg.active_mm_min_excursion_pct:
                    continue
                ekey = (round(exc.start_ts, 3), exc.direction)
                if ekey in active_seen[skey]:
                    continue
                sample = nearest_sample(c.samples, exc.start_ts, cfg.trade_sample_match_tolerance_seconds)
                if sample is None or sample.ref_disagreement_pct > cfg.active_mm_max_reference_disagreement_pct:
                    continue
                active_seen[skey].add(ekey)
                action = "SHORT" if exc.direction == "ABOVE" else "LONG"
                taker_edge = sample.hedge_short_edge_pct if action == "SHORT" else sample.hedge_long_edge_pct
                maker_edge = (
                    (sample.aster.ask - sample.external_ask) / sample.external_ask * 100.0
                    if action == "SHORT" and sample.external_ask > 0
                    else (sample.external_bid - sample.aster.bid) / sample.external_bid * 100.0
                    if action == "LONG" and sample.external_bid > 0
                    else math.nan
                )
                target_cap = sample.target_bid_notional if action == "SHORT" else sample.target_ask_notional
                ext_cap = sample.external_ask_notional if action == "SHORT" else sample.external_bid_notional
                ev = {
                    "start_ts": exc.start_ts, "direction": exc.direction, "action": action,
                    "peak_pct": exc.peak_abs_deviation_pct, "reverted": exc.reverted,
                    "clean_reversion": exc.clean_reversion, "reversion_seconds": exc.reversion_seconds,
                    "reference_disagreement_pct": sample.ref_disagreement_pct,
                    "taker_edge_pct": taker_edge if math.isfinite(taker_edge) else None,
                    "maker_quote_edge_pct": maker_edge if math.isfinite(maker_edge) else None,
                    "target_bbo_capacity_usd": target_cap, "external_bbo_capacity_usd": ext_cap,
                }
                new_events.append(ev)
                state_changed = start_or_update_mm_outcome(state, c, exc, ev, cfg) or state_changed
                c.active_mm_excursion_count += 1
                if action == "LONG": c.active_mm_long_count += 1
                else: c.active_mm_short_count += 1
                c.active_mm_peak_values_pct.append(exc.peak_abs_deviation_pct)
                c.active_mm_last_event_ts = max(c.active_mm_last_event_ts or exc.start_ts, exc.start_ts)
            # Refresh outcomes for excursions that were detected in earlier polls;
            # this is how OPEN events become CLEAN_REVERTED / REVERTED_NOT_CLEAN.
            for exc in c.excursions:
                eid = mm_outcome_id(skey, exc)
                if eid in state.setdefault("active_mm_outcomes", {}):
                    dummy = {"action": "SHORT" if exc.direction == "ABOVE" else "LONG", "taker_edge_pct": None, "maker_quote_edge_pct": None}
                    state_changed = start_or_update_mm_outcome(state, c, exc, dummy, cfg) or state_changed
            if new_events:
                active_pending[skey].extend(new_events)
                side_counts = {"LONG": sum(e["action"] == "LONG" for e in new_events), "SHORT": sum(e["action"] == "SHORT" for e in new_events)}
                record_signal_stats(state, "ACTIVE-MM-EXCURSION", skey, len(new_events), side_counts)
                state_changed = True
            pending = active_pending[skey]
            elapsed = now_ts - active_last_emit[skey] if active_last_emit[skey] > 0 else math.inf
            if pending and (force_emit or elapsed >= cfg.active_mm_alert_batch_seconds):
                peaks = [float(e["peak_pct"]) for e in pending]
                latest = max(pending, key=lambda e: float(e["start_ts"]))
                payload = {
                    "type": "ACTIVE-MM-EXCURSION", "symbol": c.symbol, "state_key": skey,
                    "target": getattr(c, "target", "aster"), "target_label": target_label(c),
                    "ts": now_ts, "events": list(pending), "event_count": len(pending),
                    "long_count": sum(e["action"] == "LONG" for e in pending),
                    "short_count": sum(e["action"] == "SHORT" for e in pending),
                    "median_peak_pct": statistics.median(peaks) if peaks else 0.0,
                    "max_peak_pct": max(peaks) if peaks else 0.0,
                    "latest_action": latest["action"], "latest_direction": latest["direction"],
                    "latest_event_age_seconds": max(0.0, now_ts - float(latest["start_ts"])),
                    "latest_reverted": bool(latest["reverted"]),
                    "regime_excursions_before_extended": int(context.get("excursion_count", 0)),
                    "regime_clean_reversion_rate": float(context.get("clean_reversion_rate", 0.0)),
                    "regime_median_reversion_seconds": context.get("median_clean_reversion_seconds"),
                }
                if active_signal_callback is not None:
                    active_signal_callback(payload)
                pending.clear()
                active_last_emit[skey] = now_ts

    next_snapshot = start
    next_trade_poll = start + max(0.5, cfg.extended_mm_trades_poll_seconds)
    sample_no = 0
    process_active_mm_events(False)
    while time.time() < end:
        now = time.time(); did_work = False
        if now >= next_snapshot:
            sample_no += 1
            try:
                snap = fetch_v10_snapshot(cfg, candidates=selected, initial=False)
                errors.extend(snap.errors)
                elapsed = time.time() - start
                for c in selected:
                    wants_mm = c in selected_mm and elapsed <= cfg.extended_mm_duration_seconds + interval
                    wants_lag = c in selected_lag and elapsed <= cfg.extended_lag_duration_seconds + interval
                    if wants_mm or wants_lag:
                        s = build_target_sample(c, snap, cfg)
                        if s:
                            c.samples.append(s)
            except Exception as e:
                errors.append(f"extended snapshot {sample_no}: {type(e).__name__}: {e}")
            next_snapshot = time.time() + interval; did_work = True
        now = time.time()
        if selected_mm and now >= next_trade_poll:
            poll_mm_trades(); process_active_mm_events(False)
            next_trade_poll = time.time() + max(1.0, cfg.extended_mm_trades_poll_seconds); did_work = True
        if not did_work:
            targets = [next_snapshot, end] + ([next_trade_poll] if selected_mm else [])
            sleep_for = min(targets) - time.time()
            if sleep_for > 0: time.sleep(min(0.25, sleep_for))

    poll_mm_trades(); process_active_mm_events(True)
    for c in selected_mm:
        skey = candidate_state_key(c)
        c.trades = map_trades_to_fair(c, list(raw_by_key[skey].values()), cfg)
        c.excursions = detect_excursions(c.trades, cfg)
    for c in selected_lag:
        c.lag_verification_status = "done"
    for c in selected:
        m = c.metrics(cfg)
        if c in selected_mm:
            print(f"Extended MM {candidate_display_name(c)}: observed={m.get('observed_seconds',0):.0f}s, excursions={m.get('excursion_count',0)}, clean={m.get('clean_reversion_rate',0)*100:.0f}%, setup={m.get('setup')}")
        if c in selected_lag:
            base = m.get("lag_baseline_median_gap_pct")
            base_txt = "bootstrap" if base is None else f"{base:.3f}%/{m.get('lag_baseline_points',0)}pts"
            print(f"Extended LAG {candidate_display_name(c)}: side={m.get('lag_detection_side')}, initial={m.get('lag_initial_gap_pct',0):.3f}%, min={m.get('lag_min_gap_pct',0):.3f}%, convergence={m.get('lag_max_convergence_fraction',0)*100:.0f}%, events={m.get('lag_convergence_events',0)}, baseline={base_txt}, setup={m.get('setup')}")
    return state_changed




# ------------------------------ V13 long-horizon LAG outcomes + funding ------------------------------

_FUNDING_HISTORY_CACHE: Dict[Tuple[str, str, int, int], Tuple[List[dict], str]] = {}


def _side_funding_pnl_pct(side: str, settled_rates_pct: List[float]) -> float:
    """Hypothetical funding PnL as % of one-leg notional.

    Positive funding means LONG pays SHORT. Therefore a SHORT earns +rate and a
    LONG pays -rate. Negative rates naturally reverse the sign.
    """
    sign = 1.0 if str(side).upper() == "SHORT" else -1.0
    return sign * sum(float(x) for x in settled_rates_pct)


def _hl_coin_from_symbol(symbol: str) -> str:
    symbol = str(symbol or "").upper()
    return symbol[:-4] if symbol.endswith("USDT") else symbol


def _funding_rows_cached(venue: str, symbol: str, start_ts: float, end_ts: float, cfg: Config) -> Tuple[List[dict], str]:
    """Return already-settled public funding rates in percentage points.

    Each row is {ts, rate_pct}. This is hypothetical carry for a fixed-notional
    pair; no account/private API is used.
    """
    if end_ts <= start_ts or not cfg.funding_tracking_enabled:
        return [], "disabled" if not cfg.funding_tracking_enabled else ""
    v = str(venue or "").lower()
    key = (v, str(symbol), int(start_ts // 300), int(end_ts // 300))
    cached = _FUNDING_HISTORY_CACHE.get(key)
    if cached is not None:
        return cached
    rows: List[dict] = []
    err = ""
    start_ms = int(start_ts * 1000)
    end_ms = int(end_ts * 1000)
    try:
        if v in {"aster", "aster-perp"}:
            data = get_json(
                f"{ASTER_BASE}/fapi/v3/fundingRate", cfg.request_timeout_seconds,
                params={"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 1000},
            )
            if not isinstance(data, list):
                # Best-effort compatibility fallback.
                data = get_json(
                    f"{ASTER_BASE}/fapi/v1/fundingRate", cfg.request_timeout_seconds,
                    params={"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 1000},
                )
            for r in data if isinstance(data, list) else []:
                ts = fnum(r.get("fundingTime")) / 1000.0
                rate = fnum(r.get("fundingRate")) * 100.0
                if start_ts < ts <= end_ts:
                    rows.append({"ts": ts, "rate_pct": rate})

        elif v in {"hyperliquid", "hyperliquid-perp"}:
            coin = _hl_coin_from_symbol(symbol)
            data = post_json(HYPERLIQUID_INFO_URL, cfg.request_timeout_seconds, {
                "type": "fundingHistory", "coin": coin, "startTime": start_ms, "endTime": end_ms,
            })
            for r in data if isinstance(data, list) else []:
                ts_raw = fnum(r.get("time", r.get("fundingTime", r.get("timestamp"))))
                ts = ts_raw / 1000.0 if ts_raw > 1e12 else ts_raw
                rate = fnum(r.get("fundingRate", r.get("rate"))) * 100.0
                if start_ts < ts <= end_ts:
                    rows.append({"ts": ts, "rate_pct": rate})

        elif v in {"bitget", "bitget-perp"}:
            data = get_json(
                f"{BITGET_BASE}/api/v2/mix/market/history-fund-rate", cfg.request_timeout_seconds,
                params={"symbol": symbol, "productType": "USDT-FUTURES", "pageSize": 100, "pageNo": 1},
            )
            d = data.get("data", []) if isinstance(data, dict) else []
            if isinstance(d, dict):
                d = d.get("resultList", [])
            for r in d if isinstance(d, list) else []:
                ts_raw = fnum(r.get("fundingTime", r.get("fundingRateTimestamp")))
                ts = ts_raw / 1000.0 if ts_raw > 1e12 else ts_raw
                rate = fnum(r.get("fundingRate")) * 100.0
                if start_ts < ts <= end_ts:
                    rows.append({"ts": ts, "rate_pct": rate})

        elif v in {"mexc", "mexc-perp"}:
            native = symbol[:-4] + "_USDT" if str(symbol).upper().endswith("USDT") else symbol
            data = get_json(
                f"{MEXC_CONTRACT_BASE}/api/v1/contract/funding_rate/history", cfg.request_timeout_seconds,
                params={"symbol": native, "page_num": 1, "page_size": 1000},
            )
            d = (data or {}).get("data", {}) if isinstance(data, dict) else {}
            rs = d.get("resultList", []) if isinstance(d, dict) else []
            for r in rs if isinstance(rs, list) else []:
                ts_raw = fnum(r.get("settleTime"))
                ts = ts_raw / 1000.0 if ts_raw > 1e12 else ts_raw
                rate = fnum(r.get("fundingRate")) * 100.0
                if start_ts < ts <= end_ts:
                    rows.append({"ts": ts, "rate_pct": rate})

        elif v in {"bybit", "bybit-perp"}:
            data = get_json(
                f"{BYBIT_BASE}/v5/market/funding/history", cfg.request_timeout_seconds,
                params={"category": "linear", "symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 200},
            )
            rs = ((data or {}).get("result", {}) or {}).get("list", []) if isinstance(data, dict) else []
            for r in rs if isinstance(rs, list) else []:
                ts_raw = fnum(r.get("fundingRateTimestamp"))
                ts = ts_raw / 1000.0 if ts_raw > 1e12 else ts_raw
                rate = fnum(r.get("fundingRate")) * 100.0
                if start_ts < ts <= end_ts:
                    rows.append({"ts": ts, "rate_pct": rate})

        elif v in {"lighter", "lighter-perp"}:
            # Lighter's public market_stats stream exposes the LAST settled
            # funding payment and its timestamp. Since the scanner runs every
            # few minutes and Lighter funding is hourly, persisting unique
            # settlement timestamps captures the settled carry across runs.
            _quotes, stats = _LIGHTER_STREAM.snapshot()
            st = stats.get(symbol, {}) if isinstance(stats, dict) else {}
            ts_raw = fnum(st.get("funding_timestamp")) if isinstance(st, dict) else 0.0
            ts = ts_raw / 1000.0 if ts_raw > 1e12 else ts_raw
            rate = fnum(st.get("funding_rate")) * 100.0 if isinstance(st, dict) else 0.0
            if start_ts < ts <= end_ts:
                rows.append({"ts": ts, "rate_pct": rate})
        else:
            err = f"unsupported venue {venue}"
    except Exception as e:
        err = f"{type(e).__name__}: {e}"

    dedup: Dict[int, dict] = {}
    for r in rows:
        ts = fnum(r.get("ts"))
        if ts > 0:
            dedup[int(ts * 1000)] = {"ts": ts, "rate_pct": fnum(r.get("rate_pct"))}
    out = sorted(dedup.values(), key=lambda x: x["ts"])
    _FUNDING_HISTORY_CACHE[key] = (out, err)
    return out, err


def _refresh_lag_outcome_funding(row: dict, cfg: Config, end_ts: float) -> bool:
    if not cfg.funding_tracking_enabled:
        row["funding_tracking_status"] = "DISABLED"
        return False
    start_ts = fnum(row.get("start_ts"))
    end_ts = max(start_ts, end_ts)
    target_venue = str(row.get("target") or "aster")
    hedge_venue = str(row.get("hedge_venue") or "")
    side = str(row.get("side") or "NONE").upper()
    hedge_side = "LONG" if side == "SHORT" else "SHORT" if side == "LONG" else "NONE"
    fund = row.setdefault("funding", {})
    target_map = fund.setdefault("target_settlements", {})
    hedge_map = fund.setdefault("hedge_settlements", {})
    changed = False

    target_rows, target_err = _funding_rows_cached(target_venue, str(row.get("symbol")), start_ts, end_ts, cfg)
    for r in target_rows:
        k = str(int(fnum(r.get("ts")) * 1000))
        if k not in target_map:
            target_map[k] = fnum(r.get("rate_pct")); changed = True

    hedge_err = "missing hedge venue"
    if hedge_venue:
        hedge_rows, hedge_err = _funding_rows_cached(hedge_venue, str(row.get("symbol")), start_ts, end_ts, cfg)
        for r in hedge_rows:
            k = str(int(fnum(r.get("ts")) * 1000))
            if k not in hedge_map:
                hedge_map[k] = fnum(r.get("rate_pct")); changed = True

    target_rates = [fnum(x) for x in target_map.values()]
    hedge_rates = [fnum(x) for x in hedge_map.values()]
    target_pnl = _side_funding_pnl_pct(side, target_rates) if side in {"LONG", "SHORT"} else 0.0
    hedge_pnl = _side_funding_pnl_pct(hedge_side, hedge_rates) if hedge_side in {"LONG", "SHORT"} else 0.0
    net_pnl = target_pnl + hedge_pnl
    fund.update({
        "target_venue": target_venue, "hedge_venue": hedge_venue,
        "target_side": side, "hedge_side": hedge_side,
        "target_pnl_pct": target_pnl, "hedge_pnl_pct": hedge_pnl,
        "net_pnl_pct": net_pnl,
        "target_settlement_count": len(target_rates), "hedge_settlement_count": len(hedge_rates),
        "target_error": target_err, "hedge_error": hedge_err,
        "updated_ts": end_ts,
    })
    row["funding_target_pnl_pct"] = target_pnl
    row["funding_hedge_pnl_pct"] = hedge_pnl
    row["funding_net_pnl_pct"] = net_pnl
    row["funding_target_settlements"] = len(target_rates)
    row["funding_hedge_settlements"] = len(hedge_rates)
    row["funding_tracking_status"] = "READY" if hedge_venue and not target_err and not hedge_err else "PARTIAL"
    row["funding_notional_usd"] = cfg.funding_notional_usd
    row["funding_net_usd_at_notional"] = net_pnl / 100.0 * cfg.funding_notional_usd
    return changed


def _horizon_label(minutes: float) -> str:
    if minutes < 60:
        return f"{int(minutes) if float(minutes).is_integer() else minutes:g}m"
    hours = minutes / 60.0
    return f"{int(hours) if float(hours).is_integer() else hours:g}h"


def _lag_pair_pnl_if_closed_pct(row: dict, cfg: Config) -> float:
    """Approximate pair PnL in percentage points of one-leg notional."""
    entry = fnum(row.get("entry_gap_pct"))
    current = fnum(row.get("last_gap_pct"), entry)
    funding = fnum(row.get("funding_net_pnl_pct"))
    return (entry - current) - cfg.estimated_roundtrip_fees_pct + funding


def ensure_v11_state(state: dict) -> bool:
    """V13 state migration; legacy closed rows remain as an audit trail."""
    changed = False
    if int(fnum(state.get("state_schema_version"))) < 13:
        state["state_schema_version"] = 13; changed = True
    for key, default in (
        ("active_lag_outcomes", {}), ("active_mm_outcomes", {}),
        ("outcome_stats", {}), ("reference_health", {}),
    ):
        if not isinstance(state.get(key), type(default)):
            state[key] = default; changed = True
        elif key not in state:
            state[key] = default; changed = True
    return changed


def _outcome_bucket(state: dict, typ: str) -> dict:
    return state.setdefault("outcome_stats", {}).setdefault(typ, {
        "total_started": 0, "full": 0, "t80": 0, "t50": 0,
        "clean_reverted": 0, "reverted_not_clean": 0, "stale": 0,
        "symbols": {}, "last_ts": 0,
    })


def start_active_lag_outcome(state: dict, event: dict, cfg: Config) -> bool:
    bucket = state.setdefault("active_lag_outcomes", {})
    skey = str(event.get("state_key")); side = str(event.get("side")); ts = float(event.get("ts", time.time()))
    event_id = f"{skey}:{side}:{int(ts*1000)}"
    if event_id in bucket:
        event["outcome_id"] = event_id
        return False
    entry = max(0.0, float(event.get("gross_edge_pct", 0.0)))
    hedge_venue = str(event.get("preferred_hedge_venue") or event.get("execution_best_external_venue") or "")
    bucket[event_id] = {
        "id": event_id, "state_key": skey, "symbol": event.get("symbol"),
        "target": event.get("target"), "target_label": event.get("target_label"),
        "side": side, "start_ts": ts, "entry_gap_pct": entry,
        "min_gap_pct": entry, "max_gap_pct": entry, "last_gap_pct": entry,
        "last_observed_ts": ts, "valid_observations": 0,
        "t50": None, "t80": None, "full_ts": None, "status": "OPEN",
        "max_adverse_extra_pct": 0.0, "trigger": event.get("trigger", "historical-profile"),
        "hedge_venue": hedge_venue, "execution_ready": bool(event.get("execution_ready", False)),
        "horizons": {}, "funding": {}, "version": 13,
    }
    st = _outcome_bucket(state, "ACTIVE-LAG")
    st["total_started"] = int(fnum(st.get("total_started"))) + 1
    sy = st.setdefault("symbols", {}); sy[skey] = int(fnum(sy.get(skey))) + 1; st["last_ts"] = int(time.time())
    event["outcome_id"] = event_id
    return True


def _capture_lag_horizon(row: dict, minutes: float, cfg: Config, observed_ts: float) -> dict:
    entry = max(1e-9, fnum(row.get("entry_gap_pct")))
    min_gap = fnum(row.get("min_gap_pct"), entry)
    current = fnum(row.get("last_gap_pct"), entry)
    max_conv = max(0.0, min(1.0, (entry - min_gap) / entry))
    result = {
        "minutes": minutes, "label": _horizon_label(minutes),
        "captured_ts": observed_ts, "observed_age_seconds": max(0.0, observed_ts - fnum(row.get("start_ts"))),
        "gap_pct": current, "min_gap_pct": min_gap, "max_gap_pct": fnum(row.get("max_gap_pct"), entry),
        "max_convergence_fraction": max_conv,
        "t50": row.get("t50"), "t80": row.get("t80"), "full_ts": row.get("full_ts"),
        "funding_target_pnl_pct": fnum(row.get("funding_target_pnl_pct")),
        "funding_hedge_pnl_pct": fnum(row.get("funding_hedge_pnl_pct")),
        "funding_net_pnl_pct": fnum(row.get("funding_net_pnl_pct")),
        "estimated_pair_pnl_if_closed_pct": _lag_pair_pnl_if_closed_pct(row, cfg),
    }
    return result


def update_active_lag_outcomes(state: dict, candidates: List[Candidate], cfg: Config, now_ts: Optional[float]=None) -> Tuple[List[dict], bool]:
    now_ts = time.time() if now_ts is None else now_ts
    by_key = {candidate_state_key(c): c for c in candidates}
    bucket = state.setdefault("active_lag_outcomes", {})
    emitted: List[dict] = []; changed = False
    max_age = max(1.0, cfg.active_lag_outcome_max_age_minutes * 60.0)
    horizons = [h for h in cfg.active_lag_outcome_horizons_minutes if h > 0 and h * 60.0 <= max_age + 1e-6]

    for event_id, row in list(bucket.items()):
        if not isinstance(row, dict) or row.get("status") != "OPEN":
            continue
        start_ts = fnum(row.get("start_ts")); cutoff_ts = start_ts + max_age
        c = by_key.get(str(row.get("state_key")))
        entry = max(1e-9, fnum(row.get("entry_gap_pct")))
        side = str(row.get("side"))
        observed_new = False
        if c and c.samples:
            for smp in sorted(c.samples, key=lambda x: x.ts):
                # Strictly ignore data outside the requested forward window.
                if smp.ts < start_ts or smp.ts > cutoff_ts:
                    continue
                edge = smp.hedge_long_edge_pct if side == "LONG" else smp.hedge_short_edge_pct
                if not math.isfinite(edge) or smp.external_bid <= 0 or smp.external_ask <= 0:
                    continue
                row["last_gap_pct"] = edge
                row["last_observed_ts"] = smp.ts
                row["valid_observations"] = int(fnum(row.get("valid_observations"))) + 1
                row["min_gap_pct"] = min(fnum(row.get("min_gap_pct"), entry), edge)
                row["max_gap_pct"] = max(fnum(row.get("max_gap_pct"), entry), edge)
                row["max_adverse_extra_pct"] = max(0.0, fnum(row.get("max_gap_pct")) - entry)
                elapsed = max(0.0, smp.ts - start_ts)
                if row.get("t50") is None and edge <= entry * 0.50:
                    row["t50"] = elapsed
                if row.get("t80") is None and edge <= entry * 0.20:
                    row["t80"] = elapsed
                if row.get("full_ts") is None and edge <= cfg.lag_full_convergence_edge_pct:
                    row["full_ts"] = elapsed
                observed_new = True
            if observed_new:
                changed = True

        funding_end = min(now_ts, cutoff_ts)
        changed = _refresh_lag_outcome_funding(row, cfg, funding_end) or changed
        row["estimated_pair_pnl_if_closed_pct"] = _lag_pair_pnl_if_closed_pct(row, cfg)

        age = now_ts - start_ts
        final = None
        if row.get("full_ts") is not None:
            final = "FULL"
        elif age >= max_age:
            final = "NOT_FULL_24H" if int(fnum(row.get("valid_observations"))) > 0 else "NO_DATA_24H"

        # Capture the first valid observation at/after each requested horizon.
        # A FULL outcome closes immediately, so no redundant checkpoint is emitted.
        hmap = row.setdefault("horizons", {})
        last_obs = fnum(row.get("last_observed_ts"))
        if final != "FULL" and last_obs > 0:
            for h in horizons:
                label = _horizon_label(h)
                if label in hmap:
                    continue
                threshold_ts = start_ts + h * 60.0
                if last_obs >= threshold_ts:
                    hr = _capture_lag_horizon(row, h, cfg, last_obs)
                    hmap[label] = hr; changed = True
                    # 24h is represented by the final outcome below, not a duplicate checkpoint.
                    if cfg.active_lag_horizon_alerts_enabled and h * 60.0 < max_age - 1e-6:
                        emitted.append({"type": "ACTIVE-LAG-CHECKPOINT", **row, "checkpoint": hr})

        if final:
            row["status"] = final; row["closed_ts"] = min(now_ts, cutoff_ts); changed = True
            st = _outcome_bucket(state, "ACTIVE-LAG")
            if final == "FULL": st["full"] = int(fnum(st.get("full"))) + 1
            if row.get("t80") is not None: st["t80"] = int(fnum(st.get("t80"))) + 1
            if row.get("t50") is not None: st["t50"] = int(fnum(st.get("t50"))) + 1
            if final != "FULL": st["stale"] = int(fnum(st.get("stale"))) + 1
            st["last_ts"] = int(now_ts)
            emitted.append({"type": "ACTIVE-LAG-OUTCOME", **row})

    # 24h rows are still compact; keep a larger audit trail for aggregate research.
    if len(bucket) > 1000:
        ordered = sorted(bucket.items(), key=lambda kv: fnum(kv[1].get("start_ts")) if isinstance(kv[1], dict) else 0)
        for k, _ in ordered[:-1000]: bucket.pop(k, None); changed = True
    return emitted, changed


def mm_outcome_id(skey: str, exc: Excursion) -> str:
    return f"{skey}:{exc.direction}:{int(exc.start_ts*1000)}"


def start_or_update_mm_outcome(state: dict, c: Candidate, exc: Excursion, ev: dict, cfg: Config) -> bool:
    bucket=state.setdefault("active_mm_outcomes", {}); skey=candidate_state_key(c); eid=mm_outcome_id(skey,exc)
    row=bucket.get(eid); changed=False
    if not isinstance(row,dict):
        row={"id":eid,"state_key":skey,"symbol":c.symbol,"target":getattr(c,"target","aster"),"target_label":target_label(c),
             "side":ev.get("action"),"direction":exc.direction,"start_ts":exc.start_ts,"signal_peak_pct":exc.peak_abs_deviation_pct,
             "max_peak_pct":exc.peak_abs_deviation_pct,"status":"OPEN","taker_edge_pct":ev.get("taker_edge_pct"),"maker_quote_edge_pct":ev.get("maker_quote_edge_pct")}
        bucket[eid]=row; st=_outcome_bucket(state,"ACTIVE-MM-EXCURSION"); st["total_started"]=int(fnum(st.get("total_started")))+1
        sy=st.setdefault("symbols",{}); sy[skey]=int(fnum(sy.get(skey)))+1; st["last_ts"]=int(time.time()); changed=True
    row["max_peak_pct"]=max(fnum(row.get("max_peak_pct")), exc.peak_abs_deviation_pct)
    row["extra_adverse_after_signal_pct"]=max(0.0, fnum(row.get("max_peak_pct"))-fnum(row.get("signal_peak_pct")))
    if exc.reverted:
        row["reversion_seconds"]=exc.reversion_seconds; row["clean_reversion"]=bool(exc.clean_reversion)
        new_status="CLEAN_REVERTED" if exc.clean_reversion else "REVERTED_NOT_CLEAN"
        if row.get("status") != new_status:
            row["status"]=new_status; row["closed_ts"]=time.time(); changed=True
    return changed


def finalize_mm_outcomes(state: dict, cfg: Config, now_ts: Optional[float]=None) -> Tuple[List[dict], bool]:
    now_ts=time.time() if now_ts is None else now_ts; bucket=state.setdefault("active_mm_outcomes",{}); emitted=[]; changed=False
    max_age=max(1.0,cfg.active_mm_outcome_max_age_seconds)
    for eid,row in list(bucket.items()):
        if not isinstance(row,dict): continue
        status=str(row.get("status","OPEN"))
        if status=="OPEN" and now_ts-fnum(row.get("start_ts"))>=max_age:
            row["status"]="STALE_UNRESOLVED"; row["closed_ts"]=now_ts; status=row["status"]; changed=True
        if status in {"CLEAN_REVERTED","REVERTED_NOT_CLEAN","STALE_UNRESOLVED"} and not row.get("counted"):
            st=_outcome_bucket(state,"ACTIVE-MM-EXCURSION")
            if status=="CLEAN_REVERTED": st["clean_reverted"]=int(fnum(st.get("clean_reverted")))+1
            elif status=="REVERTED_NOT_CLEAN": st["reverted_not_clean"]=int(fnum(st.get("reverted_not_clean")))+1
            else: st["stale"]=int(fnum(st.get("stale")))+1
            st["last_ts"]=int(now_ts); row["counted"]=True; changed=True; emitted.append({"type":"ACTIVE-MM-OUTCOME", **row})
    if len(bucket)>500:
        ordered=sorted(bucket.items(), key=lambda kv:fnum(kv[1].get("start_ts")) if isinstance(kv[1],dict) else 0)
        for k,_ in ordered[:-500]: bucket.pop(k,None); changed=True
    return emitted, changed




# ------------------------------ V12 execution readiness ------------------------------
_MEXC_CONTRACT_SIZES_CACHE: Tuple[float, Dict[str, float]] = (0.0, {})


def _depth_levels(rows: Any, multiplier: float = 1.0) -> List[Tuple[float, float]]:
    out: List[Tuple[float, float]] = []
    for row in rows if isinstance(rows, list) else []:
        if isinstance(row, (list, tuple)) and len(row) >= 2:
            px, qty = fnum(row[0]), fnum(row[1]) * multiplier
        elif isinstance(row, dict):
            px = fnum(row.get("px", row.get("price")))
            qty = fnum(row.get("sz", row.get("size", row.get("qty")))) * multiplier
        else:
            continue
        if px > 0 and qty > 0:
            out.append((px, qty))
    return out


def _mk_depth_book(bids: Any, asks: Any, multiplier: float = 1.0, venue: str = "", ts: float = 0.0) -> Optional[dict]:
    b = sorted(_depth_levels(bids, multiplier), key=lambda x: x[0], reverse=True)
    a = sorted(_depth_levels(asks, multiplier), key=lambda x: x[0])
    if not b or not a:
        return None
    return {"bids": b, "asks": a, "venue": venue, "ts": ts or time.time()}


def _simulate_vwap(book: dict, action: str, notional_usd: float) -> dict:
    levels = book.get("asks", []) if action == "BUY" else book.get("bids", [])
    remaining = max(0.0, float(notional_usd))
    base = 0.0
    quote = 0.0
    if remaining <= 0:
        return {"filled": False, "vwap": 0.0, "filled_usd": 0.0, "top": 0.0, "slippage_pct": 0.0}
    top = float(levels[0][0]) if levels else 0.0
    for px, qty in levels:
        level_usd = float(px) * float(qty)
        if level_usd <= 0:
            continue
        take_usd = min(remaining, level_usd)
        quote += take_usd
        base += take_usd / float(px)
        remaining -= take_usd
        if remaining <= 1e-9:
            break
    filled = remaining <= max(1e-6, notional_usd * 1e-6)
    vwap = quote / base if base > 0 else 0.0
    slip = 0.0
    if top > 0 and vwap > 0:
        slip = ((vwap - top) / top * 100.0) if action == "BUY" else ((top - vwap) / top * 100.0)
    return {"filled": filled, "vwap": vwap, "filled_usd": quote, "top": top, "slippage_pct": slip}


def _fetch_aster_depth(symbol: str, cfg: Config) -> Optional[dict]:
    data = get_json(f"{ASTER_BASE}/fapi/v3/depth", cfg.request_timeout_seconds, params={"symbol": symbol, "limit": max(5, min(cfg.execution_book_levels, 50))})
    if not isinstance(data, dict):
        return None
    return _mk_depth_book(data.get("bids"), data.get("asks"), venue="aster", ts=fnum(data.get("E")) / 1000.0 if fnum(data.get("E")) > 1e12 else time.time())


def _fetch_hyperliquid_depth(coin: str, cfg: Config) -> Optional[dict]:
    if not coin:
        return None
    data = post_json(HYPERLIQUID_INFO_URL, cfg.request_timeout_seconds, {"type": "l2Book", "coin": coin})
    if not isinstance(data, dict):
        return None
    levels = data.get("levels")
    if not (isinstance(levels, list) and len(levels) >= 2):
        return None
    return _mk_depth_book(levels[0], levels[1], venue="hyperliquid-perp", ts=fnum(data.get("time")) / 1000.0 if fnum(data.get("time")) > 1e12 else time.time())


def _fetch_bitget_depth(symbol: str, cfg: Config) -> Optional[dict]:
    data = get_json(
        f"{BITGET_BASE}/api/v3/market/orderbook",
        cfg.request_timeout_seconds,
        params={"category": "USDT-FUTURES", "symbol": symbol, "limit": str(max(5, min(cfg.execution_book_levels, 50)))},
    )
    if not isinstance(data, dict):
        return None
    d = data.get("data") or {}
    if isinstance(d, list) and d:
        d = d[0]
    if not isinstance(d, dict):
        return None
    ts = fnum(d.get("ts"))
    return _mk_depth_book(d.get("b", d.get("bids")), d.get("a", d.get("asks")), venue="bitget-perp", ts=ts / 1000.0 if ts > 1e12 else time.time())


def _fetch_bybit_depth(symbol: str, cfg: Config) -> Optional[dict]:
    data = get_json(
        f"{BYBIT_BASE}/v5/market/orderbook",
        cfg.request_timeout_seconds,
        params={"category": "linear", "symbol": symbol, "limit": max(1, min(cfg.execution_book_levels, 50))},
    )
    result = (data or {}).get("result", {}) if isinstance(data, dict) else {}
    if not isinstance(result, dict):
        return None
    ts = fnum(result.get("ts")) or fnum((data or {}).get("time"))
    return _mk_depth_book(result.get("b"), result.get("a"), venue="bybit-perp", ts=ts / 1000.0 if ts > 1e12 else time.time())


def _mexc_contract_sizes(cfg: Config) -> Dict[str, float]:
    global _MEXC_CONTRACT_SIZES_CACHE
    now = time.time()
    if _MEXC_CONTRACT_SIZES_CACHE[1] and now - _MEXC_CONTRACT_SIZES_CACHE[0] < 1800:
        return _MEXC_CONTRACT_SIZES_CACHE[1]
    try:
        data = get_json(f"{MEXC_CONTRACT_BASE}/api/v1/contract/detail", cfg.request_timeout_seconds)
        rows = (data or {}).get("data", []) if isinstance(data, dict) else []
        if isinstance(rows, dict):
            rows = [rows]
        out: Dict[str, float] = {}
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            sym = normalize_perp_symbol(row.get("symbol"))
            mult = fnum(row.get("contractSize"))
            if sym and mult > 0:
                out[sym] = mult
        if out:
            _MEXC_CONTRACT_SIZES_CACHE = (now, out)
            return out
    except Exception:
        pass
    return _MEXC_CONTRACT_SIZES_CACHE[1]


def _fetch_mexc_depth(symbol: str, cfg: Config) -> Optional[dict]:
    mult = _mexc_contract_sizes(cfg).get(symbol, 0.0)
    if mult <= 0:
        return None
    native = symbol[:-4] + "_USDT" if symbol.endswith("USDT") else symbol
    data = get_json(
        f"{MEXC_CONTRACT_BASE}/api/v1/contract/depth/{native}",
        cfg.request_timeout_seconds,
        params={"limit": max(5, min(cfg.execution_book_levels, 50))},
    )
    if not isinstance(data, dict):
        return None
    # MEXC may wrap depth in data, depending on deployment/API gateway.
    d = data.get("data") if isinstance(data.get("data"), dict) else data
    ts = fnum(d.get("timestamp")) if isinstance(d, dict) else 0.0
    return _mk_depth_book(d.get("bids"), d.get("asks"), multiplier=mult, venue="mexc-perp", ts=ts / 1000.0 if ts > 1e12 else time.time()) if isinstance(d, dict) else None


def _fetch_target_depth(c: Candidate, cfg: Config) -> Optional[dict]:
    target = getattr(c, "target", "aster")
    if target == "aster":
        return _fetch_aster_depth(c.symbol, cfg)
    if target == "hyperliquid":
        coin = str(getattr(c, "native_symbol", _HYPE_NATIVE.get(c.symbol, "")))
        return _fetch_hyperliquid_depth(coin, cfg)
    if target == "lighter":
        market_id = int(fnum(getattr(c, "native_id", -1), default=-1))
        book = _LIGHTER_STREAM.depth_snapshot(market_id, cfg.execution_book_levels)
        if book and time.time() - fnum(book.get("ts")) <= cfg.execution_max_book_age_seconds:
            return book
        return None
    return None


def _fetch_external_execution_books(symbol: str, refs: List[str], cfg: Config) -> Dict[str, dict]:
    names = set(refs or [])
    workers: List[Tuple[str, Callable[[], Optional[dict]]]] = []
    if "bitget-perp" in names:
        workers.append(("bitget-perp", lambda: _fetch_bitget_depth(symbol, cfg)))
    if "mexc-perp" in names:
        workers.append(("mexc-perp", lambda: _fetch_mexc_depth(symbol, cfg)))
    if "bybit-perp" in names:
        workers.append(("bybit-perp", lambda: _fetch_bybit_depth(symbol, cfg)))
    out: Dict[str, dict] = {}
    if not workers:
        return out
    with ThreadPoolExecutor(max_workers=min(3, len(workers))) as pool:
        futs = {pool.submit(fn): name for name, fn in workers}
        for fut in as_completed(futs):
            name = futs[fut]
            try:
                book = fut.result()
                if book:
                    out[name] = book
            except Exception:
                pass
    return out


def _candidate_bbo_coverage(c: Candidate) -> float:
    if not c.samples:
        return 0.0
    valid = 0
    for smp in c.samples:
        if fnum(getattr(smp, "external_bid", 0)) > 0 and fnum(getattr(smp, "external_ask", 0)) > 0:
            valid += 1
    return valid / len(c.samples)


def execution_readiness_probe(c: Candidate, m: dict, side: str, cfg: Config) -> dict:
    """Fresh public-depth recheck for informational trade readiness. Never places orders."""
    if not cfg.execution_readiness_enabled:
        return {"execution_status": "DISABLED", "execution_ready": False, "execution_checks": {}}
    started = time.time()
    target_book = _fetch_target_depth(c, cfg)
    refs = list(m.get("current_exec_refs", []))
    ext_books = _fetch_external_execution_books(c.symbol, refs, cfg)
    target_action = "SELL" if side == "SHORT" else "BUY"
    external_action = "BUY" if side == "SHORT" else "SELL"
    checks: Dict[str, dict] = {}
    ready_max = 0.0
    best_at_min = None
    notionals = sorted(set(float(x) for x in cfg.execution_probe_notionals if float(x) > 0))
    if cfg.execution_min_notional_usd > 0 and cfg.execution_min_notional_usd not in notionals:
        notionals.append(cfg.execution_min_notional_usd); notionals.sort()
    for usd in notionals:
        key = str(int(usd) if float(usd).is_integer() else usd)
        if not target_book:
            checks[key] = {"status": "UNKNOWN_TARGET_DEPTH", "net_edge_pct": None, "gross_edge_pct": None, "external_venue": None}
            continue
        tfill = _simulate_vwap(target_book, target_action, usd)
        if not tfill["filled"]:
            checks[key] = {"status": "INSUFFICIENT_TARGET_LIQUIDITY", "net_edge_pct": None, "gross_edge_pct": None, "external_venue": None, "target_filled_usd": tfill["filled_usd"]}
            continue
        best = None
        for venue, book in ext_books.items():
            efill = _simulate_vwap(book, external_action, usd)
            if not efill["filled"]:
                continue
            if side == "SHORT":
                gross = (tfill["vwap"] - efill["vwap"]) / efill["vwap"] * 100.0
            else:
                gross = (efill["vwap"] - tfill["vwap"]) / efill["vwap"] * 100.0
            net = gross - cfg.estimated_roundtrip_fees_pct
            row = {
                "status": "READY" if net >= cfg.execution_min_net_edge_pct else "EDGE_TOO_THIN",
                "gross_edge_pct": gross, "net_edge_pct": net, "external_venue": venue,
                "target_vwap": tfill["vwap"], "external_vwap": efill["vwap"],
                "target_slippage_pct": tfill["slippage_pct"], "external_slippage_pct": efill["slippage_pct"],
                "target_filled_usd": tfill["filled_usd"], "external_filled_usd": efill["filled_usd"],
            }
            if best is None or gross > best["gross_edge_pct"]:
                best = row
        if best is None:
            checks[key] = {"status": "UNKNOWN_OR_INSUFFICIENT_EXTERNAL_DEPTH", "net_edge_pct": None, "gross_edge_pct": None, "external_venue": None}
        else:
            checks[key] = best
            if best["status"] == "READY":
                ready_max = max(ready_max, usd)
            if abs(usd - cfg.execution_min_notional_usd) < 1e-9:
                best_at_min = best
    min_key = str(int(cfg.execution_min_notional_usd) if float(cfg.execution_min_notional_usd).is_integer() else cfg.execution_min_notional_usd)
    min_row = checks.get(min_key, {})
    status = str(min_row.get("status", "UNKNOWN"))
    ready = status == "READY"
    target_top_capacity = 0.0
    if target_book:
        top_side = target_book["bids"] if side == "SHORT" else target_book["asks"]
        if top_side:
            target_top_capacity = top_side[0][0] * top_side[0][1]
    external_top_capacity = 0.0
    if best_at_min and best_at_min.get("external_venue") in ext_books:
        b = ext_books[best_at_min["external_venue"]]
        top_side = b["asks"] if side == "SHORT" else b["bids"]
        if top_side:
            external_top_capacity = top_side[0][0] * top_side[0][1]
    elapsed_probe = time.time() - started
    if elapsed_probe > cfg.execution_max_probe_seconds:
        status = "PROBE_TOO_SLOW"
        ready = False
    return {
        "execution_status": status,
        "execution_ready": ready,
        "execution_ready_notional_usd": ready_max,
        "execution_min_notional_usd": cfg.execution_min_notional_usd,
        "execution_checks": checks,
        "execution_probe_seconds": elapsed_probe,
        "execution_target_top_capacity_usd": target_top_capacity,
        "execution_external_top_capacity_usd": external_top_capacity,
        "execution_best_external_venue": (best_at_min or {}).get("external_venue"),
        "execution_fresh_gross_edge_pct": (best_at_min or {}).get("gross_edge_pct"),
        "execution_fresh_net_edge_pct": (best_at_min or {}).get("net_edge_pct"),
    }


def _attach_execution_probe_to_events(events: List[dict], candidates: List[Candidate], cfg: Config, state: Optional[dict] = None) -> List[dict]:
    by_key = {candidate_state_key(c): c for c in candidates}
    for ev in events:
        c = by_key.get(str(ev.get("state_key", "")))
        if not c:
            continue
        m = c.metrics(cfg)
        side = str(ev.get("side", "NONE"))
        if side not in {"LONG", "SHORT"}:
            continue
        if fnum(ev.get("bbo_coverage_ratio")) <= 0:
            ev["bbo_coverage_ratio"] = _candidate_bbo_coverage(c)
        try:
            ev.update(execution_readiness_probe(c, m, side, cfg))
        except Exception as e:
            ev.update({"execution_status": f"PROBE_ERROR:{type(e).__name__}", "execution_ready": False, "execution_checks": {}})
        # Save the actual/preferred hedge venue for 24h funding tracking. The
        # execution probe wins; otherwise use the best visible external BBO venue.
        sample = c.samples[-1] if c.samples else None
        fallback_venue = ""
        if sample is not None:
            fallback_venue = sample.best_external_ask_venue if side == "SHORT" else sample.best_external_bid_venue
        ev["preferred_hedge_venue"] = str(ev.get("execution_best_external_venue") or fallback_venue or "")
        if ev.get("execution_ready") and state is not None:
            try:
                record_signal_stats(state, "ACTIVE-LAG-READY", str(ev.get("state_key", c.symbol)), 1, {side: 1})
            except Exception as e:
                # Accounting failure must never erase a valid depth probe.
                print(f"WARN ACTIVE-LAG-READY stats: {type(e).__name__}: {e}", file=sys.stderr)
        # same-run outcomes can be created before the execution probe. Sync the
        # selected hedge venue back into the already-open outcome row.
        oid = ev.get("outcome_id")
        if oid and state is not None:
            row = state.setdefault("active_lag_outcomes", {}).get(str(oid))
            if isinstance(row, dict):
                row["hedge_venue"] = ev.get("preferred_hedge_venue") or row.get("hedge_venue", "")
                row["execution_ready"] = bool(ev.get("execution_ready"))
    return events

def detect_same_run_active_lag_events(candidates: List[Candidate], cfg: Config, state: dict) -> Tuple[List[dict], bool]:
    """Emit ACTIVE-LAG when this very run already proved convergence and then re-expanded."""
    if not cfg.active_lag_enabled: return [],False
    events=[]; changed=False
    open_state=state.setdefault("active_signal_open",{}); existing=set(open_state.get("ACTIVE-LAG",[]) if isinstance(open_state.get("ACTIVE-LAG",[]),list) else [])
    for c in candidates:
        if c.active_lag_detected: continue
        m=c.metrics(cfg); side=str(m.get("lag_detection_side","NONE")); skey=candidate_state_key(c)
        if side not in {"LONG","SHORT"} or not m.get("lag_convergence_confirmed") or not m.get("lag_reexpanded_after_convergence"): continue
        current=float(m.get("current_executable_edge_pct",0)); net=float(m.get("current_net_edge_pct",-999))
        if str(m.get("current_edge_side","NONE"))!=side: continue
        if current<cfg.active_lag_min_gross_edge_pct or net<cfg.active_lag_min_net_edge_pct: continue
        if float(m.get("current_ref_disagreement_pct",999))>cfg.active_lag_max_reference_disagreement_pct: continue
        if float(m.get("lag_bbo_coverage_ratio",0))<cfg.lag_min_valid_bbo_coverage: continue
        baseline_values=lag_baseline_values(state,skey,side,cfg); baseline_median=statistics.median(baseline_values) if baseline_values else None
        baseline_required=max(baseline_median+cfg.lag_baseline_min_excess_pct, baseline_median*cfg.lag_baseline_min_ratio) if baseline_median is not None else None
        baseline_ready=len(baseline_values)>=cfg.lag_baseline_min_points
        if baseline_ready and baseline_required is not None and current<baseline_required: continue
        latch=f"{skey}:{side}"; existing.add(latch)
        profile=summarize_lag_profile(lag_verified_profile_rows(state,skey,side,cfg))
        ev={"type":"ACTIVE-LAG","symbol":c.symbol,"state_key":skey,"target":getattr(c,"target","aster"),"target_label":target_label(c),
            "ts":c.samples[-1].ts if c.samples else time.time(),"side":side,"gross_edge_pct":current,"net_edge_pct":net,
            "target_bid":float(m.get("current_target_bid",0)),"target_ask":float(m.get("current_target_ask",0)),"external_bid":float(m.get("current_external_bid",0)),"external_ask":float(m.get("current_external_ask",0)),
            "persistent_hit_ratio":float(m.get("persistent_exec_hit_ratio",0)),"persistent_median_edge_pct":float(m.get("persistent_median_executable_edge_pct",0)),
            "reference_disagreement_pct":float(m.get("current_ref_disagreement_pct",0)),"baseline_points":len(baseline_values),"baseline_min_points":cfg.lag_baseline_min_points,
            "baseline_median_gap_pct":baseline_median,"baseline_required_gap_pct":baseline_required,"trigger":"same-run verified re-expansion",
            "bbo_coverage_ratio":float(m.get("lag_bbo_coverage_ratio",0)),"target_bbo_capacity_usd":float(m.get("target_bbo_capacity_usd",0)),"external_bbo_capacity_usd":float(m.get("external_bbo_capacity_usd",0)),
            "verified_episodes":max(1,int(profile.get("verified_episodes",0))),"median_max_convergence_fraction":max(float(m.get("lag_max_convergence_fraction",0)),float(profile.get("median_max_convergence_fraction",0))),
            "total_full_cycles":int(m.get("lag_full_convergence_cycles",0))+int(profile.get("total_full_cycles",0)),"median_time_to_50_seconds":m.get("lag_time_to_50_seconds") or profile.get("median_time_to_50_seconds")}
        events.append(ev); record_signal_stats(state,"ACTIVE-LAG",skey,1,{side:1}); start_active_lag_outcome(state,ev,cfg)
        c.active_lag_detected=True; c.active_lag_detection_ts=ev["ts"]; c.active_lag_side=side; changed=True
    open_state["ACTIVE-LAG"]=sorted(existing)
    return events,changed


def _fmt_funding_line(event: dict) -> str:
    status = str(event.get("funding_tracking_status", "UNKNOWN"))
    target = fnum(event.get("funding_target_pnl_pct"))
    hedge = fnum(event.get("funding_hedge_pnl_pct"))
    net = fnum(event.get("funding_net_pnl_pct"))
    tcnt = int(fnum(event.get("funding_target_settlements")))
    hcnt = int(fnum(event.get("funding_hedge_settlements")))
    hedge_venue = str(event.get("hedge_venue") or "unknown")
    return (
        f"Funding carry: target {target:+.4f}% ({tcnt} settlements) | "
        f"hedge {hedge:+.4f}% ({hcnt}, {hedge_venue}) | net <b>{net:+.4f}%</b> [{status}]"
    )


def _fmt_horizon_summary(event: dict) -> str:
    hmap = event.get("horizons", {}) if isinstance(event.get("horizons"), dict) else {}
    if not hmap:
        return ""
    parts=[]
    order={"5m":5,"30m":30,"2h":120,"6h":360,"24h":1440}
    for label, row in sorted(hmap.items(), key=lambda kv: order.get(kv[0], 99999)):
        if not isinstance(row, dict):
            continue
        parts.append(
            f"{label}: gap {fnum(row.get('gap_pct')):.3f}% | conv {fnum(row.get('max_convergence_fraction'))*100:.0f}% | "
            f"fund {fnum(row.get('funding_net_pnl_pct')):+.4f}% | close-now {fnum(row.get('estimated_pair_pnl_if_closed_pct')):+.3f}%"
        )
    return "\n".join(parts)


def format_outcome_signal(event: dict, state: dict) -> str:
    typ=str(event.get("type","")); symbol=event.get("symbol",""); label=event.get("target_label","Aster")
    if typ=="ACTIVE-LAG-CHECKPOINT":
        cp = event.get("checkpoint", {}) if isinstance(event.get("checkpoint"), dict) else {}
        return (
            f"⏱ <b>ACTIVE-LAG CHECKPOINT — {symbol} @ {label}</b>\n"
            f"Horizon <b>{cp.get('label','?')}</b> | side {event.get('side')} | still open\n"
            f"Entry gap {fnum(event.get('entry_gap_pct')):.3f}% | current {fnum(cp.get('gap_pct')):.3f}% | min {fnum(cp.get('min_gap_pct')):.3f}% | max {fnum(cp.get('max_gap_pct')):.3f}%\n"
            f"Max convergence {fnum(cp.get('max_convergence_fraction'))*100:.0f}% | 50% {fmt_seconds(cp.get('t50'))} | 80% {fmt_seconds(cp.get('t80'))} | full {fmt_seconds(cp.get('full_ts'))}\n"
            f"{_fmt_funding_line(event)}\n"
            f"Estimated pair PnL if closed now: <b>{fnum(cp.get('estimated_pair_pnl_if_closed_pct')):+.3f}%</b> after fee reserve + settled funding\n"
            "Research checkpoint only. No orders are sent."
        )
    if typ=="ACTIVE-LAG-OUTCOME":
        held = max(0.0, fnum(event.get("closed_ts")) - fnum(event.get("start_ts")))
        hs = _fmt_horizon_summary(event)
        hs_block = f"\nHorizons:\n{hs}" if hs else ""
        return (
            f"📊 <b>ACTIVE-LAG OUTCOME — {symbol} @ {label}</b>\n"
            f"Side {event.get('side')} | held {fmt_seconds(held)} | entry gap {fnum(event.get('entry_gap_pct')):.3f}% | min {fnum(event.get('min_gap_pct')):.3f}% | max {fnum(event.get('max_gap_pct')):.3f}%\n"
            f"50%: {fmt_seconds(event.get('t50'))} | 80%: {fmt_seconds(event.get('t80'))} | full: {fmt_seconds(event.get('full_ts'))}\n"
            f"Max extra adverse expansion: {fnum(event.get('max_adverse_extra_pct')):.3f}% | final status <b>{event.get('status')}</b>\n"
            f"{_fmt_funding_line(event)}\n"
            f"Estimated pair PnL at final observation: <b>{fnum(event.get('estimated_pair_pnl_if_closed_pct')):+.3f}%</b> after fee reserve + settled funding"
            f"{hs_block}\n"
            "Forward outcome measured after the ACTIVE-LAG timestamp. No orders are sent."
        )
    if typ=="ACTIVE-MM-OUTCOME":
        return (f"📊 <b>ACTIVE-MM OUTCOME — {symbol} @ {label}</b>\n"
                f"Side {event.get('side')} | signal peak {fnum(event.get('signal_peak_pct')):.3f}% | max peak {fnum(event.get('max_peak_pct')):.3f}%\n"
                f"Extra adverse after detection {fnum(event.get('extra_adverse_after_signal_pct')):.3f}% | reversion {fmt_seconds(event.get('reversion_seconds'))}\n"
                f"Final status <b>{event.get('status')}</b> | clean {bool(event.get('clean_reversion'))}\n"
                "Forward outcome measured after the ACTIVE-MM event. No orders are sent.")
    return ""


def print_reference_health(snap: V10Snapshot) -> None:
    print("Reference health (initial snapshot):")
    names=sorted(set(snap.fair_refs)|set(snap.exec_refs))
    for name in names:
        fair_n=len(snap.fair_refs.get(name,{})); exec_n=len(snap.exec_refs.get(name,{})); print(f" - {name:<20} fair={fair_n:4d} execBBO={exec_n:4d}")
    print(f" - {'edgeX stream':<20} markets={len(_EDGEX_STREAM.snapshot()):4d} age={max(0,time.time()-_EDGEX_STREAM.last_message_ts):.1f}s error={_EDGEX_STREAM.last_error or 'none'}")
    lq,ls=_LIGHTER_STREAM.snapshot(); print(f" - {'Lighter stream':<20} markets={len(lq):4d} age={max(0,time.time()-_LIGHTER_STREAM.last_message_ts):.1f}s error={_LIGHTER_STREAM.last_error or 'none'}")



# Wrap the V11 ACTIVE-LAG detectors with a fresh execution-readiness probe.
_v11_detect_active_lag_events_for_v12 = detect_active_lag_events
_v11_detect_same_run_active_lag_events_for_v12 = detect_same_run_active_lag_events


def detect_active_lag_events(candidates: List[Candidate], cfg: Config, state: dict) -> Tuple[List[dict], bool]:
    events, changed = _v11_detect_active_lag_events_for_v12(candidates, cfg, state)
    return _attach_execution_probe_to_events(events, candidates, cfg, state), changed


def detect_same_run_active_lag_events(candidates: List[Candidate], cfg: Config, state: dict) -> Tuple[List[dict], bool]:
    events, changed = _v11_detect_same_run_active_lag_events_for_v12(candidates, cfg, state)
    return _attach_execution_probe_to_events(events, candidates, cfg, state), changed


def _open_lag_tracking_candidates(state: dict) -> List[Candidate]:
    """Create lightweight target candidates for OPEN ACTIVE-LAG outcomes.

    This bypasses the normal hot-market prefilter, so a pair that was opened
    yesterday is still sampled every scheduled run until it converges or reaches
    the 24h research horizon.
    """
    out: List[Candidate] = []
    seen: set[str] = set()
    bucket = state.get("active_lag_outcomes", {}) if isinstance(state, dict) else {}
    for row in bucket.values() if isinstance(bucket, dict) else []:
        if not isinstance(row, dict) or row.get("status") != "OPEN":
            continue
        skey = str(row.get("state_key", ""))
        if not skey or skey in seen:
            continue
        target = str(row.get("target") or "aster")
        symbol = str(row.get("symbol") or "")
        if target not in TARGET_LABELS or not symbol:
            continue
        c = Candidate(symbol, 0.0, 0.0)
        c.target = target
        c.native_symbol = _hl_coin_from_symbol(symbol) if target == "hyperliquid" else symbol
        c.native_id = symbol
        out.append(c); seen.add(skey)
    return out


def _hydrate_open_lag_tracking_candidates(candidates: List[Candidate], snap: V10Snapshot, cfg: Config) -> List[Candidate]:
    out: List[Candidate] = []
    for c in candidates:
        smp = build_target_sample(c, snap, cfg)
        if smp is not None:
            c.samples = [smp]
            out.append(c)
    return out


def scan(
    cfg: Config, state: dict,
    active_signal_callback: Optional[Callable[[dict], None]] = None,
) -> Tuple[List[Tuple[Candidate, dict]], List[str], bool]:
    migrated = ensure_v11_state(state)
    start_v10_streams()
    warmup = max(0.0, float(os.getenv("STREAM_WARMUP_SECONDS", "2.0")))
    if warmup > 0:
        time.sleep(warmup)
    errors: List[str] = []
    try:
        aster_stats = fetch_aster_24h(cfg) if "aster" in cfg_target_venues() else {}
    except Exception as e:
        aster_stats = {}
        errors.append(f"aster 24h: {type(e).__name__}: {e}")
    # V13: OPEN ACTIVE-LAG outcomes are sampled even if the symbol no longer
    # passes the hot-market prefilter. This is what makes 2h/6h/24h tracking real.
    tracking_candidates = _open_lag_tracking_candidates(state)
    snap = fetch_v10_snapshot(cfg, candidates=tracking_candidates or None, initial=True)
    errors.extend(snap.errors)
    print_reference_health(snap)
    tracking_candidates = _hydrate_open_lag_tracking_candidates(tracking_candidates, snap, cfg)
    tracking_events, tracking_changed = update_active_lag_outcomes(state, tracking_candidates, cfg)
    if cfg.active_outcome_alerts_enabled and active_signal_callback is not None:
        for event in tracking_events:
            active_signal_callback(event)

    candidates = prefilter_v10_candidates(snap, aster_stats, cfg)
    if not candidates:
        mm_outcomes, mm_changed = finalize_mm_outcomes(state, cfg)
        if cfg.active_outcome_alerts_enabled and active_signal_callback is not None:
            for event in mm_outcomes:
                active_signal_callback(event)
        return [], errors, bool(migrated or tracking_changed or mm_changed)
    counts = {t: sum(getattr(c, "target", "aster") == t for c in candidates) for t in cfg_target_venues()}
    print("Prefilter candidates:", ", ".join(f"{TARGET_LABELS[t]}={counts.get(t,0)}" for t in cfg_target_venues()))
    print("Selected:", ", ".join(candidate_display_name(c) for c in candidates))
    print(f"Confirming shared multi-target window for ~{cfg.confirm_duration_seconds:.0f}s every {cfg.confirm_interval_seconds:.1f}s...")
    collect_confirmation_samples_v10(candidates, cfg, errors)
    add_trade_analysis_v10(candidates, cfg, errors)

    active_events, active_changed = detect_active_lag_events(candidates, cfg, state)
    for event in active_events:
        active_changed = start_active_lag_outcome(state, event, cfg) or active_changed
        if active_signal_callback is not None:
            active_signal_callback(event)
    extended_changed = extended_regime_observation(candidates, cfg, errors, state, active_signal_callback)
    # Same-run path: convergence was proven and the same gap re-expanded before Stage 3 ended.
    same_run_events, same_run_changed = detect_same_run_active_lag_events(candidates, cfg, state)
    if active_signal_callback is not None:
        for event in same_run_events:
            active_signal_callback(event)
    # Update forward outcomes using every valid sample collected in this run.
    lag_outcomes, lag_outcome_changed = update_active_lag_outcomes(state, candidates, cfg)
    mm_outcomes, mm_outcome_changed = finalize_mm_outcomes(state, cfg)
    if cfg.active_outcome_alerts_enabled and active_signal_callback is not None:
        for event in lag_outcomes + mm_outcomes:
            active_signal_callback(event)
    ranked = []
    for c in candidates:
        m = c.metrics(cfg)
        if m:
            ranked.append((c, m))
    ranked.sort(key=lambda x: x[1]["score"], reverse=True)
    return ranked, errors, bool(migrated or tracking_changed or active_changed or extended_changed or same_run_changed or lag_outcome_changed or mm_outcome_changed)


def update_lag_baseline_state(state: dict, candidates: List[Candidate], cfg: Config) -> bool:
    now = int(time.time()); baseline = state.setdefault("lag_baseline", {}); changed = False
    cutoff = now - max(1, cfg.lag_baseline_lookback_minutes) * 60 * 3
    for c in candidates:
        side, edge = c.run_baseline_side, c.run_baseline_edge_pct
        if side not in {"LONG", "SHORT"} or edge < 0:
            continue
        skey = candidate_state_key(c)
        rows = baseline.setdefault(skey, [])
        if not isinstance(rows, list): rows = []; baseline[skey] = rows
        rows.append({"ts": now, "side": side, "edge_pct": round(edge, 6)})
        rows[:] = [r for r in rows if isinstance(r, dict) and fnum(r.get("ts")) >= cutoff]
        if len(rows) > cfg.lag_baseline_max_points * 3:
            del rows[: len(rows) - cfg.lag_baseline_max_points * 3]
        changed = True
    return changed


def update_lag_verified_profile_state(state: dict, candidates: List[Candidate], cfg: Config) -> bool:
    now = int(time.time()); profiles = state.setdefault("lag_verified_profiles", {}); changed = False
    cutoff = now - max(1, cfg.active_lag_profile_lookback_hours) * 3600 * 2
    for c in candidates:
        m = c.metrics(cfg)
        if not bool(m.get("lag_convergence_confirmed")):
            continue
        side = str(m.get("lag_detection_side", "NONE"))
        if side not in {"LONG", "SHORT"}: continue
        event_ts = int(c.lag_detection_ts or now); skey = candidate_state_key(c)
        rows = profiles.setdefault(skey, [])
        if not isinstance(rows, list): rows = []; profiles[skey] = rows
        if any(isinstance(r, dict) and int(fnum(r.get("detection_ts"))) == event_ts and str(r.get("side", "")).upper() == side for r in rows):
            continue
        rows.append({
            "ts": now, "detection_ts": event_ts, "side": side,
            "initial_gap_pct": round(float(m.get("lag_initial_gap_pct", 0)), 6),
            "max_convergence_fraction": round(float(m.get("lag_max_convergence_fraction", 0)), 6),
            "convergence_events": int(m.get("lag_convergence_events", 0)),
            "full_cycles": int(m.get("lag_full_convergence_cycles", 0)),
            "time_to_50_seconds": m.get("lag_time_to_50_seconds"),
            "time_to_80_seconds": m.get("lag_time_to_80_seconds"),
            "bbo_coverage_ratio": round(float(m.get("lag_bbo_coverage_ratio", 0.0)), 6),
            "valid_bbo_points": int(m.get("lag_valid_bbo_points", 0)),
            "version": 12,
        })
        rows[:] = [r for r in rows if isinstance(r, dict) and fnum(r.get("ts")) >= cutoff]
        if len(rows) > cfg.active_lag_profile_max_points * 2:
            del rows[: len(rows) - cfg.active_lag_profile_max_points * 2]
        changed = True
    return changed


def should_alert(symbol: str, level: str, setup: str, state: dict, cfg: Config) -> bool:
    prev = state.get("last_alerts", {}).get(symbol)
    if not prev and symbol.startswith("aster:"):
        prev = state.get("last_alerts", {}).get(symbol.split(":", 1)[1])
    if not prev: return True
    prev_level = str(prev.get("level", "NONE")).upper(); prev_setup = str(prev.get("setup", "NONE")).upper(); prev_ts = fnum(prev.get("ts"))
    if level == "CONFIRMED" and prev_level != "CONFIRMED": return True
    if level == "CONFIRMED" and setup != prev_setup: return True
    return time.time() - prev_ts >= cfg.cooldown_minutes * 60


def create_price_chart(candidate: Candidate, m: dict, cfg: Config) -> Optional[Path]:
    if len(candidate.samples) < 2: return None
    try:
        import matplotlib; matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.ticker import FuncFormatter
    except Exception as e:
        print(f"WARN: chart library unavailable: {type(e).__name__}: {e}", file=sys.stderr); return None
    label = target_label(candidate); samples = candidate.samples; t0 = samples[0].ts
    xs = [max(0, s.ts - t0) for s in samples]; fairs = [s.fair for s in samples]
    ext_bids = [s.external_bid if s.external_bid > 0 else math.nan for s in samples]
    ext_asks = [s.external_ask if s.external_ask > 0 else math.nan for s in samples]
    bids = [s.aster.bid for s in samples]; asks = [s.aster.ask for s in samples]
    out_dir = Path(cfg.charts_dir); out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{getattr(candidate,'target','aster')}_{candidate.symbol}_{m['setup']}_{int(time.time())}.png"
    fig, ax = plt.subplots(figsize=(10, 5.2), dpi=150)
    ax.plot(xs, fairs, label="Robust external PERP fair", linewidth=2.1)
    if sum(math.isfinite(x) for x in ext_bids) >= 2 and sum(math.isfinite(x) for x in ext_asks) >= 2:
        ax.fill_between(xs, ext_bids, ext_asks, alpha=0.10, label="Executable external PERP BBO consensus")
    ax.plot(xs, bids, label=f"{label} bid", linewidth=1.6); ax.plot(xs, asks, label=f"{label} ask", linewidth=1.6)
    ax.fill_between(xs, bids, asks, alpha=0.08, label=f"{label} spread")
    ex_trades = [t for t in candidate.trades if abs(t.deviation_pct) >= cfg.excursion_threshold_pct and -1 <= t.ts - t0 <= xs[-1] + 1]
    if ex_trades:
        ax.scatter([t.ts-t0 for t in ex_trades], [t.price for t in ex_trades], marker="x", s=38, label=f"{label} excursion trades vs consensus fair", zorder=5)
    if "MM" in m.get("setup", ""):
        title2 = f"{m.get('excursion_count',0)} excursions | clean {m.get('clean_reversion_rate',0)*100:.0f}% | {m.get('excursion_rate_per_minute',0):.1f}/min | median reversion {fmt_seconds(m.get('median_clean_reversion_seconds'))}"
    else:
        title2 = f"Hedgeable gross {m.get('current_executable_edge_pct',0):.3f}% {m.get('current_edge_side','NONE')} | est. net {m.get('current_net_edge_pct',0):+.3f}% | persistent {m.get('persistent_edge_side','NONE')} hit {m.get('persistent_exec_hit_ratio',0)*100:.0f}%"
    ax.set_title(f"{candidate.symbol} @ {label} | {m['setup']} | ~{xs[-1]:.0f}s\n{title2}")
    ax.set_xlabel("Seconds from start of confirmation window"); ax.set_ylabel("Price")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _p: fmt_price(float(y))))
    ax.grid(True, alpha=.20); ax.legend(loc="best"); fig.tight_layout(); fig.savefig(out, bbox_inches="tight"); plt.close(fig)
    return out


def send_telegram_photo(path: Path, candidate: Candidate, m: dict) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip(); chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id: return False
    duration = candidate.samples[-1].ts - candidate.samples[0].ts if len(candidate.samples) >= 2 else 0.0
    label = target_label(candidate)
    if "MM" in m.get("setup", ""):
        caption = (
            f"📈 <b>{candidate.symbol} @ {label} — {m['setup']}</b>\n"
            f"Observed <b>{duration:.0f}s</b> | excursions <b>{m['excursion_count']}</b> | clean <b>{m['clean_reverted_count']}/{m['excursion_count']} ({m['clean_reversion_rate']*100:.0f}%)</b>\n"
            f"Rate {m.get('excursion_rate_per_minute',0):.1f}/min | median peak {m['median_peak_excursion_pct']:.3f}% | median reversion {fmt_seconds(m['median_clean_reversion_seconds'])}\n"
            f"ABOVE {m.get('above_excursions',0)} / BELOW {m.get('below_excursions',0)} | longest quiet {m.get('longest_quiet_seconds',0):.1f}s | last excursion {m.get('last_excursion_age_seconds',0):.1f}s ago\n"
            "V13 MM statistics use actual target-venue trades matched to the time-local robust PERP consensus."
        )
    else:
        caption = (
            f"📈 <b>{candidate.symbol} @ {label} — {m['setup']}</b>\n"
            f"~{duration:.0f}s | target BBO vs executable external consensus\n"
            f"Gross hedgeable edge: <b>{m['current_executable_edge_pct']:.3f}% {m['current_edge_side']}</b> | est. net {m['current_net_edge_pct']:+.3f}%\n"
            f"Initial gap {m.get('lag_initial_gap_pct',m['current_executable_edge_pct']):.3f}% {m.get('lag_detection_side',m['current_edge_side'])} | max convergence {m.get('lag_max_convergence_fraction',0)*100:.0f}% | events {m.get('lag_convergence_events',0)}\n"
            f"Time to 50% {fmt_seconds(m.get('lag_time_to_50_seconds'))} | time to 80% {fmt_seconds(m.get('lag_time_to_80_seconds'))} | BBO coverage {m.get('lag_bbo_coverage_ratio',0)*100:.0f}%"
        )
    try:
        with path.open("rb") as fh:
            r = requests.post(f"https://api.telegram.org/bot{token}/sendPhoto", data={"chat_id":chat_id,"caption":caption,"parse_mode":"HTML"}, files={"photo":(path.name,fh,"image/png")}, timeout=20, headers=HTTP_HEADERS)
        r.raise_for_status(); return bool(r.json().get("ok"))
    except Exception as e:
        print(f"ERROR sending Telegram chart: {type(e).__name__}: {e}", file=sys.stderr); return False


def format_alert(candidate: Candidate, m: dict) -> str:
    label = target_label(candidate); refs = ", ".join(m.get("current_refs", [])); exec_refs = ", ".join(m.get("current_exec_refs", [])) or "unavailable"
    side = m.get("current_edge_side", "NONE"); side_text = "none" if side == "NONE" else side
    persistent = m.get("persistent_edge_side", "NONE")
    lag_block = ""
    if "CONVERGING-LAG" in m.get("setup", ""):
        base = m.get("lag_baseline_median_gap_pct")
        if m.get("lag_baseline_ready") and base is not None:
            req = m.get("lag_baseline_required_gap_pct")
            base_txt = f"median {base:.3f}% ({m.get('lag_baseline_points',0)} pts, READY; required {(f'{req:.3f}%' if req is not None else 'n/a')})"
        elif base is not None:
            base_txt = f"median {base:.3f}% ({m.get('lag_baseline_points',0)} pts, BOOTSTRAP)"
        else:
            base_txt = f"bootstrap / insufficient ({m.get('lag_baseline_points',0)} pts)"
        lag_block = (
            f"LAG extended observation: <b>{m.get('lag_extended_observed_seconds',0):.0f}s</b> | side <b>{m.get('lag_detection_side','NONE')}</b>\n"
            f"Initial gross gap: <b>{m.get('lag_initial_gap_pct',0):.3f}%</b> | minimum {m.get('lag_min_gap_pct',0):.3f}% | current same-side {m.get('lag_current_gap_pct',0):.3f}%\n"
            f"Max convergence: <b>{m.get('lag_max_convergence_fraction',0)*100:.0f}%</b> | time 50% {fmt_seconds(m.get('lag_time_to_50_seconds'))} | time 80% {fmt_seconds(m.get('lag_time_to_80_seconds'))}\n"
            f"Convergence events: <b>{m.get('lag_convergence_events',0)}</b> | full cycles {m.get('lag_full_convergence_cycles',0)} | baseline {base_txt}\n"
            f"Executable BBO coverage: <b>{m.get('lag_valid_bbo_points',0)}/{m.get('lag_total_bbo_points',0)} ({m.get('lag_bbo_coverage_ratio',0)*100:.0f}%)</b> | re-expanded after convergence: {'YES' if m.get('lag_reexpanded_after_convergence') else 'NO'}\n"
        )
    mm_block = ""
    if "MM" in m.get("setup", ""):
        mm_block = (
            f"MM observed: <b>{m.get('observed_seconds',0):.0f}s</b> | excursions <b>{m.get('excursion_count',0)}</b> | clean {m.get('clean_reverted_count',0)}/{m.get('excursion_count',0)} (<b>{m.get('clean_reversion_rate',0)*100:.0f}%</b>)\n"
            f"Excursion rate: {m.get('excursion_rate_per_minute',0):.1f}/min | ABOVE {m.get('above_excursions',0)} / BELOW {m.get('below_excursions',0)}\n"
            f"Median peak {m.get('median_peak_excursion_pct',0):.3f}% | P90 {m.get('p90_peak_excursion_pct',0):.3f}% | max {m.get('max_peak_excursion_pct',0):.3f}%\n"
            f"Median clean reversion {fmt_seconds(m.get('median_clean_reversion_seconds'))} | longest quiet {m.get('longest_quiet_seconds',0):.1f}s | last excursion {m.get('last_excursion_age_seconds',0):.1f}s ago\n"
        )
    hedge_hint = ""
    if m.get("best_hedge_venue"):
        action = "BUY/LONG" if persistent == "SHORT" else "SELL/SHORT"
        hedge_hint = f"Best visible external hedge: <b>{action} {m['best_hedge_venue']}</b> @ {fmt_price(m['best_hedge_price'])} | best-case gross {m['current_best_case_hedge_edge_pct']:.3f}%\n"
    spot_fair = m.get("current_spot_fair")
    spot = "SPOT fair: unavailable\n" if spot_fair is None else f"SPOT fair (diagnostic): {fmt_price(spot_fair)} [{', '.join(m.get('current_spot_refs',[]))}]\n"
    return (
        f"🔥 <b>{m['level']} / {m['setup']} — {candidate.symbol} @ {label}</b>\n"
        f"Signal strength: <b>{m['score']:.0f}/100</b> | LAG {m['lag_score']:.0f} | MM {m['mm_score']:.0f}\n"
        f"Target {label} PERP: {fmt_price(m['current_target_bid'])} / {fmt_price(m['current_target_ask'])}\n"
        f"Robust PERP consensus fair: {fmt_price(m['current_fair'])} [{refs}]\n"
        f"Executable external BBO consensus: <b>{fmt_price(m['current_external_bid'])} / {fmt_price(m['current_external_ask'])}</b> [{exec_refs}]\n"
        f"Gross hedgeable edge now: <b>{m['current_executable_edge_pct']:.3f}% {side_text}</b> | fee reserve {m['estimated_roundtrip_fees_pct']:.3f}% | est. net <b>{m['current_net_edge_pct']:+.3f}%</b>\n"
        f"BBO capacity (target/external): {m.get('target_bbo_capacity_usd',0):,.0f} / {m.get('external_bbo_capacity_usd',0):,.0f} USD | checks {', '.join('$'+k+':'+v for k,v in m.get('bbo_notional_checks',{}).items()) or 'unknown'}\n"
        f"Persistent side: <b>{persistent}</b> | hit {m['persistent_exec_hit_ratio']*100:.0f}% | median gross {m['persistent_median_executable_edge_pct']:.3f}% | median net {m['persistent_median_net_edge_pct']:+.3f}%\n"
        f"{hedge_hint}{lag_block}{mm_block}"
        f"Relative target-vs-consensus-mid edge (diagnostic): {m['current_relative_edge_pct']:.3f}% {m['current_relative_edge_side']}\n"
        f"Target midpoint deviation: {m['current_deviation_pct']:+.3f}% | fair disagreement {m['current_ref_disagreement_pct']:.3f}%\n"
        f"{spot}"
        f"Target spread: median <b>{m['median_spread_pct']:.3f}%</b> | max {m['max_spread_pct']:.3f}%\n"
        f"Target trades analyzed: <b>{m['trades_analyzed']}</b> | excursions {m['excursion_count']} | clean {m['clean_reverted_count']}/{m['excursion_count']} ({m['clean_reversion_rate']*100:.0f}%)\n"
        f"Target 24h: {candidate.move24h_pct:+.1f}% | quote vol {candidate.quote_volume24h:,.0f}\n"
        f"State: <b>{m['direction']}</b>\n\n"
        "V13 scans Aster, Hyperliquid and Lighter against a robust PERP consensus, keeps execution-readiness checks, and follows ACTIVE-LAG outcomes for up to 24h with funding carry. Signal only; no orders are sent."
    )


def format_active_signal(event: dict, state: dict) -> str:
    typ = str(event.get("type", "")); symbol = str(event.get("symbol", "")); label = str(event.get("target_label", "Aster")); skey = str(event.get("state_key", symbol))
    stats = state.get("signal_stats", {}).get(typ, {}); total = int(fnum(stats.get("total"))) if isinstance(stats, dict) else 0
    per_target = int(fnum(stats.get("symbols", {}).get(skey))) if isinstance(stats, dict) else 0
    if typ == "ACTIVE-LAG":
        side = str(event.get("side", "NONE")); pair = f"LONG {label} / SHORT external consensus" if side == "LONG" else f"SHORT {label} / LONG external consensus"
        baseline = event.get("baseline_median_gap_pct")
        bp = int(event.get("baseline_points", 0)); br = event.get("baseline_required_gap_pct")
        if baseline is None:
            btxt = f"bootstrap / insufficient ({bp} pts)"
        elif bp < int(event.get("baseline_min_points", 3)):
            btxt = f"{float(baseline):.3f}% ({bp} pts, BOOTSTRAP)"
        else:
            btxt = f"{float(baseline):.3f}% ({bp} pts, READY; required {(f'{float(br):.3f}%' if br is not None else 'n/a')})"
        exec_status = str(event.get("execution_status", "UNKNOWN"))
        exec_ready = bool(event.get("execution_ready", False))
        exec_checks = event.get("execution_checks", {}) if isinstance(event.get("execution_checks"), dict) else {}
        exec_parts = []
        for k in sorted(exec_checks, key=lambda x: fnum(x)):
            row = exec_checks.get(k) or {}
            status = str(row.get("status", "UNKNOWN"))
            netv = row.get("net_edge_pct")
            venue = row.get("external_venue")
            suffix = f" net {float(netv):+.3f}%" if netv is not None else ""
            if venue:
                suffix += f" via {venue}"
            exec_parts.append(f"${k}:{status}{suffix}")
        exec_txt = " | ".join(exec_parts) if exec_parts else "no depth result"
        fresh_gross = event.get("execution_fresh_gross_edge_pct")
        fresh_net = event.get("execution_fresh_net_edge_pct")
        fresh_txt = "n/a" if fresh_gross is None else f"{float(fresh_gross):.3f}% gross / {float(fresh_net):+.3f}% net"
        return (
            f"⚡ <b>ACTIVE-LAG — {symbol} @ {label}</b>\n"
            f"Side: <b>{side}</b> | pair interpretation: {pair}\n"
            f"Current gross gap: <b>{float(event.get('gross_edge_pct',0)):.3f}%</b> | est. net <b>{float(event.get('net_edge_pct',0)):+.3f}%</b>\n"
            f"Target bid/ask: {fmt_price(float(event.get('target_bid',0)))} / {fmt_price(float(event.get('target_ask',0)))}\n"
            f"External BBO consensus: {fmt_price(float(event.get('external_bid',0)))} / {fmt_price(float(event.get('external_ask',0)))}\n"
            f"Persistent hit {float(event.get('persistent_hit_ratio',0))*100:.0f}% | median gross {float(event.get('persistent_median_edge_pct',0)):.3f}% | fair disagreement {float(event.get('reference_disagreement_pct',0)):.3f}%\n"
            f"Prior VERIFIED episodes: <b>{int(event.get('verified_episodes',0))}</b> | median max convergence {float(event.get('median_max_convergence_fraction',0))*100:.0f}% | full cycles {int(event.get('total_full_cycles',0))}\n"
            f"Historical median time to 50%: {fmt_seconds(event.get('median_time_to_50_seconds'))} | baseline {btxt}\n"
            f"Trigger: <b>{event.get('trigger','historical-profile')}</b> | BBO coverage {float(event.get('bbo_coverage_ratio',0))*100:.0f}%\n"
            f"Execution readiness @ ${float(event.get('execution_min_notional_usd',0)):.0f}: <b>{'READY' if exec_ready else exec_status}</b> | fresh {fresh_txt}\n"
            f"Depth checks: {exec_txt}\n"
            f"Top-level capacity target/external: {float(event.get('execution_target_top_capacity_usd',0)):,.0f} / {float(event.get('execution_external_top_capacity_usd',0)):,.0f} USD | probe {float(event.get('execution_probe_seconds',0)):.2f}s\n"
            f"Funding tracking: target {side} / hedge {('LONG' if side=='SHORT' else 'SHORT')} via <b>{event.get('preferred_hedge_venue') or event.get('execution_best_external_venue') or 'unknown'}</b> | horizon up to 24h\n"
            f"V13 counter: {skey} detections <b>{per_target}</b> | all targets <b>{total}</b>\n\nSignal only. No API orders are sent."
        )
    if typ == "ACTIVE-MM-EXCURSION":
        status = "already reverted by poll time" if event.get("latest_reverted") else "still unresolved at poll time"
        return (
            f"⚡ <b>ACTIVE-MM-EXCURSION — {symbol} @ {label}</b>\n"
            f"New qualifying excursions in this batch: <b>{int(event.get('event_count',0))}</b>\n"
            f"LONG-side: {int(event.get('long_count',0))} | SHORT-side: {int(event.get('short_count',0))}\n"
            f"Median peak deviation: <b>{float(event.get('median_peak_pct',0)):.3f}%</b> | max {float(event.get('max_peak_pct',0)):.3f}%\n"
            f"Latest inferred side: <b>{event.get('latest_action','NONE')}</b> | age ~{float(event.get('latest_event_age_seconds',0)):.1f}s | {status}\n"
            f"At detection: taker edge {('n/a' if event.get('latest_taker_edge_pct') is None else f"{float(event.get('latest_taker_edge_pct')):.3f}%")} | maker-quote edge {('n/a' if event.get('latest_maker_quote_edge_pct') is None else f"{float(event.get('latest_maker_quote_edge_pct')):.3f}%")}\n"
            f"BBO capacity target/external: {float(event.get('latest_target_bbo_capacity_usd',0)):,.0f} / {float(event.get('latest_external_bbo_capacity_usd',0)):,.0f} USD\n"
            f"Regime before extended stage: {int(event.get('regime_excursions_before_extended',0))} excursions | clean {float(event.get('regime_clean_reversion_rate',0))*100:.0f}% | median reversion {fmt_seconds(event.get('regime_median_reversion_seconds'))}\n"
            f"V13 counter: {skey} ACTIVE-MM events <b>{per_target}</b> | all targets <b>{total}</b>\n\nEvents are batched; no orders are sent."
        )
    return f"⚡ <b>{typ} — {symbol} @ {label}</b>\nSignal only. No orders are sent."


def print_forward_outcome_stats(state: dict) -> None:
    stats = state.get("outcome_stats", {}) if isinstance(state, dict) else {}
    print("\nForward outcome stats:")
    lag = stats.get("ACTIVE-LAG", {}) if isinstance(stats, dict) else {}
    mm = stats.get("ACTIVE-MM-EXCURSION", {}) if isinstance(stats, dict) else {}
    open_lag = [r for r in state.get("active_lag_outcomes", {}).values() if isinstance(r, dict) and r.get("status") == "OPEN"]
    print(
        f" - ACTIVE-LAG: started={int(fnum(lag.get('total_started')))} open={len(open_lag)} "
        f"t50={int(fnum(lag.get('t50')))} t80={int(fnum(lag.get('t80')))} "
        f"full={int(fnum(lag.get('full')))} not-full/expired={int(fnum(lag.get('stale')))}"
    )
    # Show how many open/closed outcomes have reached each research horizon.
    all_lag = [r for r in state.get("active_lag_outcomes", {}).values() if isinstance(r, dict)]
    for label in ("5m","30m","2h","6h","24h"):
        reached = sum(label in (r.get("horizons", {}) if isinstance(r.get("horizons"), dict) else {}) for r in all_lag)
        if reached:
            print(f"   horizon {label}: captured={reached}")
    print(
        f" - ACTIVE-MM: started={int(fnum(mm.get('total_started')))} "
        f"clean={int(fnum(mm.get('clean_reverted')))} "
        f"reverted-not-clean={int(fnum(mm.get('reverted_not_clean')))} "
        f"stale={int(fnum(mm.get('stale')))}"
    )


def print_table(ranked: List[Tuple[Candidate, dict]]) -> None:
    if not ranked:
        print("No candidates passed the prefilter."); return
    print("\nTop results:")
    print(f"{'TARGET':<12} {'SYMBOL':<15} {'LEVEL':<10} {'SETUP':<24} {'SCORE':>5} {'SPR%':>7} {'HEDGE%':>8} {'NET%':>8} {'EXC':>4} {'C.REV%':>7}")
    for c, m in ranked[:30]:
        print(f"{target_label(c):<12} {c.symbol:<15} {m['level']:<10} {m['setup']:<24} {m['score']:>5.0f} {m['median_spread_pct']:>7.3f} {m['max_executable_edge_pct']:>8.3f} {m['max_net_edge_pct']:>8.3f} {m['excursion_count']:>4} {m['clean_reversion_rate']*100:>7.0f}")



def self_test() -> None:
    cfg = Config(
        min_aster_spread_pct=0.15,
        min_deviation_pct=0.20,
        min_executable_edge_pct=0.15,
        min_current_executable_edge_pct=0.15,
        min_hedgeable_edge_pct=0.20,
        min_current_hedgeable_edge_pct=0.20,
        min_net_hedgeable_edge_pct=0.05,
        min_hedgeable_edge_hit_ratio=0.60,
        estimated_roundtrip_fees_pct=0.20,
        min_24h_move_pct=8.0,
        min_quote_volume24h=50000,
        min_reference_exchanges=2,
        max_reference_disagreement_pct=0.20,
        min_spot_reference_exchanges=2,
        max_spot_reference_disagreement_pct=0.30,
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

    def mk_sample(
        ts: float,
        aq: Quote,
        refs: Dict[str, Quote],
        *,
        fair: Optional[float] = None,
        spot_fair: Optional[float] = None,
        spot_refs: Optional[Dict[str, Quote]] = None,
    ) -> MarketSample:
        mids = [q.mid for q in refs.values()]
        fair_value = fair if fair is not None else statistics.median(mids)
        ext_bid = statistics.median([q.bid for q in refs.values()])
        ext_ask = statistics.median([q.ask for q in refs.values()])
        best_bid_venue, best_bid_q = max(refs.items(), key=lambda kv: kv[1].bid)
        best_ask_venue, best_ask_q = min(refs.items(), key=lambda kv: kv[1].ask)
        dev = (aq.mid - fair_value) / fair_value * 100.0
        rel_short = (aq.bid - fair_value) / fair_value * 100.0
        rel_long = (fair_value - aq.ask) / fair_value * 100.0
        hedge_short = (aq.bid - ext_ask) / ext_ask * 100.0
        hedge_long = (ext_bid - aq.ask) / ext_bid * 100.0
        best_short = (aq.bid - best_ask_q.ask) / best_ask_q.ask * 100.0
        best_long = (best_bid_q.bid - aq.ask) / best_bid_q.bid * 100.0
        disagreement = (max(mids) - min(mids)) / fair_value * 100.0
        return MarketSample(
            ts=ts,
            aster=aq,
            fair=fair_value,
            refs=refs,
            ref_disagreement_pct=disagreement,
            deviation_pct=dev,
            short_edge_pct=rel_short,
            long_edge_pct=rel_long,
            external_bid=ext_bid,
            external_ask=ext_ask,
            best_external_bid=best_bid_q.bid,
            best_external_ask=best_ask_q.ask,
            best_external_bid_venue=best_bid_venue,
            best_external_ask_venue=best_ask_venue,
            hedge_short_edge_pct=hedge_short,
            hedge_long_edge_pct=hedge_long,
            best_hedge_short_edge_pct=best_short,
            best_hedge_long_edge_pct=best_long,
            spot_fair=spot_fair,
            spot_refs=spot_refs or {},
            spot_ref_disagreement_pct=0.0 if spot_fair is not None else None,
        )

    tight_refs = {
        "bitget-perp": Quote(9.999, 10.001),
        "mexc-perp": Quote(9.998, 10.002),
    }

    # 1) Repeated clean Aster trade excursions should confirm MM even without LAG.
    mm = Candidate("MMTESTUSDT", 22.0, 1_500_000)
    devs = [0.05, 0.26, 0.04, -0.28, -0.03, 0.31, 0.02, 0.22, 0.01]
    for i, dev in enumerate(devs):
        fair = 10.0
        mid = fair * (1 + dev / 100)
        width = mid * 0.22 / 100
        aq = Quote(mid - width / 2, mid + width / 2)
        mm.samples.append(mk_sample(now + i, aq, tight_refs, fair=fair))
    trade_devs = [0.02, 0.27, 0.29, 0.04, -0.25, -0.31, -0.02, 0.24, 0.03]
    mm.trades = [TradePoint(now + i, 10 * (1 + d / 100), 100, 10.0, d) for i, d in enumerate(trade_devs)]
    mm.excursions = detect_excursions(mm.trades, cfg)
    mm_m = mm.metrics(cfg)
    assert len(mm.excursions) >= 3, mm.excursions
    assert mm_m["clean_reversion_rate"] >= 0.70, mm_m
    assert mm_m["setup"] in {"CONFIRMED-MM", "CONFIRMED-BOTH"}, mm_m
    assert mm_m["level"] == "CONFIRMED", mm_m

    # 2) Genuine hedgeable SHORT: Aster bid is 0.40% above median external ask.
    lag = Candidate("LAGTESTUSDT", 25.0, 2_000_000)
    # External median BBO ~ 9.999 / 10.0015.
    for i in range(12):
        aq = Quote(10.042, 10.052)
        lag.samples.append(mk_sample(now + i, aq, tight_refs, fair=10.0))
    lag.trades = [TradePoint(now + i, 10.045, 10, 10.0, 0.45) for i in range(5)]
    lag.excursions = detect_excursions(lag.trades, cfg)
    lag_m = lag.metrics(cfg)
    assert lag_m["setup"] in {"CONFIRMED-LAG", "CONFIRMED-BOTH"}, lag_m
    assert lag_m["persistent_edge_side"] == "SHORT", lag_m
    assert lag_m["persistent_exec_hit_ratio"] >= 0.99, lag_m
    assert lag_m["current_executable_edge_pct"] > 0.35, lag_m
    assert lag_m["current_net_edge_pct"] > 0.15, lag_m
    assert lag_m["level"] == "CONFIRMED", lag_m

    # 3) CYS-like false positive: Aster bid is above midpoint fair by ~0.15%,
    # but after crossing the external ask and reserving round-trip fees the net is negative.
    thin = Candidate("THINTESTUSDT", -10.0, 8_000_000)
    refs_wider = {
        "bitget-perp": Quote(9.990, 10.010),
        "mexc-perp": Quote(9.992, 10.008),
    }
    for i in range(12):
        # Aster bid 10.015 looks +0.15% vs mid fair 10.0,
        # but median external ask is 10.009 -> gross hedgeable only ~0.06%.
        aq = Quote(10.015, 10.025)
        thin.samples.append(mk_sample(now + i, aq, refs_wider, fair=10.0))
    thin.trades = [TradePoint(now + i, 10.02, 10, 10.0, 0.20) for i in range(10)]
    thin.excursions = detect_excursions(thin.trades, cfg)
    thin_m = thin.metrics(cfg)
    assert thin_m["current_relative_edge_pct"] > 0.10, thin_m
    assert thin_m["current_executable_edge_pct"] < 0.10, thin_m
    assert thin_m["current_net_edge_pct"] < 0.0, thin_m
    assert thin_m["setup"] == "NONE", thin_m

    # 4) LA-like false positive: huge Aster spread, midpoint far from fair, zero trades.
    bad = Candidate("WIDETESTUSDT", 10.0, 100_000)
    for i in range(12):
        aq = Quote(9.80, 9.99)
        bad.samples.append(mk_sample(now + i, aq, tight_refs, fair=10.0))
    bad_m = bad.metrics(cfg)
    assert bad_m["setup"] == "NONE", bad_m
    assert bad_m["level"] == "NONE", bad_m

    # 5) Large PERP/SPOT basis must not create a signal when Aster agrees with external PERPs.
    basis = Candidate("BASISTESTUSDT", 50.0, 3_000_000)
    spot_refs = {
        "bitget-spot": Quote(10.399, 10.401),
        "mexc-spot": Quote(10.398, 10.402),
    }
    for i in range(12):
        aq = Quote(9.995, 10.005)
        basis.samples.append(
            mk_sample(
                now + i,
                aq,
                tight_refs,
                fair=10.0,
                spot_fair=10.4,
                spot_refs=spot_refs,
            )
        )
    basis.trades = [TradePoint(now + i, 10.0, 10, 10.0, 0.0) for i in range(5)]
    basis_m = basis.metrics(cfg)
    assert basis_m["setup"] == "NONE", basis_m
    assert basis_m["level"] == "NONE", basis_m
    assert basis_m["external_perp_spot_basis_pct"] < -3.0, basis_m

    # 6) V9 keeps the V8 rule: an initial LAG must actually converge during extended observation.
    conv = Candidate("CONVTESTUSDT", 20.0, 2_000_000)
    initial_q = Quote(10.042, 10.052)
    for i in range(8):
        conv.samples.append(mk_sample(now + i, initial_q, tight_refs, fair=10.0))
    conv.trades = [TradePoint(now + i, 10.045, 10, 10.0, 0.45) for i in range(5)]
    conv.excursions = detect_excursions(conv.trades, cfg)
    pre = conv.metrics(cfg)
    assert pre["raw_lag_confirmed"], pre
    conv.lag_verification_status = "done"
    conv.lag_initial_confirmed = True
    conv.lag_detection_sample_index = len(conv.samples) - 1
    conv.lag_detection_side = "SHORT"
    conv.lag_initial_edge_pct = pre["current_executable_edge_pct"]
    conv.lag_initial_score = pre["lag_score"]
    # Make extended observation long enough and shrink the same-side gap >80%.
    for j in range(1, 130):
        edge_frac = max(0.05, 1.0 - j / 20.0)
        # external ask ~10.0015; build Aster bid from desired gross edge.
        target_edge = conv.lag_initial_edge_pct * edge_frac
        ext_ask = statistics.median([q.ask for q in tight_refs.values()])
        bid = ext_ask * (1 + target_edge / 100.0)
        aq = Quote(bid, bid + 0.005)
        conv.samples.append(mk_sample(now + 7 + j, aq, tight_refs, fair=10.0))
    conv_m = conv.metrics(cfg)
    assert conv_m["lag_max_convergence_fraction"] >= 0.80, conv_m
    assert conv_m["lag_convergence_events"] >= 1, conv_m
    assert conv_m["setup"] == "CONVERGING-LAG", conv_m

    # V11 same-run ACTIVE-LAG: after convergence, a fresh re-expansion in the
    # same observation window can become ACTIVE without waiting for another run.
    ext_ask = statistics.median([q.ask for q in tight_refs.values()])
    for j in range(5):
        bid = ext_ask * (1 + 0.40 / 100.0)
        conv.samples.append(mk_sample(now + 200 + j, Quote(bid, bid + 0.005), tight_refs, fair=10.0))
    conv_m2 = conv.metrics(cfg)
    assert conv_m2["lag_reexpanded_after_convergence"], conv_m2
    sr_state = {"lag_baseline": {}, "lag_verified_profiles": {}, "active_signal_open": {}, "signal_stats": {}, "active_lag_outcomes": {}, "outcome_stats": {}}
    sr_events, sr_changed = detect_same_run_active_lag_events([conv], cfg, sr_state)
    assert sr_changed and len(sr_events) == 1, sr_events
    assert sr_events[0]["trigger"] == "same-run verified re-expansion", sr_events[0]

    # 7) V11 ACTIVE-LAG requires PRIOR verified history and only fires on a new
    # transition into an active state. Repeated scans while the same state remains
    # open do not count as a new activation.
    active_state = {
        "lag_verified_profiles": {
            "LAGTESTUSDT": [
                {
                    "ts": int(now),
                    "detection_ts": int(now - 60),
                    "side": "SHORT",
                    "initial_gap_pct": 0.42,
                    "max_convergence_fraction": 0.82,
                    "convergence_events": 2,
                    "full_cycles": 1,
                    "time_to_50_seconds": 35.0,
                    "time_to_80_seconds": 80.0,
                    "bbo_coverage_ratio": 1.0,
                    "valid_bbo_points": 30,
                    "version": 12,
                }
            ]
        },
        "lag_baseline": {},
        "active_signal_open": {},
        "signal_stats": {},
    }
    active_events, active_changed = detect_active_lag_events([lag], cfg, active_state)
    assert active_changed and len(active_events) == 1, active_events
    assert active_events[0]["type"] == "ACTIVE-LAG", active_events[0]
    assert active_events[0]["side"] == "SHORT", active_events[0]
    active_events_again, _ = detect_active_lag_events([lag], cfg, active_state)
    assert active_events_again == [], active_events_again

    # V11 regression: one missing executable BBO snapshot must be ignored,
    # never interpreted as -infinity / perfect convergence.
    bad = Candidate("BADBBOUSDT", 10.0, 1_000_000)
    bad.target = "aster"
    for i in range(30):
        s = mk_sample(now + 500 + i, Quote(10.04, 10.05), tight_refs, fair=10.0)
        if i == 15:
            s.external_bid = 0.0
            s.external_ask = 0.0
            s.hedge_short_edge_pct = -math.inf
            s.hedge_long_edge_pct = -math.inf
        bad.samples.append(s)
    bad.lag_detection_sample_index = 0
    bad.lag_detection_side = "SHORT"
    bad.lag_initial_edge_pct = bad.samples[0].hedge_short_edge_pct
    bad.lag_initial_confirmed = True
    bad.lag_verification_status = "done"
    bm = bad.metrics(cfg)
    assert math.isfinite(bm["lag_min_gap_pct"]), bm
    assert bm["lag_min_gap_pct"] > 0.0, bm
    assert bm["lag_valid_bbo_points"] == 29, bm
    assert 0.95 < bm["lag_bbo_coverage_ratio"] < 1.0, bm

    assert valid_quote("1", "1.01") is not None
    assert valid_quote("1.01", "1") is None

    # V10 robust consensus trims one stale venue instead of rejecting the symbol.
    multi_refs = {
        "bitget-perp": {"BTCUSDT": Quote(99.99, 100.01)},
        "mexc-perp": {"BTCUSDT": Quote(100.00, 100.02)},
        "bybit-perp": {"BTCUSDT": Quote(99.98, 100.00)},
        "bad-perp": {"BTCUSDT": Quote(130.0, 130.1)},
    }
    rr = robust_reference_for_symbol("BTCUSDT", multi_refs, 3, 0.20)
    assert rr is not None and "bad-perp" not in rr[1], rr
    tmp_target = Candidate("BTCUSDT", 10.0, 1_000_000)
    tmp_target.target = "hyperliquid"
    assert candidate_state_key(tmp_target) == "hyperliquid:BTCUSDT"
    # V12 execution-readiness math: depth VWAP must include slippage and refuse
    # notionals larger than the available public depth.
    test_book = {"bids": [(101.0, 1.0), (100.0, 2.0)], "asks": [(102.0, 1.0), (103.0, 2.0)]}
    sell100 = _simulate_vwap(test_book, "SELL", 100.0)
    buy250 = _simulate_vwap(test_book, "BUY", 250.0)
    buy1000 = _simulate_vwap(test_book, "BUY", 1000.0)
    assert sell100["filled"] and abs(sell100["vwap"] - 101.0) < 1e-9, sell100
    assert buy250["filled"] and buy250["vwap"] > 102.0, buy250
    assert not buy1000["filled"], buy1000

    # V13 funding sign convention: positive funding pays SHORT and charges LONG.
    assert abs(_side_funding_pnl_pct("SHORT", [0.01, -0.02]) - (-0.01)) < 1e-12
    assert abs(_side_funding_pnl_pct("LONG", [0.01, -0.02]) - (0.01)) < 1e-12

    # V13 strict horizon cutoff: a quote arriving AFTER the configured outcome
    # window must never retroactively create t50/t80/full.
    cfg_cut = replace(
        cfg,
        active_lag_outcome_max_age_minutes=1.0,
        active_lag_outcome_horizons_minutes=(0.5, 1.0),
        funding_tracking_enabled=False,
        active_lag_horizon_alerts_enabled=False,
    )
    cutoff_state = {"active_lag_outcomes": {}, "outcome_stats": {}}
    cutoff_event = {
        "state_key": "aster:CUTOFFUSDT", "symbol": "CUTOFFUSDT", "target": "aster",
        "target_label": "Aster", "side": "SHORT", "ts": now, "gross_edge_pct": 0.40,
    }
    start_active_lag_outcome(cutoff_state, cutoff_event, cfg_cut)
    cutoff_c = Candidate("CUTOFFUSDT", 0.0, 0.0); cutoff_c.target = "aster"
    cutoff_c.samples = [mk_sample(now + 120, Quote(10.0, 10.001), tight_refs, fair=10.0)]
    cutoff_events, _ = update_active_lag_outcomes(cutoff_state, [cutoff_c], cfg_cut, now_ts=now + 120)
    cutoff_row = next(iter(cutoff_state["active_lag_outcomes"].values()))
    assert cutoff_row.get("t50") is None and cutoff_row.get("full_ts") is None, cutoff_row
    assert cutoff_row.get("status") == "NO_DATA_24H", cutoff_row
    assert any(e.get("type") == "ACTIVE-LAG-OUTCOME" for e in cutoff_events), cutoff_events
    print(
        "Self-test OK (V13 multi-target MM/LAG + 24h ACTIVE-LAG horizons + settled funding carry + execution depth + baseline/sanity)"
    )


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
    state = load_state()

    def active_signal_callback(event: dict) -> None:
        etype = str(event.get("type", ""))
        is_outcome = etype.endswith("OUTCOME") or etype == "ACTIVE-LAG-CHECKPOINT"
        text = format_outcome_signal(event, state) if is_outcome else format_active_signal(event, state)
        if args.dry_run:
            print("\nDRY ACTIVE SIGNAL:\n", text.replace("<b>", "").replace("</b>", ""))
            return
        if text and send_telegram(text):
            print(f"Telegram active signal sent: {event.get('type')} {event.get('symbol')} @ {event.get('target_label', 'Aster')}")

    try:
        ranked, errors, scan_state_changed = scan(
            cfg, state, active_signal_callback=active_signal_callback
        )
    except ApiError as e:
        print(f"FATAL: {e}", file=sys.stderr)
        return 2

    print_table(ranked)
    print_forward_outcome_stats(state)
    if errors:
        unique_errors = list(dict.fromkeys(errors))
        print("\nWarnings:")
        for e in unique_errors[:20]:
            print(" -", e)

    # Cross-run state is persisted even when no final CONFIRMED Telegram alert is sent.
    # V11 persists verified convergence profiles and active-signal counters/latches.
    state_changed = bool(scan_state_changed)
    state_changed = update_lag_baseline_state(state, [c for c, _ in ranked], cfg) or state_changed
    state_changed = update_lag_verified_profile_state(state, [c for c, _ in ranked], cfg) or state_changed
    for candidate, m in ranked:
        level = m["level"]
        if level not in cfg.alert_levels:
            continue
        if not should_alert(candidate_state_key(candidate), level, m["setup"], state, cfg):
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

            state.setdefault("last_alerts", {})[candidate_state_key(candidate)] = {
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
