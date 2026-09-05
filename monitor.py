#!/usr/bin/env python3
"""Aster cross-venue algo scanner v9.

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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests


# Correct current Aster futures REST host. All hosts are overrideable from Actions env.
ASTER_BASE = os.getenv("ASTER_BASE_URL", "https://fapi.asterdex.com").rstrip("/")
BYBIT_BASE = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com").rstrip("/")
BITGET_BASE = os.getenv("BITGET_BASE_URL", "https://api.bitget.com").rstrip("/")
MEXC_BASE = os.getenv("MEXC_BASE_URL", "https://api.mexc.com").rstrip("/")

USER_AGENT = "aster-algo-scanner/9.0"
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
    # SHORT Aster -> sell Aster bid, buy external median ask.
    # LONG Aster  -> buy Aster ask, sell external median bid.
    hedge_short_edge_pct: float = 0.0
    hedge_long_edge_pct: float = 0.0

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

        # V6 LAG confirmation is based on ROBUST HEDGEABLE BBO edges:
        # SHORT = Aster bid above median external PERP ask.
        # LONG  = Aster ask below median external PERP bid.
        #
        # This is stricter than comparing Aster bid/ask with external midpoint fair.
        long_hit_ratio = sum(
            x >= cfg.min_hedgeable_edge_pct
            and (x - cfg.estimated_roundtrip_fees_pct) >= cfg.min_net_hedgeable_edge_pct
            for x in hedge_long_edges
        ) / len(hedge_long_edges)
        short_hit_ratio = sum(
            x >= cfg.min_hedgeable_edge_pct
            and (x - cfg.estimated_roundtrip_fees_pct) >= cfg.min_net_hedgeable_edge_pct
            for x in hedge_short_edges
        ) / len(hedge_short_edges)

        if long_hit_ratio > short_hit_ratio:
            lag_side = "LONG"
            side_edges = hedge_long_edges
            persistent_exec_hit_ratio = long_hit_ratio
            current_side_edge = current.hedge_long_edge_pct
            current_best_case_edge = current.best_hedge_long_edge_pct
            best_hedge_venue = current.best_external_bid_venue
            best_hedge_price = current.best_external_bid
        elif short_hit_ratio > long_hit_ratio:
            lag_side = "SHORT"
            side_edges = hedge_short_edges
            persistent_exec_hit_ratio = short_hit_ratio
            current_side_edge = current.hedge_short_edge_pct
            current_best_case_edge = current.best_hedge_short_edge_pct
            best_hedge_venue = current.best_external_ask_venue
            best_hedge_price = current.best_external_ask
        else:
            med_long = statistics.median(hedge_long_edges)
            med_short = statistics.median(hedge_short_edges)
            if max(med_long, med_short) > 0:
                lag_side = "LONG" if med_long >= med_short else "SHORT"
                side_edges = hedge_long_edges if lag_side == "LONG" else hedge_short_edges
                persistent_exec_hit_ratio = long_hit_ratio if lag_side == "LONG" else short_hit_ratio
                if lag_side == "LONG":
                    current_side_edge = current.hedge_long_edge_pct
                    current_best_case_edge = current.best_hedge_long_edge_pct
                    best_hedge_venue = current.best_external_bid_venue
                    best_hedge_price = current.best_external_bid
                else:
                    current_side_edge = current.hedge_short_edge_pct
                    current_best_case_edge = current.best_hedge_short_edge_pct
                    best_hedge_venue = current.best_external_ask_venue
                    best_hedge_price = current.best_external_ask
            else:
                lag_side = "NONE"
                side_edges = [0.0 for _ in self.samples]
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

        def lag_edge_for_sample(sample: MarketSample, side: str) -> float:
            if side == "LONG":
                return sample.hedge_long_edge_pct
            if side == "SHORT":
                return sample.hedge_short_edge_pct
            return 0.0

        if (
            self.lag_detection_sample_index is not None
            and self.lag_detection_side in {"LONG", "SHORT"}
            and 0 <= self.lag_detection_sample_index < len(self.samples)
        ):
            lag_samples = self.samples[self.lag_detection_sample_index :]
            lag_edges = [lag_edge_for_sample(x, self.lag_detection_side) for x in lag_samples]
            if lag_edges:
                lag_initial_gap_pct = self.lag_initial_edge_pct if self.lag_initial_edge_pct is not None else lag_edges[0]
                lag_initial_gap_pct = max(0.0, lag_initial_gap_pct)
                lag_min_gap_pct = min(lag_edges)
                lag_current_gap_pct = lag_edges[-1]
                lag_max_gap_pct = max(lag_edges)
                lag_extended_observed_seconds = max(0.0, lag_samples[-1].ts - lag_samples[0].ts)
                if lag_initial_gap_pct > 0:
                    lag_max_convergence_fraction = max(
                        0.0,
                        min(1.0, (lag_initial_gap_pct - lag_min_gap_pct) / lag_initial_gap_pct),
                    )
                    threshold_50 = lag_initial_gap_pct * 0.50
                    threshold_80 = lag_initial_gap_pct * 0.20
                    for sample, edge in zip(lag_samples, lag_edges):
                        elapsed = max(0.0, sample.ts - lag_samples[0].ts)
                        if lag_time_to_50_seconds is None and edge <= threshold_50:
                            lag_time_to_50_seconds = elapsed
                        if lag_time_to_80_seconds is None and edge <= threshold_80:
                            lag_time_to_80_seconds = elapsed

                    convergence_threshold = lag_initial_gap_pct * (1.0 - cfg.lag_min_convergence_fraction)
                    rearm_threshold = lag_initial_gap_pct * cfg.lag_reexpansion_fraction
                    armed = True
                    full_armed = True
                    for edge in lag_edges:
                        if armed and edge <= convergence_threshold:
                            lag_convergence_events += 1
                            armed = False
                        elif not armed and edge >= rearm_threshold:
                            armed = True

                        if full_armed and edge <= cfg.lag_full_convergence_edge_pct:
                            lag_full_convergence_cycles += 1
                            full_armed = False
                        elif not full_armed and edge >= rearm_threshold:
                            full_armed = True

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
            "baseline_median_gap_pct": baseline_median,
            "baseline_required_gap_pct": baseline_required,
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
                return data
    except Exception as e:
        print(f"WARN: cannot read state: {e}", file=sys.stderr)
    return {
        "last_alerts": {},
        "lag_baseline": {},
        "lag_verified_profiles": {},
        "active_signal_open": {},
        "signal_stats": {},
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
        "V9 keeps the verified convergence logic and additionally emits informational ACTIVE-LAG and ACTIVE-MM-EXCURSION signals; no orders are ever placed. "
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
            f"V9 counter: {symbol} ACTIVE-LAG detections <b>{symbol_total}</b> | all symbols <b>{total}</b>\n\n"
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
            f"V9 counter: {symbol} ACTIVE-MM excursion events <b>{symbol_total}</b> | all symbols <b>{total}</b>\n\n"
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

    # 7) V9 ACTIVE-LAG requires PRIOR verified history and only fires on a new
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

    assert valid_quote("1", "1.01") is not None
    assert valid_quote("1.01", "1") is None
    print(
        "Self-test OK (V9 MM + converging LAG + ACTIVE-LAG profile logic + "
        "ACTIVE-MM excursion plumbing + baseline/sanity + false-positive rejection)"
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
        text = format_active_signal(event, state)
        if args.dry_run:
            print("\nDRY ACTIVE SIGNAL:\n", text.replace("<b>", "").replace("</b>", ""))
            return
        if send_telegram(text):
            print(f"Telegram active signal sent: {event.get('type')} {event.get('symbol')}")

    try:
        ranked, errors, scan_state_changed = scan(
            cfg, state, active_signal_callback=active_signal_callback
        )
    except ApiError as e:
        print(f"FATAL: {e}", file=sys.stderr)
        return 2

    print_table(ranked)
    if errors:
        unique_errors = list(dict.fromkeys(errors))
        print("\nWarnings:")
        for e in unique_errors[:20]:
            print(" -", e)

    # Cross-run state is persisted even when no final CONFIRMED Telegram alert is sent.
    # V9 adds verified convergence profiles and active-signal counters/latches.
    state_changed = bool(scan_state_changed)
    state_changed = update_lag_baseline_state(state, [c for c, _ in ranked], cfg) or state_changed
    state_changed = update_lag_verified_profile_state(state, [c for c, _ in ranked], cfg) or state_changed
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
