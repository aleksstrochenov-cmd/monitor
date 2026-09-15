#!/usr/bin/env python3
"""
V16.5 execution-economics monitor entrypoint over monitor_core.py V16.4.

Goals
-----
1. Send a Telegram trade signal for every newly qualifying executable route whose
   fresh expected NET economics are strictly > +0.100% at $100 notional.
2. NET means: fresh target/hedge VWAP gross edge minus round-trip taker fees
   minus the configured exit-slippage reserve.
3. Do not require LAG convergence history or MM reversion history to expose a
   qualifying economic opportunity. Those legacy models may continue to collect
   research state internally, but their entry alerts are suppressed to avoid
   duplicate trade signals.
4. A continuously-positive route is alerted only once. It rearms after the route
   falls below the execution threshold / disappears, using the core V16.4 latch.
5. Every qualifying signal creates an independent persistent paper trade on the
   exact target + hedge venue and entry VWAP observed at the signal. No venue
   capital-busy gate and no one-open-trade-per-route gate are applied.
6. Send one calendar-day paper report on the first scan after midnight in the
   configured timezone (Europe/Riga by default).

This file is the repository entrypoint named monitor.py. It imports the previous
V16.4 engine from monitor_core.py in the same directory. Rename the existing
monitor.py to monitor_core.py once, then upload this file as monitor.py. Existing
GitHub Actions commands can remain `python monitor.py` and `python monitor.py --self-test`.
"""

from __future__ import annotations

import html
import math
import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo


# ---------------------------------------------------------------------------
# Hard invariants for the requested trading-signal semantics.
# These are assigned (not setdefault) so stale workflow env values cannot
# silently weaken the +0.10% NET requirement.
# ---------------------------------------------------------------------------
SIGNAL_NET_THRESHOLD_PCT = 0.10
# Core readiness uses >=.  A tiny epsilon makes the effective rule strictly >.
CORE_STRICT_THRESHOLD_PCT = 0.100001
REPORT_TIMEZONE = os.getenv("DAILY_REPORT_TZ", "Europe/Riga")
SIGNAL_LEDGER_RETENTION_DAYS = max(7, int(os.getenv("V165_SIGNAL_LEDGER_RETENTION_DAYS", "31")))
SIGNAL_LEDGER_MAX_ROWS = max(1000, int(os.getenv("V165_SIGNAL_LEDGER_MAX_ROWS", "20000")))

FORCED_ENV: Dict[str, str] = {
    # Canonical broad execution-economics layer.
    "ECONOMIC_SIGNALS_ENABLED": "true",
    "ECONOMIC_PROBE_MIN_GROSS_PCT": "0.0",
    "ECONOMIC_MIN_NET_EDGE_PCT": f"{CORE_STRICT_THRESHOLD_PCT:.6f}",
    "EXECUTION_READINESS_ENABLED": "true",
    "EXECUTION_MIN_NOTIONAL_USD": "100",
    "PAPER_PROBE_NOTIONAL_USD": "100",
    "EXECUTION_MIN_NET_EDGE_PCT": f"{CORE_STRICT_THRESHOLD_PCT:.6f}",
    # 0 means: while a latch remains open, never repeat it. Once it leaves the
    # current qualifying set, a later threshold crossing is a fresh signal.
    "ECONOMIC_SIGNAL_COOLDOWN_MINUTES": "0",
    # Make the broad layer independent of the historical ref-good hit ratio.
    # The fresh execution probe still requires a concrete hedge route and keeps
    # its own book/fair-price safety checks.
    "MIN_REF_GOOD_RATIO": "0.0",
    # Do not truncate the broad economics side-probe list if discovery limits
    # are increased later.
    "ECONOMIC_PROBE_MAX_CANDIDATES": "1000",
    # Paper simulation must accept every qualifying event, not only the first
    # trade that consumes a synthetic venue-capital bucket.
    "PAPER_TRADING_ENABLED": "true",
    "PAPER_ENFORCE_VENUE_CAPITAL": "false",
    "PAPER_MIN_ENTRY_NET_EDGE_PCT": f"{CORE_STRICT_THRESHOLD_PCT:.6f}",
    # Keep the old research layers running internally but silence their entry/
    # outcome Telegram noise; canonical alerts come from POSITIVE-ECONOMICS.
    "ACTIVE_OUTCOME_ALERTS_ENABLED": "false",
    "ACTIVE_LAG_HORIZON_ALERTS_ENABLED": "false",
    "SCAN_SUMMARY_ENABLED": "false",
    "ALERT_LEVELS": "",
    "CHART_ALERT_LEVELS": "",
    # The legacy periodic paper report is replaced below by a daily report.
    "PAPER_REPORT_ENABLED": "true",
    "PAPER_REPORT_SEND_TELEGRAM": "true",
}
for _key, _value in FORCED_ENV.items():
    os.environ[_key] = _value


# Import after forcing env, because Config() reads these values.
import monitor_core as core  # noqa: E402

# User-facing version label for this overlay.
core.SCANNER_VERSION = "16.5"

_ORIGINAL_SCAN = core.scan
_ORIGINAL_FORMAT_ACTIVE_SIGNAL = core.format_active_signal


def _fnum(value: Any, default: float = 0.0) -> float:
    """Use the core numeric parser when available, with a defensive fallback."""
    try:
        return float(core.fnum(value, default))
    except Exception:
        try:
            out = float(value)
            return out if math.isfinite(out) else float(default)
        except Exception:
            return float(default)


def _normalize_venue(value: Any) -> str:
    try:
        return str(core._paper_normalize_venue(str(value or "")))
    except Exception:
        v = str(value or "").strip().lower()
        return v[:-5] if v.endswith("-perp") else v


def _minimum_execution_row(event: dict) -> Optional[dict]:
    try:
        row = core._paper_min_execution_row(event)
        return dict(row) if isinstance(row, dict) else None
    except Exception:
        checks = event.get("execution_checks", {}) if isinstance(event.get("execution_checks"), dict) else {}
        usd = _fnum(event.get("execution_min_notional_usd"), 100.0)
        key = str(int(usd) if float(usd).is_integer() else usd)
        row = checks.get(key)
        return dict(row) if isinstance(row, dict) else None


def _unique_trade_id(bucket: dict, base_id: str) -> str:
    if base_id not in bucket:
        return base_id
    suffix = 2
    while f"{base_id}:{suffix}" in bucket:
        suffix += 1
    return f"{base_id}:{suffix}"


def _signal_is_qualifying(event: dict) -> bool:
    if not isinstance(event, dict) or str(event.get("type", "")) != "POSITIVE-ECONOMICS":
        return False
    if not bool(event.get("execution_ready")):
        return False
    net = _fnum(event.get("execution_fresh_net_edge_pct"), -999.0)
    return math.isfinite(net) and net > SIGNAL_NET_THRESHOLD_PCT


def open_economic_paper_trade(state: dict, event: dict, cfg: Any) -> bool:
    """Open one independent paper trade for one V16.5 qualifying signal.

    There is deliberately no route de-duplication, venue-capital gate, or
    max-open-trades gate here. The broad execution detector already handles
    continuous-signal de-duplication; a re-armed crossing is a new hypothetical
    entry even when an older trade on the same route is still open.
    """
    if not getattr(cfg, "paper_trading_enabled", True) or not _signal_is_qualifying(event):
        return False

    side = str(event.get("side") or "").upper()
    if side not in {"LONG", "SHORT"}:
        return False

    target = _normalize_venue(event.get("target") or "aster")
    hedge_raw = str(event.get("execution_best_external_venue") or "").strip().lower()
    hedge_norm = _normalize_venue(hedge_raw)
    if not target or not hedge_raw or not hedge_norm or target == hedge_norm:
        return False

    row = _minimum_execution_row(event)
    if not row or str(row.get("status", "")).upper() != "READY":
        return False

    entry_target = _fnum(row.get("target_vwap"), 0.0)
    entry_hedge = _fnum(row.get("external_vwap"), 0.0)
    if entry_target <= 0.0 or entry_hedge <= 0.0:
        return False

    fresh_net = _fnum(event.get("execution_fresh_net_edge_pct"), -999.0)
    if fresh_net <= SIGNAL_NET_THRESHOLD_PCT:
        return False

    bucket = state.setdefault("paper_trades_open", {})
    if not isinstance(bucket, dict):
        bucket = {}
        state["paper_trades_open"] = bucket

    opened_ts = _fnum(event.get("ts"), time.time())
    state_key = str(event.get("state_key") or f"{target}:{event.get('symbol')}")
    base_id = f"paper:ECON:{state_key}:{side}:{hedge_norm}:{int(opened_ts * 1000)}"
    trade_id = _unique_trade_id(bucket, base_id)

    target_fee = _fnum(
        row.get("target_taker_fee_pct"),
        _fnum(event.get("execution_target_taker_fee_pct"), 0.0),
    )
    hedge_fee = _fnum(
        row.get("external_taker_fee_pct"),
        _fnum(event.get("execution_external_taker_fee_pct"), 0.0),
    )
    probe_usd = _fnum(event.get("execution_min_notional_usd"), 100.0)
    entry_gap = max(
        0.0,
        _fnum(row.get("gross_edge_pct"), _fnum(event.get("execution_fresh_gross_edge_pct"), 0.0)),
    )
    initial_pnl = -(target_fee + hedge_fee)

    target_labels = getattr(core, "TARGET_LABELS", {})
    target_label = event.get("target_label")
    if not target_label and isinstance(target_labels, dict):
        target_label = target_labels.get(target)
    target_label = target_label or target.title()

    trade = {
        "id": trade_id,
        # Unique per threshold-transition signal: intentionally not a route lock.
        "dedupe_key": trade_id,
        "strategy": "ECON",
        "source_event_type": "POSITIVE-ECONOMICS",
        "state_key": state_key,
        "symbol": event.get("symbol"),
        "target": target,
        "target_label": target_label,
        "hedge_venue": hedge_raw,
        "target_side": side,
        "hedge_side": "LONG" if side == "SHORT" else "SHORT",
        "opened_ts": opened_ts,
        "status": "OPEN",
        # X accounting is kept at 1.0; USD reporting below uses actual probe USD.
        "notional_x": 1.0,
        "capital_fraction_x": 1.0,
        "probe_notional_usd": probe_usd,
        "entry_target_vwap": entry_target,
        "entry_hedge_vwap": entry_hedge,
        "entry_gap_pct": entry_gap,
        "entry_estimated_net_edge_pct": fresh_net,
        "entry_expected_net_edge_pct": fresh_net,
        "entry_convergence_adjusted_net_pct": None,
        "entry_expected_capture_pct": None,
        "entry_convergence_economics_fraction": None,
        "entry_convergence_economics_source": "fresh-executable-net",
        "entry_target_fee_pct": target_fee,
        "entry_hedge_fee_pct": hedge_fee,
        "entry_open_fees_pct_x": target_fee + hedge_fee,
        "expected_roundtrip_fees_pct_x": 2.0 * (target_fee + hedge_fee),
        "entry_exit_slippage_reserve_pct": _fnum(
            row.get("exit_slippage_reserve_pct"),
            _fnum(event.get("execution_exit_slippage_reserve_pct"), 0.0),
        ),
        "reference_disagreement_pct": _fnum(event.get("reference_disagreement_pct"), 0.0),
        "verified_episodes_at_entry": int(_fnum(event.get("verified_episodes"), 0.0)),
        "historical_median_convergence_fraction": 0.0,
        "historical_median_t50_seconds": None,
        "latest_same_side_gap_pct": entry_gap,
        "min_same_side_gap_pct": entry_gap,
        "max_same_side_gap_pct": entry_gap,
        "max_convergence_fraction": 0.0,
        "time_to_50_seconds": None,
        "time_to_80_seconds": None,
        "current_net_pnl_pct_x": initial_pnl,
        "best_net_pnl_pct_x": initial_pnl,
        "worst_net_pnl_pct_x": initial_pnl,
        "funding_net_pnl_pct_x": 0.0,
        "updates": 0,
        "missing_depth_updates": 0,
        "checkpoints": {},
        "v165_signal_net_threshold_pct": SIGNAL_NET_THRESHOLD_PCT,
        "v165_fresh_net_pct": fresh_net,
    }

    bucket[trade_id] = trade
    try:
        core._paper_note_open(state, trade)
    except Exception:
        # The paper trade itself is the source of truth. Stats are secondary.
        pass

    event["_v165_paper_opened"] = True
    event["_v165_paper_trade_id"] = trade_id
    return True


def _prune_signal_ledger(state: dict, now_ts: float) -> None:
    rows = state.setdefault("v165_signal_events", [])
    if not isinstance(rows, list):
        rows = []
        state["v165_signal_events"] = rows
    cutoff = now_ts - SIGNAL_LEDGER_RETENTION_DAYS * 86400.0
    kept = [r for r in rows if isinstance(r, dict) and _fnum(r.get("ts"), 0.0) >= cutoff]
    if len(kept) > SIGNAL_LEDGER_MAX_ROWS:
        kept = kept[-SIGNAL_LEDGER_MAX_ROWS:]
    state["v165_signal_events"] = kept


def record_v165_signal_event(state: dict, event: dict, paper_opened: bool) -> None:
    now_ts = _fnum(event.get("ts"), time.time())
    _prune_signal_ledger(state, now_ts)
    rows = state.setdefault("v165_signal_events", [])
    row = {
        "ts": now_ts,
        "type": "POSITIVE-ECONOMICS",
        "state_key": event.get("state_key"),
        "symbol": event.get("symbol"),
        "target": event.get("target"),
        "target_label": event.get("target_label"),
        "side": event.get("side"),
        "hedge_venue": event.get("execution_best_external_venue"),
        "gross_edge_pct": _fnum(event.get("execution_fresh_gross_edge_pct"), 0.0),
        "net_edge_pct": _fnum(event.get("execution_fresh_net_edge_pct"), 0.0),
        "notional_usd": _fnum(event.get("execution_min_notional_usd"), 100.0),
        "paper_opened": bool(paper_opened),
        "paper_trade_id": event.get("_v165_paper_trade_id"),
    }
    rows.append(row)
    if len(rows) > SIGNAL_LEDGER_MAX_ROWS:
        del rows[:-SIGNAL_LEDGER_MAX_ROWS]


def scan_v165(
    cfg: Any,
    state: dict,
    active_signal_callback: Optional[Callable[[dict], None]] = None,
) -> Tuple[Any, Any, bool]:
    """Wrap the core scan and expose only canonical >0.10% trade signals."""
    previous_version = state.get("scanner_version")
    state["scanner_version"] = "16.5"

    def callback(event: dict) -> None:
        typ = str(event.get("type", ""))

        if typ == "POSITIVE-ECONOMICS":
            # Defense in depth: core readiness is already forced to >0.100%.
            if not _signal_is_qualifying(event):
                return
            opened = open_economic_paper_trade(state, event, cfg)
            event["_v165_paper_opened"] = bool(opened)
            record_v165_signal_event(state, event, opened)
            if active_signal_callback is not None:
                active_signal_callback(event)
            return

        # Keep legacy LAG/MM research state running in the core, but do not send
        # duplicate entry alerts and do not create legacy paper entries.
        if typ in {"ACTIVE-LAG", "ACTIVE-MM-EXCURSION"}:
            return

        # Outcome alerts are force-disabled in Config, but delegate unknown event
        # types defensively if future core versions introduce one.
        if active_signal_callback is not None:
            active_signal_callback(event)

    ranked, errors, changed = _ORIGINAL_SCAN(
        cfg, state, active_signal_callback=callback
    )
    return ranked, errors, bool(changed or previous_version != "16.5")


def format_trade_signal_v165(event: dict, state: dict) -> str:
    if str(event.get("type", "")) != "POSITIVE-ECONOMICS":
        return _ORIGINAL_FORMAT_ACTIVE_SIGNAL(event, state)

    symbol = html.escape(str(event.get("symbol") or "?"))
    label = html.escape(str(event.get("target_label") or event.get("target") or "?"))
    side = html.escape(str(event.get("side") or "?"))
    hedge = html.escape(str(event.get("execution_best_external_venue") or "unknown"))
    usd = _fnum(event.get("execution_min_notional_usd"), 100.0)
    gross = _fnum(event.get("execution_fresh_gross_edge_pct"), 0.0)
    net = _fnum(event.get("execution_fresh_net_edge_pct"), 0.0)
    fees = _fnum(event.get("execution_roundtrip_fees_pct"), 0.0)
    reserve = _fnum(event.get("execution_exit_slippage_reserve_pct"), 0.0)
    ready_usd = _fnum(event.get("execution_ready_notional_usd"), usd)
    disagreement = _fnum(event.get("reference_disagreement_pct"), 0.0)
    row = _minimum_execution_row(event) or {}
    tvwap = _fnum(row.get("target_vwap"), 0.0)
    hvwap = _fnum(row.get("external_vwap"), 0.0)

    paper_ok = bool(event.get("_v165_paper_opened"))
    paper_line = "✅ paper entry recorded" if paper_ok else "⚠️ paper entry was NOT recorded"

    return (
        f"💰 <b>TRADE SIGNAL — {symbol} @ {label}</b>\n"
        f"Rule: executable expected NET <b>&gt; {SIGNAL_NET_THRESHOLD_PCT:.3f}%</b>\n"
        f"Route: <b>{side} {label}</b> / hedge via <b>{hedge}</b>\n"
        f"Fresh VWAP @ ${usd:,.0f}: gross <b>{gross:.4f}%</b>\n"
        f"Round-trip taker fees: {fees:.4f}% | exit-slip reserve: {reserve:.4f}%\n"
        f"Expected NET after costs: <b>{net:+.4f}%</b> ✅\n"
        f"Entry VWAP target / hedge: {tvwap:.10g} / {hvwap:.10g}\n"
        f"Executable notional verified up to: <b>${ready_usd:,.0f}</b> | ref disagreement {disagreement:.3f}%\n"
        f"Paper: <b>{paper_line}</b>\n\n"
        "Continuous route is alerted once and rearms only after it falls below the qualifying execution state. "
        "Signal/paper simulation only; no API orders are sent."
    )


def _local_date_for_ts(ts: float, tz: ZoneInfo) -> date:
    return datetime.fromtimestamp(float(ts), tz).date()


def _closed_econ_trades(state: dict) -> List[dict]:
    rows = state.get("paper_trades_closed", []) if isinstance(state, dict) else []
    if not isinstance(rows, list):
        return []
    return [r for r in rows if isinstance(r, dict) and str(r.get("strategy")) == "ECON"]


def _open_econ_trades(state: dict) -> List[dict]:
    bucket = state.get("paper_trades_open", {}) if isinstance(state, dict) else {}
    if not isinstance(bucket, dict):
        return []
    return [
        r for r in bucket.values()
        if isinstance(r, dict)
        and str(r.get("strategy")) == "ECON"
        and str(r.get("status", "OPEN")) == "OPEN"
    ]


def _trade_net_pct(trade: dict, closed: bool) -> float:
    key = "realized_net_pnl_pct_x" if closed else "current_net_pnl_pct_x"
    return _fnum(trade.get(key), 0.0)


def _trade_pnl_usd(trade: dict, net_pct: float) -> float:
    # Pair PnL is the sum of target + hedge leg percentage returns, each measured
    # against the same per-leg probe notional. Therefore pct/100 * probe_usd is
    # the dollar PnL of the hedged pair at that sizing.
    usd = max(0.0, _fnum(trade.get("probe_notional_usd"), 100.0))
    return net_pct / 100.0 * usd


def build_daily_report_text(state: dict, report_date: date, tz: ZoneInfo) -> str:
    signal_rows = state.get("v165_signal_events", []) if isinstance(state, dict) else []
    if not isinstance(signal_rows, list):
        signal_rows = []
    day_signals = [
        r for r in signal_rows
        if isinstance(r, dict)
        and _local_date_for_ts(_fnum(r.get("ts"), 0.0), tz) == report_date
    ]
    paper_entries = sum(1 for r in day_signals if bool(r.get("paper_opened")))

    closed = [
        r for r in _closed_econ_trades(state)
        if _local_date_for_ts(_fnum(r.get("opened_ts"), 0.0), tz) == report_date
    ]
    opened = [
        r for r in _open_econ_trades(state)
        if _local_date_for_ts(_fnum(r.get("opened_ts"), 0.0), tz) == report_date
    ]

    closed_values = [(_trade_net_pct(t, True), t) for t in closed]
    open_values = [(_trade_net_pct(t, False), t) for t in opened]
    all_values = closed_values + open_values

    wins = sum(1 for v, _ in closed_values if v > 1e-12)
    losses = sum(1 for v, _ in closed_values if v < -1e-12)
    breakeven = len(closed_values) - wins - losses
    win_rate = (wins / len(closed_values) * 100.0) if closed_values else None

    realized_pct_sum = sum(v for v, _ in closed_values)
    open_pct_sum = sum(v for v, _ in open_values)
    total_pct_sum = realized_pct_sum + open_pct_sum
    realized_usd = sum(_trade_pnl_usd(t, v) for v, t in closed_values)
    open_usd = sum(_trade_pnl_usd(t, v) for v, t in open_values)
    total_usd = realized_usd + open_usd

    missing_paper = max(0, len(day_signals) - paper_entries)
    untracked_entries = max(0, paper_entries - len(all_values))

    lines = [
        f"📊 <b>DAILY PAPER REPORT — {report_date.strftime('%d.%m.%Y')}</b>",
        f"Rule: fresh executable NET <b>&gt; {SIGNAL_NET_THRESHOLD_PCT:.3f}%</b> | sizing: $100 per leg by default",
        f"Signals: <b>{len(day_signals)}</b> | paper entries: <b>{paper_entries}</b> | entry failures: <b>{missing_paper}</b>",
        f"Closed: <b>{len(closed)}</b> | still open: <b>{len(opened)}</b>",
        (
            f"Wins / losses / flat: <b>{wins} / {losses} / {breakeven}</b> | win rate <b>{win_rate:.1f}%</b>"
            if win_rate is not None
            else "Wins / losses / flat: <b>0 / 0 / 0</b> | win rate <b>n/a</b>"
        ),
        "",
        f"Realized PnL: <b>{realized_usd:+.4f} USD</b> | Σ net {realized_pct_sum:+.4f}%",
        f"Open MTM: <b>{open_usd:+.4f} USD</b> | Σ net {open_pct_sum:+.4f}%",
        f"TOTAL hypothetical PnL: <b>{total_usd:+.4f} USD</b> | Σ net across trades {total_pct_sum:+.4f}%",
    ]

    if all_values:
        best_v, best_t = max(all_values, key=lambda x: x[0])
        worst_v, worst_t = min(all_values, key=lambda x: x[0])
        lines.extend([
            "",
            f"Best: <b>{html.escape(str(best_t.get('symbol') or '?'))} {best_v:+.4f}%</b>",
            f"Worst: <b>{html.escape(str(worst_t.get('symbol') or '?'))} {worst_v:+.4f}%</b>",
        ])

    if untracked_entries:
        lines.append(f"⚠️ {untracked_entries} paper entries are not present in the retained open/closed ledger.")

    lines.extend([
        "",
        "PnL uses the exact stored target + hedge route, entry VWAP, actual close/mark VWAP, taker fees and tracked funding. "
        "Open positions are marked to the latest available books. Σ net is a sum of per-trade returns, not a compounded portfolio return.",
    ])
    return "\n".join(lines)


def maybe_send_daily_report(
    state: dict,
    cfg: Any,
    dry_run: bool = False,
    now_ts: Optional[float] = None,
) -> bool:
    """Send exactly one report for the just-completed local calendar day."""
    if not getattr(cfg, "paper_report_enabled", True) or not getattr(cfg, "paper_report_send_telegram", True):
        return False

    try:
        tz = ZoneInfo(REPORT_TIMEZONE)
    except Exception:
        tz = ZoneInfo("Europe/Riga")

    now_ts = time.time() if now_ts is None else float(now_ts)
    now_local = datetime.fromtimestamp(now_ts, tz)
    completed_date = now_local.date() - timedelta(days=1)

    root = state.setdefault("v165_daily_report", {})
    if not isinstance(root, dict):
        root = {}
        state["v165_daily_report"] = root

    # First installation should not emit a fake zero report for the day before
    # V16.5 existed. Initialize the cursor and start reporting from the next
    # calendar rollover.
    if not root.get("initialized"):
        if dry_run:
            return False
        root["initialized"] = True
        root["timezone"] = REPORT_TIMEZONE
        root["last_reported_date"] = completed_date.isoformat()
        root["initialized_ts"] = int(now_ts)
        return True

    if str(root.get("last_reported_date") or "") == completed_date.isoformat():
        return False

    text = build_daily_report_text(state, completed_date, tz)
    if dry_run:
        print("\nDRY DAILY PAPER REPORT:\n", text.replace("<b>", "").replace("</b>", ""))
        return False

    if not core.send_telegram(text):
        return False

    root["last_reported_date"] = completed_date.isoformat()
    root["last_sent_ts"] = int(now_ts)
    root["timezone"] = REPORT_TIMEZONE
    history = root.setdefault("history", [])
    if not isinstance(history, list):
        history = []
        root["history"] = history
    history.append({"date": completed_date.isoformat(), "sent_ts": int(now_ts)})
    if len(history) > 90:
        del history[:-90]
    print(f"Daily V16.5 paper report sent for {completed_date.isoformat()} ({REPORT_TIMEZONE})")
    return True


def overlay_self_test() -> None:
    """Deterministic local tests for V16.5-specific overlay logic."""
    cfg = core.Config()
    state: dict = {}
    now = time.time()
    event = {
        "type": "POSITIVE-ECONOMICS",
        "symbol": "TESTUSDT",
        "state_key": "aster:TESTUSDT",
        "target": "aster",
        "target_label": "Aster",
        "ts": now,
        "side": "SHORT",
        "execution_ready": True,
        "execution_min_notional_usd": 100,
        "execution_ready_notional_usd": 100,
        "execution_best_external_venue": "lighter-perp",
        "execution_fresh_gross_edge_pct": 0.22,
        "execution_fresh_net_edge_pct": 0.1201,
        "execution_roundtrip_fees_pct": 0.08,
        "execution_exit_slippage_reserve_pct": 0.0199,
        "execution_checks": {
            "100": {
                "status": "READY",
                "gross_edge_pct": 0.22,
                "net_edge_pct": 0.1201,
                "external_venue": "lighter-perp",
                "target_vwap": 100.22,
                "external_vwap": 100.00,
                "target_taker_fee_pct": 0.04,
                "external_taker_fee_pct": 0.00,
                "exit_slippage_reserve_pct": 0.0199,
            }
        },
    }
    assert _signal_is_qualifying(event)
    assert open_economic_paper_trade(state, event, cfg)
    # Same route is allowed to open again if the detector emits a new crossing.
    event2 = dict(event)
    event2["ts"] = now + 1.0
    event2["execution_checks"] = {"100": dict(event["execution_checks"]["100"])}
    assert open_economic_paper_trade(state, event2, cfg)
    assert len(state.get("paper_trades_open", {})) == 2
    assert all(t.get("strategy") == "ECON" for t in state["paper_trades_open"].values())

    too_thin = dict(event)
    too_thin["execution_fresh_net_edge_pct"] = 0.10
    assert not _signal_is_qualifying(too_thin)

    record_v165_signal_event(state, event, True)
    tz = ZoneInfo(REPORT_TIMEZONE)
    report = build_daily_report_text(state, _local_date_for_ts(now, tz), tz)
    assert "Signals: <b>1</b>" in report
    assert "TOTAL hypothetical PnL" in report
    print("V16.5 overlay self-test OK (>0.100% fresh NET signal + independent paper entry + daily report)")


# Install monkey patches used by core.main().
core.scan = scan_v165
core.format_active_signal = format_trade_signal_v165
core.maybe_send_paper_performance_report = maybe_send_daily_report
# Guarantee no compact scan-audit messages and no old final CONFIRMED alerts,
# even if a stale workflow later changes those env values.
core.format_scan_summary_chunks = lambda *args, **kwargs: []
core.should_alert = lambda *args, **kwargs: False


def main() -> int:
    # Run overlay tests in addition to the original V16.4 deterministic self-test.
    if "--self-test" in sys.argv:
        overlay_self_test()
    return int(core.main())


if __name__ == "__main__":
    raise SystemExit(main())
