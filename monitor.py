#!/usr/bin/env python3
"""
V16.8 business-thesis execution-economics monitor.

This is a single-file repository entrypoint. It does NOT require monitor_core.py.
To preserve the proven 7k+ line V16.4 market-data/execution engine without
duplicating it by hand, this file loads the last V16.4 version of monitor.py
directly from this repository's Git history into memory, then installs the V16.8
signal/paper-reporting overlay.

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
5. Every qualifying signal still creates an independent all-signals paper trade
   on the exact target + hedge route. In parallel, the daily report reconstructs
   a capital-constrained portfolio that cannot reuse capital while a trade is open.
6. Report PnL both excluding funding and including tracked funding.
7. Send one calendar-day paper report on the first scan after midnight in the
   configured timezone (Europe/Riga by default).

GitHub Actions normally checks out a shallow clone. If the V16.4 file is outside
that shallow boundary, this entrypoint automatically deepens the existing Git
checkout before loading the core. Existing commands remain:
    python monitor.py
    python monitor.py --self-test
"""

from __future__ import annotations

import html
import math
import os
import subprocess
import sys
import time
import types
from datetime import date, datetime, timedelta
from pathlib import Path
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

# V16.8: ECON exits are independent from legacy LAG/MM consensus outcomes.
# A trade is marked/closed only from the exact target + hedge route stored at entry.
ECON_EXIT_LOGIC_VERSION = 4
ECON_MAX_HOLD_HOURS = max(1.0, float(os.getenv("ECON_MAX_HOLD_HOURS", "24")))
ECON_EXIT_EPSILON_PCT = 1e-9

# Parallel capital-constrained simulation. All-signals paper trading remains
# untouched so signal quality can still be measured independently. The capital
# model reserves both $100 legs (or the actual probe notional) from one shared
# notional-equivalent balance until that accepted trade closes.
CAPITAL_SIM_ENABLED = os.getenv("PAPER_CAPITAL_SIM_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}
CAPITAL_SIM_START_USD = max(200.0, float(os.getenv("PAPER_CAPITAL_SIM_START_USD", "1000")))
CAPITAL_SIM_EPSILON_USD = 1e-9

# Business-thesis / funding forecast controls. Funding forecasts are only an
# annotation and exit-policy input; they do NOT weaken the canonical >0.100%
# executable spread-entry filter. A funding leg is considered forecastable only
# when a public current rate and a future settlement timestamp are both known.
FUNDING_FORECAST_ENABLED = os.getenv("FUNDING_FORECAST_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}
FUNDING_FORECAST_TTL_SECONDS = max(5.0, float(os.getenv("FUNDING_FORECAST_TTL_SECONDS", "30")))
FUNDING_CLASSIFICATION_MIN_NET_PCT = float(os.getenv("FUNDING_CLASSIFICATION_MIN_NET_PCT", "0.0"))
_FUNDING_FORECAST_CACHE: Dict[Tuple[str, str], Tuple[float, dict]] = {}

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


# ---------------------------------------------------------------------------
# Load the V16.4 engine from Git history.
#
# The repository currently contains the complete V16.4 engine as an earlier
# version of monitor.py. Keeping it in Git history lets this remain a single
# physical file while preserving the battle-tested exchange adapters.
# ---------------------------------------------------------------------------
_CORE_MARKER = "Multi-target perp inefficiency scanner v16.4"
_OVERLAY_MARKER = "V16.8 business-thesis execution-economics monitor"
_CORE_MIN_BYTES = 100_000
_GIT_TIMEOUT_SECONDS = max(10, int(os.getenv("V165_GIT_TIMEOUT_SECONDS", "45")))
_GIT_HISTORY_DEPTHS = (25, 100, 500)


def _run_git(repo_root: Path, *args: str, timeout: Optional[int] = None) -> subprocess.CompletedProcess:
    """Run git without a shell and always capture bytes."""
    return subprocess.run(
        ["git", *args],
        cwd=str(repo_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=timeout or _GIT_TIMEOUT_SECONDS,
    )


def _looks_like_v164_core(data: bytes) -> bool:
    if not isinstance(data, (bytes, bytearray)) or len(data) < _CORE_MIN_BYTES:
        return False
    raw = bytes(data)
    head = raw[:80_000].decode("utf-8", errors="ignore")
    whole = raw.decode("utf-8", errors="ignore")
    return (
        _CORE_MARKER in head
        and ("SCANNER_VERSION = \"16.4\"" in head or "SCANNER_VERSION = '16.4'" in head)
        and "class Config" in head
        and "def scan(" in whole
        and _OVERLAY_MARKER not in head
    )


def _repo_root_for_this_file() -> Optional[Path]:
    here = Path(__file__).resolve().parent
    proc = _run_git(here, "rev-parse", "--show-toplevel")
    if proc.returncode != 0:
        return None
    try:
        return Path(proc.stdout.decode("utf-8", errors="replace").strip()).resolve()
    except Exception:
        return None


def _candidate_core_from_local_file() -> Optional[Tuple[bytes, str]]:
    """Optional emergency fallback; monitor_core.py is NOT required."""
    explicit = os.getenv("MONITOR_CORE_SOURCE_PATH", "").strip()
    candidates: List[Path] = []
    if explicit:
        candidates.append(Path(explicit).expanduser())
    candidates.append(Path(__file__).resolve().with_name("monitor_core.py"))
    for path in candidates:
        try:
            data = path.read_bytes()
        except Exception:
            continue
        if _looks_like_v164_core(data):
            return data, str(path)
    return None


def _core_from_git_history(repo_root: Path) -> Optional[Tuple[bytes, str]]:
    try:
        rel_path = Path(__file__).resolve().relative_to(repo_root).as_posix()
    except Exception:
        rel_path = "monitor.py"

    def search_loaded_history() -> Optional[Tuple[bytes, str]]:
        log = _run_git(repo_root, "log", "--follow", "--format=%H", "--", rel_path)
        if log.returncode != 0:
            return None
        commits = [
            line.strip()
            for line in log.stdout.decode("ascii", errors="ignore").splitlines()
            if line.strip()
        ]
        for sha in commits:
            show = _run_git(repo_root, "show", f"{sha}:{rel_path}")
            if show.returncode == 0 and _looks_like_v164_core(show.stdout):
                return show.stdout, f"git:{sha}:{rel_path}"
        return None

    found = search_loaded_history()
    if found is not None:
        return found

    branch = (os.getenv("GITHUB_REF_NAME") or "").strip()
    if not branch:
        current_branch = _run_git(repo_root, "rev-parse", "--abbrev-ref", "HEAD")
        if current_branch.returncode == 0:
            candidate = current_branch.stdout.decode("utf-8", errors="replace").strip()
            if candidate and candidate != "HEAD":
                branch = candidate
    if not branch:
        branch = "main"

    for depth in _GIT_HISTORY_DEPTHS:
        try:
            _run_git(
                repo_root,
                "fetch",
                "--no-tags",
                f"--deepen={depth}",
                "origin",
                branch,
                timeout=max(_GIT_TIMEOUT_SECONDS, 90),
            )
        except Exception:
            pass
        found = search_loaded_history()
        if found is not None:
            return found

    try:
        shallow = _run_git(repo_root, "rev-parse", "--is-shallow-repository")
        is_shallow = shallow.returncode == 0 and shallow.stdout.strip() == b"true"
        if is_shallow:
            _run_git(
                repo_root,
                "fetch",
                "--no-tags",
                "--unshallow",
                "origin",
                branch,
                timeout=max(_GIT_TIMEOUT_SECONDS, 180),
            )
        else:
            _run_git(
                repo_root,
                "fetch",
                "--no-tags",
                "origin",
                branch,
                timeout=max(_GIT_TIMEOUT_SECONDS, 90),
            )
    except Exception:
        pass
    return search_loaded_history()


def _load_v164_core() -> types.ModuleType:
    local = _candidate_core_from_local_file()
    if local is not None:
        source, origin = local
    else:
        repo_root = _repo_root_for_this_file()
        if repo_root is None:
            raise RuntimeError(
                "Cannot load V16.4 core: this monitor.py is not inside a Git checkout. "
                "Run it from the GitHub repository checkout (normal Actions usage)."
            )
        found = _core_from_git_history(repo_root)
        if found is None:
            raise RuntimeError(
                "Cannot find the complete V16.4 monitor.py in Git history. "
                "The repository must retain the current V16.4 monitor.py commit before "
                "replacing it with V16.8."
            )
        source, origin = found

    module_name = "_monitor_v164_core_runtime"
    module = types.ModuleType(module_name)
    module.__file__ = str(Path(__file__).resolve())
    module.__package__ = ""
    module.__doc__ = f"Runtime-loaded V16.4 core from {origin}"
    sys.modules[module_name] = module
    code = compile(source.decode("utf-8"), module.__file__, "exec")
    exec(code, module.__dict__)
    print(f"Loaded V16.4 core from {origin}")
    return module


# Load after forcing env, because Config() reads these values at import time.
core = _load_v164_core()

# User-facing version label for this overlay.
core.SCANNER_VERSION = "16.8"
try:
    core.USER_AGENT = "perp-inefficiency-scanner/16.8"
    if isinstance(getattr(core, "HTTP_HEADERS", None), dict):
        core.HTTP_HEADERS["User-Agent"] = core.USER_AGENT
except Exception:
    pass

_ORIGINAL_SCAN = core.scan
_ORIGINAL_FORMAT_ACTIVE_SIGNAL = core.format_active_signal
_ORIGINAL_PAPER_NOTE_CLOSE = getattr(core, "_paper_note_close", None)
_ORIGINAL_UPDATE_PAPER_TRADES = getattr(core, "update_paper_trades", None)


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


def _parse_ts_seconds(value: Any) -> float:
    """Parse epoch seconds/ms or a simple ISO-8601 timestamp to seconds."""
    raw = _fnum(value, 0.0)
    if raw > 0.0:
        return raw / 1000.0 if raw > 1e12 else raw
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value.strip().replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0.0
    return 0.0


def _funding_leg_pnl_from_rate(side: str, rate_pct: float) -> float:
    side = str(side or "").upper()
    if side == "SHORT":
        return float(rate_pct)
    if side == "LONG":
        return -float(rate_pct)
    return 0.0


def _next_hour_boundary(now_ts: float) -> float:
    return (math.floor(float(now_ts) / 3600.0) + 1.0) * 3600.0


def _first_dict(value: Any) -> Optional[dict]:
    if isinstance(value, dict):
        return value
    if isinstance(value, list):
        for row in value:
            if isinstance(row, dict):
                return row
    return None


def _funding_forecast_raw(venue: str, symbol: str, cfg: Any, now_ts: float) -> dict:
    """Best-effort public forecast for ONE upcoming funding settlement.

    rate_pct is expressed in percentage points for the venue's next settlement
    (e.g. 0.12 means 0.12%). Positive rate means LONG pays SHORT. Unknown venues
    stay unavailable rather than being guessed.
    """
    venue_norm = _normalize_venue(venue)
    symbol = str(symbol or "").upper()
    key = (venue_norm, symbol)
    cached = _FUNDING_FORECAST_CACHE.get(key)
    if cached and now_ts - cached[0] <= FUNDING_FORECAST_TTL_SECONDS:
        return dict(cached[1])

    out = {
        "venue": venue_norm,
        "symbol": symbol,
        "available": False,
        "rate_pct": 0.0,
        "next_settlement_ts": 0.0,
        "source": "unavailable",
        "error": "",
    }
    if not FUNDING_FORECAST_ENABLED or not symbol:
        out["error"] = "forecast disabled" if not FUNDING_FORECAST_ENABLED else "missing symbol"
        _FUNDING_FORECAST_CACHE[key] = (now_ts, dict(out))
        return out

    timeout = _fnum(getattr(cfg, "request_timeout_seconds", 10.0), 10.0)
    try:
        if venue_norm == "aster":
            data = core.get_json(
                f"{core.ASTER_BASE}/fapi/v1/premiumIndex", timeout,
                params={"symbol": symbol},
            )
            row = _first_dict(data)
            if row:
                out["rate_pct"] = _fnum(row.get("lastFundingRate", row.get("fundingRate"))) * 100.0
                out["next_settlement_ts"] = _parse_ts_seconds(row.get("nextFundingTime"))
                out["source"] = "aster-premiumIndex"

        elif venue_norm == "bitget":
            data = core.get_json(
                f"{core.BITGET_BASE}/api/v2/mix/market/ticker", timeout,
                params={"symbol": symbol, "productType": "USDT-FUTURES"},
            )
            payload = data.get("data") if isinstance(data, dict) else data
            row = _first_dict(payload)
            if row:
                out["rate_pct"] = _fnum(row.get("fundingRate")) * 100.0
                out["next_settlement_ts"] = _parse_ts_seconds(row.get("nextFundingTime"))
                out["source"] = "bitget-ticker"

        elif venue_norm == "bybit":
            data = core.get_json(
                f"{core.BYBIT_BASE}/v5/market/tickers", timeout,
                params={"category": "linear", "symbol": symbol},
            )
            payload = ((data or {}).get("result", {}) or {}).get("list", []) if isinstance(data, dict) else []
            row = _first_dict(payload)
            if row:
                out["rate_pct"] = _fnum(row.get("fundingRate")) * 100.0
                out["next_settlement_ts"] = _parse_ts_seconds(row.get("nextFundingTime"))
                out["source"] = "bybit-ticker"

        elif venue_norm == "mexc":
            native = symbol[:-4] + "_USDT" if symbol.endswith("USDT") else symbol
            data = core.get_json(
                f"{core.MEXC_CONTRACT_BASE}/api/v1/contract/funding_rate/{native}", timeout,
            )
            row = data.get("data") if isinstance(data, dict) and isinstance(data.get("data"), dict) else _first_dict(data)
            if row:
                out["rate_pct"] = _fnum(row.get("fundingRate")) * 100.0
                out["next_settlement_ts"] = _parse_ts_seconds(row.get("nextSettleTime"))
                out["source"] = "mexc-funding-rate"

        elif venue_norm == "hyperliquid":
            coin = symbol[:-4] if symbol.endswith("USDT") else symbol
            data = core.post_json(core.HYPERLIQUID_INFO_URL, timeout, {"type": "metaAndAssetCtxs"})
            if isinstance(data, list) and len(data) >= 2 and isinstance(data[0], dict) and isinstance(data[1], list):
                universe = data[0].get("universe", [])
                for asset, ctx in zip(universe if isinstance(universe, list) else [], data[1]):
                    if not isinstance(asset, dict) or not isinstance(ctx, dict):
                        continue
                    if str(asset.get("name", "")).upper() == coin.upper():
                        out["rate_pct"] = _fnum(ctx.get("funding")) * 100.0
                        out["next_settlement_ts"] = _next_hour_boundary(now_ts)
                        out["source"] = "hyperliquid-metaAndAssetCtxs"
                        break

        elif venue_norm == "lighter":
            stats = {}
            try:
                _quotes, stats = core._LIGHTER_STREAM.snapshot()
            except Exception:
                try:
                    _quotes, stats = core.fetch_lighter_bootstrap(cfg)
                except Exception:
                    stats = {}
            row = stats.get(symbol, {}) if isinstance(stats, dict) else {}
            if isinstance(row, dict) and row:
                out["rate_pct"] = _fnum(row.get("current_funding_rate")) * 100.0
                last_ts = _parse_ts_seconds(row.get("funding_timestamp"))
                next_ts = (last_ts + 3600.0) if last_ts > 0.0 else _next_hour_boundary(now_ts)
                while next_ts <= now_ts:
                    next_ts += 3600.0
                out["next_settlement_ts"] = next_ts
                out["source"] = "lighter-current_funding_rate"

        elif venue_norm == "dydx":
            data = core.get_json(f"{core.DYDX_BASE}/perpetualMarkets", timeout)
            markets = data.get("markets", data.get("perpetualMarkets", data)) if isinstance(data, dict) else data
            rows: Iterable[Tuple[str, dict]] = []
            if isinstance(markets, dict):
                rows = [(str(k), v) for k, v in markets.items() if isinstance(v, dict)]
            elif isinstance(markets, list):
                rows = [(str(v.get("ticker", v.get("market", ""))), v) for v in markets if isinstance(v, dict)]
            base = symbol[:-4] if symbol.endswith("USDT") else symbol
            for k, row in rows:
                ticker = str(row.get("ticker", row.get("market", k))).upper()
                if ticker in {f"{base}-USD", symbol, base}:
                    out["rate_pct"] = _fnum(row.get("nextFundingRate", row.get("fundingRate"))) * 100.0
                    out["next_settlement_ts"] = _parse_ts_seconds(row.get("nextFundingAt", row.get("nextFundingTime")))
                    out["source"] = "dydx-perpetualMarkets"
                    break

        # edgeX and any future unsupported venue remain explicitly unavailable.
        out["available"] = bool(out["next_settlement_ts"] > now_ts and math.isfinite(out["rate_pct"]))
        if not out["available"] and not out["error"]:
            out["error"] = "missing current rate or future settlement timestamp"
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"

    _FUNDING_FORECAST_CACHE[key] = (now_ts, dict(out))
    return out


def _funding_leg_forecast(venue: str, symbol: str, side: str, cfg: Any, now_ts: float) -> dict:
    raw = _funding_forecast_raw(venue, symbol, cfg, now_ts)
    next_ts = _fnum(raw.get("next_settlement_ts"), 0.0)
    within_hold = bool(raw.get("available")) and next_ts <= now_ts + ECON_MAX_HOLD_HOURS * 3600.0
    pnl_pct = _funding_leg_pnl_from_rate(side, _fnum(raw.get("rate_pct"), 0.0)) if within_hold else 0.0
    return {
        **raw,
        "side": str(side or "").upper(),
        "within_max_hold": within_hold,
        "expected_pnl_pct": pnl_pct,
        "minutes_to_settlement": max(0.0, (next_ts - now_ts) / 60.0) if next_ts > now_ts else None,
    }


def _annotate_event_business_thesis(event: dict, cfg: Any) -> None:
    """Attach SPREAD / SPREAD+FUNDING semantics to a qualifying event.

    The existing scanner only emits POSITIVE-ECONOMICS when spread expected NET
    is >0.100%, so FUNDING-only events are not manufactured here. The FUNDING
    thesis value is nevertheless supported by the exit/reporting code for a
    future dedicated funding scanner.
    """
    now_ts = _fnum(event.get("ts"), time.time())
    side = str(event.get("side") or "").upper()
    hedge_side = "LONG" if side == "SHORT" else "SHORT"
    target = str(event.get("target") or "")
    hedge = str(event.get("execution_best_external_venue") or "")
    symbol = str(event.get("symbol") or "")
    spread_net = _fnum(event.get("execution_fresh_net_edge_pct"), 0.0)

    target_fc = _funding_leg_forecast(target, symbol, side, cfg, now_ts)
    hedge_fc = _funding_leg_forecast(hedge, symbol, hedge_side, cfg, now_ts)
    ready = bool(target_fc.get("within_max_hold")) and bool(hedge_fc.get("within_max_hold"))
    net_funding = _fnum(target_fc.get("expected_pnl_pct"), 0.0) + _fnum(hedge_fc.get("expected_pnl_pct"), 0.0)
    funding_positive = ready and net_funding > FUNDING_CLASSIFICATION_MIN_NET_PCT + 1e-12
    spread_positive = spread_net > SIGNAL_NET_THRESHOLD_PCT
    if spread_positive and funding_positive:
        thesis = "SPREAD+FUNDING"
    elif spread_positive:
        thesis = "SPREAD"
    elif funding_positive:
        thesis = "FUNDING"
    else:
        thesis = "NONE"

    event["business_thesis"] = thesis
    event["entry_spread_expected_net_pct"] = spread_net
    event["entry_funding_forecast_status"] = "READY" if ready else ("PARTIAL" if target_fc.get("available") or hedge_fc.get("available") else "UNAVAILABLE")
    event["entry_funding_target_forecast"] = target_fc
    event["entry_funding_hedge_forecast"] = hedge_fc
    event["entry_expected_net_funding_pct"] = net_funding if ready else 0.0
    event["entry_expected_combined_net_pct"] = spread_net + (net_funding if funding_positive else 0.0)


def _refresh_trade_funding_breakdown(trade: dict, cfg: Any, end_ts: float) -> None:
    """Persist actual settled funding PnL for BOTH legs, including negative legs.

    The core's existing NET funding remains the accounting source of truth. If
    one venue's historical API cannot reconstruct an older settlement (notably a
    stale Lighter settlement), derive the missing leg only when the other leg is
    known and the saved NET makes the reconciliation unambiguous. Never replace
    a non-zero saved NET with an incomplete zero breakdown.
    """
    if cfg is None or not getattr(cfg, "funding_tracking_enabled", True):
        return
    start_ts = _fnum(trade.get("opened_ts"), 0.0)
    if end_ts <= start_ts:
        return
    symbol = str(trade.get("symbol") or "")
    is_closed = str(trade.get("status") or "").upper() == "CLOSED"
    existing_net = _fnum(
        trade.get("realized_funding_pct_x") if is_closed else trade.get("funding_net_pnl_pct_x"),
        _fnum(trade.get("funding_net_pnl_pct_x"), 0.0),
    )
    try:
        target_rows, target_err = core._funding_rows_cached(str(trade.get("target")), symbol, start_ts, end_ts, cfg)
        hedge_rows, hedge_err = core._funding_rows_cached(str(trade.get("hedge_venue")), symbol, start_ts, end_ts, cfg)
        target_rates = [_fnum(x.get("rate_pct")) for x in target_rows if isinstance(x, dict)]
        hedge_rates = [_fnum(x.get("rate_pct")) for x in hedge_rows if isinstance(x, dict)]
        target_pnl = _fnum(core._side_funding_pnl_pct(str(trade.get("target_side")), target_rates))
        hedge_pnl = _fnum(core._side_funding_pnl_pct(str(trade.get("hedge_side")), hedge_rates))
        computed = target_pnl + hedge_pnl
        target_known = bool(target_rates) or (not target_err and abs(existing_net) <= 1e-12)
        hedge_known = bool(hedge_rates) or (not hedge_err and abs(existing_net) <= 1e-12)
        source = "public-settlements"

        if abs(existing_net - computed) > 1e-9:
            if target_rates and not hedge_rates:
                hedge_pnl = existing_net - target_pnl
                hedge_known = True
                source = "target-public+hedge-derived-from-saved-net"
            elif hedge_rates and not target_rates:
                target_pnl = existing_net - hedge_pnl
                target_known = True
                source = "hedge-public+target-derived-from-saved-net"
            elif not target_rates and not hedge_rates:
                # The saved net is valid, but there is no defensible split.
                trade["funding_breakdown_status"] = "NET_ONLY"
                trade["funding_breakdown_source"] = "saved-net-no-leg-history"
                trade["funding_net_pnl_pct_x"] = existing_net
                return
            else:
                # Both APIs returned data but disagree with the saved core net.
                # Preserve the saved net and flag the leg split as unreliable.
                trade["funding_breakdown_status"] = "MISMATCH"
                trade["funding_breakdown_source"] = "saved-net-preferred"
                trade["funding_breakdown_computed_net_pct"] = computed
                trade["funding_net_pnl_pct_x"] = existing_net
                return

        trade["funding_target_pnl_pct_x"] = target_pnl
        trade["funding_hedge_pnl_pct_x"] = hedge_pnl
        trade["funding_net_pnl_pct_x"] = existing_net if abs(existing_net) > 1e-12 else (target_pnl + hedge_pnl)
        trade["funding_target_settlement_count"] = len(target_rates)
        trade["funding_hedge_settlement_count"] = len(hedge_rates)
        trade["funding_target_settlement_ts"] = [_fnum(x.get("ts")) for x in target_rows if isinstance(x, dict)]
        trade["funding_hedge_settlement_ts"] = [_fnum(x.get("ts")) for x in hedge_rows if isinstance(x, dict)]
        trade["funding_breakdown_status"] = "READY" if target_known and hedge_known else "PARTIAL"
        trade["funding_breakdown_source"] = source
        trade["funding_status"] = "READY" if not target_err and not hedge_err else "PARTIAL"
    except Exception as exc:
        trade["funding_breakdown_status"] = "ERROR"
        trade["funding_breakdown_error"] = f"{type(exc).__name__}: {exc}"


def _trade_business_thesis(trade: dict) -> str:
    thesis = str(trade.get("business_thesis") or "").upper()
    return thesis if thesis in {"SPREAD", "FUNDING", "SPREAD+FUNDING"} else "LEGACY"


def _funding_settlement_seen(trade: dict) -> bool:
    return int(_fnum(trade.get("funding_target_settlement_count"), 0.0)) > 0 or int(_fnum(trade.get("funding_hedge_settlement_count"), 0.0)) > 0


def _exact_route_gap_pct(trade: dict, target_vwap: float, hedge_vwap: float) -> Optional[float]:
    """Return the executable residual spread for the exact stored route.

    Positive means the original target-vs-hedge dislocation is still present;
    zero means parity; negative means it has crossed through parity.
    """
    target_vwap = _fnum(target_vwap, 0.0)
    hedge_vwap = _fnum(hedge_vwap, 0.0)
    if target_vwap <= 0.0 or hedge_vwap <= 0.0:
        return None
    side = str(trade.get("target_side") or "").upper()
    if side == "SHORT":
        return (target_vwap / hedge_vwap - 1.0) * 100.0
    if side == "LONG":
        return (hedge_vwap / target_vwap - 1.0) * 100.0
    return None


def _leg_pnl_pct(side: str, entry: float, exit_vwap: float) -> Optional[float]:
    entry = _fnum(entry, 0.0)
    exit_vwap = _fnum(exit_vwap, 0.0)
    if entry <= 0.0 or exit_vwap <= 0.0:
        return None
    side = str(side or "").upper()
    if side == "LONG":
        return (exit_vwap / entry - 1.0) * 100.0
    if side == "SHORT":
        return (entry / exit_vwap - 1.0) * 100.0
    return None


def _exact_route_mark(trade: dict, target_vwap: Optional[float] = None, hedge_vwap: Optional[float] = None) -> Optional[dict]:
    """Recompute MTM from exact entry/exit VWAPs, fees and funding.

    The core already fetches the exact target and stored hedge books for paper
    trades. This function deliberately ignores any consensus/fair-price gap.
    """
    tvwap = _fnum(
        target_vwap if target_vwap is not None else trade.get("last_target_close_vwap"),
        0.0,
    )
    hvwap = _fnum(
        hedge_vwap if hedge_vwap is not None else trade.get("last_hedge_close_vwap"),
        0.0,
    )
    if tvwap <= 0.0 or hvwap <= 0.0:
        return None

    target_leg = _leg_pnl_pct(
        str(trade.get("target_side") or ""),
        _fnum(trade.get("entry_target_vwap"), 0.0),
        tvwap,
    )
    hedge_leg = _leg_pnl_pct(
        str(trade.get("hedge_side") or ""),
        _fnum(trade.get("entry_hedge_vwap"), 0.0),
        hvwap,
    )
    if target_leg is None or hedge_leg is None:
        return None

    exact_gap = _exact_route_gap_pct(trade, tvwap, hvwap)
    if exact_gap is None:
        return None
    gross = target_leg + hedge_leg
    fees = max(0.0, _fnum(trade.get("expected_roundtrip_fees_pct_x"), 0.0))
    funding = _fnum(trade.get("funding_net_pnl_pct_x"), 0.0)
    spread_net = gross - fees
    net = spread_net + funding
    entry_gap = max(0.0, _fnum(trade.get("entry_gap_pct"), 0.0))
    convergence = ((entry_gap - exact_gap) / entry_gap) if entry_gap > 1e-12 else 0.0
    return {
        "target_vwap": tvwap,
        "hedge_vwap": hvwap,
        "exact_gap_pct": exact_gap,
        "convergence_fraction": convergence,
        "target_leg_pnl_pct": target_leg,
        "hedge_leg_pnl_pct": hedge_leg,
        "gross_pnl_pct": gross,
        "fees_pct": fees,
        "spread_net_pnl_pct": spread_net,
        "funding_pct": funding,
        "funding_target_pct": _fnum(trade.get("funding_target_pnl_pct_x"), 0.0),
        "funding_hedge_pct": _fnum(trade.get("funding_hedge_pnl_pct_x"), 0.0),
        "net_pnl_pct": net,
    }


def _update_exact_route_trade_metrics(trade: dict, mark: dict, now_ts: float) -> None:
    """Overwrite legacy consensus-derived paper metrics with exact-route values."""
    trade["last_target_close_vwap"] = mark["target_vwap"]
    trade["last_hedge_close_vwap"] = mark["hedge_vwap"]
    trade["latest_same_side_gap_pct"] = mark["exact_gap_pct"]
    trade["exact_route_gap_pct"] = mark["exact_gap_pct"]
    trade["exact_route_convergence_fraction"] = mark["convergence_fraction"]
    trade["current_gross_pnl_pct_x"] = mark["gross_pnl_pct"]
    trade["current_fees_pct_x"] = mark["fees_pct"]
    trade["current_spread_net_pnl_pct_x"] = mark["spread_net_pnl_pct"]
    trade["current_funding_target_pnl_pct_x"] = mark["funding_target_pct"]
    trade["current_funding_hedge_pnl_pct_x"] = mark["funding_hedge_pct"]
    trade["current_net_pnl_pct_x"] = mark["net_pnl_pct"]
    trade["best_net_pnl_pct_x"] = max(
        _fnum(trade.get("best_net_pnl_pct_x"), mark["net_pnl_pct"]),
        mark["net_pnl_pct"],
    )
    trade["worst_net_pnl_pct_x"] = min(
        _fnum(trade.get("worst_net_pnl_pct_x"), mark["net_pnl_pct"]),
        mark["net_pnl_pct"],
    )
    trade["min_same_side_gap_pct"] = min(
        _fnum(trade.get("min_same_side_gap_pct"), mark["exact_gap_pct"]),
        mark["exact_gap_pct"],
    )
    trade["max_same_side_gap_pct"] = max(
        _fnum(trade.get("max_same_side_gap_pct"), mark["exact_gap_pct"]),
        mark["exact_gap_pct"],
    )
    trade["max_convergence_fraction"] = max(
        _fnum(trade.get("max_convergence_fraction"), 0.0),
        mark["convergence_fraction"],
    )
    age = max(0.0, now_ts - _fnum(trade.get("opened_ts"), now_ts))
    if trade.get("time_to_50_seconds") is None and mark["convergence_fraction"] >= 0.50:
        trade["time_to_50_seconds"] = age
    if trade.get("time_to_80_seconds") is None and mark["convergence_fraction"] >= 0.80:
        trade["time_to_80_seconds"] = age
    trade["v166_exit_logic_version"] = ECON_EXIT_LOGIC_VERSION
    trade["v166_exact_route_mark_ts"] = now_ts
    trade["v167_exit_logic_version"] = ECON_EXIT_LOGIC_VERSION
    trade["v167_exact_route_mark_ts"] = now_ts
    trade["v168_exit_logic_version"] = ECON_EXIT_LOGIC_VERSION
    trade["v168_exact_route_mark_ts"] = now_ts


def _econ_exact_exit_reason(trade: dict, mark: dict, now_ts: float) -> Optional[str]:
    """Business-aware exact-route exit policy.

    SPREAD: funding can never rescue the spread exit; only price PnL after fees
    may satisfy the target/convergence exit.
    FUNDING: require at least one actual settlement while the trade was open,
    then require positive total executable PnL after price, fees and funding.
    SPREAD+FUNDING: may exit early on a successful spread, or after a settlement
    once combined realized NET reaches the combined entry target.
    LEGACY open ECON trades are treated conservatively as SPREAD.
    """
    thesis = _trade_business_thesis(trade)
    expected_spread = max(0.0, _fnum(trade.get("entry_spread_expected_net_pct"), _fnum(trade.get("entry_expected_net_edge_pct"), 0.0)))
    expected_combined = max(expected_spread, _fnum(trade.get("entry_expected_combined_net_pct"), expected_spread))
    reserve = max(0.0, _fnum(trade.get("entry_exit_slippage_reserve_pct"), 0.0))
    spread_net = _fnum(mark.get("spread_net_pnl_pct"), _fnum(mark.get("net_pnl_pct")) - _fnum(mark.get("funding_pct")))
    total_net = _fnum(mark.get("net_pnl_pct"), 0.0)
    settlement_seen = _funding_settlement_seen(trade)

    if thesis in {"SPREAD", "LEGACY"}:
        if spread_net + ECON_EXIT_EPSILON_PCT >= expected_spread:
            return "ECON_EXACT_ROUTE_SPREAD_NET_TARGET"
        if mark["exact_gap_pct"] <= reserve + ECON_EXIT_EPSILON_PCT and spread_net > ECON_EXIT_EPSILON_PCT:
            return "ECON_EXACT_ROUTE_SPREAD_CONVERGENCE"

    elif thesis == "FUNDING":
        if settlement_seen and total_net > ECON_EXIT_EPSILON_PCT:
            return "ECON_EXACT_ROUTE_FUNDING_POSITIVE_AFTER_SETTLEMENT"

    elif thesis == "SPREAD+FUNDING":
        # Never wait for funding if the spread thesis alone has already paid.
        if spread_net + ECON_EXIT_EPSILON_PCT >= expected_spread:
            return "ECON_EXACT_ROUTE_SPREAD_NET_TARGET"
        if mark["exact_gap_pct"] <= reserve + ECON_EXIT_EPSILON_PCT and spread_net > ECON_EXIT_EPSILON_PCT:
            return "ECON_EXACT_ROUTE_SPREAD_CONVERGENCE"
        if settlement_seen and total_net + ECON_EXIT_EPSILON_PCT >= expected_combined:
            return "ECON_EXACT_ROUTE_COMBINED_NET_TARGET"

    opened_ts = _fnum(trade.get("opened_ts"), now_ts)
    if now_ts - opened_ts >= ECON_MAX_HOLD_HOURS * 3600.0:
        return f"ECON_EXACT_ROUTE_{'FUNDING' if thesis == 'FUNDING' else 'SPREAD' if thesis in {'SPREAD','LEGACY'} else 'COMBINED'}_MAX_HOLD"
    return None


def _finalize_exact_econ_trade(state: dict, trade_id: str, trade: dict, mark: dict, reason: str, now_ts: float, cfg: Any = None) -> bool:
    """Move one ECON trade from open to closed using exact-route accounting."""
    bucket = state.get("paper_trades_open", {})
    if not isinstance(bucket, dict) or trade_id not in bucket:
        return False
    _update_exact_route_trade_metrics(trade, mark, now_ts)
    trade["status"] = "CLOSED"
    trade["close_reason"] = reason
    trade["closed_ts"] = now_ts
    trade["holding_seconds"] = max(0.0, now_ts - _fnum(trade.get("opened_ts"), now_ts))
    trade["exit_target_vwap"] = mark["target_vwap"]
    trade["exit_hedge_vwap"] = mark["hedge_vwap"]
    trade["exit_same_side_gap_pct"] = mark["exact_gap_pct"]
    trade["realized_target_leg_pnl_pct_x"] = mark["target_leg_pnl_pct"]
    trade["realized_hedge_leg_pnl_pct_x"] = mark["hedge_leg_pnl_pct"]
    trade["realized_gross_pnl_pct_x"] = mark["gross_pnl_pct"]
    trade["realized_fees_pct_x"] = mark["fees_pct"]
    trade["realized_spread_net_pnl_pct_x"] = mark["spread_net_pnl_pct"]
    trade["realized_funding_target_pnl_pct_x"] = mark["funding_target_pct"]
    trade["realized_funding_hedge_pnl_pct_x"] = mark["funding_hedge_pct"]
    trade["realized_funding_pct_x"] = mark["funding_pct"]
    trade["realized_net_pnl_pct_x"] = mark["net_pnl_pct"]
    trade["realized_pnl_x"] = mark["net_pnl_pct"] / 100.0 * max(0.0, _fnum(trade.get("notional_x"), 1.0))

    bucket.pop(trade_id, None)
    closed = state.setdefault("paper_trades_closed", [])
    if not isinstance(closed, list):
        closed = []
        state["paper_trades_closed"] = closed
    closed.append(trade)
    try:
        if callable(_ORIGINAL_PAPER_NOTE_CLOSE):
            if cfg is not None:
                _ORIGINAL_PAPER_NOTE_CLOSE(state, trade, cfg)
            else:
                _ORIGINAL_PAPER_NOTE_CLOSE(state, trade)
    except Exception:
        pass
    return True


def _strip_legacy_close_fields(trade: dict) -> None:
    for key in (
        "close_reason", "closed_ts", "holding_seconds", "exit_target_vwap",
        "exit_hedge_vwap", "exit_same_side_gap_pct", "realized_target_leg_pnl_pct_x",
        "realized_hedge_leg_pnl_pct_x", "realized_gross_pnl_pct_x",
        "realized_fees_pct_x", "realized_spread_net_pnl_pct_x",
        "realized_funding_target_pnl_pct_x", "realized_funding_hedge_pnl_pct_x",
        "realized_funding_pct_x", "realized_net_pnl_pct_x", "realized_pnl_x",
    ):
        trade.pop(key, None)
    trade["status"] = "OPEN"


def _repair_and_apply_exact_econ_exits(state: dict, pre_open_ids: set, now_ts: float, cfg: Any = None) -> bool:
    """Undo false legacy closes, then apply V16.8 business-aware exact-route exit rules."""
    changed = False
    bucket = state.setdefault("paper_trades_open", {})
    if not isinstance(bucket, dict):
        bucket = {}
        state["paper_trades_open"] = bucket
    closed = state.setdefault("paper_trades_closed", [])
    if not isinstance(closed, list):
        closed = []
        state["paper_trades_closed"] = closed

    # Core V16.4 may have just closed ECON trades because a consensus LAG outcome
    # said FULL. Re-evaluate those closes from their stored exact route marks.
    kept_closed: List[dict] = []
    for trade in closed:
        if not isinstance(trade, dict):
            kept_closed.append(trade)
            continue
        trade_id = str(trade.get("id") or "")
        close_reason = str(trade.get("close_reason") or "")
        is_new_legacy_close = (
            trade_id in pre_open_ids
            and str(trade.get("strategy") or "") == "ECON"
            and not close_reason.startswith("ECON_EXACT_ROUTE_")
        )
        if not is_new_legacy_close:
            kept_closed.append(trade)
            continue

        _refresh_trade_funding_breakdown(trade, cfg, now_ts)
        mark = _exact_route_mark(
            trade,
            _fnum(trade.get("exit_target_vwap"), 0.0),
            _fnum(trade.get("exit_hedge_vwap"), 0.0),
        )
        if mark is None:
            # No exact marks => never accept a legacy consensus close.
            _strip_legacy_close_fields(trade)
            bucket[trade_id] = trade
            changed = True
            continue

        _update_exact_route_trade_metrics(trade, mark, now_ts)
        reason = _econ_exact_exit_reason(trade, mark, now_ts)
        if reason is None:
            _strip_legacy_close_fields(trade)
            bucket[trade_id] = trade
            changed = True
            continue

        # Closure is valid, but rewrite all close accounting from exact route.
        trade["close_reason"] = reason
        trade["closed_ts"] = now_ts
        trade["holding_seconds"] = max(0.0, now_ts - _fnum(trade.get("opened_ts"), now_ts))
        trade["exit_target_vwap"] = mark["target_vwap"]
        trade["exit_hedge_vwap"] = mark["hedge_vwap"]
        trade["exit_same_side_gap_pct"] = mark["exact_gap_pct"]
        trade["realized_target_leg_pnl_pct_x"] = mark["target_leg_pnl_pct"]
        trade["realized_hedge_leg_pnl_pct_x"] = mark["hedge_leg_pnl_pct"]
        trade["realized_gross_pnl_pct_x"] = mark["gross_pnl_pct"]
        trade["realized_fees_pct_x"] = mark["fees_pct"]
        trade["realized_spread_net_pnl_pct_x"] = mark["spread_net_pnl_pct"]
        trade["realized_funding_target_pnl_pct_x"] = mark["funding_target_pct"]
        trade["realized_funding_hedge_pnl_pct_x"] = mark["funding_hedge_pct"]
        trade["realized_funding_pct_x"] = mark["funding_pct"]
        trade["realized_net_pnl_pct_x"] = mark["net_pnl_pct"]
        trade["current_net_pnl_pct_x"] = mark["net_pnl_pct"]
        trade["realized_pnl_x"] = mark["net_pnl_pct"] / 100.0 * max(0.0, _fnum(trade.get("notional_x"), 1.0))
        try:
            if callable(_ORIGINAL_PAPER_NOTE_CLOSE):
                if cfg is not None:
                    _ORIGINAL_PAPER_NOTE_CLOSE(state, trade, cfg)
                else:
                    _ORIGINAL_PAPER_NOTE_CLOSE(state, trade)
        except Exception:
            pass
        kept_closed.append(trade)
        changed = True

    if len(kept_closed) != len(closed) or any(a is not b for a, b in zip(kept_closed, closed)):
        state["paper_trades_closed"] = kept_closed
        closed = kept_closed

    # Correct marks for every still-open ECON trade and proactively close only
    # when its own exact route satisfies the V16.8 condition.
    for trade_id, trade in list(bucket.items()):
        if not isinstance(trade, dict) or str(trade.get("strategy") or "") != "ECON":
            continue
        _refresh_trade_funding_breakdown(trade, cfg, now_ts)
        mark = _exact_route_mark(trade)
        if mark is None:
            continue
        _update_exact_route_trade_metrics(trade, mark, now_ts)
        reason = _econ_exact_exit_reason(trade, mark, now_ts)
        if reason is not None:
            if _finalize_exact_econ_trade(state, trade_id, trade, mark, reason, now_ts, cfg):
                changed = True
        else:
            changed = True
    return changed


def open_economic_paper_trade(state: dict, event: dict, cfg: Any) -> bool:
    """Open one independent paper trade for one qualifying economic signal.

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
        "business_thesis": str(event.get("business_thesis") or "SPREAD"),
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
        "entry_spread_expected_net_pct": _fnum(event.get("entry_spread_expected_net_pct"), fresh_net),
        "entry_expected_net_funding_pct": _fnum(event.get("entry_expected_net_funding_pct"), 0.0),
        "entry_expected_combined_net_pct": _fnum(event.get("entry_expected_combined_net_pct"), fresh_net),
        "entry_funding_forecast_status": str(event.get("entry_funding_forecast_status") or "UNAVAILABLE"),
        "entry_funding_target_forecast": event.get("entry_funding_target_forecast") if isinstance(event.get("entry_funding_target_forecast"), dict) else {},
        "entry_funding_hedge_forecast": event.get("entry_funding_hedge_forecast") if isinstance(event.get("entry_funding_hedge_forecast"), dict) else {},
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
        "funding_target_pnl_pct_x": 0.0,
        "funding_hedge_pnl_pct_x": 0.0,
        "funding_net_pnl_pct_x": 0.0,
        "funding_target_settlement_count": 0,
        "funding_hedge_settlement_count": 0,
        "updates": 0,
        "missing_depth_updates": 0,
        "checkpoints": {},
        "v165_signal_net_threshold_pct": SIGNAL_NET_THRESHOLD_PCT,
        "v165_fresh_net_pct": fresh_net,
        "v166_exit_logic_version": ECON_EXIT_LOGIC_VERSION,
        "v166_exact_route_exit": True,
        "v167_exit_logic_version": ECON_EXIT_LOGIC_VERSION,
        "v167_exact_route_exit": True,
        "v168_exit_logic_version": ECON_EXIT_LOGIC_VERSION,
        "v168_exact_route_exit": True,
        "engine_version_at_entry": "16.8",
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
        "business_thesis": event.get("business_thesis"),
        "expected_net_funding_pct": _fnum(event.get("entry_expected_net_funding_pct"), 0.0),
        "expected_combined_net_pct": _fnum(event.get("entry_expected_combined_net_pct"), _fnum(event.get("execution_fresh_net_edge_pct"), 0.0)),
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
    state["scanner_version"] = "16.8"

    def callback(event: dict) -> None:
        typ = str(event.get("type", ""))

        if typ == "POSITIVE-ECONOMICS":
            # Defense in depth: core readiness is already forced to >0.100%.
            if not _signal_is_qualifying(event):
                return
            _annotate_event_business_thesis(event, cfg)
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

    pre_open = state.get("paper_trades_open", {})
    pre_open_ids = {
        str(k) for k, v in pre_open.items()
        if isinstance(pre_open, dict) and isinstance(v, dict) and str(v.get("strategy") or "") == "ECON"
    } if isinstance(pre_open, dict) else set()

    ranked, errors, changed = _ORIGINAL_SCAN(
        cfg, state, active_signal_callback=callback
    )

    exit_changed = _repair_and_apply_exact_econ_exits(state, pre_open_ids, time.time(), cfg)
    return ranked, errors, bool(changed or exit_changed or previous_version != "16.8")


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
    thesis = html.escape(str(event.get("business_thesis") or "SPREAD"))
    fund_status = str(event.get("entry_funding_forecast_status") or "UNAVAILABLE")
    target_fc = event.get("entry_funding_target_forecast") if isinstance(event.get("entry_funding_target_forecast"), dict) else {}
    hedge_fc = event.get("entry_funding_hedge_forecast") if isinstance(event.get("entry_funding_hedge_forecast"), dict) else {}
    fund_net = _fnum(event.get("entry_expected_net_funding_pct"), 0.0)
    combined = _fnum(event.get("entry_expected_combined_net_pct"), net)

    def fc_line(name: str, fc: dict) -> str:
        if not fc or not fc.get("within_max_hold"):
            return f"  {name}: unavailable"
        mins = fc.get("minutes_to_settlement")
        mins_txt = f" in {float(mins):.0f}m" if mins is not None else ""
        return f"  {name} {html.escape(str(fc.get('side') or '?'))}: {_fnum(fc.get('expected_pnl_pct')):+.4f}%{mins_txt}"

    paper_ok = bool(event.get("_v165_paper_opened"))
    paper_line = "✅ paper entry recorded" if paper_ok else "⚠️ paper entry was NOT recorded"
    funding_block = (
        f"Funding forecast [{html.escape(fund_status)}]:\n"
        f"{fc_line('target', target_fc)}\n"
        f"{fc_line('hedge', hedge_fc)}\n"
        f"  NET expected funding: <b>{fund_net:+.4f}%</b>\n"
        f"Combined expected economics: <b>{combined:+.4f}%</b>"
    ) if fund_status != "UNAVAILABLE" else "Funding forecast: unavailable — thesis defaults to spread economics only."

    return (
        f"💰 <b>TRADE SIGNAL — {symbol} @ {label}</b>\n"
        f"Thesis: <b>{thesis}</b>\n"
        f"Rule: executable spread expected NET <b>&gt; {SIGNAL_NET_THRESHOLD_PCT:.3f}%</b>\n"
        f"Route: <b>{side} {label}</b> / hedge via <b>{hedge}</b>\n"
        f"Fresh VWAP @ ${usd:,.0f}: gross <b>{gross:.4f}%</b>\n"
        f"Round-trip taker fees: {fees:.4f}% | exit-slip reserve: {reserve:.4f}%\n"
        f"Spread expected NET after costs: <b>{net:+.4f}%</b> ✅\n"
        f"{funding_block}\n"
        f"Entry VWAP target / hedge: <code>{tvwap:.10g}</code> / <code>{hvwap:.10g}</code>\n"
        f"Executable notional verified up to: <b>${ready_usd:,.0f}</b> | ref disagreement {disagreement:.3f}%\n"
        f"Paper: <b>{paper_line}</b>\n\n"
        "Funding forecast is indicative until an actual settlement occurs. "
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



def _trade_funding_pct(trade: dict, closed: bool) -> float:
    key = "realized_funding_pct_x" if closed else "funding_net_pnl_pct_x"
    return _fnum(trade.get(key), 0.0)


def _trade_funding_leg_pct(trade: dict, closed: bool, leg: str) -> float:
    if leg not in {"target", "hedge"}:
        return 0.0
    if closed:
        key = f"realized_funding_{leg}_pnl_pct_x"
        fallback = f"funding_{leg}_pnl_pct_x"
    else:
        key = f"current_funding_{leg}_pnl_pct_x"
        fallback = f"funding_{leg}_pnl_pct_x"
    return _fnum(trade.get(key), _fnum(trade.get(fallback), 0.0))


def _trade_funding_leg_breakdown_known(trade: dict) -> bool:
    status = str(trade.get("funding_breakdown_status") or "").upper()
    if status == "READY":
        return True
    return ("funding_target_pnl_pct_x" in trade and "funding_hedge_pnl_pct_x" in trade and status not in {"NET_ONLY", "MISMATCH", "ERROR"})


def _trade_fees_pct(trade: dict, closed: bool) -> float:
    key = "realized_fees_pct_x" if closed else "current_fees_pct_x"
    return max(0.0, _fnum(trade.get(key), _fnum(trade.get("expected_roundtrip_fees_pct_x"), 0.0)))


def _trade_gross_pct(trade: dict, closed: bool) -> float:
    key = "realized_gross_pnl_pct_x" if closed else "current_gross_pnl_pct_x"
    if key in trade:
        return _fnum(trade.get(key), 0.0)
    return _trade_net_pct(trade, closed) - _trade_funding_pct(trade, closed) + _trade_fees_pct(trade, closed)


def _trade_net_ex_funding_pct(trade: dict, closed: bool) -> float:
    key = "realized_spread_net_pnl_pct_x" if closed else "current_spread_net_pnl_pct_x"
    if key in trade:
        return _fnum(trade.get(key), 0.0)
    return _trade_net_pct(trade, closed) - _trade_funding_pct(trade, closed)


def _aggregate_trade_pnl(rows: List[Tuple[dict, bool]]) -> dict:
    out = {k: 0.0 for k in ("gross_pct","fees_pct","spread_pct","target_funding_pct","hedge_funding_pct","funding_pct","total_pct","gross_usd","fees_usd","spread_usd","target_funding_usd","hedge_funding_usd","funding_usd","total_usd")}
    out["funding_leg_unknown_count"] = 0
    for trade, closed in rows:
        gross = _trade_gross_pct(trade, closed)
        fees = _trade_fees_pct(trade, closed)
        spread = _trade_net_ex_funding_pct(trade, closed)
        leg_known = _trade_funding_leg_breakdown_known(trade)
        tf = _trade_funding_leg_pct(trade, closed, "target") if leg_known else 0.0
        hf = _trade_funding_leg_pct(trade, closed, "hedge") if leg_known else 0.0
        if not leg_known and abs(_trade_funding_pct(trade, closed)) > 1e-12:
            out["funding_leg_unknown_count"] += 1
        funding = _trade_funding_pct(trade, closed)
        total = _trade_net_pct(trade, closed)
        for k,v in (("gross_pct",gross),("fees_pct",fees),("spread_pct",spread),("target_funding_pct",tf),("hedge_funding_pct",hf),("funding_pct",funding),("total_pct",total)):
            out[k] += v
        for k,v in (("gross_usd",gross),("fees_usd",fees),("spread_usd",spread),("target_funding_usd",tf),("hedge_funding_usd",hf),("funding_usd",funding),("total_usd",total)):
            out[k] += _trade_pnl_usd(trade, v)
    return out


def _trade_reserve_usd(trade: dict) -> float:
    """Notional-equivalent capital reserved while both perp legs are open."""
    per_leg = max(0.0, _fnum(trade.get("probe_notional_usd"), 100.0))
    return 2.0 * per_leg


def _econ_trade_rows(state: dict) -> List[Tuple[dict, bool]]:
    """Return de-duplicated ECON trades as (trade, is_closed)."""
    by_id: Dict[str, Tuple[dict, bool]] = {}
    for trade in _closed_econ_trades(state):
        trade_id = str(trade.get("id") or trade.get("dedupe_key") or id(trade))
        by_id[trade_id] = (trade, True)
    for trade in _open_econ_trades(state):
        trade_id = str(trade.get("id") or trade.get("dedupe_key") or id(trade))
        # OPEN is authoritative if an inconsistent duplicate exists.
        by_id[trade_id] = (trade, False)
    return list(by_id.values())


def _capital_constrained_snapshot(
    state: dict,
    report_date: date,
    tz: ZoneInfo,
    as_of_ts: Optional[float] = None,
) -> dict:
    """Replay ECON entries against one shared finite capital pool.

    This is deliberately parallel to the all-signals ledger. Each accepted trade
    reserves 2 * probe_notional_usd (target leg + hedge leg). Reserved capital
    cannot be reused until that exact paper trade closes. On close, realized PnL
    is added to the balance before later entries are considered.

    The simulation is not a leverage/margin model; it is a conservative
    notional-equivalent capital-occupancy model, which is the requested answer to
    "could I have taken every signal with one finite pool of money?".
    """
    as_of_ts = time.time() if as_of_ts is None else float(as_of_ts)
    rows = [
        (t, closed)
        for t, closed in _econ_trade_rows(state)
        if 0.0 < _fnum(t.get("opened_ts"), 0.0) <= as_of_ts
    ]
    rows.sort(key=lambda item: (_fnum(item[0].get("opened_ts"), 0.0), str(item[0].get("id") or "")))

    start_balance = float(CAPITAL_SIM_START_USD)
    balance = start_balance
    reserved: Dict[str, Tuple[float, float, dict, bool]] = {}
    accepted: Dict[str, Tuple[dict, bool]] = {}
    skipped: Dict[str, Tuple[dict, bool]] = {}

    def realized_usd_for(trade: dict) -> float:
        return _trade_pnl_usd(trade, _trade_net_pct(trade, True))

    def release_until(ts: float) -> None:
        nonlocal balance
        due = []
        for trade_id, (close_ts, reserve_usd, trade, was_closed) in reserved.items():
            if was_closed and 0.0 < close_ts <= ts:
                due.append((close_ts, trade_id, reserve_usd, trade))
        due.sort(key=lambda x: (x[0], x[1]))
        for _, trade_id, _reserve_usd, trade in due:
            balance += realized_usd_for(trade)
            reserved.pop(trade_id, None)

    for trade, is_closed in rows:
        opened_ts = _fnum(trade.get("opened_ts"), 0.0)
        release_until(opened_ts)
        trade_id = str(trade.get("id") or trade.get("dedupe_key") or id(trade))
        reserve_usd = _trade_reserve_usd(trade)
        reserved_total = sum(v[1] for v in reserved.values())
        free_usd = balance - reserved_total
        if free_usd + CAPITAL_SIM_EPSILON_USD >= reserve_usd:
            accepted[trade_id] = (trade, is_closed)
            close_ts = _fnum(trade.get("closed_ts"), 0.0) if is_closed else 0.0
            # A close after as_of is not available to fund another entry yet.
            if close_ts <= 0.0 or close_ts > as_of_ts:
                close_ts = 0.0
                is_closed_for_release = False
            else:
                is_closed_for_release = True
            reserved[trade_id] = (close_ts, reserve_usd, trade, is_closed_for_release)
        else:
            skipped[trade_id] = (trade, is_closed)

    release_until(as_of_ts)
    reserved_total = sum(v[1] for v in reserved.values())
    free_usd = balance - reserved_total
    open_mtm_usd = 0.0
    for trade_id, (_close_ts, _reserve_usd, trade, _was_closed) in reserved.items():
        if trade_id in accepted:
            open_mtm_usd += _trade_pnl_usd(trade, _trade_net_pct(trade, False))
    equity_usd = balance + open_mtm_usd

    def is_report_day(trade: dict) -> bool:
        return _local_date_for_ts(_fnum(trade.get("opened_ts"), 0.0), tz) == report_date

    day_accepted = [(t, c) for t, c in accepted.values() if is_report_day(t)]
    day_skipped = [(t, c) for t, c in skipped.values() if is_report_day(t)]

    day_ex_funding_pct = 0.0
    day_funding_pct = 0.0
    day_total_pct = 0.0
    day_ex_funding_usd = 0.0
    day_funding_usd = 0.0
    day_total_usd = 0.0
    day_closed = 0
    day_open = 0
    for trade, is_closed in day_accepted:
        closed_as_of = bool(is_closed and 0.0 < _fnum(trade.get("closed_ts"), 0.0) <= as_of_ts)
        if closed_as_of:
            day_closed += 1
        else:
            day_open += 1
        net_pct = _trade_net_pct(trade, closed_as_of)
        funding_pct = _trade_funding_pct(trade, closed_as_of)
        ex_funding_pct = net_pct - funding_pct
        day_total_pct += net_pct
        day_funding_pct += funding_pct
        day_ex_funding_pct += ex_funding_pct
        day_total_usd += _trade_pnl_usd(trade, net_pct)
        day_funding_usd += _trade_pnl_usd(trade, funding_pct)
        day_ex_funding_usd += _trade_pnl_usd(trade, ex_funding_pct)

    day_breakdown = _aggregate_trade_pnl([(t, bool(c and 0.0 < _fnum(t.get("closed_ts"), 0.0) <= as_of_ts)) for t, c in day_accepted])

    return {
        "enabled": bool(CAPITAL_SIM_ENABLED),
        "start_balance_usd": start_balance,
        "balance_usd": balance,
        "reserved_usd": reserved_total,
        "free_usd": free_usd,
        "open_mtm_usd": open_mtm_usd,
        "equity_usd": equity_usd,
        "return_pct": ((equity_usd - start_balance) / start_balance * 100.0) if start_balance > 0 else 0.0,
        "accepted_count": len(day_accepted),
        "skipped_count": len(day_skipped),
        "closed_count": day_closed,
        "open_count": day_open,
        "ex_funding_pct": day_ex_funding_pct,
        "funding_pct": day_funding_pct,
        "total_pct": day_total_pct,
        "ex_funding_usd": day_ex_funding_usd,
        "funding_usd": day_funding_usd,
        "total_usd": day_total_usd,
        "gross_usd": day_breakdown["gross_usd"],
        "fees_usd": day_breakdown["fees_usd"],
        "target_funding_usd": day_breakdown["target_funding_usd"],
        "hedge_funding_usd": day_breakdown["hedge_funding_usd"],
        "funding_leg_unknown_count": int(day_breakdown["funding_leg_unknown_count"]),
    }

def build_daily_report_text(state: dict, report_date: date, tz: ZoneInfo, cfg: Any = None) -> str:
    signal_rows = state.get("v165_signal_events", []) if isinstance(state, dict) else []
    if not isinstance(signal_rows, list):
        signal_rows = []
    day_signals = [r for r in signal_rows if isinstance(r, dict) and _local_date_for_ts(_fnum(r.get("ts"), 0.0), tz) == report_date]
    paper_entries = sum(1 for r in day_signals if bool(r.get("paper_opened")))

    closed = [r for r in _closed_econ_trades(state) if _local_date_for_ts(_fnum(r.get("opened_ts"), 0.0), tz) == report_date]
    opened = [r for r in _open_econ_trades(state) if _local_date_for_ts(_fnum(r.get("opened_ts"), 0.0), tz) == report_date]
    if cfg is not None:
        now_ts = time.time()
        for t in closed:
            _refresh_trade_funding_breakdown(t, cfg, _fnum(t.get("closed_ts"), now_ts))
        for t in opened:
            _refresh_trade_funding_breakdown(t, cfg, now_ts)

    closed_values = [(_trade_net_pct(t, True), t) for t in closed]
    open_values = [(_trade_net_pct(t, False), t) for t in opened]
    all_values = closed_values + open_values
    all_rows = [(t, True) for t in closed] + [(t, False) for t in opened]
    agg = _aggregate_trade_pnl(all_rows)

    wins = sum(1 for v, _ in closed_values if v > 1e-12)
    losses = sum(1 for v, _ in closed_values if v < -1e-12)
    breakeven = len(closed_values) - wins - losses
    win_rate = (wins / len(closed_values) * 100.0) if closed_values else None
    realized_usd = sum(_trade_pnl_usd(t, v) for v, t in closed_values)
    open_usd = sum(_trade_pnl_usd(t, v) for v, t in open_values)
    missing_paper = max(0, len(day_signals) - paper_entries)
    untracked_entries = max(0, paper_entries - len(all_values))

    if int(agg["funding_leg_unknown_count"]) > 0:
        target_funding_line = f"Target/Hedge funding split: <b>n/a for {int(agg['funding_leg_unknown_count'])} legacy trades</b>; NET funding below remains authoritative"
        hedge_funding_line = None
    else:
        target_funding_line = f"Target-leg funding: <b>{agg['target_funding_usd']:+.4f} USD</b> | Σ {agg['target_funding_pct']:+.4f}%"
        hedge_funding_line = f"Hedge-leg funding: <b>{agg['hedge_funding_usd']:+.4f} USD</b> | Σ {agg['hedge_funding_pct']:+.4f}%"

    lines = [
        f"📊 <b>DAILY PAPER REPORT — {report_date.strftime('%d.%m.%Y')}</b>",
        f"Rule: fresh executable spread NET <b>&gt; {SIGNAL_NET_THRESHOLD_PCT:.3f}%</b> | sizing: $100 per leg by default",
        f"Signals: <b>{len(day_signals)}</b> | paper entries: <b>{paper_entries}</b> | entry failures: <b>{missing_paper}</b>",
        f"Closed: <b>{len(closed)}</b> | still open: <b>{len(opened)}</b>",
        (f"Wins / losses / flat: <b>{wins} / {losses} / {breakeven}</b> | win rate <b>{win_rate:.1f}%</b>" if win_rate is not None else "Wins / losses / flat: <b>0 / 0 / 0</b> | win rate <b>n/a</b>"),
        "",
        "<b>ALL-SIGNALS PnL — source decomposition</b>",
        f"Gross price PnL: <b>{agg['gross_usd']:+.4f} USD</b> | Σ {agg['gross_pct']:+.4f}%",
        f"Taker fees: <b>-{agg['fees_usd']:.4f} USD</b> | Σ -{agg['fees_pct']:.4f}%",
        f"Spread NET (price-fees): <b>{agg['spread_usd']:+.4f} USD</b> | Σ {agg['spread_pct']:+.4f}%",
        target_funding_line,
        f"NET funding: <b>{agg['funding_usd']:+.4f} USD</b> | Σ {agg['funding_pct']:+.4f}%",
        f"TOTAL with funding: <b>{agg['total_usd']:+.4f} USD</b> | Σ {agg['total_pct']:+.4f}% (realized {realized_usd:+.4f}, open {open_usd:+.4f})",
        "",
        "<b>BY BUSINESS THESIS</b>",
    ]
    if hedge_funding_line is not None:
        # Keep target and hedge adjacent, before NET funding, for new V16.8 cohorts.
        idx = next((i for i, x in enumerate(lines) if x.startswith("NET funding:")), len(lines))
        lines.insert(idx, hedge_funding_line)

    for thesis in ("SPREAD", "SPREAD+FUNDING", "FUNDING", "LEGACY"):
        subset = [(t, c) for t, c in all_rows if _trade_business_thesis(t) == thesis]
        if not subset:
            continue
        a = _aggregate_trade_pnl(subset)
        c_count = sum(1 for _, c in subset if c)
        o_count = len(subset) - c_count
        lines.append(
            f"{thesis}: <b>{len(subset)}</b> trades ({c_count} closed/{o_count} open) | "
            f"spread {a['spread_usd']:+.4f} | funding {a['funding_usd']:+.4f} | total <b>{a['total_usd']:+.4f} USD</b>"
        )

    if CAPITAL_SIM_ENABLED:
        cap = _capital_constrained_snapshot(state, report_date, tz, time.time())
        cap_funding_split = (
            f"Target funding: <b>{cap['target_funding_usd']:+.4f} USD</b> | hedge funding: <b>{cap['hedge_funding_usd']:+.4f} USD</b>"
            if int(cap.get("funding_leg_unknown_count", 0)) == 0
            else f"Target/Hedge funding split: <b>n/a for {int(cap.get('funding_leg_unknown_count', 0))} legacy trades</b>"
        )
        lines.extend([
            "",
            f"<b>CAPITAL-CONSTRAINED SIM — ${cap['start_balance_usd']:,.0f} shared pool</b>",
            f"Entered: <b>{cap['accepted_count']}</b> | skipped (capital busy): <b>{cap['skipped_count']}</b> | closed/open: <b>{cap['closed_count']} / {cap['open_count']}</b>",
            f"Gross price PnL: <b>{cap['gross_usd']:+.4f} USD</b>",
            f"Taker fees: <b>-{cap['fees_usd']:.4f} USD</b>",
            f"Spread NET: <b>{cap['ex_funding_usd']:+.4f} USD</b> | Σ {cap['ex_funding_pct']:+.4f}%",
            cap_funding_split,
            f"NET funding: <b>{cap['funding_usd']:+.4f} USD</b> | Σ {cap['funding_pct']:+.4f}%",
            f"TOTAL with funding: <b>{cap['total_usd']:+.4f} USD</b> | Σ {cap['total_pct']:+.4f}%",
            f"Portfolio: free <b>${cap['free_usd']:.2f}</b> + reserved <b>${cap['reserved_usd']:.2f}</b> + open MTM <b>{cap['open_mtm_usd']:+.2f}</b> = equity <b>${cap['equity_usd']:.2f}</b> ({cap['return_pct']:+.3f}% vs start)",
        ])

    if all_values:
        best_v, best_t = max(all_values, key=lambda x: x[0])
        worst_v, worst_t = min(all_values, key=lambda x: x[0])
        lines.extend(["", f"Best: <b>{html.escape(str(best_t.get('symbol') or '?'))} {best_v:+.4f}%</b>", f"Worst: <b>{html.escape(str(worst_t.get('symbol') or '?'))} {worst_v:+.4f}%</b>"])
    if untracked_entries:
        lines.append(f"⚠️ {untracked_entries} paper entries are not present in the retained open/closed ledger.")
    lines.extend([
        "",
        "V16.8: SPREAD exits ignore funding; FUNDING exits require an actual settlement and positive total NET; "
        "SPREAD+FUNDING may exit on spread alone or after settlement on the combined target. Funding PnL is the signed sum of target + hedge legs. "
        "All marks use the exact stored route and executable VWAPs. Capital simulation reserves both leg notionals until close.",
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

    text = build_daily_report_text(state, completed_date, tz, cfg)
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
    """Deterministic local tests for V16.8-specific overlay logic."""
    cfg = core.Config()
    state: dict = {}
    now = time.time()
    event = {
        "type": "POSITIVE-ECONOMICS",
        "business_thesis": "SPREAD",
        "entry_spread_expected_net_pct": 0.1201,
        "entry_expected_net_funding_pct": 0.0,
        "entry_expected_combined_net_pct": 0.1201,
        "entry_funding_forecast_status": "UNAVAILABLE",
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

    # Exact-route exit regression: consensus can claim FULL while the stored
    # route has widened. That must remain OPEN.
    regression = {
        "id": "paper:ECON:test:SHORT:hedge:1",
        "strategy": "ECON",
        "status": "OPEN",
        "target_side": "SHORT",
        "hedge_side": "LONG",
        "opened_ts": now,
        "entry_target_vwap": 0.95112,
        "entry_hedge_vwap": 0.9492253908842765,
        "entry_gap_pct": 0.19959528410407393,
        "entry_expected_net_edge_pct": 0.1169203068309551,
        "entry_exit_slippage_reserve_pct": 0.002674977273118823,
        "expected_roundtrip_fees_pct_x": 0.08,
        "funding_net_pnl_pct_x": 0.0,
        "last_target_close_vwap": 0.9451,
        "last_hedge_close_vwap": 0.9416751456766251,
    }
    widened = _exact_route_mark(regression)
    assert widened is not None
    assert widened["exact_gap_pct"] > regression["entry_gap_pct"]
    assert _econ_exact_exit_reason(regression, widened, now + 600) is None

    # When the exact stored route itself converges, the ECON exit is valid.
    converged = _exact_route_mark(regression, 0.94170, 0.94168)
    assert converged is not None
    assert converged["exact_gap_pct"] < 0.01
    assert _econ_exact_exit_reason(regression, converged, now + 1200) is not None

    # Regression guard: if the legacy core physically moves an ECON trade into
    # closed on consensus FULL, V16.8 must restore it to OPEN when the exact
    # route itself has not met an ECON exit condition.
    repair_trade = dict(regression)
    repair_trade["id"] = "paper:ECON:repair:1"
    repair_trade["close_reason"] = "LAG_FULL_CONVERGENCE"
    repair_trade["closed_ts"] = now + 600
    repair_trade["exit_target_vwap"] = 0.9451
    repair_trade["exit_hedge_vwap"] = 0.9416751456766251
    repair_trade["status"] = "CLOSED"
    repair_state = {"paper_trades_open": {}, "paper_trades_closed": [repair_trade]}
    assert _repair_and_apply_exact_econ_exits(repair_state, {repair_trade["id"]}, now + 600, cfg)
    assert repair_trade["id"] in repair_state["paper_trades_open"]
    assert not repair_state["paper_trades_closed"]

    # Business-thesis regression: funding must NOT rescue a SPREAD trade.
    spread_only = dict(regression)
    spread_only["business_thesis"] = "SPREAD"
    spread_only["entry_spread_expected_net_pct"] = 0.10
    spread_mark = dict(widened)
    spread_mark["spread_net_pnl_pct"] = -0.05
    spread_mark["funding_pct"] = 0.50
    spread_mark["net_pnl_pct"] = 0.45
    assert _econ_exact_exit_reason(spread_only, spread_mark, now + 1200) is None

    combined = dict(regression)
    combined["business_thesis"] = "SPREAD+FUNDING"
    combined["entry_spread_expected_net_pct"] = 0.10
    combined["entry_expected_combined_net_pct"] = 0.20
    combined["funding_target_settlement_count"] = 1
    combined["funding_hedge_settlement_count"] = 1
    combined_mark = dict(widened)
    combined_mark["spread_net_pnl_pct"] = -0.05
    combined_mark["funding_pct"] = 0.30
    combined_mark["net_pnl_pct"] = 0.25
    assert _econ_exact_exit_reason(combined, combined_mark, now + 1200) == "ECON_EXACT_ROUTE_COMBINED_NET_TARGET"

    # Capital-occupancy regression: two simultaneous trades that each reserve
    # 60% of the shared balance cannot both be accepted.
    cap_probe = CAPITAL_SIM_START_USD * 0.30
    cap_state = {"paper_trades_open": {}}
    for idx in range(2):
        cap_state["paper_trades_open"][f"cap-{idx}"] = {
            "id": f"cap-{idx}",
            "strategy": "ECON",
            "status": "OPEN",
            "symbol": "CAPUSDT",
            "opened_ts": now + idx,
            "probe_notional_usd": cap_probe,
            "current_net_pnl_pct_x": 0.0,
            "funding_net_pnl_pct_x": 0.0,
        }
    tz = ZoneInfo(REPORT_TIMEZONE)
    cap = _capital_constrained_snapshot(cap_state, _local_date_for_ts(now, tz), tz, now + 10)
    assert cap["accepted_count"] == 1
    assert cap["skipped_count"] == 1

    record_v165_signal_event(state, event, True)
    report = build_daily_report_text(state, _local_date_for_ts(now, tz), tz, cfg)
    assert "Signals: <b>1</b>" in report
    assert "ALL-SIGNALS PnL" in report
    assert "Spread NET" in report
    assert "Target-leg funding" in report
    assert "BY BUSINESS THESIS" in report
    assert "CAPITAL-CONSTRAINED SIM" in report
    print("V16.8 overlay self-test OK (>0.100% spread NET + business-thesis exits + two-leg funding + capital occupancy)")


def _guarded_paper_note_close(state: dict, trade: dict, *args: Any, **kwargs: Any) -> None:
    """Prevent legacy-driven ECON closes from polluting paper stats.

    The V16.4 core calls ``_paper_note_close`` with an additional positional
    argument in some close paths.  Preserve the core call signature generically
    so the overlay remains compatible with those paths instead of assuming a
    fixed two-argument helper.

    V16.8 finalizes ECON trades itself after validating the exact stored route.
    Legacy ECON closes are suppressed; every non-ECON close is forwarded to the
    original core helper with its arguments unchanged.
    """
    if isinstance(trade, dict) and str(trade.get("strategy") or "") == "ECON":
        reason = str(trade.get("close_reason") or "")
        if not reason.startswith("ECON_EXACT_ROUTE_"):
            return
    if callable(_ORIGINAL_PAPER_NOTE_CLOSE):
        _ORIGINAL_PAPER_NOTE_CLOSE(state, trade, *args, **kwargs)


def _state_from_update_call(args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Optional[dict]:
    """Locate the persistent scanner state in a generic core call signature."""
    explicit = kwargs.get("state")
    if isinstance(explicit, dict):
        return explicit
    state_markers = {
        "paper_trades_open", "paper_trades_closed", "active_lag_outcomes",
        "scanner_version", "v165_signal_events", "v165_daily_report",
    }
    for value in args:
        if isinstance(value, dict) and any(k in value for k in state_markers):
            return value
    return None


def _cfg_from_update_call(args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Any:
    explicit = kwargs.get("cfg")
    if explicit is not None:
        return explicit
    for value in args:
        if hasattr(value, "paper_trading_enabled") and hasattr(value, "funding_tracking_enabled"):
            return value
    return None


def _guarded_update_paper_trades(*args: Any, **kwargs: Any) -> Any:
    """Run core marking/funding, then enforce exact-route ECON close semantics.

    This wrapper fixes the remaining V16.6 ordering bug: core.main() calls
    update_paper_trades *after* scan(), so a repair performed only inside the
    scan wrapper was too early. We intentionally let the V16.4 updater refresh
    marks, funding and checkpoints; immediately afterwards we undo every legacy
    ECON close that is not valid on the stored exact target+hedge route and then
    apply the V16.8 business-aware exact-route exit rules.
    """
    if not callable(_ORIGINAL_UPDATE_PAPER_TRADES):
        return None

    state = _state_from_update_call(args, kwargs)
    cfg = _cfg_from_update_call(args, kwargs)
    pre_open_ids: set = set()
    if isinstance(state, dict):
        bucket = state.get("paper_trades_open", {})
        if isinstance(bucket, dict):
            pre_open_ids = {
                str(k) for k, trade in bucket.items()
                if isinstance(trade, dict) and str(trade.get("strategy") or "") == "ECON"
            }

    result = _ORIGINAL_UPDATE_PAPER_TRADES(*args, **kwargs)

    repaired = False
    if isinstance(state, dict) and pre_open_ids:
        repaired = _repair_and_apply_exact_econ_exits(state, pre_open_ids, time.time(), cfg)

    # Preserve the core return contract. If it is a boolean "state changed"
    # flag, include overlay changes; otherwise return the original object.
    if isinstance(result, bool):
        return bool(result or repaired)
    return result


# Install monkey patches used by core.main().
if callable(_ORIGINAL_PAPER_NOTE_CLOSE):
    core._paper_note_close = _guarded_paper_note_close
if callable(_ORIGINAL_UPDATE_PAPER_TRADES):
    core.update_paper_trades = _guarded_update_paper_trades
core.scan = scan_v165
core.format_active_signal = format_trade_signal_v165
core.maybe_send_paper_performance_report = maybe_send_daily_report
# Guarantee no compact scan-audit messages and no old final CONFIRMED alerts,
# even if a stale workflow later changes those env values.
core.format_scan_summary_chunks = lambda *args, **kwargs: []
core.should_alert = lambda *args, **kwargs: False


def main() -> int:
    # Run V16.8 overlay tests in addition to the original V16.4 deterministic self-test.
    if "--self-test" in sys.argv:
        overlay_self_test()
    return int(core.main())


if __name__ == "__main__":
    raise SystemExit(main())
