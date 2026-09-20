#!/usr/bin/env python3
"""
V16.10 funding-aware profit-lock execution-economics monitor.

This is a single-file repository entrypoint. It does NOT require monitor_core.py.
To preserve the proven 7k+ line V16.4 market-data/execution engine without
duplicating it by hand, this file loads the last V16.4 version of monitor.py
directly from this repository's Git history into memory, then installs the V16.10
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
from collections import Counter
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

# V16.10: ECON exits are independent from legacy LAG/MM consensus outcomes.
# A trade is marked/closed only from the exact target + hedge route stored at entry.
ECON_EXIT_LOGIC_VERSION = 6
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
# V16.9 does not extrapolate a current funding rate indefinitely.  Entry
# economics only credits/debits the NEXT known settlements that fall inside a
# conservative expected-hold window.  Positive funding is never allowed to
# weaken the canonical spread >0.100% gate; known negative funding can veto an
# otherwise marginal spread signal when it is due before the expected exit.
FUNDING_ENTRY_RISK_HORIZON_MINUTES = max(5.0, float(os.getenv("FUNDING_ENTRY_RISK_HORIZON_MINUTES", "60")))
FUNDING_SETTLEMENT_SYNC_TOLERANCE_SECONDS = max(0.0, float(os.getenv("FUNDING_SETTLEMENT_SYNC_TOLERANCE_SECONDS", "90")))
FUNDING_EXTREME_RATE_WARN_PCT = max(0.0, float(os.getenv("FUNDING_EXTREME_RATE_WARN_PCT", "0.50")))
FUNDING_NEGATIVE_ENTRY_GUARD_ENABLED = os.getenv("FUNDING_NEGATIVE_ENTRY_GUARD_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}

# V16.10 money-management controls.  These affect exits only; they never turn
# funding into a successful SPREAD thesis.  A confirmed settlement can justify
# locking real total PnL, while the spread/funding components remain reported
# separately for research.
ECON_TOTAL_PROFIT_LOCK_ENABLED = os.getenv("ECON_TOTAL_PROFIT_LOCK_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}
ECON_TOTAL_PROFIT_LOCK_MIN_NET_PCT = max(0.0, float(os.getenv("ECON_TOTAL_PROFIT_LOCK_MIN_NET_PCT", "0.10")))
FUNDING_DYNAMIC_MANAGEMENT_ENABLED = os.getenv("FUNDING_DYNAMIC_MANAGEMENT_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}
FUNDING_DYNAMIC_HORIZON_MINUTES = max(5.0, float(os.getenv("FUNDING_DYNAMIC_HORIZON_MINUTES", "60")))
FUNDING_NEGATIVE_PROFIT_PROTECT_ENABLED = os.getenv("FUNDING_NEGATIVE_PROFIT_PROTECT_ENABLED", "true").strip().lower() not in {"0", "false", "no", "off"}
FUNDING_AUDIT_MAX_ROWS_PER_LEG = max(24, int(os.getenv("FUNDING_AUDIT_MAX_ROWS_PER_LEG", "100")))
_FUNDING_FORECAST_CACHE: Dict[Tuple[str, str], Tuple[float, dict]] = {}
_V169_LIGHTER_FUNDING_CACHE: Dict[Tuple[str, int, int], Tuple[List[dict], str]] = {}

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
_OVERLAY_MARKER = "V16.10 funding-aware profit-lock execution-economics monitor"
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
    """Return True only for the real monolithic V16.4 engine, never an overlay.

    V16.8+ overlays intentionally contain V16.4 marker strings in their bootstrap
    code.  A substring-only test can therefore mistake an older overlay for the
    core, execute it, and recursively bootstrap overlays until Python hits the
    recursion limit.  Reject bootstrap signatures first and require the real
    module-level V16.4 version assignment.
    """
    if not isinstance(data, (bytes, bytearray)) or len(data) < _CORE_MIN_BYTES:
        return False

    raw = bytes(data)

    # Any of these means this is one of our single-file overlay/bootstrap files,
    # not the original self-contained V16.4 engine.  Check the complete file so
    # an older overlay cannot be accepted merely because its banner differs from
    # the current V16.10 banner.
    overlay_sentinels = (
        b"def _load_v164_core(",
        b"Runtime-loaded V16.4 core",
        b"_CORE_MARKER =",
        b"_core_from_git_history(",
    )
    if any(token in raw for token in overlay_sentinels):
        return False

    head = raw[:120_000].decode("utf-8", errors="ignore")
    whole = raw.decode("utf-8", errors="ignore")

    # Require the actual top-level assignment.  Do not accept the same text when
    # it merely appears inside a string literal used by an overlay detector.
    version_assignment = any(
        line in {
            'SCANNER_VERSION = "16.4"',
            "SCANNER_VERSION = '16.4'",
        }
        for line in whole.splitlines()
    )

    return (
        _CORE_MARKER in head
        and version_assignment
        and "class Config" in whole
        and "def scan(" in whole
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
                "replacing it with V16.10."
            )
        source, origin = found

    # Belt-and-suspenders guard: never execute another overlay as the core.
    # This converts a bad history match into one clear startup error instead of
    # an unbounded recursive bootstrap.
    if b"def _load_v164_core(" in source or not _looks_like_v164_core(source):
        raise RuntimeError(
            f"Refusing to execute non-V16.4 bootstrap candidate as core: {origin}"
        )

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
core.SCANNER_VERSION = "16.10"
STATISTICS_SCHEMA_VERSION = "16.10.1"  # reporting-only patch; trading rules remain V16.10
try:
    core.USER_AGENT = "perp-inefficiency-scanner/16.10"
    if isinstance(getattr(core, "HTTP_HEADERS", None), dict):
        core.HTTP_HEADERS["User-Agent"] = core.USER_AGENT
except Exception:
    pass

_ORIGINAL_SCAN = core.scan
_ORIGINAL_FORMAT_ACTIVE_SIGNAL = core.format_active_signal
_ORIGINAL_PAPER_NOTE_CLOSE = getattr(core, "_paper_note_close", None)
_ORIGINAL_UPDATE_PAPER_TRADES = getattr(core, "update_paper_trades", None)
_ORIGINAL_FUNDING_ROWS_CACHED = getattr(core, "_funding_rows_cached", None)


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
    """Return the venue's NEXT known funding cashflow for one paper leg.

    V16.9 deliberately forecasts only the next published settlement.  It does
    not multiply the current rate by an assumed number of future intervals.
    """
    raw = _funding_forecast_raw(venue, symbol, cfg, now_ts)
    next_ts = _fnum(raw.get("next_settlement_ts"), 0.0)
    available = bool(raw.get("available")) and next_ts > now_ts
    rate_pct = _fnum(raw.get("rate_pct"), 0.0)
    pnl_pct = _funding_leg_pnl_from_rate(side, rate_pct) if available else 0.0
    return {
        **raw,
        "side": str(side or "").upper(),
        "available": available,
        "next_settlement_pnl_pct": pnl_pct,
        # Backward-compatible name retained from the V16.8 formatter/state.
        "expected_pnl_pct": pnl_pct,
        "minutes_to_settlement": max(0.0, (next_ts - now_ts) / 60.0) if available else None,
        "extreme_rate_warning": bool(available and FUNDING_EXTREME_RATE_WARN_PCT > 0.0 and abs(rate_pct) > FUNDING_EXTREME_RATE_WARN_PCT),
    }


def _event_forecast_hold_seconds(event: dict) -> float:
    """Conservative expected holding window used only for entry forecasts.

    If an event carries a real historical median convergence time, use it.
    Broad ECON events usually do not, so the explicit 60-minute fallback is
    used.  The hard maximum is still ECON_MAX_HOLD_HOURS.
    """
    candidates = (
        event.get("historical_median_t50_seconds"),
        event.get("median_t50_seconds"),
        event.get("lag_historical_median_t50_seconds"),
        event.get("time_to_50_seconds"),
    )
    hist = next((_fnum(x, 0.0) for x in candidates if _fnum(x, 0.0) > 0.0), 0.0)
    fallback = FUNDING_ENTRY_RISK_HORIZON_MINUTES * 60.0
    hold = hist if hist > 0.0 else fallback
    return min(ECON_MAX_HOLD_HOURS * 3600.0, max(300.0, hold))


def _combine_funding_timeline(target_fc: dict, hedge_fc: dict, now_ts: float, hold_seconds: float) -> dict:
    """Combine next funding cashflows on a common time axis.

    A leg contributes only if its own next settlement occurs inside the expected
    hold window. If one venue is unavailable, known positive carry is not
    credited. Known negative carry still counts as risk. Asynchronous funding
    events are evaluated in timestamp order so a later positive payment cannot
    hide an earlier negative cashflow in the entry guard.
    """
    horizon_ts = float(now_ts) + max(0.0, float(hold_seconds))
    legs = []
    for name, raw in (("target", target_fc), ("hedge", hedge_fc)):
        fc = dict(raw or {})
        available = bool(fc.get("available"))
        ts = _fnum(fc.get("next_settlement_ts"), 0.0)
        counted = bool(available and now_ts < ts <= horizon_ts)
        pnl = _fnum(fc.get("next_settlement_pnl_pct", fc.get("expected_pnl_pct")), 0.0) if counted else 0.0
        fc["counted_in_timeline"] = counted
        fc["timeline_pnl_pct"] = pnl
        fc["outside_forecast_horizon"] = bool(available and ts > horizon_ts)
        legs.append((name, fc))

    t = legs[0][1]
    h = legs[1][1]
    ready = bool(t.get("available")) and bool(h.get("available"))
    any_available = bool(t.get("available")) or bool(h.get("available"))
    raw_net = _fnum(t.get("timeline_pnl_pct"), 0.0) + _fnum(h.get("timeline_pnl_pct"), 0.0)

    counted_events = sorted(
        (
            _fnum(fc.get("next_settlement_ts"), 0.0),
            _fnum(fc.get("timeline_pnl_pct"), 0.0),
            name,
        )
        for name, fc in legs
        if fc.get("counted_in_timeline") and _fnum(fc.get("next_settlement_ts"), 0.0) > 0.0
    )
    first_ts = counted_events[0][0] if counted_events else 0.0
    first_cluster_net = 0.0
    if first_ts > 0.0:
        for ts, pnl, _name in counted_events:
            if abs(ts - first_ts) <= FUNDING_SETTLEMENT_SYNC_TOLERANCE_SECONDS:
                first_cluster_net += pnl

    worst_prefix = 0.0
    cumulative = 0.0
    i = 0
    while i < len(counted_events):
        cluster_ts = counted_events[i][0]
        cluster_pnl = 0.0
        j = i
        while j < len(counted_events) and abs(counted_events[j][0] - cluster_ts) <= FUNDING_SETTLEMENT_SYNC_TOLERANCE_SECONDS:
            cluster_pnl += counted_events[j][1]
            j += 1
        cumulative += cluster_pnl
        worst_prefix = min(worst_prefix, cumulative)
        i = j

    # Conservative entry adjustment:
    # - never credit positive funding when either venue forecast is unavailable;
    # - never let a later positive settlement mask an earlier negative prefix;
    # - debit an overall negative timeline even if the path never went lower.
    if ready:
        conservative_adjustment = min(0.0, raw_net, worst_prefix)
    else:
        known_negative = min(0.0, _fnum(t.get("timeline_pnl_pct"), 0.0)) + min(0.0, _fnum(h.get("timeline_pnl_pct"), 0.0))
        conservative_adjustment = min(0.0, known_negative, worst_prefix)

    return {
        "status": "READY" if ready else ("PARTIAL" if any_available else "UNAVAILABLE"),
        "target": t,
        "hedge": h,
        "hold_seconds": max(0.0, float(hold_seconds)),
        "horizon_ts": horizon_ts,
        "raw_net_pct": raw_net,
        "conservative_adjustment_pct": conservative_adjustment,
        "worst_prefix_net_pct": worst_prefix,
        "first_settlement_ts": first_ts,
        "first_settlement_cluster_net_pct": first_cluster_net,
        "extreme_rate_warning": bool(t.get("extreme_rate_warning") or h.get("extreme_rate_warning")),
    }


def _annotate_event_business_thesis(event: dict, cfg: Any) -> None:
    """Attach V16.9 SPREAD / SPREAD+FUNDING timeline semantics."""
    now_ts = _fnum(event.get("ts"), time.time())
    side = str(event.get("side") or "").upper()
    hedge_side = "LONG" if side == "SHORT" else "SHORT"
    target = str(event.get("target") or "")
    hedge = str(event.get("execution_best_external_venue") or "")
    symbol = str(event.get("symbol") or "")
    spread_net = _fnum(event.get("execution_fresh_net_edge_pct"), 0.0)

    target_fc = _funding_leg_forecast(target, symbol, side, cfg, now_ts)
    hedge_fc = _funding_leg_forecast(hedge, symbol, hedge_side, cfg, now_ts)
    hold_seconds = _event_forecast_hold_seconds(event)
    timeline = _combine_funding_timeline(target_fc, hedge_fc, now_ts, hold_seconds)
    target_fc = timeline["target"]
    hedge_fc = timeline["hedge"]
    ready = timeline["status"] == "READY"
    partial = timeline["status"] == "PARTIAL"
    timeline_net = _fnum(timeline.get("raw_net_pct"), 0.0)
    risk_adjustment = _fnum(timeline.get("conservative_adjustment_pct"), 0.0)

    # Expected funding and entry risk are intentionally different concepts:
    # - READY: both legs are known, so show the signed timeline NET as forecast;
    # - PARTIAL: never credit an unknown positive leg, but still debit known
    #   negative carry that can hit before the expected exit horizon;
    # - risk_adjustment is always <= 0 and is the only forecast component that
    #   can change entry qualification. Positive forecast carry never bypasses
    #   the canonical >0.100% spread gate.
    if ready:
        expected_funding_net = timeline_net
    elif partial:
        expected_funding_net = min(0.0, risk_adjustment)
    else:
        expected_funding_net = 0.0

    funding_positive = ready and expected_funding_net > FUNDING_CLASSIFICATION_MIN_NET_PCT + 1e-12
    spread_positive = spread_net > SIGNAL_NET_THRESHOLD_PCT

    if spread_positive and funding_positive:
        thesis = "SPREAD+FUNDING"
    elif spread_positive:
        thesis = "SPREAD"
    elif funding_positive:
        thesis = "FUNDING"
    else:
        thesis = "NONE"

    combined = spread_net + expected_funding_net
    effective_entry_net = spread_net + (risk_adjustment if FUNDING_NEGATIVE_ENTRY_GUARD_ENABLED else 0.0)

    event["business_thesis"] = thesis
    event["entry_spread_expected_net_pct"] = spread_net
    event["entry_funding_forecast_status"] = str(timeline["status"])
    event["entry_funding_target_forecast"] = target_fc
    event["entry_funding_hedge_forecast"] = hedge_fc
    event["entry_funding_forecast_horizon_seconds"] = hold_seconds
    event["entry_funding_forecast_horizon_ts"] = _fnum(timeline.get("horizon_ts"), 0.0)
    event["entry_funding_first_settlement_ts"] = _fnum(timeline.get("first_settlement_ts"), 0.0)
    event["entry_funding_first_cluster_net_pct"] = _fnum(timeline.get("first_settlement_cluster_net_pct"), 0.0)
    event["entry_funding_worst_prefix_net_pct"] = _fnum(timeline.get("worst_prefix_net_pct"), 0.0)
    event["entry_expected_net_funding_pct"] = expected_funding_net
    event["entry_funding_risk_adjustment_pct"] = risk_adjustment
    event["entry_expected_raw_timeline_funding_pct"] = timeline_net
    event["entry_expected_combined_net_pct"] = combined
    event["entry_business_effective_net_pct"] = effective_entry_net
    event["entry_funding_extreme_rate_warning"] = bool(timeline.get("extreme_rate_warning"))


def _entry_business_is_qualifying(event: dict) -> bool:
    if not _signal_is_qualifying(event):
        return False
    if not FUNDING_NEGATIVE_ENTRY_GUARD_ENABLED:
        return True
    effective = _fnum(event.get("entry_business_effective_net_pct"), _fnum(event.get("execution_fresh_net_edge_pct"), -999.0))
    return math.isfinite(effective) and effective > SIGNAL_NET_THRESHOLD_PCT


def _record_v169_entry_rejection(state: dict, event: dict) -> None:
    rows = state.setdefault("v169_funding_guard_rejections", [])
    if not isinstance(rows, list):
        rows = []
        state["v169_funding_guard_rejections"] = rows
    rows.append({
        "ts": _fnum(event.get("ts"), time.time()),
        "symbol": event.get("symbol"),
        "state_key": event.get("state_key"),
        "target": event.get("target"),
        "side": event.get("side"),
        "hedge_venue": event.get("execution_best_external_venue"),
        "spread_net_pct": _fnum(event.get("execution_fresh_net_edge_pct"), 0.0),
        "expected_net_funding_pct": _fnum(event.get("entry_expected_net_funding_pct"), 0.0),
        "funding_risk_adjustment_pct": _fnum(event.get("entry_funding_risk_adjustment_pct"), 0.0),
        "funding_worst_prefix_net_pct": _fnum(event.get("entry_funding_worst_prefix_net_pct"), 0.0),
        "effective_net_pct": _fnum(event.get("entry_business_effective_net_pct"), 0.0),
        "forecast_status": event.get("entry_funding_forecast_status"),
        "reason": "IMMINENT_NEGATIVE_FUNDING_REDUCES_EFFECTIVE_NET_TO_THRESHOLD_OR_BELOW",
    })
    if len(rows) > SIGNAL_LEDGER_MAX_ROWS:
        del rows[:-SIGNAL_LEDGER_MAX_ROWS]


def _lighter_market_id_for_symbol(symbol: str, cfg: Any) -> int:
    symbol = str(symbol or "").upper()
    stats: Any = {}
    try:
        _quotes, stats = core._LIGHTER_STREAM.snapshot()
    except Exception:
        stats = {}
    row = stats.get(symbol, {}) if isinstance(stats, dict) else {}
    market_id = int(_fnum(row.get("market_id"), -1.0)) if isinstance(row, dict) else -1
    if market_id >= 0:
        return market_id
    try:
        _quotes, stats = core.fetch_lighter_bootstrap(cfg)
        row = stats.get(symbol, {}) if isinstance(stats, dict) else {}
        return int(_fnum(row.get("market_id"), -1.0)) if isinstance(row, dict) else -1
    except Exception:
        return -1


def _lighter_public_funding_rows(symbol: str, start_ts: float, end_ts: float, cfg: Any) -> Tuple[List[dict], str]:
    """Fetch settled Lighter hourly funding observations from /api/v1/fundings.

    The endpoint returns an unsigned decimal ``rate`` plus ``direction``; a
    ``long`` direction means longs paid shorts, therefore the signed rate is
    positive. Response timestamps are seconds while request bounds are ms.
    """
    if end_ts <= start_ts:
        return [], ""
    key = (str(symbol).upper(), int(start_ts // 300), int(end_ts // 300))
    cached = _V169_LIGHTER_FUNDING_CACHE.get(key)
    if cached is not None:
        return list(cached[0]), str(cached[1])
    market_id = _lighter_market_id_for_symbol(symbol, cfg)
    if market_id < 0:
        out = ([], "lighter market_id unavailable")
        _V169_LIGHTER_FUNDING_CACHE[key] = out
        return out
    try:
        data = core.get_json(
            f"{core.LIGHTER_BASE}/api/v1/fundings",
            _fnum(getattr(cfg, "request_timeout_seconds", 10.0), 10.0),
            params={
                "market_id": market_id,
                "resolution": "1h",
                "start_timestamp": int(start_ts * 1000),
                "end_timestamp": int(end_ts * 1000),
                "count_back": 750,
            },
        )
        raw_rows = data.get("fundings", []) if isinstance(data, dict) else []
        rows: List[dict] = []
        for r in raw_rows if isinstance(raw_rows, list) else []:
            if not isinstance(r, dict):
                continue
            ts = _parse_ts_seconds(r.get("timestamp"))
            if not (start_ts < ts <= end_ts):
                continue
            raw_rate = _fnum(r.get("rate"), 0.0)
            direction = str(r.get("direction") or "").strip().lower()
            magnitude = abs(raw_rate)
            if direction == "long":
                signed = magnitude
            elif direction == "short":
                signed = -magnitude
            else:
                # Defensive fallback if Lighter ever changes the schema and
                # starts returning a signed rate without direction.
                signed = raw_rate
            rows.append({
                "ts": ts,
                "rate_pct": signed * 100.0,
                "source": "lighter-public-fundings",
                "direction": direction,
                # Keep the venue payload semantics auditable.  Lighter's
                # public history is normalized to signed percentage points for
                # PnL, but the raw API rate is retained alongside it.
                "raw_rate": raw_rate,
                "raw_rate_unit": "api-native",
                "normalization_multiplier_to_pct": 100.0,
                "normalized_rate_pct": signed * 100.0,
            })
        dedup: Dict[int, dict] = {}
        for row in rows:
            dedup[int(_fnum(row.get("ts")) * 1000)] = row
        out_rows = sorted(dedup.values(), key=lambda x: _fnum(x.get("ts")))
        out = (out_rows, "")
        _V169_LIGHTER_FUNDING_CACHE[key] = out
        return list(out_rows), ""
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        # The old helper's Lighter fallback reads the last *settled* stream row,
        # so it is acceptable as a degraded actual-settlement source, but it may
        # miss multiple hourly rows. Preserve that limitation in the error text.
        if callable(_ORIGINAL_FUNDING_ROWS_CACHED):
            try:
                rows, old_err = _ORIGINAL_FUNDING_ROWS_CACHED("lighter", symbol, start_ts, end_ts, cfg)
                if rows:
                    return rows, f"lighter /fundings unavailable ({err}); fallback last-settled stream row; {old_err}".strip("; ")
            except Exception:
                pass
        return [], err


def _funding_rows_cached_v169(venue: str, symbol: str, start_ts: float, end_ts: float, cfg: Any) -> Tuple[List[dict], str]:
    if _normalize_venue(venue) == "lighter":
        return _lighter_public_funding_rows(symbol, start_ts, end_ts, cfg)
    if callable(_ORIGINAL_FUNDING_ROWS_CACHED):
        return _ORIGINAL_FUNDING_ROWS_CACHED(venue, symbol, start_ts, end_ts, cfg)
    return [], f"funding history helper unavailable for {venue}"


def _funding_row_audit(venue: str, symbol: str, side: str, row: dict, probe_usd: float) -> dict:
    rate_pct = _fnum(row.get("rate_pct"), 0.0)
    cashflow_pct = _funding_leg_pnl_from_rate(side, rate_pct)
    out = {
        "venue": _normalize_venue(venue),
        "symbol": str(symbol or ""),
        "side": str(side or "").upper(),
        "settlement_ts": _fnum(row.get("ts"), 0.0),
        "rate_pct": rate_pct,
        "cashflow_pct": cashflow_pct,
        "cashflow_usd_at_probe": cashflow_pct / 100.0 * max(0.0, probe_usd),
        "source": str(row.get("source") or "public-funding-history"),
    }
    for key in ("direction", "raw_rate", "raw_rate_unit", "normalization_multiplier_to_pct", "normalized_rate_pct"):
        if key in row:
            out[key] = row.get(key)
    return out


def _build_funding_audit(trade: dict, target_rows: List[dict], hedge_rows: List[dict], complete: bool, asof_ts: float) -> dict:
    probe_usd = max(0.0, _fnum(trade.get("probe_notional_usd"), 100.0))
    symbol = str(trade.get("symbol") or "")
    target = [
        _funding_row_audit(str(trade.get("target") or ""), symbol, str(trade.get("target_side") or ""), r, probe_usd)
        for r in target_rows[-FUNDING_AUDIT_MAX_ROWS_PER_LEG:] if isinstance(r, dict)
    ]
    hedge = [
        _funding_row_audit(str(trade.get("hedge_venue") or ""), symbol, str(trade.get("hedge_side") or ""), r, probe_usd)
        for r in hedge_rows[-FUNDING_AUDIT_MAX_ROWS_PER_LEG:] if isinstance(r, dict)
    ]
    return {
        "complete_two_leg_snapshot": bool(complete),
        "asof_ts": float(asof_ts),
        "target": target,
        "hedge": hedge,
        "target_cashflow_pct": sum(_fnum(x.get("cashflow_pct"), 0.0) for x in target),
        "hedge_cashflow_pct": sum(_fnum(x.get("cashflow_pct"), 0.0) for x in hedge),
        "verification_scope": "public-market-funding-history",
        "account_cashflow_verified": False,
    }


def _refresh_trade_funding_breakdown(trade: dict, cfg: Any, end_ts: float) -> None:
    """Persist signed, settled funding for BOTH exact trade legs.

    V16.9 separates forecast from realized accounting.  A new V16.9 trade only
    advances its authoritative funding snapshot when BOTH venue histories are
    available for the whole open interval.  If one venue is temporarily
    unavailable, the last complete two-leg snapshot is preserved; a one-leg
    partial response is diagnostic only and can never trigger a funding exit.
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
    engine_version = str(trade.get("engine_version_at_entry") or "")
    is_v169 = engine_version.startswith("16.9") or engine_version.startswith("16.10") or bool(trade.get("v169_settlement_funding"))
    try:
        target_rows, target_err = core._funding_rows_cached(str(trade.get("target")), symbol, start_ts, end_ts, cfg)
        hedge_rows, hedge_err = core._funding_rows_cached(str(trade.get("hedge_venue")), symbol, start_ts, end_ts, cfg)
        target_rates = [_fnum(x.get("rate_pct")) for x in target_rows if isinstance(x, dict)]
        hedge_rates = [_fnum(x.get("rate_pct")) for x in hedge_rows if isinstance(x, dict)]
        target_pnl_live = _fnum(core._side_funding_pnl_pct(str(trade.get("target_side")), target_rates))
        hedge_pnl_live = _fnum(core._side_funding_pnl_pct(str(trade.get("hedge_side")), hedge_rates))
        computed_live = target_pnl_live + hedge_pnl_live
        target_known = not bool(target_err)
        hedge_known = not bool(hedge_err)

        if is_v169 and target_known and hedge_known:
            target_pnl = target_pnl_live
            hedge_pnl = hedge_pnl_live
            authoritative_net = computed_live
            target_count = len(target_rates)
            hedge_count = len(hedge_rates)
            target_ts = [_fnum(x.get("ts")) for x in target_rows if isinstance(x, dict)]
            hedge_ts = [_fnum(x.get("ts")) for x in hedge_rows if isinstance(x, dict)]
            source = "complete-public-settlements-v1610" if engine_version.startswith("16.10") else "complete-public-settlements-v169"
            audit = _build_funding_audit(trade, target_rows, hedge_rows, True, end_ts)
            trade["funding_settlement_audit"] = audit
            trade["v1610_funding_audit_version"] = 1

            # Persist the last COMPLETE two-leg snapshot.  It is the only
            # funding state allowed to affect exact-route exit logic.
            trade["v169_last_complete_funding_target_pct"] = target_pnl
            trade["v169_last_complete_funding_hedge_pct"] = hedge_pnl
            trade["v169_last_complete_funding_net_pct"] = authoritative_net
            trade["v169_last_complete_target_settlement_count"] = target_count
            trade["v169_last_complete_hedge_settlement_count"] = hedge_count
            trade["v169_last_complete_target_settlement_ts"] = list(target_ts)
            trade["v169_last_complete_hedge_settlement_ts"] = list(hedge_ts)
            trade["v169_last_complete_funding_asof_ts"] = float(end_ts)
        elif is_v169:
            # Never use a current one-leg response as realized two-leg carry.
            # Preserve the last complete snapshot (zero until the first one).
            target_pnl = _fnum(trade.get("v169_last_complete_funding_target_pct"), 0.0)
            hedge_pnl = _fnum(trade.get("v169_last_complete_funding_hedge_pct"), 0.0)
            authoritative_net = _fnum(trade.get("v169_last_complete_funding_net_pct"), 0.0)
            target_count = int(_fnum(trade.get("v169_last_complete_target_settlement_count"), 0.0))
            hedge_count = int(_fnum(trade.get("v169_last_complete_hedge_settlement_count"), 0.0))
            target_ts = list(trade.get("v169_last_complete_target_settlement_ts", []) or [])
            hedge_ts = list(trade.get("v169_last_complete_hedge_settlement_ts", []) or [])
            source = "partial-public-history-last-complete-preserved"
            trade["v169_partial_target_observed_pct"] = target_pnl_live
            trade["v169_partial_hedge_observed_pct"] = hedge_pnl_live
            trade["v169_partial_computed_net_pct"] = computed_live
            trade["v169_partial_target_error"] = str(target_err or "")
            trade["v169_partial_hedge_error"] = str(hedge_err or "")
            trade["v1610_partial_funding_audit"] = _build_funding_audit(trade, target_rows, hedge_rows, False, end_ts)
        else:
            # Backward compatibility for pre-V16.9 cohorts: keep their saved
            # NET as source of truth and reconstruct leg split only where safe.
            target_pnl = target_pnl_live
            hedge_pnl = hedge_pnl_live
            computed = computed_live
            target_count = len(target_rates)
            hedge_count = len(hedge_rates)
            target_ts = [_fnum(x.get("ts")) for x in target_rows if isinstance(x, dict)]
            hedge_ts = [_fnum(x.get("ts")) for x in hedge_rows if isinstance(x, dict)]
            source = "public-settlements"
            authoritative_net = existing_net if abs(existing_net) > 1e-12 else computed
            if abs(existing_net - computed) > 1e-9:
                if target_rates and not hedge_rates:
                    hedge_pnl = existing_net - target_pnl
                    source = "target-public+hedge-derived-from-saved-net"
                elif hedge_rates and not target_rates:
                    target_pnl = existing_net - hedge_pnl
                    source = "hedge-public+target-derived-from-saved-net"
                elif not target_rates and not hedge_rates:
                    trade["funding_breakdown_status"] = "NET_ONLY"
                    trade["funding_breakdown_source"] = "saved-net-no-leg-history"
                    trade["funding_net_pnl_pct_x"] = existing_net
                    return
                else:
                    trade["funding_breakdown_status"] = "MISMATCH"
                    trade["funding_breakdown_source"] = "saved-net-preferred"
                    trade["funding_breakdown_computed_net_pct"] = computed
                    trade["funding_net_pnl_pct_x"] = existing_net
                    return

        trade["funding_target_pnl_pct_x"] = target_pnl
        trade["funding_hedge_pnl_pct_x"] = hedge_pnl
        trade["funding_net_pnl_pct_x"] = authoritative_net
        trade["funding_target_settlement_count"] = target_count
        trade["funding_hedge_settlement_count"] = hedge_count
        trade["funding_target_settlement_ts"] = target_ts
        trade["funding_hedge_settlement_ts"] = hedge_ts
        trade["funding_breakdown_status"] = "READY" if target_known and hedge_known else "PARTIAL"
        trade["funding_breakdown_source"] = source
        trade["funding_status"] = "READY" if target_known and hedge_known else "PARTIAL"
        trade["v169_settlement_funding"] = bool(is_v169)

        if is_closed and is_v169:
            # Closed-trade accounting is also based only on the last complete
            # two-leg snapshot. If history is partial at this instant, this is
            # the last fully verified funding state rather than a one-leg guess.
            trade["realized_funding_target_pnl_pct_x"] = target_pnl
            trade["realized_funding_hedge_pnl_pct_x"] = hedge_pnl
            trade["realized_funding_pct_x"] = authoritative_net
            spread_net = _fnum(
                trade.get("realized_spread_net_pnl_pct_x"),
                _fnum(trade.get("realized_gross_pnl_pct_x"), 0.0) - _fnum(trade.get("realized_fees_pct_x"), 0.0),
            )
            total_net = spread_net + authoritative_net
            trade["realized_net_pnl_pct_x"] = total_net
            trade["current_net_pnl_pct_x"] = total_net
            trade["realized_pnl_x"] = total_net / 100.0 * max(0.0, _fnum(trade.get("notional_x"), 1.0))
    except Exception as exc:
        trade["funding_breakdown_status"] = "ERROR"
        trade["funding_breakdown_error"] = f"{type(exc).__name__}: {exc}"
        if is_v169:
            trade["funding_target_pnl_pct_x"] = _fnum(trade.get("v169_last_complete_funding_target_pct"), 0.0)
            trade["funding_hedge_pnl_pct_x"] = _fnum(trade.get("v169_last_complete_funding_hedge_pct"), 0.0)
            trade["funding_net_pnl_pct_x"] = _fnum(trade.get("v169_last_complete_funding_net_pct"), 0.0)
            trade["funding_target_settlement_count"] = int(_fnum(trade.get("v169_last_complete_target_settlement_count"), 0.0))
            trade["funding_hedge_settlement_count"] = int(_fnum(trade.get("v169_last_complete_hedge_settlement_count"), 0.0))


def _refresh_open_trade_funding_timeline(trade: dict, cfg: Any, now_ts: float) -> dict:
    """Refresh the NEXT known funding cashflows for an already-open trade.

    This is money-management data only.  It never rewrites the entry thesis and
    never enters realized PnL until a settlement appears in public history.
    """
    empty = {
        "status": "UNAVAILABLE",
        "raw_net_pct": 0.0,
        "conservative_adjustment_pct": 0.0,
        "worst_prefix_net_pct": 0.0,
        "first_settlement_ts": 0.0,
        "first_settlement_cluster_net_pct": 0.0,
    }
    if not FUNDING_DYNAMIC_MANAGEMENT_ENABLED or cfg is None:
        return empty
    opened_ts = _fnum(trade.get("opened_ts"), now_ts)
    remaining = max(0.0, ECON_MAX_HOLD_HOURS * 3600.0 - max(0.0, now_ts - opened_ts))
    if remaining <= 0.0:
        return empty
    hold_seconds = min(remaining, FUNDING_DYNAMIC_HORIZON_MINUTES * 60.0)
    target_fc = _funding_leg_forecast(str(trade.get("target") or ""), str(trade.get("symbol") or ""), str(trade.get("target_side") or ""), cfg, now_ts)
    hedge_fc = _funding_leg_forecast(str(trade.get("hedge_venue") or ""), str(trade.get("symbol") or ""), str(trade.get("hedge_side") or ""), cfg, now_ts)
    timeline = _combine_funding_timeline(target_fc, hedge_fc, now_ts, hold_seconds)
    status = str(timeline.get("status") or "UNAVAILABLE")
    risk = _fnum(timeline.get("conservative_adjustment_pct"), 0.0)
    raw_net = _fnum(timeline.get("raw_net_pct"), 0.0)
    expected = raw_net if status == "READY" else min(0.0, risk) if status == "PARTIAL" else 0.0
    trade["live_funding_forecast_status"] = status
    trade["live_funding_target_forecast"] = timeline.get("target", {})
    trade["live_funding_hedge_forecast"] = timeline.get("hedge", {})
    trade["live_funding_forecast_horizon_seconds"] = hold_seconds
    trade["live_funding_forecast_asof_ts"] = now_ts
    trade["live_funding_expected_net_pct"] = expected
    trade["live_funding_raw_timeline_net_pct"] = raw_net
    trade["live_funding_risk_adjustment_pct"] = risk
    trade["live_funding_worst_prefix_net_pct"] = _fnum(timeline.get("worst_prefix_net_pct"), 0.0)
    trade["live_funding_first_settlement_ts"] = _fnum(timeline.get("first_settlement_ts"), 0.0)
    trade["live_funding_first_cluster_net_pct"] = _fnum(timeline.get("first_settlement_cluster_net_pct"), 0.0)
    return timeline


def _total_profit_lock_target_pct(trade: dict) -> float:
    expected_spread = max(0.0, _fnum(trade.get("entry_spread_expected_net_pct"), _fnum(trade.get("entry_expected_net_edge_pct"), 0.0)))
    return max(ECON_TOTAL_PROFIT_LOCK_MIN_NET_PCT, expected_spread)


def _annotate_component_outcome(trade: dict, mark: dict, reason: str) -> None:
    spread = _fnum(mark.get("spread_net_pnl_pct"), 0.0)
    funding = _fnum(mark.get("funding_pct"), 0.0)
    total = _fnum(mark.get("net_pnl_pct"), 0.0)
    def label(v: float) -> str:
        return "WIN" if v > ECON_EXIT_EPSILON_PCT else "LOSS" if v < -ECON_EXIT_EPSILON_PCT else "FLAT"
    trade["spread_component_result"] = label(spread)
    trade["funding_component_result"] = label(funding)
    trade["total_component_result"] = label(total)
    if total > ECON_EXIT_EPSILON_PCT:
        if spread > ECON_EXIT_EPSILON_PCT and funding > ECON_EXIT_EPSILON_PCT:
            driver = "SPREAD+FUNDING"
        elif funding > ECON_EXIT_EPSILON_PCT:
            driver = "FUNDING"
        elif spread > ECON_EXIT_EPSILON_PCT:
            driver = "SPREAD"
        else:
            driver = "OTHER"
    else:
        driver = "NONE"
    trade["realized_profit_driver"] = driver
    trade["money_management_exit"] = reason.startswith("ECON_TOTAL_PROFIT_")


def _trade_business_thesis(trade: dict) -> str:
    thesis = str(trade.get("business_thesis") or "").upper()
    return thesis if thesis in {"SPREAD", "FUNDING", "SPREAD+FUNDING"} else "LEGACY"


def _is_v1610_forward_trade(trade: dict) -> bool:
    """True only for trades whose entry was created by the V16.10 engine.

    This is deliberately an attribution helper only. It must never influence
    entry, exit, sizing, funding, or execution decisions.
    """
    return str(trade.get("engine_version_at_entry") or "").strip() == "16.10"


def _is_legacy_policy_migration_trade(trade: dict) -> bool:
    """Older trade closed by a V16.10 money-management policy.

    Such PnL is economically real in the paper ledger, but it was accumulated
    under an older entry/holding policy. Daily forward-strategy statistics must
    therefore show it separately instead of attributing it to the V16.10 cohort.
    """
    if _is_v1610_forward_trade(trade):
        return False
    reason = str(trade.get("close_reason") or "")
    return reason in {
        "ECON_TOTAL_PROFIT_LOCK_AFTER_SETTLEMENT",
        "ECON_TOTAL_PROFIT_PROTECT_BEFORE_NEGATIVE_FUNDING",
    }


def _trade_closed_on_date(trade: dict, report_date: date, tz: ZoneInfo) -> bool:
    ts = _fnum(trade.get("closed_ts"), 0.0)
    return ts > 0.0 and _local_date_for_ts(ts, tz) == report_date


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
    trade["v169_exit_logic_version"] = ECON_EXIT_LOGIC_VERSION
    trade["v169_exact_route_mark_ts"] = now_ts
    trade["v1610_exit_logic_version"] = ECON_EXIT_LOGIC_VERSION
    trade["v1610_exact_route_mark_ts"] = now_ts


def _econ_exact_exit_reason(trade: dict, mark: dict, now_ts: float) -> Optional[str]:
    """Business-aware exact-route exit policy with V16.10 money management.

    Thesis attribution stays strict: a SPREAD trade is still evaluated on its
    spread component.  Separately, after at least one *settled* funding event,
    V16.10 may lock an economically positive total PnL so already-realized carry
    is not given back while waiting for the original spread thesis.
    """
    thesis = _trade_business_thesis(trade)
    expected_spread = max(0.0, _fnum(trade.get("entry_spread_expected_net_pct"), _fnum(trade.get("entry_expected_net_edge_pct"), 0.0)))
    expected_combined = max(expected_spread, _fnum(trade.get("entry_expected_combined_net_pct"), expected_spread))
    reserve = max(0.0, _fnum(trade.get("entry_exit_slippage_reserve_pct"), 0.0))
    spread_net = _fnum(mark.get("spread_net_pnl_pct"), _fnum(mark.get("net_pnl_pct")) - _fnum(mark.get("funding_pct")))
    total_net = _fnum(mark.get("net_pnl_pct"), 0.0)
    settlement_seen = _funding_settlement_seen(trade)

    # First preserve the research semantics: if the original thesis itself paid,
    # use its thesis-specific close reason.
    if thesis in {"SPREAD", "LEGACY"}:
        if spread_net + ECON_EXIT_EPSILON_PCT >= expected_spread:
            return "ECON_EXACT_ROUTE_SPREAD_NET_TARGET"
        if mark["exact_gap_pct"] <= reserve + ECON_EXIT_EPSILON_PCT and spread_net > ECON_EXIT_EPSILON_PCT:
            return "ECON_EXACT_ROUTE_SPREAD_CONVERGENCE"

    elif thesis == "FUNDING":
        if settlement_seen and total_net > ECON_EXIT_EPSILON_PCT:
            return "ECON_EXACT_ROUTE_FUNDING_POSITIVE_AFTER_SETTLEMENT"

    elif thesis == "SPREAD+FUNDING":
        if spread_net + ECON_EXIT_EPSILON_PCT >= expected_spread:
            return "ECON_EXACT_ROUTE_SPREAD_NET_TARGET"
        if mark["exact_gap_pct"] <= reserve + ECON_EXIT_EPSILON_PCT and spread_net > ECON_EXIT_EPSILON_PCT:
            return "ECON_EXACT_ROUTE_SPREAD_CONVERGENCE"
        if settlement_seen and total_net + ECON_EXIT_EPSILON_PCT >= expected_combined:
            return "ECON_EXACT_ROUTE_COMBINED_NET_TARGET"

    # V16.10 money management is intentionally separate from thesis success.
    # It can close a SPREAD-labelled trade because funding made the *cash PnL*
    # attractive, but the report continues to show the spread component as a
    # loss when that is what actually happened.
    lock_target = _total_profit_lock_target_pct(trade)
    trade["total_profit_lock_target_pct"] = lock_target
    if ECON_TOTAL_PROFIT_LOCK_ENABLED and settlement_seen and total_net + ECON_EXIT_EPSILON_PCT >= lock_target:
        return "ECON_TOTAL_PROFIT_LOCK_AFTER_SETTLEMENT"

    # If the trade is currently profitable but the next known funding prefix can
    # turn it non-positive, bank the existing PnL before that settlement.  A
    # PARTIAL forecast may protect against known negative carry but never credits
    # an unknown positive leg.
    dynamic_risk = min(0.0, _fnum(trade.get("live_funding_risk_adjustment_pct"), 0.0))
    if (
        FUNDING_NEGATIVE_PROFIT_PROTECT_ENABLED
        and total_net > ECON_EXIT_EPSILON_PCT
        and dynamic_risk < -ECON_EXIT_EPSILON_PCT
        and total_net + dynamic_risk <= ECON_EXIT_EPSILON_PCT
    ):
        trade["v1610_projected_total_after_known_negative_funding_pct"] = total_net + dynamic_risk
        return "ECON_TOTAL_PROFIT_PROTECT_BEFORE_NEGATIVE_FUNDING"

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
    _annotate_component_outcome(trade, mark, reason)

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
    """Undo false legacy closes, then apply V16.10 business-aware exact-route exit rules."""
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
        _refresh_open_trade_funding_timeline(trade, cfg, now_ts)
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
        _annotate_component_outcome(trade, mark, reason)
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
    # when its own exact route satisfies the V16.9 condition.
    for trade_id, trade in list(bucket.items()):
        if not isinstance(trade, dict) or str(trade.get("strategy") or "") != "ECON":
            continue
        _refresh_trade_funding_breakdown(trade, cfg, now_ts)
        _refresh_open_trade_funding_timeline(trade, cfg, now_ts)
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
        "entry_funding_risk_adjustment_pct": _fnum(event.get("entry_funding_risk_adjustment_pct"), 0.0),
        "entry_funding_worst_prefix_net_pct": _fnum(event.get("entry_funding_worst_prefix_net_pct"), 0.0),
        "entry_expected_combined_net_pct": _fnum(event.get("entry_expected_combined_net_pct"), fresh_net),
        "entry_funding_forecast_status": str(event.get("entry_funding_forecast_status") or "UNAVAILABLE"),
        "entry_funding_target_forecast": event.get("entry_funding_target_forecast") if isinstance(event.get("entry_funding_target_forecast"), dict) else {},
        "entry_funding_hedge_forecast": event.get("entry_funding_hedge_forecast") if isinstance(event.get("entry_funding_hedge_forecast"), dict) else {},
        "entry_funding_forecast_horizon_seconds": _fnum(event.get("entry_funding_forecast_horizon_seconds"), FUNDING_ENTRY_RISK_HORIZON_MINUTES * 60.0),
        "entry_funding_forecast_horizon_ts": _fnum(event.get("entry_funding_forecast_horizon_ts"), 0.0),
        "entry_funding_first_settlement_ts": _fnum(event.get("entry_funding_first_settlement_ts"), 0.0),
        "entry_funding_first_cluster_net_pct": _fnum(event.get("entry_funding_first_cluster_net_pct"), 0.0),
        "entry_business_effective_net_pct": _fnum(event.get("entry_business_effective_net_pct"), fresh_net),
        "entry_expected_raw_timeline_funding_pct": _fnum(event.get("entry_expected_raw_timeline_funding_pct"), 0.0),
        "entry_funding_extreme_rate_warning": bool(event.get("entry_funding_extreme_rate_warning")),
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
        "v169_exit_logic_version": ECON_EXIT_LOGIC_VERSION,
        "v169_exact_route_exit": True,
        "v169_settlement_funding": True,
        "v1610_exit_logic_version": ECON_EXIT_LOGIC_VERSION,
        "v1610_total_profit_lock": True,
        "v1610_dynamic_funding_management": True,
        "engine_version_at_entry": "16.10",
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
        "funding_risk_adjustment_pct": _fnum(event.get("entry_funding_risk_adjustment_pct"), 0.0),
        "funding_worst_prefix_net_pct": _fnum(event.get("entry_funding_worst_prefix_net_pct"), 0.0),
        "expected_combined_net_pct": _fnum(event.get("entry_expected_combined_net_pct"), _fnum(event.get("execution_fresh_net_edge_pct"), 0.0)),
        "business_effective_net_pct": _fnum(event.get("entry_business_effective_net_pct"), _fnum(event.get("execution_fresh_net_edge_pct"), 0.0)),
        "funding_forecast_status": event.get("entry_funding_forecast_status"),
        "funding_forecast_horizon_seconds": _fnum(event.get("entry_funding_forecast_horizon_seconds"), 0.0),
        "funding_extreme_rate_warning": bool(event.get("entry_funding_extreme_rate_warning")),
    }
    rows.append(row)
    if len(rows) > SIGNAL_LEDGER_MAX_ROWS:
        del rows[:-SIGNAL_LEDGER_MAX_ROWS]


def scan_v165(
    cfg: Any,
    state: dict,
    active_signal_callback: Optional[Callable[[dict], None]] = None,
) -> Tuple[Any, Any, bool]:
    """Wrap the core scan and expose canonical V16.10 business-qualified signals."""
    previous_version = state.get("scanner_version")
    state["scanner_version"] = "16.10"
    rejected_latches: set[str] = set()

    def event_latch(event: dict) -> str:
        return f"{event.get('state_key')}:{event.get('side')}:{event.get('execution_best_external_venue') or ''}"

    def callback(event: dict) -> None:
        typ = str(event.get("type", ""))

        if typ == "POSITIVE-ECONOMICS":
            # Core readiness still enforces strict spread NET >0.100%. V16.10
            # then applies the conservative imminent-negative-funding veto.
            if not _signal_is_qualifying(event):
                return
            _annotate_event_business_thesis(event, cfg)
            if not _entry_business_is_qualifying(event):
                rejected_latches.add(event_latch(event))
                _record_v169_entry_rejection(state, event)
                print(
                    "V16.10 funding guard rejected "
                    f"{event.get('symbol')} {event.get('side')} via {event.get('execution_best_external_venue')}: "
                    f"spread={_fnum(event.get('execution_fresh_net_edge_pct')):+.4f}% "
                    f"funding_risk={_fnum(event.get('entry_funding_risk_adjustment_pct')):+.4f}% "
                    f"effective={_fnum(event.get('entry_business_effective_net_pct')):+.4f}%"
                )
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

    # Core's positive-economics latch is updated before our callback runs. If
    # the funding guard vetoed the entry, remove that latch and timestamp so the
    # same continuously-positive spread is re-evaluated on the next scan rather
    # than being incorrectly silenced until the spread disappears.
    if rejected_latches:
        root = state.get("active_signal_open", {})
        if isinstance(root, dict):
            rows = root.get("POSITIVE-ECONOMICS", [])
            if isinstance(rows, list):
                root["POSITIVE-ECONOMICS"] = [x for x in rows if str(x) not in rejected_latches]
        last_map = state.get("economic_signal_last_ts", {})
        if isinstance(last_map, dict):
            for latch in rejected_latches:
                last_map.pop(latch, None)
        changed = True

    exit_changed = _repair_and_apply_exact_econ_exits(state, pre_open_ids, time.time(), cfg)
    return ranked, errors, bool(changed or exit_changed or previous_version != "16.9")


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
    expected_fund_net = _fnum(event.get("entry_expected_net_funding_pct"), 0.0)
    risk_adjustment = _fnum(event.get("entry_funding_risk_adjustment_pct"), 0.0)
    worst_prefix = _fnum(event.get("entry_funding_worst_prefix_net_pct"), 0.0)
    raw_timeline_net = _fnum(event.get("entry_expected_raw_timeline_funding_pct"), expected_fund_net)
    combined = _fnum(event.get("entry_expected_combined_net_pct"), net)
    effective = _fnum(event.get("entry_business_effective_net_pct"), net)
    hold_mins = _fnum(event.get("entry_funding_forecast_horizon_seconds"), FUNDING_ENTRY_RISK_HORIZON_MINUTES * 60.0) / 60.0
    extreme = bool(event.get("entry_funding_extreme_rate_warning"))

    def fc_line(name: str, fc: dict) -> str:
        if not fc or not fc.get("available"):
            return f"  {name}: unavailable"
        mins = fc.get("minutes_to_settlement")
        mins_txt = f" in {float(mins):.0f}m" if mins is not None else ""
        pnl = _fnum(fc.get("next_settlement_pnl_pct", fc.get("expected_pnl_pct")), 0.0)
        counted = bool(fc.get("counted_in_timeline"))
        tag = "counted" if counted else "later — not counted"
        warn = " ⚠️ extreme" if fc.get("extreme_rate_warning") else ""
        return f"  {name} {html.escape(str(fc.get('side') or '?'))}: {pnl:+.4f}%{mins_txt} [{tag}]{warn}"

    paper_ok = bool(event.get("_v165_paper_opened"))
    paper_line = "✅ paper entry recorded" if paper_ok else "⚠️ paper entry was NOT recorded"
    if fund_status != "UNAVAILABLE":
        funding_block = (
            f"Funding timeline [{html.escape(fund_status)} | horizon {hold_mins:.0f}m]:\n"
            f"{fc_line('target', target_fc)}\n"
            f"{fc_line('hedge', hedge_fc)}\n"
            f"  Raw known timeline funding: <b>{raw_timeline_net:+.4f}%</b>\n"
            f"  Expected NET funding used in forecast: <b>{expected_fund_net:+.4f}%</b>\n"
            f"  Worst funding prefix inside horizon: <b>{worst_prefix:+.4f}%</b>\n"
            f"  Entry risk adjustment: <b>{risk_adjustment:+.4f}%</b>\n"
            f"Combined forecast through horizon: <b>{combined:+.4f}%</b>\n"
            f"Entry effective NET (negative-funding guard): <b>{effective:+.4f}%</b>"
        )
        if extreme:
            funding_block += "\n⚠️ At least one forecast funding rate exceeds the configured extreme-rate warning threshold; realized PnL will use settled history only."
    else:
        funding_block = "Funding forecast: unavailable — no positive funding credit and no funding-based entry adjustment."

    return (
        f"💰 <b>TRADE SIGNAL — {symbol} @ {label}</b>\n"
        f"Thesis: <b>{thesis}</b>\n"
        f"Rule: executable spread expected NET <b>&gt; {SIGNAL_NET_THRESHOLD_PCT:.3f}%</b>; imminent known negative funding may veto a marginal entry\n"
        f"Route: <b>{side} {label}</b> / hedge via <b>{hedge}</b>\n"
        f"Fresh VWAP @ ${usd:,.0f}: gross <b>{gross:.4f}%</b>\n"
        f"Round-trip taker fees: {fees:.4f}% | exit-slip reserve: {reserve:.4f}%\n"
        f"Spread expected NET after costs: <b>{net:+.4f}%</b> ✅\n"
        f"{funding_block}\n"
        f"Entry VWAP target / hedge: <code>{tvwap:.10g}</code> / <code>{hvwap:.10g}</code>\n"
        f"Executable notional verified up to: <b>${ready_usd:,.0f}</b> | ref disagreement {disagreement:.3f}%\n"
        f"Paper: <b>{paper_line}</b>\n\n"
        "Funding forecast is indicative and never enters realized PnL. Realized funding uses settled history while the position was open. "
        "Continuous route is alerted once after it passes the business entry guard. Signal/paper simulation only; no API orders are sent."
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

    # Reporting-only attribution: forward V16.10 entries are separated from
    # older positions whose accumulated PnL happened to be realized after the
    # V16.10 money-management policy was installed. Capital replay itself above
    # is unchanged, so old positions still occupy/release capital correctly.
    forward_day_accepted = [(t, c) for t, c in day_accepted if _is_v1610_forward_trade(t)]
    forward_day_skipped = [(t, c) for t, c in day_skipped if _is_v1610_forward_trade(t)]
    migration_accepted = [
        (t, True) for t, c in accepted.values()
        if c
        and _is_legacy_policy_migration_trade(t)
        and 0.0 < _fnum(t.get("closed_ts"), 0.0) <= as_of_ts
        and _trade_closed_on_date(t, report_date, tz)
    ]

    def _rows_as_of(rows_in):
        return [
            (t, bool(c and 0.0 < _fnum(t.get("closed_ts"), 0.0) <= as_of_ts))
            for t, c in rows_in
        ]

    forward_rows_as_of = _rows_as_of(forward_day_accepted)
    forward_breakdown = _aggregate_trade_pnl(forward_rows_as_of)
    forward_closed = sum(1 for _t, c in forward_rows_as_of if c)
    forward_open = len(forward_rows_as_of) - forward_closed
    migration_breakdown = _aggregate_trade_pnl(migration_accepted)

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
        # Forward-attribution fields (statistics only).
        "forward_accepted_count": len(forward_day_accepted),
        "forward_skipped_count": len(forward_day_skipped),
        "forward_closed_count": forward_closed,
        "forward_open_count": forward_open,
        "forward_gross_usd": forward_breakdown["gross_usd"],
        "forward_fees_usd": forward_breakdown["fees_usd"],
        "forward_spread_usd": forward_breakdown["spread_usd"],
        "forward_funding_usd": forward_breakdown["funding_usd"],
        "forward_total_usd": forward_breakdown["total_usd"],
        "forward_total_pct_sum": forward_breakdown["total_pct"],
        "forward_roi_contribution_pct": (forward_breakdown["total_usd"] / start_balance * 100.0) if start_balance > 0 else 0.0,
        "migration_accepted_count": len(migration_accepted),
        "migration_realized_usd": migration_breakdown["total_usd"],
        "migration_spread_usd": migration_breakdown["spread_usd"],
        "migration_funding_usd": migration_breakdown["funding_usd"],
    }

def build_daily_report_text(state: dict, report_date: date, tz: ZoneInfo, cfg: Any = None) -> str:
    """Daily report with forward-cohort attribution separated from migration PnL.

    V16.10.1 is a reporting-only change. Trading mechanics remain V16.10.
    """
    signal_rows = state.get("v165_signal_events", []) if isinstance(state, dict) else []
    if not isinstance(signal_rows, list):
        signal_rows = []
    day_signals = [r for r in signal_rows if isinstance(r, dict) and _local_date_for_ts(_fnum(r.get("ts"), 0.0), tz) == report_date]
    paper_entries = sum(1 for r in day_signals if bool(r.get("paper_opened")))
    extreme_forecasts = sum(1 for r in day_signals if bool(r.get("funding_extreme_rate_warning")))

    reject_rows = state.get("v169_funding_guard_rejections", []) if isinstance(state, dict) else []
    if not isinstance(reject_rows, list):
        reject_rows = []
    day_rejects = [r for r in reject_rows if isinstance(r, dict) and _local_date_for_ts(_fnum(r.get("ts"), 0.0), tz) == report_date]

    # Existing open-date diagnostic is preserved, but no longer drives the
    # headline strategy statistics.
    opened_date_closed = [r for r in _closed_econ_trades(state) if _local_date_for_ts(_fnum(r.get("opened_ts"), 0.0), tz) == report_date]
    opened_date_open = [r for r in _open_econ_trades(state) if _local_date_for_ts(_fnum(r.get("opened_ts"), 0.0), tz) == report_date]

    forward_closed = [r for r in opened_date_closed if _is_v1610_forward_trade(r)]
    forward_open = [r for r in opened_date_open if _is_v1610_forward_trade(r)]
    migration_closed = [
        r for r in _closed_econ_trades(state)
        if _is_legacy_policy_migration_trade(r) and _trade_closed_on_date(r, report_date, tz)
    ]

    if cfg is not None:
        now_ts = time.time()
        refresh = {str(t.get("id") or id(t)): t for t in (opened_date_closed + opened_date_open + migration_closed)}
        for t in refresh.values():
            end_ts = _fnum(t.get("closed_ts"), now_ts) if str(t.get("status")) == "CLOSED" else now_ts
            _refresh_trade_funding_breakdown(t, cfg, end_ts)

    forward_rows = [(t, True) for t in forward_closed] + [(t, False) for t in forward_open]
    forward_agg = _aggregate_trade_pnl(forward_rows)
    migration_rows = [(t, True) for t in migration_closed]
    migration_agg = _aggregate_trade_pnl(migration_rows)

    forward_closed_values = [(_trade_net_pct(t, True), t) for t in forward_closed]
    forward_open_values = [(_trade_net_pct(t, False), t) for t in forward_open]
    forward_values = forward_closed_values + forward_open_values
    wins = sum(1 for v, _ in forward_closed_values if v > 1e-12)
    losses = sum(1 for v, _ in forward_closed_values if v < -1e-12)
    flat = len(forward_closed_values) - wins - losses
    win_rate = (wins / len(forward_closed_values) * 100.0) if forward_closed_values else None
    forward_realized_usd = sum(_trade_pnl_usd(t, v) for v, t in forward_closed_values)
    forward_open_usd = sum(_trade_pnl_usd(t, v) for v, t in forward_open_values)

    forward_locks = sum(1 for t in forward_closed if str(t.get("close_reason") or "").startswith("ECON_TOTAL_PROFIT_LOCK"))
    forward_protects = sum(1 for t in forward_closed if str(t.get("close_reason") or "") == "ECON_TOTAL_PROFIT_PROTECT_BEFORE_NEGATIVE_FUNDING")
    forward_funding_wins = sum(1 for t in forward_closed if str(t.get("realized_profit_driver") or "") == "FUNDING" and _trade_net_pct(t, True) > 0.0)

    # Preserve the all-opened-on-date diagnostic for research continuity.
    all_opened_rows = [(t, True) for t in opened_date_closed] + [(t, False) for t in opened_date_open]
    all_opened_agg = _aggregate_trade_pnl(all_opened_rows)
    all_opened_notional = sum(_trade_reserve_usd(t) for t, _c in all_opened_rows)
    all_opened_notional_return = (all_opened_agg["total_usd"] / all_opened_notional * 100.0) if all_opened_notional > 0 else 0.0

    missing_paper = max(0, len(day_signals) - paper_entries)
    migration_versions = Counter(str(t.get("engine_version_at_entry") or "legacy") for t in migration_closed)
    migration_versions_text = ", ".join(f"{html.escape(k)}×{v}" for k, v in sorted(migration_versions.items())) if migration_versions else "none"

    lines = [
        f"📊 <b>DAILY PAPER REPORT — {report_date.strftime('%d.%m.%Y')}</b>",
        f"Rule unchanged: fresh executable spread NET <b>&gt; {SIGNAL_NET_THRESHOLD_PCT:.3f}%</b>; V16.10 trading mechanics unchanged",
        f"Signals: <b>{len(day_signals)}</b> | funding vetoes: <b>{len(day_rejects)}</b> | paper entries: <b>{paper_entries}</b> | failures: <b>{missing_paper}</b>",
        (f"⚠️ Extreme funding forecast warning: <b>{extreme_forecasts}</b> signals" if extreme_forecasts else "Funding forecast sanity: no extreme-rate warnings"),
        "",
        "<b>NEW V16.10 FORWARD COHORT — strategy statistics</b>",
        f"Entries: <b>{len(forward_rows)}</b> | closed/open: <b>{len(forward_closed)} / {len(forward_open)}</b>",
        (f"Wins/losses/flat: <b>{wins}/{losses}/{flat}</b> | win rate <b>{win_rate:.1f}%</b>" if win_rate is not None else "Wins/losses/flat: <b>0/0/0</b> | win rate <b>n/a</b>"),
        f"Spread NET: <b>{forward_agg['spread_usd']:+.4f} USD</b> | settled funding: <b>{forward_agg['funding_usd']:+.4f} USD</b>",
        f"Forward PnL: <b>{forward_agg['total_usd']:+.4f} USD</b> (realized {forward_realized_usd:+.4f}, open MTM {forward_open_usd:+.4f})",
        f"Σ forward per-trade return: <b>{forward_agg['total_pct']:+.4f}%</b> — diagnostic, not account ROI",
        f"Money-management: locks <b>{forward_locks}</b> | pre-negative-funding protects <b>{forward_protects}</b> | funding-driven wins <b>{forward_funding_wins}</b>",
        "",
        "<b>LEGACY MIGRATION — excluded from forward strategy ROI</b>",
        f"Old V16.8/V16.9 positions closed by V16.10 money-management today: <b>{len(migration_closed)}</b> ({migration_versions_text})",
        f"Migration spread: <b>{migration_agg['spread_usd']:+.4f} USD</b> | settled funding: <b>{migration_agg['funding_usd']:+.4f} USD</b> | realized total: <b>{migration_agg['total_usd']:+.4f} USD</b>",
        "These dollars remain in the paper ledger/equity, but are not attributed to today's V16.10 forward strategy performance.",
        "",
        "<b>ALL ENTRIES OPENED ON REPORT DATE — research diagnostic</b>",
        f"Trades: <b>{len(all_opened_rows)}</b> | spread {all_opened_agg['spread_usd']:+.4f} | funding {all_opened_agg['funding_usd']:+.4f} | total <b>{all_opened_agg['total_usd']:+.4f} USD</b>",
        f"PnL / summed pair notional ${all_opened_notional:,.2f}: <b>{all_opened_notional_return:+.4f}%</b>",
    ]

    if forward_rows:
        lines.extend(["", "<b>FORWARD COHORT BY THESIS</b>"])
        for thesis in ("SPREAD", "SPREAD+FUNDING", "FUNDING", "LEGACY"):
            subset = [(t, c) for t, c in forward_rows if _trade_business_thesis(t) == thesis]
            if not subset:
                continue
            a = _aggregate_trade_pnl(subset)
            cc = sum(1 for _t, c in subset if c)
            lines.append(f"{thesis}: {len(subset)} ({cc} closed/{len(subset)-cc} open) | spread {a['spread_usd']:+.4f} | funding {a['funding_usd']:+.4f} | total <b>{a['total_usd']:+.4f}</b>")

    if CAPITAL_SIM_ENABLED:
        cap = _capital_constrained_snapshot(state, report_date, tz, time.time())
        lines.extend([
            "",
            f"<b>CAPITAL-CONSTRAINED — ${cap['start_balance_usd']:,.0f} shared pool</b>",
            f"Forward V16.10 accepted/skipped: <b>{cap['forward_accepted_count']} / {cap['forward_skipped_count']}</b> | closed/open <b>{cap['forward_closed_count']} / {cap['forward_open_count']}</b>",
            f"Forward accepted-cohort: spread <b>{cap['forward_spread_usd']:+.4f}</b> | funding <b>{cap['forward_funding_usd']:+.4f}</b> | total <b>{cap['forward_total_usd']:+.4f} USD</b>",
            f"Forward report-date contribution vs ${cap['start_balance_usd']:,.0f} start: <b>{cap['forward_roi_contribution_pct']:+.3f}%</b> (not annualized)",
            f"Legacy migration accepted by capital replay: <b>{cap['migration_accepted_count']}</b> | realized <b>{cap['migration_realized_usd']:+.4f} USD</b> — excluded above",
            f"Portfolio identity (includes all historical realized PnL): free <b>${cap['free_usd']:.2f}</b> + reserved <b>${cap['reserved_usd']:.2f}</b> + open MTM <b>{cap['open_mtm_usd']:+.2f}</b> = equity <b>${cap['equity_usd']:.2f}</b>",
            f"Cumulative portfolio ROI since simulator start (includes legacy history): <b>{cap['return_pct']:+.3f}%</b>",
        ])

    if forward_values:
        best_v, best_t = max(forward_values, key=lambda x: x[0])
        worst_v, worst_t = min(forward_values, key=lambda x: x[0])
        lines.extend(["", f"Forward best: <b>{html.escape(str(best_t.get('symbol') or '?'))} {best_v:+.4f}%</b> | worst: <b>{html.escape(str(worst_t.get('symbol') or '?'))} {worst_v:+.4f}%</b>"])

    lines.extend([
        "",
        f"Stats schema <b>{STATISTICS_SCHEMA_VERSION}</b>: attribution only. Entry/exit/funding mechanics remain V16.10. Legacy migration is separated by entry engine version + V16.10 money-management close reason; no trade is reclassified or repriced.",
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
    """Deterministic local tests for V16.10-specific overlay logic."""
    cfg = core.Config()
    state: dict = {}
    now = time.time()
    event = {
        "type": "POSITIVE-ECONOMICS",
        "business_thesis": "SPREAD",
        "entry_spread_expected_net_pct": 0.1201,
        "entry_expected_net_funding_pct": 0.0,
        "entry_funding_risk_adjustment_pct": 0.0,
        "entry_funding_worst_prefix_net_pct": 0.0,
        "entry_expected_combined_net_pct": 0.1201,
        "entry_business_effective_net_pct": 0.1201,
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

    # Funding-timeline regression: settlements at different times are not
    # blindly netted into one cashflow. With a 60m expected hold, a +0.72%
    # target settlement in 9m counts while a -0.005% hedge settlement in 189m
    # is explicitly outside the forecast horizon.
    t_fc = {"available": True, "next_settlement_ts": now + 9 * 60, "next_settlement_pnl_pct": 0.72}
    h_fc = {"available": True, "next_settlement_ts": now + 189 * 60, "next_settlement_pnl_pct": -0.005}
    tl = _combine_funding_timeline(t_fc, h_fc, now, 60 * 60)
    assert abs(_fnum(tl.get("raw_net_pct")) - 0.72) < 1e-12
    assert bool(tl["target"].get("counted_in_timeline"))
    assert not bool(tl["hedge"].get("counted_in_timeline"))

    # Partial positive funding is not credited; partial negative funding is
    # conservatively debited.
    partial_pos = _combine_funding_timeline(t_fc, {"available": False}, now, 60 * 60)
    assert abs(_fnum(partial_pos.get("conservative_adjustment_pct"))) < 1e-12
    t_neg = {"available": True, "next_settlement_ts": now + 2 * 60, "next_settlement_pnl_pct": 0.0013}
    h_neg = {"available": True, "next_settlement_ts": now + 2 * 60, "next_settlement_pnl_pct": -0.12}
    tl_neg = _combine_funding_timeline(t_neg, h_neg, now, 60 * 60)
    assert _fnum(tl_neg.get("conservative_adjustment_pct")) < -0.118
    guarded = dict(event)
    guarded["execution_fresh_net_edge_pct"] = 0.1323
    guarded["entry_business_effective_net_pct"] = 0.1323 + _fnum(tl_neg.get("conservative_adjustment_pct"))
    assert not _entry_business_is_qualifying(guarded)

    # A later positive funding payment must not hide an earlier negative cashflow.
    # Raw horizon carry is +0.08%, but the first funding event is -0.12%, so
    # the entry-risk adjustment must remain -0.12%.
    early_negative = {"available": True, "next_settlement_ts": now + 2 * 60, "next_settlement_pnl_pct": -0.12}
    later_positive = {"available": True, "next_settlement_ts": now + 50 * 60, "next_settlement_pnl_pct": 0.20}
    tl_prefix = _combine_funding_timeline(early_negative, later_positive, now, 60 * 60)
    assert abs(_fnum(tl_prefix.get("raw_net_pct")) - 0.08) < 1e-12
    assert abs(_fnum(tl_prefix.get("worst_prefix_net_pct")) + 0.12) < 1e-12
    assert abs(_fnum(tl_prefix.get("conservative_adjustment_pct")) + 0.12) < 1e-12
    prefix_guarded = dict(event)
    prefix_guarded["execution_fresh_net_edge_pct"] = 0.15
    prefix_guarded["entry_business_effective_net_pct"] = 0.15 + _fnum(tl_prefix.get("conservative_adjustment_pct"))
    assert not _entry_business_is_qualifying(prefix_guarded)

    # Lighter settled-history parser regression: /api/v1/fundings is queried
    # with millisecond bounds, response timestamps are interpreted as seconds,
    # and direction determines the sign of the otherwise unsigned rate.
    original_mid_lookup = globals()["_lighter_market_id_for_symbol"]
    original_get_json = core.get_json
    captured_params: dict = {}
    test_symbol = "V169TESTUSDT"
    _V169_LIGHTER_FUNDING_CACHE.clear()
    try:
        globals()["_lighter_market_id_for_symbol"] = lambda _symbol, _cfg: 77

        def fake_lighter_fundings(_url: str, _timeout: float, params: Optional[dict] = None) -> dict:
            captured_params.update(params or {})
            base_ts = int(now)
            return {
                "fundings": [
                    {"timestamp": base_ts, "rate": "0.0012", "direction": "long"},
                    {"timestamp": base_ts + 1, "rate": "0.0005", "direction": "short"},
                ]
            }

        core.get_json = fake_lighter_fundings
        settled_rows, settled_err = _lighter_public_funding_rows(test_symbol, now - 10.0, now + 10.0, cfg)
        assert not settled_err
        assert len(settled_rows) == 2
        assert abs(_fnum(settled_rows[0].get("rate_pct")) - 0.12) < 1e-12
        assert abs(_fnum(settled_rows[1].get("rate_pct")) + 0.05) < 1e-12
        assert int(_fnum(captured_params.get("start_timestamp"))) == int((now - 10.0) * 1000)
        assert int(_fnum(captured_params.get("end_timestamp"))) == int((now + 10.0) * 1000)
    finally:
        globals()["_lighter_market_id_for_symbol"] = original_mid_lookup
        core.get_json = original_get_json
        _V169_LIGHTER_FUNDING_CACHE.clear()

    # Realized-funding integrity regression: if one leg's history is temporarily
    # unavailable, preserve the last complete two-leg snapshot. A fresh one-leg
    # observation must not alter authoritative realized funding or trigger exits.
    original_rows_helper = core._funding_rows_cached
    partial_trade = {
        "id": "paper:ECON:v169:partial-funding",
        "strategy": "ECON",
        "engine_version_at_entry": "16.9",
        "v169_settlement_funding": True,
        "status": "OPEN",
        "symbol": "PARTIALUSDT",
        "target": "lighter",
        "hedge_venue": "bitget-perp",
        "target_side": "SHORT",
        "hedge_side": "LONG",
        "opened_ts": now - 3600.0,
        "funding_net_pnl_pct_x": 0.10,
        "v169_last_complete_funding_target_pct": 0.20,
        "v169_last_complete_funding_hedge_pct": -0.10,
        "v169_last_complete_funding_net_pct": 0.10,
        "v169_last_complete_target_settlement_count": 1,
        "v169_last_complete_hedge_settlement_count": 1,
        "v169_last_complete_target_settlement_ts": [now - 1800.0],
        "v169_last_complete_hedge_settlement_ts": [now - 1800.0],
    }
    try:
        def fake_partial_rows(venue: str, _symbol: str, _start: float, _end: float, _cfg: Any):
            if _normalize_venue(venue) == "lighter":
                return [{"ts": now - 300.0, "rate_pct": 0.30}], ""
            return [], "temporary hedge funding-history timeout"
        core._funding_rows_cached = fake_partial_rows
        _refresh_trade_funding_breakdown(partial_trade, cfg, now)
        assert abs(_fnum(partial_trade.get("funding_net_pnl_pct_x")) - 0.10) < 1e-12
        assert abs(_fnum(partial_trade.get("funding_target_pnl_pct_x")) - 0.20) < 1e-12
        assert abs(_fnum(partial_trade.get("funding_hedge_pnl_pct_x")) + 0.10) < 1e-12
        assert int(_fnum(partial_trade.get("funding_target_settlement_count"))) == 1
        assert int(_fnum(partial_trade.get("funding_hedge_settlement_count"))) == 1
        assert partial_trade.get("funding_breakdown_status") == "PARTIAL"
    finally:
        core._funding_rows_cached = original_rows_helper

    audit_trade = {
        "probe_notional_usd": 100.0, "symbol": "TESTUSDT",
        "target": "lighter", "target_side": "SHORT",
        "hedge_venue": "mexc-perp", "hedge_side": "LONG",
    }
    audit = _build_funding_audit(
        audit_trade,
        [{"ts": now - 60, "rate_pct": 1.10, "raw_rate": 0.011, "raw_rate_unit": "api-native", "normalization_multiplier_to_pct": 100.0, "direction": "long", "source": "lighter-public-fundings"}],
        [{"ts": now - 60, "rate_pct": 0.005, "source": "mexc-history"}],
        True, now,
    )
    assert abs(audit["target_cashflow_pct"] - 1.10) < 1e-12
    assert abs(audit["hedge_cashflow_pct"] + 0.005) < 1e-12
    assert abs(audit["target"][0]["cashflow_usd_at_probe"] - 1.10) < 1e-12

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
    # closed on consensus FULL, V16.10 must restore it to OPEN when the exact
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

    # Business-thesis vs money-management regression: funding must NOT turn
    # SPREAD into a spread win, but after a real settlement V16.10 may lock the
    # economically positive total PnL.
    spread_only = dict(regression)
    spread_only["business_thesis"] = "SPREAD"
    spread_only["entry_spread_expected_net_pct"] = 0.10
    spread_only["funding_target_settlement_count"] = 1
    spread_mark = dict(widened)
    spread_mark["spread_net_pnl_pct"] = -0.05
    spread_mark["funding_pct"] = 0.50
    spread_mark["net_pnl_pct"] = 0.45
    assert _econ_exact_exit_reason(spread_only, spread_mark, now + 1200) == "ECON_TOTAL_PROFIT_LOCK_AFTER_SETTLEMENT"
    outcome_probe = dict(spread_only)
    _annotate_component_outcome(outcome_probe, spread_mark, "ECON_TOTAL_PROFIT_LOCK_AFTER_SETTLEMENT")
    assert outcome_probe["spread_component_result"] == "LOSS"
    assert outcome_probe["funding_component_result"] == "WIN"
    assert outcome_probe["realized_profit_driver"] == "FUNDING"

    # Dynamic negative-funding protection: a currently positive trade can be
    # closed before a known funding debit would turn the total PnL negative.
    protect = dict(regression)
    protect["business_thesis"] = "SPREAD"
    protect["entry_spread_expected_net_pct"] = 0.20
    protect["live_funding_risk_adjustment_pct"] = -0.08
    protect_mark = dict(widened)
    protect_mark["spread_net_pnl_pct"] = 0.05
    protect_mark["funding_pct"] = 0.0
    protect_mark["net_pnl_pct"] = 0.05
    assert _econ_exact_exit_reason(protect, protect_mark, now + 1200) == "ECON_TOTAL_PROFIT_PROTECT_BEFORE_NEGATIVE_FUNDING"

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
    assert _econ_exact_exit_reason(combined, combined_mark, now + 1200) in {"ECON_EXACT_ROUTE_COMBINED_NET_TARGET", "ECON_TOTAL_PROFIT_LOCK_AFTER_SETTLEMENT"}

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
    report = build_daily_report_text(state, _local_date_for_ts(now, tz), tz, None)
    assert "Signals: <b>1</b>" in report
    assert "NEW V16.10 FORWARD COHORT" in report
    assert "LEGACY MIGRATION" in report
    assert "ALL ENTRIES OPENED ON REPORT DATE" in report
    assert "CAPITAL-CONSTRAINED" in report
    assert "Cumulative portfolio ROI since simulator start" in report

    # Reporting-only migration attribution regression: an older entry closed by
    # V16.10 money management is shown separately and excluded from the forward
    # cohort, without changing the trade itself.
    migration_probe = dict(regression)
    migration_probe.update({
        "id": "migration-probe",
        "status": "CLOSED",
        "engine_version_at_entry": "16.9",
        "opened_ts": now - 3600.0,
        "closed_ts": now + 60.0,
        "close_reason": "ECON_TOTAL_PROFIT_LOCK_AFTER_SETTLEMENT",
        "realized_net_pnl_pct_x": 1.0,
        "realized_spread_net_pnl_pct_x": -0.1,
        "realized_funding_pct_x": 1.1,
        "realized_fees_pct_x": 0.12,
        "realized_gross_pnl_pct_x": 0.02,
    })
    assert _is_legacy_policy_migration_trade(migration_probe)
    assert not _is_v1610_forward_trade(migration_probe)
    print("V16.10 overlay self-test OK (trading rules unchanged + V16.10.1 forward/migration statistics attribution)")


def _guarded_paper_note_close(state: dict, trade: dict, *args: Any, **kwargs: Any) -> None:
    """Prevent legacy-driven ECON closes from polluting paper stats.

    The V16.4 core calls ``_paper_note_close`` with an additional positional
    argument in some close paths.  Preserve the core call signature generically
    so the overlay remains compatible with those paths instead of assuming a
    fixed two-argument helper.

    V16.10 finalizes ECON trades itself after validating the exact stored route.
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
    apply the V16.10 business-aware exact-route exit rules.
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
# Funding history must be patched first because core.update_paper_trades() calls
# the helper while marking open paper positions.
if callable(_ORIGINAL_FUNDING_ROWS_CACHED):
    core._funding_rows_cached = _funding_rows_cached_v169
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
    # Run V16.10 overlay tests in addition to the original V16.4 deterministic self-test.
    if "--self-test" in sys.argv:
        overlay_self_test()
    return int(core.main())


if __name__ == "__main__":
    raise SystemExit(main())
