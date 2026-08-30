#!/usr/bin/env python3
"""Scheduled Aster cross-venue screener.

Purpose:
- Read PUBLIC market data only (no exchange API keys).
- Scan Aster USDT perpetuals against Bitget/MEXC/Bybit spot references.
- Sample the market for a short burst during each GitHub Actions run.
- Send WATCH/HOT candidates to Telegram.

This is a screener, NOT an auto-trader and NOT a sub-second latency detector.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests


ASTER_BASE = os.getenv("ASTER_BASE_URL", "https://fapi.asterdex.com")
BYBIT_BASE = os.getenv("BYBIT_BASE_URL", "https://api.bybit.com")
BITGET_BASE = os.getenv("BITGET_BASE_URL", "https://api.bitget.com")
MEXC_BASE = os.getenv("MEXC_BASE_URL", "https://api.mexc.com")

USER_AGENT = "aster-algo-scanner/1.0"
STATE_PATH = Path(os.getenv("STATE_PATH", "state/state.json"))


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


@dataclass
class Candidate:
    symbol: str
    move24h_pct: float
    quote_volume24h: float
    samples: List[MarketSample] = field(default_factory=list)

    def metrics(self, cfg: "Config") -> dict:
        spreads = [s.aster.spread_pct for s in self.samples]
        deviations = [s.deviation_pct for s in self.samples]
        disagreements = [s.ref_disagreement_pct for s in self.samples]

        if not spreads:
            return {}

        med_spread = statistics.median(spreads)
        max_spread = max(spreads)
        max_abs_dev = max(abs(x) for x in deviations)
        med_abs_dev = statistics.median(abs(x) for x in deviations)
        current_dev = deviations[-1]
        current = self.samples[-1]

        spread_hit_ratio = sum(x >= cfg.min_aster_spread_pct for x in spreads) / len(spreads)
        dev_hit_ratio = sum(abs(x) >= cfg.min_deviation_pct for x in deviations) / len(deviations)
        ref_good_ratio = sum(x <= cfg.max_reference_disagreement_pct for x in disagreements) / len(disagreements)

        # Transparent heuristic score. It ranks candidates; it is not a PnL estimate.
        spread_score = min(25.0, 12.5 * med_spread / max(cfg.min_aster_spread_pct, 1e-9))
        dev_score = min(30.0, 15.0 * max_abs_dev / max(cfg.min_deviation_pct, 1e-9))
        persistence_score = 15.0 * spread_hit_ratio
        dev_persistence_score = 15.0 * dev_hit_ratio
        ref_quality_score = 10.0 * ref_good_ratio
        move_bonus = min(5.0, 2.5 * abs(self.move24h_pct) / max(cfg.min_24h_move_pct, 1e-9))
        score = min(100.0, spread_score + dev_score + persistence_score + dev_persistence_score + ref_quality_score + move_bonus)

        level = "NONE"
        if (
            score >= cfg.hot_score
            and med_spread >= cfg.min_aster_spread_pct
            and max_abs_dev >= cfg.min_deviation_pct
            and dev_hit_ratio >= cfg.hot_min_deviation_hit_ratio
            and ref_good_ratio >= 0.8
        ):
            level = "HOT"
        elif (
            score >= cfg.watch_score
            and med_spread >= cfg.min_aster_spread_pct
            and ref_good_ratio >= 0.6
        ):
            level = "WATCH"

        if current_dev <= -cfg.min_deviation_pct:
            direction = "Aster BELOW fair"
        elif current_dev >= cfg.min_deviation_pct:
            direction = "Aster ABOVE fair"
        else:
            direction = "Aster near fair; wide-spread regime"

        return {
            "level": level,
            "score": score,
            "median_spread_pct": med_spread,
            "max_spread_pct": max_spread,
            "max_abs_deviation_pct": max_abs_dev,
            "median_abs_deviation_pct": med_abs_dev,
            "current_deviation_pct": current_dev,
            "spread_hit_ratio": spread_hit_ratio,
            "deviation_hit_ratio": dev_hit_ratio,
            "ref_good_ratio": ref_good_ratio,
            "current_fair": current.fair,
            "current_aster_bid": current.aster.bid,
            "current_aster_ask": current.aster.ask,
            "current_refs": sorted(current.refs.keys()),
            "current_ref_disagreement_pct": current.ref_disagreement_pct,
            "direction": direction,
            "samples": len(self.samples),
        }


@dataclass(frozen=True)
class Config:
    min_aster_spread_pct: float = float(os.getenv("MIN_ASTER_SPREAD_PCT", "0.15"))
    min_deviation_pct: float = float(os.getenv("MIN_DEVIATION_PCT", "0.20"))
    min_24h_move_pct: float = float(os.getenv("MIN_24H_MOVE_PCT", "8.0"))
    min_quote_volume24h: float = float(os.getenv("MIN_ASTER_QUOTE_VOLUME_24H", "50000"))
    min_reference_exchanges: int = int(os.getenv("MIN_REFERENCE_EXCHANGES", "2"))
    max_reference_disagreement_pct: float = float(os.getenv("MAX_REFERENCE_DISAGREEMENT_PCT", "0.20"))
    sample_count: int = int(os.getenv("SAMPLE_COUNT", "6"))
    sample_interval_seconds: float = float(os.getenv("SAMPLE_INTERVAL_SECONDS", "4"))
    max_candidates: int = int(os.getenv("MAX_CANDIDATES", "25"))
    watch_score: float = float(os.getenv("WATCH_SCORE", "55"))
    hot_score: float = float(os.getenv("HOT_SCORE", "72"))
    hot_min_deviation_hit_ratio: float = float(os.getenv("HOT_MIN_DEVIATION_HIT_RATIO", "0.25"))
    alert_levels: Tuple[str, ...] = tuple(x.strip().upper() for x in os.getenv("ALERT_LEVELS", "WATCH,HOT").split(",") if x.strip())
    cooldown_minutes: int = int(os.getenv("ALERT_COOLDOWN_MINUTES", "60"))
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


def get_json(url: str, timeout: float) -> object:
    r = requests.get(url, timeout=timeout, headers={"User-Agent": USER_AGENT})
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


def fetch_bybit_spot(cfg: Config) -> Dict[str, Quote]:
    data = get_json(f"{BYBIT_BASE}/v5/market/tickers?category=spot", cfg.request_timeout_seconds)
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


def fetch_snapshot(cfg: Config) -> Tuple[Dict[str, Quote], Dict[str, Dict[str, Quote]], List[str]]:
    funcs = {
        "aster": fetch_aster_book,
        "bybit": fetch_bybit_spot,
        "bitget": fetch_bitget_spot,
        "mexc": fetch_mexc_spot,
    }
    results: Dict[str, Dict[str, Quote]] = {}
    errors: List[str] = []
    with ThreadPoolExecutor(max_workers=4) as pool:
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

    refs = {k: v for k, v in results.items() if k != "aster" and v}
    return results["aster"], refs, errors


def reference_for_symbol(symbol: str, refs: Dict[str, Dict[str, Quote]], cfg: Config) -> Optional[Tuple[float, Dict[str, Quote], float]]:
    selected: Dict[str, Quote] = {}
    mids: List[float] = []
    for name, market in refs.items():
        q = market.get(symbol)
        if q:
            selected[name] = q
            mids.append(q.mid)

    if len(mids) < cfg.min_reference_exchanges:
        return None

    fair = statistics.median(mids)
    disagreement = ((max(mids) - min(mids)) / fair * 100.0) if fair > 0 else math.inf
    if disagreement > cfg.max_reference_disagreement_pct:
        return None
    return fair, selected, disagreement


def build_sample(symbol: str, aster_book: Dict[str, Quote], refs: Dict[str, Dict[str, Quote]], cfg: Config) -> Optional[MarketSample]:
    aq = aster_book.get(symbol)
    if not aq:
        return None
    ref = reference_for_symbol(symbol, refs, cfg)
    if not ref:
        return None
    fair, selected, disagreement = ref
    dev = (aq.mid - fair) / fair * 100.0
    return MarketSample(time.time(), aq, fair, selected, disagreement, dev)


def prefilter_candidates(
    aster_book: Dict[str, Quote],
    aster_24h: Dict[str, dict],
    refs: Dict[str, Dict[str, Quote]],
    cfg: Config,
) -> List[Candidate]:
    ranked: List[Tuple[float, Candidate]] = []
    for symbol, aq in aster_book.items():
        stats = aster_24h.get(symbol, {})
        move = fnum(stats.get("move24h_pct"))
        qvol = fnum(stats.get("quote_volume24h"))
        if abs(move) < cfg.min_24h_move_pct or qvol < cfg.min_quote_volume24h:
            continue
        sample = build_sample(symbol, aster_book, refs, cfg)
        if not sample:
            continue

        spread = aq.spread_pct
        dev = abs(sample.deviation_pct)
        # Pass if local spread is interesting OR the cross-venue mispricing is already interesting.
        if spread < cfg.min_aster_spread_pct and dev < cfg.min_deviation_pct:
            continue

        # Rank only to cap the expensive burst stage.
        pre_score = (
            spread / max(cfg.min_aster_spread_pct, 1e-9)
            + dev / max(cfg.min_deviation_pct, 1e-9)
            + min(abs(move) / max(cfg.min_24h_move_pct, 1e-9), 3.0) * 0.25
        )
        ranked.append((pre_score, Candidate(symbol, move, qvol, [sample])))

    ranked.sort(key=lambda x: x[0], reverse=True)
    return [c for _, c in ranked[: cfg.max_candidates]]


def scan(cfg: Config) -> Tuple[List[Tuple[Candidate, dict]], List[str]]:
    # 24h stats change slowly; fetch once.
    try:
        aster_24h = fetch_aster_24h(cfg)
    except Exception as e:
        raise ApiError(f"Failed to fetch Aster 24h stats: {type(e).__name__}: {e}") from e

    aster_book, refs, errors = fetch_snapshot(cfg)
    candidates = prefilter_candidates(aster_book, aster_24h, refs, cfg)

    if not candidates:
        return [], errors

    by_symbol = {c.symbol: c for c in candidates}
    print(f"Prefilter candidates: {', '.join(by_symbol.keys())}")

    # The first sample is already stored. Collect the rest as a short burst.
    for i in range(1, max(cfg.sample_count, 1)):
        time.sleep(cfg.sample_interval_seconds)
        try:
            aster_book, refs, sample_errors = fetch_snapshot(cfg)
            errors.extend(sample_errors)
        except Exception as e:
            errors.append(f"burst sample {i + 1}: {type(e).__name__}: {e}")
            continue

        for symbol, candidate in by_symbol.items():
            s = build_sample(symbol, aster_book, refs, cfg)
            if s:
                candidate.samples.append(s)

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


def should_alert(symbol: str, level: str, score: float, state: dict, cfg: Config) -> bool:
    prev = state.get("last_alerts", {}).get(symbol)
    if not prev:
        return True
    prev_level = str(prev.get("level", "NONE")).upper()
    prev_ts = fnum(prev.get("ts"))
    # Allow an immediate WATCH -> HOT escalation.
    if level == "HOT" and prev_level != "HOT":
        return True
    return time.time() - prev_ts >= cfg.cooldown_minutes * 60


def fmt_price(x: float) -> str:
    if x >= 1000:
        return f"{x:,.2f}"
    if x >= 1:
        return f"{x:.6f}".rstrip("0").rstrip(".")
    return f"{x:.10f}".rstrip("0").rstrip(".")


def format_alert(candidate: Candidate, m: dict) -> str:
    icon = "🔥" if m["level"] == "HOT" else "👀"
    refs = ", ".join(m["current_refs"])
    return (
        f"{icon} <b>{m['level']} — {candidate.symbol}</b>\n"
        f"Score: <b>{m['score']:.0f}/100</b>\n"
        f"Aster: {fmt_price(m['current_aster_bid'])} / {fmt_price(m['current_aster_ask'])}\n"
        f"Median spread: <b>{m['median_spread_pct']:.3f}%</b> (max {m['max_spread_pct']:.3f}%)\n"
        f"Reference fair: {fmt_price(m['current_fair'])} [{refs}]\n"
        f"Current deviation: <b>{m['current_deviation_pct']:+.3f}%</b>\n"
        f"Max |deviation| in burst: <b>{m['max_abs_deviation_pct']:.3f}%</b>\n"
        f"Deviation hits: {m['deviation_hit_ratio'] * 100:.0f}% of samples\n"
        f"Reference disagreement: {m['current_ref_disagreement_pct']:.3f}%\n"
        f"Aster 24h: {candidate.move24h_pct:+.1f}% | quote vol {candidate.quote_volume24h:,.0f}\n"
        f"State: <b>{m['direction']}</b>\n\n"
        "Signal = candidate for manual inspection in MetaScalp, not a buy/sell command."
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
            headers={"User-Agent": USER_AGENT},
        )
        r.raise_for_status()
        payload = r.json()
        if not payload.get("ok"):
            raise ApiError(f"Telegram returned ok=false: {payload.get('description', 'unknown error')}")
        return True
    except Exception as e:
        # Never print the request URL because it contains the bot token.
        print(f"ERROR sending Telegram alert: {type(e).__name__}: {e}", file=sys.stderr)
        return False


def print_table(ranked: List[Tuple[Candidate, dict]]) -> None:
    if not ranked:
        print("No candidates passed the prefilter.")
        return
    print("\nTop results:")
    print(f"{'SYMBOL':<16} {'LEVEL':<6} {'SCORE':>5} {'SPR%':>8} {'DEVmax%':>9} {'DEVnow%':>9} {'24H%':>8} {'REFS':>5}")
    for c, m in ranked[:20]:
        print(
            f"{c.symbol:<16} {m['level']:<6} {m['score']:>5.0f} "
            f"{m['median_spread_pct']:>8.3f} {m['max_abs_deviation_pct']:>9.3f} "
            f"{m['current_deviation_pct']:>+9.3f} {c.move24h_pct:>+8.1f} {len(m['current_refs']):>5}"
        )


def self_test() -> None:
    cfg = Config(
        min_aster_spread_pct=0.15,
        min_deviation_pct=0.20,
        min_24h_move_pct=8.0,
        min_quote_volume24h=50000,
        min_reference_exchanges=2,
        max_reference_disagreement_pct=0.20,
        sample_count=4,
        sample_interval_seconds=0,
        max_candidates=25,
        watch_score=55,
        hot_score=72,
        hot_min_deviation_hit_ratio=0.25,
        alert_levels=("WATCH", "HOT"),
        cooldown_minutes=60,
        request_timeout_seconds=1,
    )
    c = Candidate("TESTUSDT", 22.0, 1_500_000)
    for dev, spread in [(-0.31, 0.31), (-0.25, 0.28), (-0.05, 0.26), (-0.29, 0.30)]:
        fair = 10.0
        mid = fair * (1 + dev / 100)
        width = mid * spread / 100
        aq = Quote(mid - width / 2, mid + width / 2)
        refs = {"bitget": Quote(9.999, 10.001), "mexc": Quote(9.998, 10.002)}
        c.samples.append(MarketSample(time.time(), aq, fair, refs, 0.0, dev))
    m = c.metrics(cfg)
    assert m["level"] == "HOT", m
    assert m["score"] >= 72, m
    assert valid_quote("1", "1.01") is not None
    assert valid_quote("1.01", "1") is None
    print("Self-test OK")


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
        for e in unique_errors[:12]:
            print(" -", e)

    state = load_state()
    state_changed = False
    for candidate, m in ranked:
        level = m["level"]
        if level not in cfg.alert_levels:
            continue
        if not should_alert(candidate.symbol, level, m["score"], state, cfg):
            continue
        text = format_alert(candidate, m)
        if args.dry_run:
            print("\nDRY ALERT:\n", text.replace("<b>", "").replace("</b>", ""))
            continue
        if send_telegram(text):
            state.setdefault("last_alerts", {})[candidate.symbol] = {
                "ts": int(time.time()),
                "level": level,
                "score": round(m["score"], 2),
            }
            state_changed = True

    if state_changed:
        save_state(state)
        print(f"State updated: {STATE_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
