import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

BYBIT_BASE = os.getenv("BYBIT_BASE", "https://api.bybit.com")
STATE_FILE = Path(os.getenv("STATE_FILE", ".monitor-state/state.json"))

MIN_APR_PCT = float(os.getenv("MIN_APR_PCT", "20"))
HIGH_APR_PCT = float(os.getenv("HIGH_APR_PCT", "35"))
MIN_SPOT_TURNOVER_24H = float(os.getenv("MIN_SPOT_TURNOVER_24H", "5000000"))
MIN_PERP_TURNOVER_24H = float(os.getenv("MIN_PERP_TURNOVER_24H", "5000000"))
MAX_ABS_ENTRY_SPREAD_PCT = float(os.getenv("MAX_ABS_ENTRY_SPREAD_PCT", "0.30"))
MIN_RISE_RATIO = float(os.getenv("MIN_RISE_RATIO", "1.20"))
MIN_RATE_DELTA_PCT = float(os.getenv("MIN_RATE_DELTA_PCT", "0.005"))
ALERT_COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", "360"))
ASSUMED_ROUND_TRIP_COST_PCT = float(os.getenv("ASSUMED_ROUND_TRIP_COST_PCT", "0.31"))
MAX_HISTORY_CHECKS = int(os.getenv("MAX_HISTORY_CHECKS", "20"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

USER_AGENT = "bybit-funding-monitor/1.0 github-actions"


def http_json(url, method="GET", data=None, timeout=20):
    body = None
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if data is not None:
        body = urlencode(data).encode("utf-8")
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    req = Request(url, data=body, headers=headers, method=method)
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw)
    except HTTPError as e:
        payload = e.read().decode("utf-8", errors="replace")
        if e.code == 403:
            raise RuntimeError(
                "Bybit API returned HTTP 403. Bybit blocks requests from some regions, including US IPs. "
                "GitHub-hosted runner egress location is not guaranteed, so this deployment may be unsuitable. "
                f"Response: {payload[:300]}"
            ) from e
        raise RuntimeError(f"HTTP {e.code} for {url}: {payload[:500]}") from e
    except URLError as e:
        raise RuntimeError(f"Network error for {url}: {e}") from e


def bybit(path, **params):
    url = f"{BYBIT_BASE}{path}"
    if params:
        url += "?" + urlencode(params)
    data = http_json(url)
    if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit error {data.get('retCode')}: {data.get('retMsg')}")
    return data


def load_state():
    try:
        return json.loads(STATE_FILE.read_text("utf-8"))
    except FileNotFoundError:
        return {"symbols": {}, "created_at": int(time.time())}
    except Exception:
        return {"symbols": {}, "created_at": int(time.time())}


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, separators=(",", ":")), "utf-8")
    tmp.replace(STATE_FILE)


def f(x, default=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def fmt_money(x):
    if x >= 1_000_000_000:
        return f"${x/1_000_000_000:.2f}B"
    if x >= 1_000_000:
        return f"${x/1_000_000:.2f}M"
    if x >= 1_000:
        return f"${x/1_000:.1f}K"
    return f"${x:.0f}"


def pct_from_rate(rate):
    return rate * 100.0


def annualized_apr_pct(rate, interval_hours):
    if interval_hours <= 0:
        return 0.0
    return rate * (24.0 / interval_hours) * 365.0 * 100.0


def daily_pct(rate, interval_hours):
    if interval_hours <= 0:
        return 0.0
    return rate * (24.0 / interval_hours) * 100.0


def format_next_funding(ms):
    if not ms:
        return "неизвестно"
    dt = datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
    return dt.strftime("%d.%m %H:%M UTC")


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not configured")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": "true",
    }
    result = http_json(url, method="POST", data=payload)
    if not result.get("ok"):
        raise RuntimeError(f"Telegram API error: {result}")


def get_history(symbol, limit=6):
    data = bybit("/v5/market/funding/history", category="linear", symbol=symbol, limit=str(limit))
    rows = data["result"].get("list", [])
    return [f(row.get("fundingRate")) for row in rows]


def main():
    now = int(time.time())
    state = load_state()
    old_symbols = state.get("symbols", {})

    linear = bybit("/v5/market/tickers", category="linear")["result"].get("list", [])
    spot = bybit("/v5/market/tickers", category="spot")["result"].get("list", [])
    spot_by_symbol = {x.get("symbol"): x for x in spot if x.get("symbol")}

    candidates = []
    new_symbols = {}

    for p in linear:
        symbol = p.get("symbol", "")
        if not symbol.endswith("USDT") or symbol not in spot_by_symbol:
            continue
        funding_raw = p.get("fundingRate")
        interval_raw = p.get("fundingIntervalHour")
        if funding_raw in (None, "") or interval_raw in (None, ""):
            continue

        rate = f(funding_raw)
        interval_h = f(interval_raw)
        if interval_h <= 0:
            continue

        old = old_symbols.get(symbol, {})
        new_symbols[symbol] = {
            "rate": rate,
            "seen_at": now,
            "last_alert_at": old.get("last_alert_at", 0),
            "last_alert_rate": old.get("last_alert_rate", 0),
        }

        if rate <= 0:
            continue

        s = spot_by_symbol[symbol]
        spot_turnover = f(s.get("turnover24h"))
        perp_turnover = f(p.get("turnover24h"))
        if spot_turnover < MIN_SPOT_TURNOVER_24H or perp_turnover < MIN_PERP_TURNOVER_24H:
            continue

        spot_ask = f(s.get("ask1Price"))
        perp_bid = f(p.get("bid1Price"))
        if spot_ask <= 0 or perp_bid <= 0:
            continue
        entry_spread_pct = (perp_bid / spot_ask - 1.0) * 100.0
        if abs(entry_spread_pct) > MAX_ABS_ENTRY_SPREAD_PCT:
            continue

        apr = annualized_apr_pct(rate, interval_h)
        if apr < MIN_APR_PCT:
            continue

        prev_rate = f(old.get("rate"))
        candidates.append({
            "symbol": symbol,
            "rate": rate,
            "interval_h": interval_h,
            "apr": apr,
            "daily_pct": daily_pct(rate, interval_h),
            "nextFundingTime": p.get("nextFundingTime"),
            "spot_turnover": spot_turnover,
            "perp_turnover": perp_turnover,
            "entry_spread_pct": entry_spread_pct,
            "prev_rate": prev_rate,
            "last_alert_at": int(old.get("last_alert_at", 0) or 0),
            "last_alert_rate": f(old.get("last_alert_rate")),
        })

    candidates.sort(key=lambda x: x["apr"], reverse=True)
    candidates = candidates[:MAX_HISTORY_CHECKS]

    print(f"Scanned linear tickers: {len(linear)}")
    print(f"Matched spot/perp state symbols: {len(new_symbols)}")
    print(f"Preliminary candidates: {len(candidates)}")

    alerts = []
    delta_decimal = MIN_RATE_DELTA_PCT / 100.0

    for c in candidates:
        hist = get_history(c["symbol"], 6)
        last_settled = hist[0] if hist else 0.0
        avg_recent = sum(hist) / len(hist) if hist else 0.0

        rise_prev = (
            c["prev_rate"] > 0
            and c["rate"] >= c["prev_rate"] * MIN_RISE_RATIO
            and c["rate"] - c["prev_rate"] >= delta_decimal
        )
        rise_settled = (
            last_settled > 0
            and c["rate"] >= last_settled * MIN_RISE_RATIO
            and c["rate"] - last_settled >= delta_decimal
        )
        high_apr = c["apr"] >= HIGH_APR_PCT

        # On the first observation, only alert for very high APR. Otherwise require growth/high APR.
        interesting = high_apr or rise_prev or rise_settled
        if not interesting:
            continue

        cooldown_sec = ALERT_COOLDOWN_MINUTES * 60
        cooldown_over = now - c["last_alert_at"] >= cooldown_sec
        materially_higher_than_last_alert = (
            c["last_alert_rate"] > 0 and c["rate"] >= c["last_alert_rate"] * 1.5
        )
        if c["last_alert_at"] and not cooldown_over and not materially_higher_than_last_alert:
            continue

        reasons = []
        if rise_prev:
            reasons.append("ставка заметно выросла с прошлого сканирования")
        if rise_settled:
            reasons.append("текущая ставка заметно выше последнего settlement")
        if high_apr:
            reasons.append(f"текущий simple APR ≥ {HIGH_APR_PCT:.0f}%")

        d_pct = c["daily_pct"]
        breakeven_days = ASSUMED_ROUND_TRIP_COST_PCT / d_pct if d_pct > 0 else 9999
        avg_pct = pct_from_rate(avg_recent)
        last_pct = pct_from_rate(last_settled)
        prev_pct = pct_from_rate(c["prev_rate"])

        text = (
            f"🚨 Bybit Funding Alert: {c['symbol']}\n\n"
            f"Текущий funding: +{pct_from_rate(c['rate']):.4f}% / {c['interval_h']:g}ч\n"
            f"Simple APR: {c['apr']:.1f}%\n"
            f"Эквивалент в сутки: {d_pct:.4f}%\n"
            f"Следующий settlement: {format_next_funding(c['nextFundingTime'])}\n\n"
            f"Предыдущий скан: {prev_pct:+.4f}%\n"
            f"Последний settled: {last_pct:+.4f}%\n"
            f"Среднее последних {len(hist)} settlement: {avg_pct:+.4f}%\n\n"
            f"Spot turnover 24h: {fmt_money(c['spot_turnover'])}\n"
            f"Perp turnover 24h: {fmt_money(c['perp_turnover'])}\n"
            f"Entry spread Spot ask → Perp bid: {c['entry_spread_pct']:+.3f}%\n"
            f"Оценка окупаемости {ASSUMED_ROUND_TRIP_COST_PCT:.2f}% round-trip costs: ~{breakeven_days:.1f} дн.\n\n"
            f"Почему интересно: {'; '.join(reasons)}"
        )
        send_telegram(text)
        alerts.append(c["symbol"])
        new_symbols[c["symbol"]]["last_alert_at"] = now
        new_symbols[c["symbol"]]["last_alert_rate"] = c["rate"]

    state = {
        "updated_at": now,
        "symbols": new_symbols,
    }
    save_state(state)

    print("Alerts sent:", alerts if alerts else "none")
    if candidates:
        print("Top current candidates:")
        for c in candidates[:10]:
            print(
                f"  {c['symbol']}: funding={pct_from_rate(c['rate']):+.4f}%/{c['interval_h']:g}h "
                f"APR={c['apr']:.1f}% spread={c['entry_spread_pct']:+.3f}%"
            )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
