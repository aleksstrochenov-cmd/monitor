"""Continuous READ-ONLY scanner. No order endpoint exists in this package."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import signal
import sys
import time
from decimal import Decimal
from typing import Any

import requests
from .core import parse_arcus, parse_variational, parse_reya, find_pairs, evaluate

LOG = logging.getLogger("funding_watch")

# Primary Reya routes follow the current perp-specific V2 API.
# Deprecated routes are attempted only when the primary route returns HTTP 404.
URLS = {
    "variational": (
        "https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/stats",
    ),
    "arcus": (
        "https://api.arcus.xyz/v1/markets",
    ),
    "reya_defs": (
        "https://api.reya.xyz/v2/perpMarketDefinitions",
        "https://api.reya.xyz/v2/marketDefinitions",
    ),
    "reya_summary": (
        "https://api.reya.xyz/v2/perpMarkets/summary",
        "https://api.reya.xyz/v2/markets/summary",
    ),
}

# Descriptive client identity and standard JSON request headers.
# No cookies, wallet credentials, API keys, or private keys are sent.
BASE_HEADERS = {
    "User-Agent": "FundingWatch/0.4 (+https://github.com/aleksstrochenov-cmd/monitor)",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}
REYA_HEADERS = {
    **BASE_HEADERS,
    "Origin": "https://app.reya.xyz",
    "Referer": "https://app.reya.xyz/",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

DIAGNOSTIC_RESPONSE_HEADERS = (
    "server", "content-type", "cf-ray", "cf-mitigated", "cf-cache-status",
    "via", "x-cache", "x-request-id", "x-amzn-requestid",
    "x-amz-cf-id", "retry-after", "www-authenticate",
)


def response_diagnostics(response: Any, venue: str, request_profile: str) -> dict:
    """Capture bounded, non-secret diagnostics for an HTTP failure."""
    headers = getattr(response, "headers", {}) or {}
    selected = {}
    for name in DIAGNOSTIC_RESPONSE_HEADERS:
        value = headers.get(name)
        if value is not None:
            selected[name] = str(value)[:300]

    full_body = str(getattr(response, "text", "") or "")
    body = full_body.replace("\\x00", "")[:2000]
    body_lower = body.lower()
    cf_mitigated = str(selected.get("cf-mitigated", "")).lower()
    server = str(selected.get("server", "")).lower()

    if "challenge" in cf_mitigated or "challenge-platform" in body_lower:
        classification = "cdn_challenge"
    elif "cf-ray" in selected or "cloudflare" in server:
        classification = "cdn_or_waf_403"
    else:
        classification = "origin_or_api_gateway_403"

    return {
        "venue": venue,
        "url": str(getattr(response, "url", "") or ""),
        "status_code": getattr(response, "status_code", None),
        "request_profile": request_profile,
        "classification_hint": classification,
        "response_headers": selected,
        "body_preview": body,
        "body_truncated": len(full_body) > 2000,
    }


def utc_now(): return datetime.now(timezone.utc).isoformat(timespec='seconds')


def append_jsonl(path: Path, obj: dict, max_bytes: int=20_000_000):
    path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists() and path.stat().st_size > max_bytes:
        target=path.with_name(path.stem+'.previous'+path.suffix)
        path.replace(target)
    with path.open('a',encoding='utf-8') as fp:
        fp.write(json.dumps(obj,ensure_ascii=False,default=str,separators=(',',':'))+'\n')


def public_payload_summary(data:Any,limit:int=3)->Any:
    # An exact fragment of the received payload, not an invented example.
    if isinstance(data,list):return data[:limit]
    if not isinstance(data,dict):return data
    result={}
    for k,v in data.items():
        if isinstance(v,list): result[k]=v[:limit];result[k+'_total_items']=len(v)
        elif isinstance(v,dict) and len(v)>limit:
            result[k]={x:v[x] for x in list(v)[:limit]};result[k+'_total_keys']=len(v)
        else: result[k]=v
    return result


class Monitor:
    def __init__(self,root:Path,session=None,now=time.time):
        self.root=root
        self.root.mkdir(parents=True,exist_ok=True)
        self.session=session or requests.Session()
        self.now=now
        self.notified={}
        self.active={}
        self.last_rejected={}
        self.last_sample={}
        self.last_endpoint={}
        self.last_http_diagnostics={}
        self._load_state()
        self.poll_s=max(10,int(os.environ.get('FW_POLL_SECONDS','30')))
        self.sample_s=max(60,int(os.environ.get('FW_SAMPLE_SECONDS','600')))
        self.rejected_s=max(60,int(os.environ.get('FW_REJECTED_LOG_SECONDS','300')))
        self.threshold=Decimal(os.environ.get('FW_THRESHOLD_8H_PCT','0.05'))
        self.hours=Decimal(os.environ.get('FW_HORIZON_HOURS','8'))
        self.notional=Decimal(os.environ.get('FW_NOTIONAL_EACH_USD','100'))
        self.variational_basis=os.environ.get('FW_VARIATIONAL_RATE_BASIS','unknown').strip()
        self.telegram_token=os.environ.get('TELEGRAM_BOT_TOKEN','').strip()
        self.telegram_chat=os.environ.get('TELEGRAM_CHAT_ID','').strip()

    def _load_state(self):
        try:
            with (self.root/'alert_state.json').open(encoding='utf-8') as fp:
                state=json.load(fp)
            self.active=state.get('active',{})
        except (FileNotFoundError,ValueError,OSError): self.active={}

    def _save_state(self):
        target=self.root/'alert_state.json'
        tmp=target.with_suffix('.tmp')
        tmp.write_text(json.dumps({'active':self.active,'written_at':utc_now()},ensure_ascii=False,indent=2),encoding='utf-8')
        tmp.replace(target)

    def _request(self, venue: str) -> Any:
        """Fetch public data and preserve actionable HTTP failure details.

        A 404 on a primary Reya path triggers the compatibility path. A 403
        is not treated as a missing route: its response body and safe headers
        are recorded to identify CDN/WAF versus origin/API-gateway rejection.
        """
        urls = URLS[venue]
        last_error = None
        headers = REYA_HEADERS if venue.startswith("reya_") else BASE_HEADERS
        request_profile = (
            "reya_public_api_headers" if venue.startswith("reya_")
            else "standard_public_api"
        )

        for index, url in enumerate(urls):
            response = None
            try:
                response = self.session.get(
                    url,
                    timeout=(5, 18),
                    headers=headers,
                )
                status_code = getattr(response, "status_code", 200)

                if status_code == 404 and index < len(urls) - 1:
                    last_error = f"HTTP 404 for {url}; trying compatibility route"
                    continue

                response.raise_for_status()
                data = response.json()
                self.last_endpoint[venue] = url
                self.last_http_diagnostics.pop(venue, None)

                now = self.now()
                if now - self.last_sample.get(venue, 0) >= self.sample_s:
                    append_jsonl(
                        self.root / "api_samples.ndjson",
                        {
                            "observed_at": utc_now(),
                            "venue": venue,
                            "endpoint": url,
                            "status_code": status_code,
                            "request_profile": request_profile,
                            "payload_fragment": public_payload_summary(data),
                            "capture_note": (
                                "Unmodified live-response fragment; "
                                "truncated to first 3 items per array"
                            ),
                        },
                    )
                    self.last_sample[venue] = now
                return data

            except requests.HTTPError as exc:
                failed_response = getattr(exc, "response", None) or response
                status_code = getattr(failed_response, "status_code", None)

                if status_code == 404 and index < len(urls) - 1:
                    last_error = f"HTTP 404 for {url}; trying compatibility route"
                    continue

                if failed_response is not None:
                    diag = response_diagnostics(
                        failed_response, venue, request_profile
                    )
                    self.last_http_diagnostics[venue] = diag
                    append_jsonl(
                        self.root / "api_errors.ndjson",
                        {
                            "observed_at": utc_now(),
                            "error_type": "http_response",
                            **diag,
                        },
                    )
                    last_error = (
                        f"HTTP {status_code} for {url}; "
                        f"classification_hint={diag['classification_hint']}; "
                        f"response_headers={json.dumps(diag['response_headers'], ensure_ascii=False)}; "
                        f"body_preview={diag['body_preview'][:500]}"
                    )
                else:
                    last_error = f"{type(exc).__name__}: {exc}"
                break

            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                break

        attempted = " | ".join(urls)
        raise RuntimeError(
            f"{venue} request failed; attempted [{attempted}]; last error: {last_error}"
        )

    def _telegram(self,msg:str):
        if not (self.telegram_token and self.telegram_chat):
            LOG.info('Telegram not configured. Would notify: %s',msg.replace('\n',' | '))
            return
        endpoint=f'https://api.telegram.org/bot{self.telegram_token}/sendMessage'
        resp=self.session.post(endpoint,data={'chat_id':self.telegram_chat,'text':msg,
                                              'disable_web_page_preview':'true'},timeout=(5,15))
        if resp.status_code >= 400:
            raise RuntimeError(f'Telegram HTTP {resp.status_code} (URL/token withheld)')

    def _format_alert(self,rec:dict)->str:
        sh=rec['short'];lo=rec['long']
        def rate(leg):
            f=leg['funding']; hourly=f['hourly_fraction']
            if hourly is None:return 'не определён'
            apr=Decimal(hourly)*Decimal(8760)*Decimal(100)
            return f'{apr:+.2f}% APR ({f["rate_type"]})'
        return ('🔎 FUNDING WATCH — ИНДИКАТИВНЫЙ СИГНАЛ\n'
                f'{rec["asset"]}: SHORT {sh["venue"].upper()} / LONG {lo["venue"].upper()}\n'
                f'SHORT funding: {rate(sh)}\nLONG funding: {rate(lo)}\n'
                f'Разница за {rec["horizon_hours"]} ч: {rec["gross_funding_8h_pct"]}%\n'
                f'Разница mark-price (НЕ исполнимый спред): {rec["mark_reference_basis_pct"] or "н/д"}%\n'
                f'При ${rec["notional_each_usd"]} на ногу: ${Decimal(rec["gross_funding_8h_usd"]):.4f} (условно)\n'
                'Не учтены комиссии, цена входа/выхода, basis и риск изменения funding.\n'
                f'Время наблюдения (UTC): {utc_now()}')

    def run_cycle(self):
        observed=utc_now(); data={}; errors={}
        for venue in URLS:
            try: data[venue]=self._request(venue)
            except Exception as exc:
                errors[venue]=f'{type(exc).__name__}: {exc}'
                append_jsonl(self.root/'api_errors.ndjson',{'observed_at':observed,'venue':venue,'error':errors[venue]})
        markets={}
        if 'variational' in data:
            try: markets['variational']=parse_variational(data['variational'],self.variational_basis)
            except Exception as exc:
                errors['variational_parse']=str(exc)
                append_jsonl(self.root/'api_errors.ndjson',{'observed_at':observed,'venue':'variational_parse','error':str(exc)})
        if 'arcus' in data:
            try: markets['arcus']=parse_arcus(data['arcus'])
            except Exception as exc:
                errors['arcus_parse']=str(exc)
                append_jsonl(self.root/'api_errors.ndjson',{'observed_at':observed,'venue':'arcus_parse','error':str(exc)})
        # Parse Reya from whichever endpoint succeeded. The definitions endpoint
        # provides the market universe; the summary endpoint provides funding.
        # A partial response is useful for diagnostics, but missing funding stays
        # NULL and can never become a passing signal.
        if 'reya_defs' in data or 'reya_summary' in data:
            try:
                markets['reya'] = parse_reya(
                    data.get('reya_defs', []),
                    data.get('reya_summary', []),
                )
            except Exception as exc:
                errors['reya_parse'] = str(exc)
                append_jsonl(
                    self.root / 'api_errors.ndjson',
                    {'observed_at': observed, 'venue': 'reya_parse', 'error': str(exc)},
                )
        # Never manufacture a zero funding rate from missing JSON.
        pairs=find_pairs(markets)
        present=set(); passing=0; rejected=0
        for a,b in pairs:
            for short,long in ((a,b),(b,a)):
                rec=evaluate(short,long,self.notional,self.hours,self.threshold)
                rec['observed_at']=observed
                key=f'{rec["asset"]}:{short.venue}[{short.raw_symbol}]>{long.venue}[{long.raw_symbol}]'
                if rec['passes']:
                    passing+=1; present.add(key)
                    if key not in self.active:
                        rec['event']='OPENED'
                        append_jsonl(self.root/'funding_opportunities.ndjson',rec)
                        self.active[key]={'since':observed,'last_seen':observed}
                        try:self._telegram(self._format_alert(rec))
                        except Exception as exc:
                            append_jsonl(self.root/'api_errors.ndjson',{'observed_at':observed,'venue':'telegram','error':str(exc)})
                    else:
                        self.active[key]['last_seen']=observed
                else:
                    rejected+=1
                    fingerprint=(key,','.join(rec['reasons']))
                    now=self.now()
                    if now-self.last_rejected.get(fingerprint,0)>self.rejected_s:
                        append_jsonl(self.root/'candidates_rejected.ndjson',rec)
                        self.last_rejected[fingerprint]=now
        # Do not close alerts when an upstream venue failed: missing data is not
        # evidence that the opportunity disappeared.
        if not errors:
            for key in list(self.active):
                if key not in present:
                    append_jsonl(self.root/'funding_opportunities.ndjson',{
                        'event':'CLOSED','observed_at':observed,'route':key,
                        'first_seen_at':self.active[key]['since'],'last_seen_at':self.active[key]['last_seen']})
                    del self.active[key]
        self._save_state()
        summary={'observed_at':observed,'markets':{k:len(v) for k,v in markets.items()},
                 'matching_instrument_pairs':len(pairs),'passing_directions':passing,
                 'rejected_directions':rejected,'errors':errors,
                 'variational_basis':self.variational_basis,'api_endpoints':self.last_endpoint,
                 'api_diagnostics':self.last_http_diagnostics,
                 'telegram_configured':bool(self.telegram_token and self.telegram_chat)}
        (self.root/'health.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
        LOG.info('Summary %s',summary)
        return summary


def main(argv=None):
    p=argparse.ArgumentParser(description='Continuous read-only three-DEX funding monitor')
    p.add_argument('--once',action='store_true',help='One live REST poll then exit')
    p.add_argument('--cycles',type=int,default=None,help='Run exactly N scans, then exit (ideal for GitHub Actions)')
    p.add_argument('--poll-seconds',type=int,default=None,help='Seconds between scan START times; e.g. 60')
    p.add_argument('--state-dir',default=os.environ.get('FW_STATE_DIR','state/funding_watch'))
    p.add_argument('--demo',action='store_true',help='Use synthetic fixtures; no external connections')
    args=p.parse_args(argv)
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    monitor=Monitor(Path(args.state_dir))
    if args.demo:
        from .sample_data import DemoSession
        monitor.session=DemoSession()
        monitor.telegram_token=''
    if args.cycles is not None and args.cycles < 1:
        p.error('--cycles must be positive')
    if args.once and args.cycles is not None:
        p.error('use either --once or --cycles, not both')
    interval = args.poll_seconds if args.poll_seconds is not None else monitor.poll_s
    if interval < 1:
        p.error('--poll-seconds must be positive')
    remaining = 1 if args.once else args.cycles
    running=True
    def stop(*_):
        nonlocal running
        running=False
    signal.signal(signal.SIGTERM,stop)
    signal.signal(signal.SIGINT,stop)
    next_start = time.monotonic()
    while running and (remaining is None or remaining > 0):
        wait = next_start - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        if not running:
            break
        cycle_start = time.monotonic()
        try: monitor.run_cycle()
        except Exception:
            LOG.exception('Cycle failed, continuing')
        if remaining is not None:
            remaining -= 1
            if remaining == 0:
                break  # No unnecessary sleep after the last scan.
        # Do not accumulate drift if an API cycle takes > one interval.
        next_start = max(next_start + interval, cycle_start + 0.001)
    return 0

if __name__=='__main__':sys.exit(main())
