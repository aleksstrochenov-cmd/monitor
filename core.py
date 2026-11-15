"""Conservative, auditable funding-rate normalization and market matching."""
from __future__ import annotations
from dataclasses import dataclass, asdict
from decimal import Decimal, InvalidOperation
from itertools import combinations
from typing import Any
import re

D = Decimal
HOURS_PER_YEAR = D('8760')


@dataclass(frozen=True)
class Funding:
    raw: str | None
    basis: str              # hourly_fraction / annual_fraction / unknown
    hourly_fraction: Decimal | None
    rate_type: str          # current / predicted / indicative / unknown
    next_settlement_s: int | None = None
    note: str = ''

    def record(self):
        return {
            'raw': self.raw, 'basis': self.basis,
            'hourly_fraction': str(self.hourly_fraction) if self.hourly_fraction is not None else None,
            'rate_type': self.rate_type, 'next_settlement_s': self.next_settlement_s,
            'note': self.note,
        }


@dataclass(frozen=True)
class Market:
    venue: str
    raw_symbol: str
    asset: str
    funding: Funding
    status: str = 'UNKNOWN'
    category: str = 'UNKNOWN'
    market_kind: str = 'PERP'
    mark_price: str | None = None
    bid: str | None = None
    ask: str | None = None
    raw: dict | None = None

    def record(self, include_raw=False):
        r = {k:v for k,v in asdict(self).items() if k not in ('funding','raw')}
        r['funding'] = self.funding.record()
        if include_raw: r['raw'] = self.raw
        return r


def number(v: Any) -> Decimal | None:
    if v is None or isinstance(v, bool): return None
    try:
        n = D(str(v))
        return n if n.is_finite() else None
    except (InvalidOperation, TypeError, ValueError): return None


def normalize_rate(raw: Any, basis: str, rate_type: str, next_settlement_s=None, note='') -> Funding:
    n = number(raw)
    normalized = None
    if n is not None:
        if basis == 'hourly_fraction': normalized = n
        elif basis == 'annual_fraction': normalized = n/HOURS_PER_YEAR
        elif basis == 'interval_fraction':
            raise ValueError('interval_fraction requires explicit interval; never infer')
    if normalized is not None and abs(normalized) > D('0.02'):
        # Venue-dependent extreme rate could be real; quarantine for operator inspection.
        note = (note + '; ' if note else '') + 'EXTREME_RATE_REQUIRE_REVIEW'
        normalized = None
    return Funding(str(raw) if raw is not None else None, basis, normalized,
                   rate_type, next_settlement_s, note)


def normalize_symbol(symbol: Any, venue: str) -> str | None:
    if not isinstance(symbol, str) or not symbol.strip(): return None
    s=symbol.upper().strip()
    if venue=='reya' and s.endswith('RUSDPERP'):
        s=s[:-8]  # BTCRUSDPERP -> BTC
    else:
        for suf in ('-USD-PERP','-USDC-PERP','-USDT-PERP','-PERP','-USD','-USDC','-USDT','USDPERP','USDTPERP'):
            if s.endswith(suf):
                s=s[:-len(suf)]; break
    if not re.fullmatch(r'[A-Z0-9._]{1,32}', s): return None
    return s


def _array(payload: Any, preferred: tuple[str,...]) -> list[dict]:
    if isinstance(payload, list): return [r for r in payload if isinstance(r,dict)]
    if not isinstance(payload,dict): return []
    for k in preferred:
        value=payload.get(k)
        if isinstance(value,list): return [r for r in value if isinstance(r,dict)]
        if isinstance(value,dict):
            # Sometimes API maps market symbol -> market body.
            return [dict(r, symbol=r.get('symbol',name)) for name,r in value.items() if isinstance(r,dict)]
    if all(isinstance(r,dict) for r in payload.values()) and payload:
        return [dict(r, symbol=r.get('symbol',name)) for name,r in payload.items()]
    return []


def parse_variational(payload: dict, rate_basis: str = 'unknown') -> list[Market]:
    # Official API establishes that this is a decimal but does not identify its
    # temporal base. DO NOT infer it from funding_interval_s.
    if rate_basis not in ('unknown', 'annual_fraction'):
        raise ValueError('Variational basis must remain unknown until verified; annual_fraction opt-in only')
    out=[]
    for r in _array(payload, ('listings',)):
        symbol=r.get('ticker'); asset=normalize_symbol(symbol,'variational')
        if not asset: continue
        quotes=r.get('quotes') or {}
        quote=quotes.get('size_1k') or {}
        f=normalize_rate(r.get('funding_rate'), rate_basis, 'indicative',
           note='API documentation does not specify whether funding_rate is annualized or per interval; never infer from funding_interval_s' if rate_basis=='unknown' else 'OPERATOR_OVERRIDE: annualized fraction; not an actual next settlement quote')
        out.append(Market('variational', str(symbol), asset, f, 'ONLINE', 'UNKNOWN', 'PERP',
                          str(r.get('mark_price')) if r.get('mark_price') is not None else None,
                          quote.get('bid'),quote.get('ask'),r))
    return out


def parse_arcus(payload:dict) -> list[Market]:
    out=[]
    for r in _array(payload, ('markets',)):
        raw=r.get('marketDisplayName') or r.get('symbol')
        asset=normalize_symbol(raw,'arcus')
        if not asset: continue
        # Predicted next hourly settlement is most useful for opportunity; fallback
        # is current observed hourly estimate, never claim a next-hour payment.
        predicted=r.get('nextFundingRate')
        source=predicted if predicted is not None else r.get('fundingRate')
        next_ts=r.get('nextFundingAt')
        if next_ts is not None:
            try: next_ts=int(next_ts)
            except (TypeError,ValueError): next_ts=None
        f=normalize_rate(source, 'hourly_fraction',
                         'predicted' if predicted is not None else 'current',next_ts)
        out.append(Market('arcus',str(raw),asset,f,str(r.get('status','UNKNOWN')).upper(),
                          str(r.get('category','UNKNOWN')).upper(),'PERP',
                          str(r.get('markPrice')) if r.get('markPrice') is not None else None,
                          None,None,r))
    return out


def parse_reya(definitions: Any, summaries: Any) -> list[Market]:
    definitions_rows=_array(definitions,('marketDefinitions','perpMarketDefinitions','markets','data'))
    summary_rows=_array(summaries,('markets','marketSummaries','perpMarkets','summaries','data'))
    summary_by_sym={str(r.get('symbol') or r.get('marketSymbol') or r.get('market') or r.get('ticker') or '').upper():r
                    for r in summary_rows}
    # Definitions may list symbols while summaries carry funding values; retain both
    # so a missing fundingRate is logged instead of quietly dropping the market.
    defs_by_sym={str(r.get('symbol') or r.get('marketSymbol') or r.get('market') or r.get('ticker') or '').upper():r
                 for r in definitions_rows}
    out=[]
    for raw_symbol in sorted(set(defs_by_sym) | set(summary_by_sym)):
        if not raw_symbol: continue
        d=defs_by_sym.get(raw_symbol,{})
        s=summary_by_sym.get(raw_symbol,{})
        kind=str(d.get('marketType') or d.get('type') or s.get('marketType') or 'PERP').upper()
        if 'SPOT' in kind or ('PERP' not in kind and not raw_symbol.endswith('PERP')): continue
        asset=normalize_symbol(raw_symbol,'reya')
        if not asset: continue
        # Per Reya documented SDK, market summaries include hourly fundingRate.
        # If schema differs at runtime leave it NULL and save the raw API sample.
        rate=s.get('fundingRate')
        f=normalize_rate(rate,'hourly_fraction','current',note='Continuous/current hourly rate, not a locked 8h payout')
        status=str(d.get('status') or s.get('status') or 'UNKNOWN').upper()
        out.append(Market('reya',raw_symbol,asset,f,status,
                          str(d.get('category') or s.get('category') or 'UNKNOWN').upper(),'PERP',
                          str(s.get('markPrice')) if s.get('markPrice') is not None else None,
                          s.get('bestBid'),s.get('bestAsk'),{'definition':d,'summary':s}))
    return out


def eligible(m:Market)->bool:
    return m.status not in {'CLOSED','OFFLINE','SUSPENDED','DELISTED','HALTED','DISABLED','SETTLEMENT_ONLY'}


def find_pairs(markets:dict[str,list[Market]]) -> list[tuple[Market,Market]]:
    # Every two-venue overlap, not only three-way; preserves duplicate tickers in
    # multiple venues for explicit examination, avoids cross-product duplicates.
    out=[]
    # Keep ALL markets per underlying, rather than overwriting ticker duplicates.
    indexed={}
    for v, rows in markets.items():
        symbols={}
        for m in rows:
            if eligible(m): symbols.setdefault(m.asset, []).append(m)
        indexed[v]=symbols
    for a,b in combinations(sorted(indexed),2):
        for asset in sorted(set(indexed[a]) & set(indexed[b])):
            for x in indexed[a][asset]:
                for y in indexed[b][asset]:
                    if x.category!='UNKNOWN' and y.category!='UNKNOWN' and x.category!=y.category: continue
                    out.append((x,y))
    return out


def evaluate(short:Market,long:Market,notional_usd:D=D('100'),hours:D=D('8'), threshold_pct:D=D('0.05')) -> dict:
    reasons=[]
    for m in (short,long):
        if not eligible(m): reasons.append(f'{m.venue}:INACTIVE_MARKET')
        if m.funding.hourly_fraction is None: reasons.append(f'{m.venue}:UNKNOWN_OR_INVALID_FUNDING_UNIT')
    if short.market_kind!='PERP' or long.market_kind!='PERP': reasons.append('NON_PERP_MARKET')
    # Mark prices show reference basis ONLY. They are not executable bid/ask quotes.
    mark_basis_pct=None
    sp=number(short.mark_price); lp=number(long.mark_price)
    if sp is not None and lp is not None and lp>0:
        mark_basis_pct=(sp-lp)/lp*D('100')
    est_pct=None
    funding_usd=None
    if short.funding.hourly_fraction is not None and long.funding.hourly_fraction is not None:
        est_pct=(short.funding.hourly_fraction-long.funding.hourly_fraction)*hours*D('100')
        funding_usd=notional_usd*est_pct/D('100')
        if est_pct<threshold_pct: reasons.append('BELOW_8H_FUNDING_THRESHOLD')
    return {
        'asset':short.asset,
        'short':short.record(include_raw=True),'long':long.record(include_raw=True),
        'horizon_hours':str(hours),'notional_each_usd':str(notional_usd),
        'mark_reference_basis_pct':str(mark_basis_pct) if mark_basis_pct is not None else None,
        'gross_funding_8h_pct':str(est_pct) if est_pct is not None else None,
        'gross_funding_8h_usd':str(funding_usd) if funding_usd is not None else None,
        'required_gross_8h_pct':str(threshold_pct),
        'passes':not reasons, 'reasons':reasons,
        'warning':'Indicative extrapolation; rates may change. Excludes trading fees, spreads, basis, slippage, liquidation and funding schedule mismatch.',
    }
