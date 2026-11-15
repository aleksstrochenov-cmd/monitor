"""Synthetic sample data: never confuse these with captured API responses."""
from __future__ import annotations

class DemoResponse:
    status_code=200
    def __init__(self, payload):self.payload=payload
    def raise_for_status(self):pass
    def json(self):return self.payload

class DemoSession:
    def get(self,url,**kwargs):
        if '/metadata/stats' in url:
            return DemoResponse({'listings':[{'ticker':'BTC','mark_price':'100000','funding_rate':'0.15','funding_interval_s':28800}]})
        if '/v1/markets' in url:
            return DemoResponse({'markets':[{'marketDisplayName':'BTC-USD','status':'ONLINE',
               'category':'CRYPTO','fundingRate':'0.00001','nextFundingRate':'0.00001',
               'nextFundingAt':1794710400,'markPrice':'100010'}]})
        if '/v2/perpMarketDefinitions' in url or '/v2/marketDefinitions' in url:
            return DemoResponse([{'symbol':'BTCRUSDPERP','marketId':1,'minOrderQty':'0.001','qtyStepSize':'0.001','tickSize':'0.1','liquidationMarginParameter':'0.05','initialMarginParameter':'0.04','maxLeverage':40,'oiCap':'10000'}])
        if '/v2/perpMarkets/summary' in url or '/v2/markets/summary' in url:
            return DemoResponse([{'symbol':'BTCRUSDPERP','updatedAt':1794710400000,'longOiQty':'10','shortOiQty':'10','oiQty':'20','fundingRate':'-0.00015','longFundingValue':'100','shortFundingValue':'100','fundingRateVelocity':'0','volume24h':'100000'}])
        raise ValueError('Unexpected demo request: '+url)
    def post(self,*args,**kwargs): return DemoResponse({'ok':True})
