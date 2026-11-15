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
        if '/v2/marketDefinitions' in url:
            return DemoResponse({'marketDefinitions':[{'symbol':'BTCRUSDPERP','marketType':'PERP','status':'OPEN'}]})
        if '/v2/markets/summary' in url:
            return DemoResponse({'markets':[{'symbol':'BTCRUSDPERP','fundingRate':'-0.00015','markPrice':'100010'}]})
        raise ValueError('Unexpected demo request: '+url)
    def post(self,*args,**kwargs): return DemoResponse({'ok':True})
