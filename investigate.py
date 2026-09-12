import os

from src.exchange.rest import BinanceRestClient

fut = BinanceRestClient(
    os.environ['BINANCE_API_KEY_LIVE'],
    os.environ['BINANCE_API_SECRET_LIVE'],
    environment='live',
)

print('=== ORDENES RECIENTES ETHUSDT (Futuros) ===')
orders = fut._request('GET', '/fapi/v1/allOrders', {'symbol': 'ETHUSDT', 'limit': 20}, signed=True)
for o in orders:
    print(
        'clientOrderId=', o.get('clientOrderId'),
        'status=', o.get('status'),
        'side=', o.get('side'),
        'positionSide=', o.get('positionSide'),
        'origQty=', o.get('origQty'),
        'executedQty=', o.get('executedQty'),
        'avgPrice=', o.get('avgPrice'),
        'time=', o.get('time'),
        'updateTime=', o.get('updateTime'),
    )

print('=== TRADES RECIENTES ETHUSDT (Futuros) ===')
trades = fut._request('GET', '/fapi/v1/userTrades', {'symbol': 'ETHUSDT', 'limit': 20}, signed=True)
for t in trades:
    print(
        'orderId=', t.get('orderId'),
        'side=', t.get('side'),
        'qty=', t.get('qty'),
        'price=', t.get('price'),
        'realizedPnl=', t.get('realizedPnl'),
        'time=', t.get('time'),
    )
