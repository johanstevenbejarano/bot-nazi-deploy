import os

from src.exchange.rest import BinanceRestClient

fut = BinanceRestClient(
    os.environ['BINANCE_API_KEY_LIVE'],
    os.environ['BINANCE_API_SECRET_LIVE'],
    environment='live',
)

print('=== FUNDING FEE cobrado (ETHUSDT, desde la entrada de hoy) ===')
income = fut._request(
    'GET', '/fapi/v1/income',
    {'symbol': 'ETHUSDT', 'incomeType': 'FUNDING_FEE', 'limit': 20},
    signed=True,
)
total_funding = 0.0
for i in income:
    amt = float(i.get('income', 0))
    total_funding += amt
    print('time=', i.get('time'), 'income=', i.get('income'), 'asset=', i.get('asset'))
print('TOTAL_FUNDING_FEE:', round(total_funding, 6))

print('=== COMISIONES pagadas (ETHUSDT, ultimas ordenes) ===')
commission = fut._request(
    'GET', '/fapi/v1/income',
    {'symbol': 'ETHUSDT', 'incomeType': 'COMMISSION', 'limit': 20},
    signed=True,
)
total_commission = 0.0
for i in commission:
    amt = float(i.get('income', 0))
    total_commission += amt
    print('time=', i.get('time'), 'income=', i.get('income'))
print('TOTAL_COMMISSION_PERP:', round(total_commission, 6))

print('=== Posicion actual + PnL no realizado ===')
pos = fut.get_position('ETHUSDT')
print('positionAmt=', pos.get('positionAmt'), 'entryPrice=', pos.get('entryPrice'),
      'unRealizedProfit=', pos.get('unRealizedProfit'), 'markPrice=', pos.get('markPrice'))
