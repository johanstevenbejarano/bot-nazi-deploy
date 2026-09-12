import os

from src.exchange.spot_rest import BinanceSpotRestClient
from src.exchange.rest import BinanceRestClient

spot = BinanceSpotRestClient(os.environ['BINANCE_API_KEY_SPOT'], os.environ['BINANCE_API_SECRET_SPOT'], environment='live')
fut = BinanceRestClient(os.environ['BINANCE_API_KEY_LIVE'], os.environ['BINANCE_API_SECRET_LIVE'], environment='live')

bal = spot.get_balances()
print('SPOT_USDT_FREE:', bal.get('USDT', {}).get('free'))
print('SPOT_ETH_FREE:', bal.get('ETH', {}).get('free'))

account = fut.get_account()
for a in account.get('assets', []):
    if a.get('asset') == 'USDT':
        print('FUT_USDT_AVAILABLE:', a.get('availableBalance'))

info = fut.get_exchange_info('ETHUSDT')
for f in info.get('filters', []):
    if f.get('filterType') == 'LOT_SIZE':
        print('PERP_STEP_SIZE:', f.get('stepSize'))
