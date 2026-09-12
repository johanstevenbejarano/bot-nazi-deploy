from src.exchange.spot_rest import BinanceSpotRestClient
from src.exchange.rest import BinanceRestClient
import os

spot = BinanceSpotRestClient(os.environ['BINANCE_API_KEY_SPOT'], os.environ['BINANCE_API_SECRET_SPOT'], environment='live')
fut = BinanceRestClient(os.environ['BINANCE_API_KEY_LIVE'], os.environ['BINANCE_API_SECRET_LIVE'], environment='live')

bal = spot.get_balances()
print('ETH_SPOT_FREE:', bal.get('ETH'))

pos = fut.get_position('ETHUSDT')
print('PERP_POSITION_AMT:', pos.get('positionAmt'))
