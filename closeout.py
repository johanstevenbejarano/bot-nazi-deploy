import math
import os

from src.exchange.spot_rest import BinanceSpotRestClient

spot = BinanceSpotRestClient(
    os.environ['BINANCE_API_KEY_SPOT'],
    os.environ['BINANCE_API_SECRET_SPOT'],
    environment='live',
)

bal = spot.get_balances()
free_eth = float(bal.get('ETH', {}).get('free', 0))
print('FREE_ETH_ANTES:', free_eth)

info = spot.get_exchange_info('ETHUSDT')
step = None
for f in info.get('filters', []):
    if f.get('filterType') == 'LOT_SIZE':
        step = float(f.get('stepSize', 0) or 0)
print('STEP_SIZE:', step)

if not step or free_eth <= 0:
    print('ABORTANDO: sin balance libre o sin stepSize valido, no se manda ninguna orden')
else:
    qty = math.floor(free_eth / step) * step
    qty = round(qty, 8)
    print('QTY_A_VENDER:', qty)

    if qty <= 0:
        print('ABORTANDO: qty calculada es 0')
    else:
        order = spot.create_order(
            symbol='ETHUSDT',
            side='SELL',
            order_type='MARKET',
            quantity=qty,
        )
        print('ORDEN_RESULTADO:', order)

bal_after = spot.get_balances()
print('FREE_ETH_DESPUES:', bal_after.get('ETH', {}).get('free', 0))
