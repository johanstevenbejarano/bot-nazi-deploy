import os

from src.exchange.rest import BinanceRestClient

fut = BinanceRestClient(
    os.environ['BINANCE_API_KEY_LIVE'],
    os.environ['BINANCE_API_SECRET_LIVE'],
    environment='live',
)

pos = fut.get_position('ETHUSDT')
amt = float(pos.get('positionAmt', 0) or 0)
print('POSITION_AMT_ANTES:', amt)

if amt == 0:
    print('ABORTANDO: no hay posicion perp abierta')
else:
    qty = abs(amt)
    side = 'BUY' if amt < 0 else 'SELL'
    position_side = 'SHORT' if amt < 0 else 'LONG'
    print('CERRANDO:', side, qty, position_side)
    order = fut.create_order(
        symbol='ETHUSDT',
        side=side,
        position_side=position_side,
        order_type='MARKET',
        quantity=qty,
        reduce_only=True,
    )
    print('ORDEN_RESULTADO:', order)

pos_after = fut.get_position('ETHUSDT')
print('POSITION_AMT_DESPUES:', pos_after.get('positionAmt'))
