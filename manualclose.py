"""Cierre manual URGENTE de la posicion real de Funding Carry, reusando
la misma funcion close_carry_position() del ejecutor (perp primero con
reduceOnly + chequeo de estado real antes de reintentar, luego spot con
el balance libre real) en vez de logica improvisada."""
import os

from scripts.funding_carry_live import (
    BinanceRestClient,
    BinanceSpotRestClient,
    OrderManager,
    PERP_SYMBOL,
    SPOT_SYMBOL,
    SpotOrderManager,
    close_carry_position,
    init_db,
)

db = init_db()

spot_client = BinanceSpotRestClient(
    os.environ['BINANCE_API_KEY_SPOT'], os.environ['BINANCE_API_SECRET_SPOT'], environment='live'
)
futures_client = BinanceRestClient(
    os.environ['BINANCE_API_KEY_LIVE'], os.environ['BINANCE_API_SECRET_LIVE'], environment='live'
)
spot_om = SpotOrderManager(spot_client, SPOT_SYMBOL)
perp_om = OrderManager(futures_client, PERP_SYMBOL)

print('Cerrando posicion real (perp primero, luego spot)...')
result = close_carry_position(db, spot_om, perp_om, spot_client, futures_client, dry_run=False)
print('CLOSE_RESULT:', result)

pos = futures_client.get_position(PERP_SYMBOL)
print('PERP_POSITION_AMT_DESPUES:', pos.get('positionAmt'))
bal = spot_client.get_balances()
print('ETH_SPOT_FREE_DESPUES:', bal.get(SPOT_SYMBOL.replace('USDT', ''), {}).get('free'))
