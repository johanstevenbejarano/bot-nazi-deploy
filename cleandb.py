"""Limpia posiciones fantasma en funding_carry_live.db despues de confirmar
(via checkpos.py) que el exchange real esta plano en ambas patas -- el
cierre automatico crasheo antes de poder actualizar la DB."""
from scripts.funding_carry_live import PERP_POSITION_SYMBOL, SPOT_POSITION_SYMBOL, init_db

db = init_db()

before_spot = db.get_position(SPOT_POSITION_SYMBOL)
before_perp = db.get_position(PERP_POSITION_SYMBOL)
print('ANTES -- spot:', before_spot, '| perp:', before_perp)

db.clear_position(SPOT_POSITION_SYMBOL)
db.clear_position(PERP_POSITION_SYMBOL)

after_spot = db.get_position(SPOT_POSITION_SYMBOL)
after_perp = db.get_position(PERP_POSITION_SYMBOL)
print('DESPUES -- spot:', after_spot, '| perp:', after_perp)
