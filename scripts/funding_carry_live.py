"""
Ejecutor EN VIVO de Funding Carry (ETHUSDT) — Etapa 3 del plan de derisking.

A diferencia de scripts/funding_carry_paper_watch.py (solo lectura, sin
credenciales), este script SI puede colocar ordenes reales: compra ETH en
Binance Spot + abre un corto en el perpetuo ETHUSDT, en notional igual
(delta-neutral), siguiendo la MISMA regla validada (ver
src/signals/funding_carry_rule.py — fuente unica de verdad, compartida con
el paper-watcher).

SEGURIDAD — arranca SIEMPRE en dry-run (solo loguea/notifica que ordenaria,
nunca coloca una orden real) salvo que se den DOS cosas a la vez:
  1. la variable de entorno FUNDING_CARRY_LIVE_CONFIRM=yes
  2. la flag --live al ejecutar el script
La ausencia de cualquiera de las dos deja el script en modo seguro. Mismo
patron fail-secure que DASHBOARD_CONTROLS_PASSWORD/BOT2_WEBHOOK_TOKEN en
este proyecto.

Colocar la primera orden real (activar el modo en vivo) requiere ademas
haber corrido scripts/funding_carry_preflight.py y tener confirmacion
explicita del usuario — ver plan de implementacion, Fase 4 esta
deliberadamente fuera de alcance hasta ese paso.

Uso:
  python scripts/funding_carry_live.py                 # loop, dry-run
  python scripts/funding_carry_live.py --once           # un chequeo, dry-run
  python scripts/funding_carry_live.py --once --live    # requiere ademas
                                                          # FUNDING_CARRY_LIVE_CONFIRM=yes

Estado persistido en data/funding_carry_live.db (separada de bot_trading.db
y de funding_carry_paper.db).
"""
from __future__ import annotations

import argparse
import logging
import logging.handlers
import os
import secrets
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))
load_dotenv(BASE_DIR / ".env")

from src.data.order_manager import OrderManager  # noqa: E402
from src.exchange.rest import BinanceRestClient  # noqa: E402
from src.exchange.spot_rest import BinanceSpotRestClient  # noqa: E402
from src.observability.ntfy_alert import build_from_env  # noqa: E402
from src.risk.engine import RiskEngine  # noqa: E402
from src.signals.funding_carry_rule import ENTRY_THRESHOLD, EXIT_THRESHOLD, MA_WINDOW, next_state  # noqa: E402
from src.storage.db import Database  # noqa: E402

# Simbolo configurable via FUNDING_CARRY_SYMBOL (default ETHUSDT) -- ver
# scripts/funding_carry_paper_watch.py para la misma convencion. DB_PATH y
# LOG_PATH mantienen los nombres originales para ETHUSDT (no romper el
# historial ya acumulado); otros simbolos llevan sufijo.
SPOT_SYMBOL = os.environ.get("FUNDING_CARRY_SYMBOL", "ETHUSDT")
PERP_SYMBOL = SPOT_SYMBOL
SPOT_POSITION_SYMBOL = f"{SPOT_SYMBOL}-SPOT"
PERP_POSITION_SYMBOL = f"{PERP_SYMBOL}-PERP"

_SUFFIX = "" if SPOT_SYMBOL == "ETHUSDT" else f"_{SPOT_SYMBOL.lower()}"
DB_PATH = BASE_DIR / "data" / f"funding_carry_live{_SUFFIX}.db"
LOG_PATH = BASE_DIR / "logs" / f"funding_carry_live{_SUFFIX}.log"

LEVERAGE = 2
# .get(...) or "25" (no .get(..., "25")) a proposito: docker-compose puede
# pasar la variable seteada pero vacia ("") si no esta en .env, y
# os.environ.get con default solo aplica cuando la clave esta ausente.
TARGET_NOTIONAL_USDT = float(os.environ.get("FUNDING_CARRY_NOTIONAL_USDT") or "25")
# spot 100% del notional + margen perp a LEVERAGE x
CAPITAL_NEEDED_USDT = TARGET_NOTIONAL_USDT * (1.0 + 1.0 / LEVERAGE)

CHECK_EVERY_SECONDS = 900  # 15 min — funding liquida cada 8h, igual que el paper watcher
MAX_LEG_RETRIES = 3
RETRY_DELAY_S = 5
FILL_CONFIRM_TIMEOUT_S = 30
FILL_POLL_INTERVAL_S = 2
SPOT_RECONCILE_TOLERANCE_USDT = 1.25  # en USD, no en cantidad de activo -- valido para cualquier symbol

LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [FUNDING-LIVE] %(levelname)s %(message)s",
    handlers=[
        logging.handlers.RotatingFileHandler(LOG_PATH, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

_ntfy = build_from_env()


def live_mode_enabled(args: argparse.Namespace) -> bool:
    env_confirm = os.environ.get("FUNDING_CARRY_LIVE_CONFIRM", "").strip().lower() == "yes"
    return env_confirm and bool(args.live)


# ---------------------------------------------------------------------------
# Persistencia
# ---------------------------------------------------------------------------

def init_db() -> Database:
    db = Database(str(DB_PATH))
    db.execute("CREATE TABLE IF NOT EXISTS carry_state (key TEXT PRIMARY KEY, value TEXT)")
    return db


def get_state(db: Database, key: str, default: str = "0") -> str:
    rows = db.query("SELECT value FROM carry_state WHERE key=?", (key,))
    return rows[0]["value"] if rows else default


def set_state(db: Database, key: str, value: Any) -> None:
    db.execute(
        "INSERT INTO carry_state (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value)),
    )


# ---------------------------------------------------------------------------
# Datos de mercado
# ---------------------------------------------------------------------------

def fetch_recent_funding(limit: int = MA_WINDOW + 5) -> list:
    """Descarga los ultimos N eventos de funding (publico, sin auth).
    Duplicado deliberado de la misma funcion en funding_carry_paper_watch.py
    -- ~5 lineas, no vale forzar un import cruzado entre dos scripts
    standalone solo por esto (ver plan de implementacion)."""
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    resp = requests.get(url, params={"symbol": PERP_SYMBOL, "limit": limit}, timeout=15)
    resp.raise_for_status()
    return resp.json()


def generate_client_order_id(prefix: str) -> str:
    micros = int(time.time() * 1e6)
    suffix = secrets.token_hex(2)
    return f"BOT-{prefix}-{micros}{suffix}"


# ---------------------------------------------------------------------------
# Ordenes Spot — wrapper liviano, unico call-site hoy
# ---------------------------------------------------------------------------

class SpotOrderManager:
    def __init__(self, client: BinanceSpotRestClient, symbol: str):
        self.client = client
        self.symbol = symbol
        self._step_size: Optional[float] = None
        self._load_step_size()

    def _load_step_size(self) -> None:
        try:
            info = self.client.get_exchange_info(self.symbol)
            for f in info.get("filters", []):
                if str(f.get("filterType", "")).upper() == "LOT_SIZE":
                    self._step_size = float(f.get("stepSize", 0) or 0) or None
        except Exception:
            self._step_size = self._step_size or 0.0001

    def place_market_order(self, side: str, quantity: float) -> Dict[str, Any]:
        # Para SELL, nunca pedir mas que el balance libre real. Binance
        # descuenta la comision de una compra en el propio activo base (ETH),
        # no en USDT -- si comprastes X, el balance libre queda en X*(1-fee),
        # un poco menos de lo que "deberia" haber. Sin este clamp, tanto el
        # unwind de una entrada fallida como el cierre normal de la posicion
        # (que usan la cantidad ORDENADA, guardada en DB al entrar) piden
        # vender mas de lo que realmente esta libre -> Binance rechaza con
        # -2010 "insufficient balance", y la pata queda sin cerrar. Incidente
        # real del 12/09/2026: la pata spot quedo huerfana (sin cobertura
        # perp) porque el unwind automatico fallo exactamente por esto.
        if side.upper() == "SELL":
            try:
                base_asset = self.symbol.replace("USDT", "")
                balances = self.client.get_balances()
                free = float(balances.get(base_asset, {}).get("free", 0) or 0)
                if free > 0:
                    quantity = min(quantity, free)
            except Exception:
                pass  # si falla la consulta, seguir con la qty pedida (comportamiento previo)

        quantity = OrderManager._floor_to_step(quantity, self._step_size)
        client_order_id = generate_client_order_id(f"SPOT-{side}")
        response = self.client.create_order(
            symbol=self.symbol, side=side, order_type="MARKET",
            quantity=quantity, client_order_id=client_order_id,
        )
        return {
            "order_id": str(response.get("orderId") or client_order_id),
            "client_order_id": client_order_id,
            "status": response.get("status", "PENDING"),
            "quantity": quantity,
            "raw": response,
        }

    def get_order_status(self, client_order_id: str) -> Optional[Dict[str, Any]]:
        try:
            return self.client.get_order(self.symbol, client_order_id=client_order_id)
        except Exception as exc:
            logger.warning("Error consultando orden spot %s: %s", client_order_id, exc)
            return None


def confirm_fill(get_status_fn, client_order_id: str, timeout_s: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """Poll hasta status FILLED o timeout. None si no llega a confirmarse.

    timeout_s se resuelve DENTRO del cuerpo (no como default del parametro)
    a proposito: un default evaluado en la firma queda fijo al importar el
    modulo, y tests que hacen monkeypatch de FILL_CONFIRM_TIMEOUT_S no
    tendrian ningun efecto sobre llamadas que no pasan timeout_s explicito."""
    if timeout_s is None:
        timeout_s = FILL_CONFIRM_TIMEOUT_S
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        order = get_status_fn(client_order_id)
        if order and order.get("status") == "FILLED":
            return order
        time.sleep(FILL_POLL_INTERVAL_S)
    return None


# ---------------------------------------------------------------------------
# Entrada / salida de la posicion combinada (dos patas)
# ---------------------------------------------------------------------------

def open_carry_position(
    db: Database,
    spot_om: SpotOrderManager,
    perp_om: OrderManager,
    spot_client: BinanceSpotRestClient,
    futures_client: BinanceRestClient,
    dry_run: bool,
) -> Optional[Dict[str, Any]]:
    spot_price = float(spot_client.get_avg_price(SPOT_SYMBOL)["price"])
    spot_qty = TARGET_NOTIONAL_USDT / spot_price

    perp_mark = float(futures_client.get_mark_price(PERP_SYMBOL)["markPrice"])
    perp_qty = TARGET_NOTIONAL_USDT / perp_mark

    if dry_run:
        logger.info(
            "[DRY-RUN] Abriria carry: SPOT BUY %.6f %s (~$%.2f @ %.2f) + PERP SHORT %.6f %s (~$%.2f @ %.2f)",
            spot_qty, SPOT_SYMBOL, TARGET_NOTIONAL_USDT, spot_price,
            perp_qty, PERP_SYMBOL, TARGET_NOTIONAL_USDT, perp_mark,
        )
        _ntfy.send(
            f"Entraria: SPOT BUY ~${TARGET_NOTIONAL_USDT:.2f} @ {spot_price:.2f} + "
            f"PERP SHORT ~${TARGET_NOTIONAL_USDT:.2f} @ {perp_mark:.2f} (leverage {LEVERAGE}x)",
            title="[DRY-RUN] Funding Carry — ENTRADA simulada",
            priority="3",
        )
        return {"dry_run": True}

    # --- Pata spot primero: ETHUSDT spot es liquido, fill casi garantizado ---
    # A diferencia de las otras 3 colocaciones de orden de este archivo (perp
    # de entrada, unwind, y ambas patas de salida), esta es la unica que
    # corria sin try/except: si Binance rechazaba la orden de entrada (ej.
    # balance insuficiente, el motivo mas probable con el capital minimo que
    # maneja esta estrategia), la excepcion se propagaba sin capturar y
    # crasheaba el proceso (--once) o repetia el mismo intento fallido en
    # cada ciclo del loop sin nunca avisar con una alerta clara.
    try:
        spot_order = spot_om.place_market_order("BUY", spot_qty)
    except Exception as exc:
        logger.error("Pata spot rechazada al colocar la orden de entrada: %s -- abortando, NO se toca la pata perp", exc)
        _ntfy.send(
            f"La orden de entrada en Spot fue rechazada: {exc} -- entrada abortada, sin exposicion abierta.",
            title="Funding Carry — entrada abortada", priority="4",
        )
        return None

    filled_spot = confirm_fill(spot_om.get_order_status, spot_order["client_order_id"])
    if not filled_spot:
        logger.error("Pata spot no confirmo fill -- abortando entrada, NO se toca la pata perp")
        _ntfy.send(
            "Pata spot no confirmo fill al entrar -- entrada abortada, sin exposicion abierta.",
            title="Funding Carry — entrada abortada", priority="4",
        )
        return None

    db.insert_order(SPOT_POSITION_SYMBOL, spot_order["client_order_id"], "BUY", "MARKET",
                     spot_order["quantity"], status="FILLED", source="FUNDING_CARRY_LIVE")

    # --- Pata perp, con reintentos acotados ---
    perp_filled = None
    perp_order = None
    for attempt in range(1, MAX_LEG_RETRIES + 1):
        try:
            perp_order = perp_om.place_order(
                side="SELL", position_side="SHORT", order_type="MARKET",
                quantity=perp_qty, symbol=PERP_SYMBOL, reduce_only=False,
            )
            perp_filled = confirm_fill(perp_om.get_order_status, perp_order["client_order_id"])
            if perp_filled:
                break
        except Exception as exc:
            logger.error("Intento %d/%d de pata perp fallo: %s", attempt, MAX_LEG_RETRIES, exc)
        time.sleep(RETRY_DELAY_S)

    if not perp_filled:
        logger.error("Pata perp fallo tras %d intentos -- deshaciendo pata spot", MAX_LEG_RETRIES)
        _ntfy.send(
            "URGENTE: la pata perp fallo tras varios intentos y la pata spot ya estaba llena. "
            "Intentando deshacer la pata spot automaticamente (venta a mercado).",
            title="Funding Carry — FALLO PARCIAL, revisar", priority="5",
        )
        try:
            filled_qty = float(filled_spot.get("executedQty", 0)) or spot_order["quantity"]
            unwind = spot_om.place_market_order("SELL", filled_qty)
            confirm_fill(spot_om.get_order_status, unwind["client_order_id"])
            db.insert_order(SPOT_POSITION_SYMBOL, unwind["client_order_id"], "SELL", "MARKET",
                             filled_qty, status="FILLED", source="FUNDING_CARRY_LIVE_UNWIND")
        except Exception as exc:
            logger.error("Fallo tambien deshaciendo la pata spot: %s -- REQUIERE INTERVENCION MANUAL", exc)
            _ntfy.send(
                f"No se pudo deshacer la pata spot automaticamente: {exc} -- INTERVENCION MANUAL YA.",
                title="Funding Carry — INTERVENCION MANUAL REQUERIDA", priority="5",
            )
        return None

    db.insert_order(PERP_POSITION_SYMBOL, perp_order["client_order_id"], "SELL", "MARKET",
                     perp_qty, status="FILLED", source="FUNDING_CARRY_LIVE")

    entry_time = datetime.now(timezone.utc).isoformat()
    db.update_position(SPOT_POSITION_SYMBOL, "LONG", spot_qty, spot_price, entry_time=entry_time)
    db.update_position(PERP_POSITION_SYMBOL, "SHORT", perp_qty, perp_mark,
                        margin_used=TARGET_NOTIONAL_USDT / LEVERAGE, entry_time=entry_time)

    _ntfy.send(
        f"Entrada confirmada: SPOT BUY {spot_qty:.6f} @ {spot_price:.2f} + PERP SHORT {perp_qty:.6f} @ {perp_mark:.2f}",
        title="Funding Carry — ENTRADA en vivo", priority="4",
    )
    return {"spot": spot_order, "perp": perp_order}


def close_carry_position(
    db: Database,
    spot_om: SpotOrderManager,
    perp_om: OrderManager,
    spot_client: BinanceSpotRestClient,
    futures_client: BinanceRestClient,
    dry_run: bool,
) -> Optional[Dict[str, Any]]:
    if dry_run:
        logger.info("[DRY-RUN] Cerraria la posicion de carry (pata perp primero, luego spot)")
        _ntfy.send(
            "Cerraria la posicion de carry (perp primero, luego spot).",
            title="[DRY-RUN] Funding Carry — SALIDA simulada", priority="3",
        )
        return {"dry_run": True}

    spot_pos = db.get_position(SPOT_POSITION_SYMBOL)
    perp_pos = db.get_position(PERP_POSITION_SYMBOL)
    if not spot_pos or not perp_pos:
        logger.warning("close_carry_position llamado sin posiciones registradas en DB -- nada que cerrar")
        return None

    perp_qty = float(perp_pos["quantity"])
    spot_qty = float(spot_pos["quantity"])
    entry_time = perp_pos.get("entry_time")
    exit_time = datetime.now(timezone.utc).isoformat()

    # --- Pata perp primero: es la apalancada/con riesgo de liquidacion.
    # Minimiza el tiempo que queda abierta sola -- si algo falla a mitad de
    # la salida, el peor caso es terminar con solo la pata spot (sin
    # apalancamiento, sin riesgo de liquidacion) en vez de un short desnudo. ---
    perp_filled = None
    perp_order = None
    for attempt in range(1, MAX_LEG_RETRIES + 1):
        try:
            perp_order = perp_om.place_order(
                side="BUY", position_side="SHORT", order_type="MARKET",
                quantity=perp_qty, symbol=PERP_SYMBOL, reduce_only=True,
            )
            perp_filled = confirm_fill(perp_om.get_order_status, perp_order["client_order_id"])
            if perp_filled:
                break
        except Exception as exc:
            logger.error("Intento %d/%d de cierre de pata perp fallo: %s", attempt, MAX_LEG_RETRIES, exc)
        time.sleep(RETRY_DELAY_S)

    if not perp_filled:
        _ntfy.send(
            "URGENTE: no se pudo cerrar la pata perp tras varios intentos. Posicion sigue abierta -- revisar manualmente.",
            title="Funding Carry — FALLO AL CERRAR, revisar", priority="5",
        )
        return None

    perp_exit_price = float(futures_client.get_mark_price(PERP_SYMBOL)["markPrice"])
    db.insert_order(PERP_POSITION_SYMBOL, perp_order["client_order_id"], "BUY", "MARKET",
                     perp_qty, status="FILLED", source="FUNDING_CARRY_LIVE")
    db.insert_trade(
        PERP_POSITION_SYMBOL, "SHORT", float(perp_pos["entry_price"]), perp_exit_price, perp_qty,
        entry_time or exit_time, exit_time,
        pnl_percent=(float(perp_pos["entry_price"]) - perp_exit_price) / float(perp_pos["entry_price"]) * 100,
        pnl_usdt=(float(perp_pos["entry_price"]) - perp_exit_price) * perp_qty,
        reason_exit="FUNDING_CARRY_EXIT", source="FUNDING_CARRY_LIVE",
    )
    db.clear_position(PERP_POSITION_SYMBOL)

    # --- Pata spot despues ---
    spot_filled = None
    spot_order = None
    for attempt in range(1, MAX_LEG_RETRIES + 1):
        try:
            spot_order = spot_om.place_market_order("SELL", spot_qty)
            spot_filled = confirm_fill(spot_om.get_order_status, spot_order["client_order_id"])
            if spot_filled:
                break
        except Exception as exc:
            logger.error("Intento %d/%d de cierre de pata spot fallo: %s", attempt, MAX_LEG_RETRIES, exc)
        time.sleep(RETRY_DELAY_S)

    if not spot_filled:
        _ntfy.send(
            "URGENTE: la pata perp se cerro pero la pata spot no confirmo cierre tras varios intentos. "
            "Queda ETH spot sin cobertura -- revisar manualmente.",
            title="Funding Carry — FALLO PARCIAL AL CERRAR", priority="5",
        )
        return None

    spot_exit_price = float(spot_client.get_avg_price(SPOT_SYMBOL)["price"])
    db.insert_order(SPOT_POSITION_SYMBOL, spot_order["client_order_id"], "SELL", "MARKET",
                     spot_qty, status="FILLED", source="FUNDING_CARRY_LIVE")
    db.insert_trade(
        SPOT_POSITION_SYMBOL, "LONG", float(spot_pos["entry_price"]), spot_exit_price, spot_qty,
        entry_time or exit_time, exit_time,
        pnl_percent=(spot_exit_price - float(spot_pos["entry_price"])) / float(spot_pos["entry_price"]) * 100,
        pnl_usdt=(spot_exit_price - float(spot_pos["entry_price"])) * spot_qty,
        reason_exit="FUNDING_CARRY_EXIT", source="FUNDING_CARRY_LIVE",
    )
    db.clear_position(SPOT_POSITION_SYMBOL)

    _ntfy.send("Salida confirmada: perp y spot cerrados.", title="Funding Carry — SALIDA en vivo", priority="4")
    return {"perp": perp_order, "spot": spot_order}


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------

def check_reconciliation(
    db: Database,
    spot_client: BinanceSpotRestClient,
    futures_client: BinanceRestClient,
    live_mode: bool,
) -> None:
    """
    Chequeo liviano de solo lectura de AMBAS patas: compara lo que la DB cree
    que esta abierto contra lo que el exchange realmente tiene (perp via
    posicion de Futuros, spot via balance libre de ETH). NO auto-corrige (a
    diferencia del reconciler del bot principal) -- para una posicion tan
    chica, es preferible alertar y que se revise a mano antes que aplicar
    logica de auto-correccion no probada sobre plata real. En dry-run no
    tiene sentido (nunca hay posicion real que comparar).
    """
    if not live_mode:
        return

    discrepancies = []

    perp_pos = db.get_position(PERP_POSITION_SYMBOL)
    try:
        exchange_perp = futures_client.get_position(PERP_SYMBOL)
        exchange_perp_qty = abs(float(exchange_perp.get("positionAmt", 0))) if exchange_perp else 0.0
        db_has_perp = perp_pos is not None
        if db_has_perp != (exchange_perp_qty > 0):
            discrepancies.append(
                f"PERP: DB dice {'ABIERTA' if db_has_perp else 'CERRADA'}, "
                f"exchange dice {'ABIERTA' if exchange_perp_qty > 0 else 'CERRADA'} (qty={exchange_perp_qty})"
            )
    except Exception as exc:
        logger.warning("No se pudo leer la posicion de Futuros para reconciliar: %s", exc)

    spot_pos = db.get_position(SPOT_POSITION_SYMBOL)
    try:
        base_asset = SPOT_SYMBOL.replace("USDT", "")
        balances = spot_client.get_balances()
        exchange_qty = float(balances.get(base_asset, {}).get("free", 0))
        db_spot_qty = float(spot_pos["quantity"]) if spot_pos else 0.0
        spot_price = float(spot_client.get_avg_price(SPOT_SYMBOL)["price"])
        tolerance_qty = SPOT_RECONCILE_TOLERANCE_USDT / spot_price
        if abs(exchange_qty - db_spot_qty) > tolerance_qty:
            discrepancies.append(
                f"SPOT: DB dice {db_spot_qty:.6f} {base_asset}, exchange tiene {exchange_qty:.6f} {base_asset} libre "
                f"(diferencia > ${SPOT_RECONCILE_TOLERANCE_USDT} de tolerancia)"
            )
    except Exception as exc:
        logger.warning("No se pudo leer el balance Spot para reconciliar: %s", exc)

    if discrepancies:
        msg = "DESINCRONIZACION detectada:\n" + "\n".join(discrepancies) + \
              "\nRevisar manualmente antes de que el bot tome otra decision."
        logger.error(msg)
        _ntfy.send(msg, title="Funding Carry — DESINCRONIZACION DB/exchange", priority="5")


def process_new_events(
    db: Database,
    spot_om: SpotOrderManager,
    perp_om: OrderManager,
    spot_client: BinanceSpotRestClient,
    futures_client: BinanceRestClient,
    risk_engine: RiskEngine,
    live_mode: bool,
) -> None:
    set_state(db, "last_checked_at", datetime.now(timezone.utc).isoformat())
    check_reconciliation(db, spot_client, futures_client, live_mode)

    events = fetch_recent_funding()
    if not events:
        logger.warning("Sin eventos de funding devueltos por la API")
        return

    events.sort(key=lambda e: int(e["fundingTime"]))
    rates = [float(e["fundingRate"]) for e in events]

    last_recorded_ms = int(get_state(db, "last_funding_time_ms", "0"))
    new_events = [e for e in events if int(e["fundingTime"]) > last_recorded_ms]

    if not new_events:
        logger.info("Sin eventos nuevos de funding desde el ultimo chequeo (normal, funding liquida cada 8h)")
        return

    # En vivo, in_position se deriva de si hay una posicion perp registrada
    # en DB -- fuente unica de verdad, evita que un flag separado se
    # desincronice de lo que de verdad esta abierto en el exchange.
    #
    # En dry-run no existe esa fuente de verdad (nunca se escribe una
    # posicion real), asi que se persiste el estado SIMULADO aparte
    # (dry_run_in_position) -- sin esto, cada corrida nueva del loop
    # arrancaria creyendo que esta FUERA, repitiendo "ENTRADA" en cada
    # evento nuevo indefinidamente y sin poder simular nunca una SALIDA
    # entre corridas separadas (rompe la comparacion con el paper watcher,
    # que si mantiene su estado real entre corridas).
    if live_mode:
        in_position = db.get_position(PERP_POSITION_SYMBOL) is not None
    else:
        in_position = get_state(db, "dry_run_in_position", "0") == "1"

    for ev in new_events:
        ft_ms = int(ev["fundingTime"])
        rate = float(ev["fundingRate"])
        idx = next((i for i, e in enumerate(events) if int(e["fundingTime"]) == ft_ms), None)
        window = rates[max(0, idx - MA_WINDOW + 1): idx + 1] if idx is not None else [rate]

        decision = next_state(window, rate, in_position)
        ma_str = f"{decision.ma*100:.5f}%" if decision.ma is not None else "n/a"
        logger.info(
            "Evento %s  rate=%.5f%%  ma=%s  in_position=%s->%s  %s",
            datetime.utcfromtimestamp(ft_ms / 1000).isoformat(), rate * 100, ma_str,
            in_position, decision.in_position, decision.note,
        )

        # in_position solo cambia si la transicion realmente se concreto --
        # no si el RiskEngine la bloqueo o si open/close_carry_position
        # fallo (devuelven None en cualquier fallo, incluido el aborto sin
        # tocar la otra pata). Sin este chequeo, un evento posterior dentro
        # del mismo batch decidiria sobre un estado que nunca se alcanzo.
        if decision.in_position != in_position:
            if decision.in_position and not risk_engine.can_trade():
                reason = risk_engine.get_stop_reason_if_triggered()
                logger.warning("RiskEngine bloqueo la entrada: %s", reason)
                _ntfy.send(f"Entrada bloqueada por RiskEngine: {reason}",
                           title="Funding Carry — entrada bloqueada", priority="4")
            elif decision.in_position:
                result = open_carry_position(db, spot_om, perp_om, spot_client, futures_client, dry_run=not live_mode)
                if result is not None:
                    in_position = True
            else:
                result = close_carry_position(db, spot_om, perp_om, spot_client, futures_client, dry_run=not live_mode)
                if result is not None:
                    in_position = False

        last_recorded_ms = ft_ms

    if not live_mode:
        set_state(db, "dry_run_in_position", "1" if in_position else "0")
    set_state(db, "last_funding_time_ms", last_recorded_ms)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ejecutor en vivo de Funding Carry (dry-run por defecto)")
    parser.add_argument("--once", action="store_true", help="Un solo chequeo y salir (debug)")
    parser.add_argument("--live", action="store_true",
                         help="Habilita ordenes reales (requiere ademas FUNDING_CARRY_LIVE_CONFIRM=yes)")
    args = parser.parse_args()

    live_mode = live_mode_enabled(args)
    banner = ("*** MODO EN VIVO — SE PUEDEN COLOCAR ORDENES REALES ***" if live_mode
              else "modo DRY-RUN (solo simula, no coloca ninguna orden)")
    logger.info("Iniciando funding_carry_live.py -- %s", banner)
    logger.info("Regla: entry=%.3f%% exit=%.3f%% MA(%d) | notional objetivo=$%.2f | leverage perp=%dx",
                ENTRY_THRESHOLD * 100, EXIT_THRESHOLD * 100, MA_WINDOW, TARGET_NOTIONAL_USDT, LEVERAGE)
    if not live_mode:
        logger.info("Para habilitar ordenes reales hacen falta AMBAS cosas: --live Y "
                     "FUNDING_CARRY_LIVE_CONFIRM=yes en el entorno.")

    db = init_db()

    spot_api_key = os.environ.get("BINANCE_API_KEY_SPOT", "")
    spot_api_secret = os.environ.get("BINANCE_API_SECRET_SPOT", "")
    futures_api_key = os.environ.get("BINANCE_API_KEY_LIVE", "")
    futures_api_secret = os.environ.get("BINANCE_API_SECRET_LIVE", "")

    # Los clientes SIEMPRE se instancian en modo "live" (necesitan sesion real
    # para leer precios publicos, incluso en dry-run, para loguear numeros
    # realistas). El unico gate real contra ordenes reales es el parametro
    # dry_run de open_carry_position/close_carry_position, derivado de
    # live_mode (doble candado --live + FUNDING_CARRY_LIVE_CONFIRM).
    spot_client = BinanceSpotRestClient(spot_api_key, spot_api_secret, environment="live")
    futures_client = BinanceRestClient(futures_api_key, futures_api_secret, environment="live")

    spot_om = SpotOrderManager(spot_client, SPOT_SYMBOL)
    perp_om = OrderManager(futures_client, PERP_SYMBOL)

    risk_engine = RiskEngine(
        initial_equity=CAPITAL_NEEDED_USDT,
        max_drawdown_percent=50.0,
        max_daily_loss_percent=20.0,
        max_weekly_loss_percent=30.0,
        max_trades_per_day=6,
        min_notional_usdt=5.0,
    )

    if args.once:
        process_new_events(db, spot_om, perp_om, spot_client, futures_client, risk_engine, live_mode)
        return

    while True:
        try:
            process_new_events(db, spot_om, perp_om, spot_client, futures_client, risk_engine, live_mode)
        except Exception as exc:
            logger.error("Error en ciclo de chequeo: %s", exc)
        time.sleep(CHECK_EVERY_SECONDS)


if __name__ == "__main__":
    main()
