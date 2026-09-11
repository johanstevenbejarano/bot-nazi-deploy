"""
ntfy_alert.py — Alertas push para Bot-Nazi (via ntfy.sh).

Envia notificaciones push al movil cuando el bot:
  - Abre o cierra una posicion
  - Detecta y cierra una posicion zombie
  - Activa un cierre de emergencia o margin call
  - Entra en estado de error critico
  - Arranca o se reinicia

API: https://ntfy.sh (gratuita, sin cuenta requerida)
Setup (una sola vez):
  1. Instala la app "ntfy" en tu movil:
       Android: https://play.google.com/store/apps/details?id=io.heckel.ntfy
       iOS:     https://apps.apple.com/app/ntfy/id1625396347
  2. Elige un topic secreto (ej: bot-nazi-alertas-x7k2m9)
  3. En la app: Añadir suscripcion -> bot-nazi-alertas-x7k2m9
  4. En .env:
       NTFY_TOPIC=bot-nazi-alertas-x7k2m9
       ALERT_ENABLED=true

Implementado con stdlib pura (urllib) — sin pip install.
Todas las llamadas son no-bloqueantes (thread separado).
Nunca crashea el bot: toda excepcion queda silenciada.
"""

from __future__ import annotations

import logging
import os
import queue
import threading
import time
import urllib.error
import urllib.request
from typing import Optional

logger = logging.getLogger(__name__)

# Eventos de trade que disparan alerta
TRADE_ALERT_EVENTS = {
    "ENTRY_SIGNAL",
    "EXIT_SIGNAL",
}

# Eventos de riesgo que disparan alerta
RISK_ALERT_EVENTS = {
    "ZOMBIE_POSITION_CLOSED",
    "EMERGENCY_CLOSE_SENT",
    "MARGIN_CALL",
    "RECONCILE_AUTO_CORRECTED",
    "ERROR_SAFE_RECOVERED",
    "STRATEGY_CONFIG_DRIFT",
}

# Eventos de error que disparan alerta
ERROR_ALERT_EVENTS = {
    "RATE_LIMIT_BACKOFF",
    "ERROR_SAFE_TRANSITION",
}

_NTFY_BASE = "https://ntfy.sh"

# Prioridades ntfy (1=min, 3=default, 4=high, 5=urgent)
_PRIO_DEFAULT = "3"
_PRIO_HIGH    = "4"
_PRIO_URGENT  = "5"


class NtfyAlerter:
    """
    Envia notificaciones push via ntfy.sh de forma asincrona y robusta.

    Uso:
        alerter = NtfyAlerter(topic="bot-nazi-alertas-x7k2m9")
        alerter.send("Hola desde el bot", title="Bot-Nazi", priority="4")
        alerter.on_trade_event("ENTRY_SIGNAL", symbol="ETHUSDT", side="LONG", ...)
    """

    TIMEOUT       = 10    # segundos por request
    RETRY_DELAY   = 4     # segundos entre reintentos
    MAX_RETRIES   = 3
    RATE_LIMIT_DELAY = 1.0  # min segundos entre mensajes

    def __init__(self, topic: str, enabled: bool = True):
        self.topic = topic.strip()
        self.enabled = enabled and bool(topic)
        self._url = f"{_NTFY_BASE}/{self.topic}"

        # Cola de tuplas (message, title, priority)
        self._queue: queue.Queue = queue.Queue(maxsize=50)
        self._last_sent = 0.0
        self._worker: Optional[threading.Thread] = None

        if self.enabled:
            self._start_worker()
            logger.info("NtfyAlerter initialized — topic: %s", self.topic)
        else:
            logger.info("NtfyAlerter disabled (NTFY_TOPIC missing or ALERT_ENABLED=false)")

    # ── API publica ──────────────────────────────────────────────────────────

    def send(self, message: str, title: str = "Bot-Nazi", priority: str = _PRIO_DEFAULT) -> None:
        """Encola un mensaje push. No bloquea."""
        if not self.enabled:
            return
        try:
            self._queue.put_nowait((message, title, priority))
        except queue.Full:
            logger.debug("Ntfy queue full — message dropped")

    def wait_until_sent(self, timeout: float = 15.0) -> bool:
        """
        Bloquea hasta que la cola quede vacia (o hasta `timeout`). El hilo
        que manda las notificaciones es `daemon=True` -- en un proceso de
        vida larga (loop infinito) nunca importa, pero en un script de una
        sola pasada el proceso puede terminar y matar el hilo ANTES de que
        alcance a mandar el HTTP real, dejando el mensaje "encolado" para
        siempre. Los scripts que se ejecutan una vez y salen (ej. desde
        cron) deben llamar a esto antes de terminar. Devuelve False si se
        agoto el timeout sin vaciarse (igual es seguro salir, el mensaje
        se pierde en vez de colgar el proceso).
        """
        if not self.enabled:
            return True
        deadline = time.monotonic() + timeout
        while not self._queue.empty():
            if time.monotonic() >= deadline:
                logger.warning("wait_until_sent: timeout con mensajes aun en cola")
                return False
            time.sleep(0.2)
        return True

    def on_trade_event(self, event_type: str, **kwargs) -> None:
        """Hook para _emit_trade_event en BotEngine."""
        if event_type not in TRADE_ALERT_EVENTS:
            return
        result = self._format_trade(event_type, kwargs)
        if result:
            msg, title, prio = result
            self.send(msg, title=title, priority=prio)

    def on_risk_event(self, event_type: str, **kwargs) -> None:
        """Hook para _emit_risk_event en BotEngine."""
        if event_type not in RISK_ALERT_EVENTS:
            return
        result = self._format_risk(event_type, kwargs)
        if result:
            msg, title, prio = result
            self.send(msg, title=title, priority=prio)

    def on_error_event(self, event_type: str, message: str = "", **kwargs) -> None:
        """Hook para _emit_error_event en BotEngine."""
        if event_type not in ERROR_ALERT_EVENTS:
            return
        result = self._format_error(event_type, message, kwargs)
        if result:
            msg, title, prio = result
            self.send(msg, title=title, priority=prio)

    def send_startup(self, environment: str, symbol: str, balance: float = 0.0) -> None:
        """Mensaje de inicio del bot."""
        env_label = {"live": "LIVE", "testnet": "TESTNET"}.get(environment, "PAPER")
        lines = [f"Entorno: {env_label}", f"Par: {symbol}"]
        if balance > 0:
            lines.append(f"Balance: ${balance:,.2f} USDT")
        self.send("\n".join(lines), title="Bot-Nazi iniciado", priority=_PRIO_DEFAULT)

    def stop(self) -> None:
        """Detiene el worker. Llama en shutdown del bot."""
        if self._worker and self._worker.is_alive():
            try:
                self._queue.put_nowait(None)
            except queue.Full:
                pass
            self._worker.join(timeout=5)

    # ── Formatters — retornan (message, title, priority) o None ─────────────

    def _format_trade(self, event_type: str, data: dict):
        symbol  = data.get("symbol", "ETHUSDT")
        side    = str(data.get("side", "")).upper()
        qty     = data.get("quantity", 0)
        price   = data.get("price", 0)
        reason  = data.get("reason", "")
        sl      = data.get("stop_loss", None)

        if event_type == "ENTRY_SIGNAL":
            side_label = "LONG" if side == "LONG" else "SHORT"
            notional = float(qty or 0) * float(price or 0)
            lines = [
                f"Par: {symbol}",
                f"Cantidad: {float(qty):.4f} ETH",
                f"Precio: ${float(price):,.2f}",
                f"Notional: ${notional:,.2f} USDT",
            ]
            if sl:
                lines.append(f"Stop Loss: ${float(sl):,.2f}")
            if reason:
                lines.append(f"Senal: {reason}")
            title = f"ENTRADA {side_label}"
            return "\n".join(lines), title, _PRIO_HIGH

        if event_type == "EXIT_SIGNAL":
            pnl      = data.get("pnl_usdt", data.get("pnl", None))
            pnl_pct  = data.get("pnl_percent", None)
            exit_reason = data.get("exit_reason", reason or "")
            lines = [f"Par: {symbol}"]
            if pnl is not None:
                sign = "+" if float(pnl) >= 0 else ""
                pnl_line = f"PnL: {sign}{float(pnl):.2f} USDT"
                if pnl_pct is not None:
                    pnl_line += f" ({sign}{float(pnl_pct):.2f}%)"
                lines.append(pnl_line)
            if exit_reason:
                lines.append(f"Razon: {exit_reason}")
            result_label = "GANANCIA" if (pnl or 0) >= 0 else "PERDIDA"
            title = f"SALIDA [{result_label}]"
            return "\n".join(lines), title, _PRIO_HIGH

        return None

    def _format_risk(self, event_type: str, data: dict):
        symbol = data.get("symbol", "ETHUSDT")

        if event_type == "ZOMBIE_POSITION_CLOSED":
            side = data.get("position_side", "")
            qty  = data.get("quantity", 0)
            msg = (
                f"Par: {symbol}\n"
                f"Posicion: {side} {float(qty):.4f} ETH\n"
                f"Cerrada automaticamente."
            )
            return msg, "Zombie Position Cerrada", _PRIO_HIGH

        if event_type == "EMERGENCY_CLOSE_SENT":
            side = data.get("position_side", "")
            qty  = data.get("quantity", 0)
            msg = f"Par: {symbol}\nPosicion: {side} {float(qty):.4f} ETH"
            return msg, "CIERRE DE EMERGENCIA", _PRIO_URGENT

        if event_type == "MARGIN_CALL":
            return f"Par: {symbol}\nAccion inmediata requerida.", "MARGIN CALL", _PRIO_URGENT

        if event_type == "RECONCILE_AUTO_CORRECTED":
            reason = data.get("reason", "")
            if "ZOMBIE" in reason.upper():
                msg = f"Par: {symbol}\nAccion: {reason}"
                return msg, "Auto-Correccion Aplicada", _PRIO_HIGH
            return None

        if event_type == "ERROR_SAFE_RECOVERED":
            return "Volvio a IDLE correctamente.", "Bot Recuperado", _PRIO_DEFAULT

        if event_type == "STRATEGY_CONFIG_DRIFT":
            detail = data.get("detail", "")
            msg = f"Par: {symbol}\n{detail}\nVerifica config.yaml si no fue intencional."
            return msg, "Config de Estrategia Distinta a la Validada", _PRIO_HIGH

        return None

    def _format_error(self, event_type: str, message: str, data: dict):
        if event_type == "RATE_LIMIT_BACKOFF":
            return "Pausando 60s. Continua automaticamente.", "Rate Limit Binance", _PRIO_DEFAULT

        if event_type == "ERROR_SAFE_TRANSITION":
            return f"Razon: {message}\nRevision manual recomendada.", "BOT EN ERROR", _PRIO_URGENT

        return None

    # ── Worker interno ───────────────────────────────────────────────────────

    def _start_worker(self) -> None:
        self._worker = threading.Thread(
            target=self._run,
            name="ntfy-alert-worker",
            daemon=True,
        )
        self._worker.start()

    def _run(self) -> None:
        while True:
            try:
                item = self._queue.get(timeout=30)
            except queue.Empty:
                continue

            if item is None:  # sentinel — shutdown
                break

            message, title, priority = item

            # Rate limiting
            elapsed = time.monotonic() - self._last_sent
            if elapsed < self.RATE_LIMIT_DELAY:
                time.sleep(self.RATE_LIMIT_DELAY - elapsed)

            self._send_with_retry(message, title, priority)
            self._last_sent = time.monotonic()

    def _send_with_retry(self, message: str, title: str, priority: str) -> None:
        data = message.encode("utf-8")
        headers = {
            # urllib encodes headers as latin-1; encode title as UTF-8 first so
            # any emoji or non-ASCII chars survive the round-trip to ntfy.sh.
            "Title":    title.encode("utf-8").decode("latin-1"),
            "Priority": priority,
            "Tags":     "chart_with_upwards_trend",
            "Content-Type": "text/plain; charset=utf-8",
        }

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                req = urllib.request.Request(
                    self._url,
                    data=data,
                    headers=headers,
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=self.TIMEOUT) as resp:
                    if resp.status == 200:
                        logger.debug("Ntfy alert sent OK: %s", title)
                        return
                    logger.warning("Ntfy returned %s", resp.status)
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="ignore")
                logger.warning("Ntfy HTTP error %s: %s", e.code, body[:200])
                if e.code == 429:
                    time.sleep(10)
            except Exception as exc:
                logger.debug("Ntfy send attempt %d failed: %s", attempt, exc)

            if attempt < self.MAX_RETRIES:
                time.sleep(self.RETRY_DELAY)


def build_from_env() -> NtfyAlerter:
    """
    Crea un NtfyAlerter leyendo del entorno.
    Compatible con python-dotenv (.env ya cargado por ConfigManager).

    Variables en .env:
        NTFY_TOPIC=bot-nazi-alertas-x7k2m9   (topic secreto)
        ALERT_ENABLED=true
    """
    topic   = os.environ.get("NTFY_TOPIC", "")
    enabled = os.environ.get("ALERT_ENABLED", "false").lower() == "true"
    return NtfyAlerter(topic=topic, enabled=enabled)
