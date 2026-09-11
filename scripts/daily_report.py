"""
Reporte diario de salud + progreso de Funding Carry, y chequeo de seguridad
de que el bot direccional sigue pausado. Corre una vez al dia via cron,
manda UN mensaje consolidado por ntfy.

Por que esto y no mas alertas por evento: ya existen alertas cada vez que
liquida un funding (~cada 8h, ver funding_carry_paper_watch.py) -- este
reporte NO las reemplaza, junta en un solo mensaje legible lo que conviene
mirar con la cabeza fria una vez al dia:

  1. Salud operativa: heartbeat de cada servicio, y cuantos ERROR/WARNING
     aparecieron en los logs de las ultimas 24h (algo puede estar "vivo"
     segun el heartbeat pero fallando en silencio -- esto lo detecta).
  2. Que el bot direccional siga pausado de verdad (si algo lo reactivo por
     accidente -- ya paso dos veces con `docker compose up` sin --no-deps --
     esto lo nota al dia siguiente, no en semanas).
  3. PnL acumulado como CONTEXTO, no como alarma diaria. El criterio real de
     corte es -1% acumulado (definido en la conversacion del 10/09/2026,
     ver docstring de funding_carry_paper_watch.py) -- el dia a dia no
     importa por diseño, por eso el reporte no dice "subio/bajo hoy", dice
     "a tanto% del umbral de corte".

Uso:
  python scripts/daily_report.py           # corre y manda por ntfy
  python scripts/daily_report.py --print    # solo imprime, no manda (debug)
"""
from __future__ import annotations

import argparse
import os
import re
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR))


def _load_env_file(path: Path) -> None:
    """Parser minimo de .env (KEY=VALUE, ignora comentarios/lineas vacias),
    sin depender de python-dotenv -- este script corre via cron directo en
    el host (no dentro de un contenedor Docker), y el host no necesariamente
    tiene python-dotenv instalado (solo las imagenes Docker lo instalan via
    requirements.txt). No pisa variables que ya esten en el entorno."""
    if not path.exists():
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            if key and key not in os.environ:
                os.environ[key] = value.strip()


_load_env_file(BASE_DIR / ".env")

from src.observability.ntfy_alert import build_from_env  # noqa: E402

DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"

SYMBOLS = ["ETHUSDT", "BTCUSDT"]
STOP_THRESHOLD_PCT = -1.0  # ver criterio de corte, docstring de funding_carry_paper_watch.py
HEARTBEAT_MAX_AGE_S = 1200  # igual que los healthcheck scripts
LOG_LOOKBACK_HOURS = 24
BOT_RECENT_ACTIVITY_HOURS = 24


def _suffix(symbol: str) -> str:
    return "" if symbol == "ETHUSDT" else f"_{symbol.lower()}"


def _read_sqlite(db_path: Path, query: str, params: tuple = ()) -> List[tuple]:
    if not db_path.exists():
        return []
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5)
        rows = conn.execute(query, params).fetchall()
        conn.close()
        return rows
    except Exception:
        return []


def _heartbeat_age_s(iso_ts: Optional[str]) -> Optional[float]:
    if not iso_ts:
        return None
    try:
        clean = str(iso_ts).replace("Z", "").replace("+00:00", "")
        last = datetime.fromisoformat(clean)
        return (datetime.utcnow() - last).total_seconds()
    except Exception:
        return None


def check_paper(symbol: str) -> Dict[str, Any]:
    db_path = DATA_DIR / f"funding_carry_paper{_suffix(symbol)}.db"
    state = dict(_read_sqlite(db_path, "SELECT key, value FROM state") or [])
    n_events = _read_sqlite(db_path, "SELECT COUNT(*) FROM funding_events")
    return {
        "symbol": symbol,
        "cum_pnl_pct": float(state.get("cum_pnl_pct", 0) or 0) * 100,
        "in_position": state.get("in_position") == "1",
        "heartbeat_age_s": _heartbeat_age_s(state.get("last_checked_at")),
        "n_events": n_events[0][0] if n_events else 0,
    }


def check_live(symbol: str) -> Dict[str, Any]:
    db_path = DATA_DIR / f"funding_carry_live{_suffix(symbol)}.db"
    state = dict(_read_sqlite(db_path, "SELECT key, value FROM carry_state") or [])
    n_positions = _read_sqlite(db_path, "SELECT COUNT(*) FROM positions")
    n_orders = _read_sqlite(db_path, "SELECT COUNT(*) FROM orders")
    return {
        "symbol": symbol,
        "heartbeat_age_s": _heartbeat_age_s(state.get("last_checked_at")),
        "has_open_position": bool(n_positions and n_positions[0][0] > 0),
        "ever_traded_real": bool(n_orders and n_orders[0][0] > 0),
    }


def count_log_issues(log_path: Path, hours: int = LOG_LOOKBACK_HOURS) -> Dict[str, int]:
    """Cuenta lineas ERROR/WARNING en las ultimas `hours` horas de un log.
    Los logs de este proyecto empiezan con 'YYYY-MM-DD HH:MM:SS' -- se filtra
    por esa fecha en vez de leer el archivo entero (pueden ser grandes)."""
    counts = {"ERROR": 0, "WARNING": 0}
    if not log_path.exists():
        return counts
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    ts_re = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
    try:
        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                m = ts_re.match(line)
                if not m:
                    continue
                try:
                    ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue
                if ts < cutoff:
                    continue
                if " ERROR " in line or "ERROR" in line.split("]", 1)[-1][:10]:
                    counts["ERROR"] += 1
                elif " WARNING " in line or "WARNING" in line.split("]", 1)[-1][:10]:
                    counts["WARNING"] += 1
    except Exception:
        pass
    return counts


def check_bot_paused() -> Dict[str, Any]:
    """Verifica que el bot direccional no haya operado en las ultimas 24h --
    deberia estar pausado. No usa docker.sock (evita ese permiso); en cambio
    chequea actividad real en bot_trading.db, que es lo que de verdad
    importa (¿abrio o cerro algo sin que nadie lo decidiera?)."""
    db_path = DATA_DIR / "bot_trading.db"
    cutoff = (datetime.utcnow() - timedelta(hours=BOT_RECENT_ACTIVITY_HOURS)).isoformat()
    recent_trades = _read_sqlite(
        db_path, "SELECT COUNT(*) FROM trades WHERE entry_time > ? OR exit_time > ?",
        (cutoff, cutoff),
    )
    recent_orders = _read_sqlite(
        db_path, "SELECT COUNT(*) FROM orders WHERE created_at > ?", (cutoff,),
    )
    n_trades = recent_trades[0][0] if recent_trades else 0
    n_orders = recent_orders[0][0] if recent_orders else 0
    return {"recent_trades": n_trades, "recent_orders": n_orders, "clean": (n_trades == 0 and n_orders == 0)}


def build_report() -> tuple[str, str, str]:
    """Devuelve (mensaje, titulo, prioridad)."""
    lines: List[str] = []
    urgent = False

    bot_check = check_bot_paused()
    if not bot_check["clean"]:
        urgent = True
        lines.append(
            f"** BOT DIRECCIONAL: actividad detectada en las ultimas {BOT_RECENT_ACTIVITY_HOURS}h "
            f"({bot_check['recent_trades']} trades, {bot_check['recent_orders']} ordenes) "
            f"-- deberia estar pausado, revisar YA **"
        )
    else:
        lines.append(f"Bot direccional: pausado (sin actividad en {BOT_RECENT_ACTIVITY_HOURS}h) OK")

    lines.append("")
    lines.append("Funding Carry:")
    for symbol in SYMBOLS:
        paper = check_paper(symbol)
        live = check_live(symbol)

        hb_paper = paper["heartbeat_age_s"]
        hb_live = live["heartbeat_age_s"]
        paper_ok = hb_paper is not None and hb_paper <= HEARTBEAT_MAX_AGE_S
        live_ok = hb_live is not None and hb_live <= HEARTBEAT_MAX_AGE_S

        if not paper_ok or not live_ok:
            urgent = True

        stop_pct_of_threshold = (
            (paper["cum_pnl_pct"] / STOP_THRESHOLD_PCT * 100) if paper["cum_pnl_pct"] < 0 else 0
        )
        breached = paper["cum_pnl_pct"] <= STOP_THRESHOLD_PCT
        if breached:
            urgent = True

        lines.append(f"")
        lines.append(f"  {symbol}:")
        lines.append(
            f"    Paper: PnL acumulado {paper['cum_pnl_pct']:+.4f}% "
            f"({'CORTE ALCANZADO -- revisar' if breached else f'{stop_pct_of_threshold:.0f}% del umbral de corte (-1%)'}), "
            f"posicion {'DENTRO' if paper['in_position'] else 'FUERA'}, "
            f"{paper['n_events']} eventos, heartbeat {'OK' if paper_ok else 'VIEJO -- revisar'}"
        )
        lines.append(
            f"    Live (dry-run): {'con posicion real abierta' if live['has_open_position'] else 'sin posicion'}, "
            f"{'YA OPERO REAL' if live['ever_traded_real'] else 'nunca ordeno real'}, "
            f"heartbeat {'OK' if live_ok else 'VIEJO -- revisar'}"
        )

    lines.append("")
    lines.append("Logs (ultimas 24h):")
    log_files = {
        f"funding-carry-watch ({s})": LOGS_DIR / f"funding_carry_paper{_suffix(s)}.log" for s in SYMBOLS
    }
    log_files.update({
        f"funding-carry-live ({s})": LOGS_DIR / f"funding_carry_live{_suffix(s)}.log" for s in SYMBOLS
    })
    any_errors = False
    for label, path in log_files.items():
        issues = count_log_issues(path)
        if issues["ERROR"] > 0:
            any_errors = True
            urgent = True
        if issues["ERROR"] or issues["WARNING"]:
            lines.append(f"  {label}: {issues['ERROR']} errores, {issues['WARNING']} warnings")
    if not any_errors:
        lines.append("  Sin errores nuevos.")

    title = "Reporte diario BOT-NAZI" + (" -- REVISAR" if urgent else " -- todo OK")
    priority = "5" if urgent else "3"
    return "\n".join(lines), title, priority


def main() -> None:
    parser = argparse.ArgumentParser(description="Reporte diario de salud de Funding Carry")
    parser.add_argument("--print", action="store_true", dest="print_only",
                         help="Solo imprimir, no mandar por ntfy (debug)")
    args = parser.parse_args()

    message, title, priority = build_report()

    if args.print_only:
        print(f"=== {title} (priority={priority}) ===")
        print(message)
        return

    ntfy = build_from_env()
    ntfy.send(message, title=title, priority=priority)
    # Script de una sola pasada (no loop infinito) -- sin esto, el proceso
    # termina y mata el hilo daemon de ntfy antes de que llegue a mandar el
    # HTTP real. Ver NtfyAlerter.wait_until_sent.
    ntfy.wait_until_sent()
    print(f"Reporte enviado: {title}")
    print(message)


if __name__ == "__main__":
    main()
