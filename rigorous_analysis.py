"""
Analisis riguroso de viabilidad Etapa 3 (Funding Carry) -- compara el mes de
paper trading real contra los criterios explicitos ya definidos ANTES de
arrancar (ver docstring de funding_carry_paper_watch.py y daily_report.py),
no un umbral nuevo inventado ahora.

Referencia del backtest original (backtests/funding_carry_results*.json,
ventanas de 6 meses, walk-forward anidado):
  ETHUSDT: Sharpe medio 18.80, 7/8 ventanas OOS positivas, peor ventana -0.85%
  BTCUSDT: Sharpe medio 19.54, 8/8 ventanas OOS positivas, peor ventana +1.32% (nunca negativo)

Solo lectura. No coloca ninguna orden, no modifica nada.
"""
import glob
import math
import re
import sqlite3
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

DATA_DIR = "data"
LOGS_DIR = "logs"

SYMBOLS = {
    "ETHUSDT": {"paper_db": f"{DATA_DIR}/funding_carry_paper.db", "log_glob": f"{LOGS_DIR}/funding_carry_live.log*",
                "bt_sharpe": 18.80, "bt_worst_window_pct": -0.85},
    "BTCUSDT": {"paper_db": f"{DATA_DIR}/funding_carry_paper_btcusdt.db", "log_glob": f"{LOGS_DIR}/funding_carry_live_btcusdt.log*",
                "bt_sharpe": 19.54, "bt_worst_window_pct": 1.32},
}

EVENTS_PER_YEAR = 3 * 365  # funding liquida cada 8h


def load_events(db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    df = pd.read_sql_query(
        "SELECT funding_time, funding_rate, funding_ma, in_position, pnl_pct, cum_pnl_pct, note "
        "FROM funding_events ORDER BY funding_time ASC", conn,
    )
    conn.close()
    df["funding_time"] = pd.to_datetime(df["funding_time"], format="ISO8601")
    return df


def max_drawdown_pct(cum_pnl_pct: pd.Series) -> float:
    """Drawdown maximo sobre la curva de PnL acumulado (en puntos porcentuales,
    no en % relativo -- la serie ya esta en pnl acumulado absoluto)."""
    running_max = cum_pnl_pct.cummax()
    drawdown = cum_pnl_pct - running_max
    return float(drawdown.min())


def analyze_symbol(symbol: str, cfg: dict) -> None:
    print(f"\n{'='*70}\n  {symbol}\n{'='*70}")
    df = load_events(cfg["paper_db"])
    if df.empty:
        print("  Sin eventos registrados.")
        return

    n = len(df)
    start = df["funding_time"].iloc[0]
    end = df["funding_time"].iloc[-1]
    days = (end - start).total_seconds() / 86400
    cum_final = df["cum_pnl_pct"].iloc[-1] * 100

    per_event_returns = df["pnl_pct"].to_numpy()
    mean_r = per_event_returns.mean()
    std_r = per_event_returns.std(ddof=1) if n > 1 else 0.0
    sharpe_annualized = (mean_r / std_r) * math.sqrt(EVENTS_PER_YEAR) if std_r > 0 else float("nan")

    win_rate = float((per_event_returns > 0).sum()) / n * 100
    n_entries = int((df["note"].str.contains("ENTRADA", na=False)).sum())
    n_exits = int((df["note"].str.contains("SALIDA", na=False)).sum())

    dd = max_drawdown_pct(df["cum_pnl_pct"] * 100)

    print(f"  Periodo:            {start.date()} -> {end.date()}  ({days:.1f} dias, {n} eventos)")
    print(f"  PnL acumulado:      {cum_final:+.4f}%")
    print(f"  Umbral de corte:    -1.00%  ->  {'ROTO' if cum_final <= -1.0 else 'no roto'} "
          f"({cum_final / -1.0 * 100:.0f}% del umbral)" if cum_final < 0 else
          f"  Umbral de corte:    -1.00%  ->  no aplica (PnL positivo)")
    print(f"  Win rate por evento:{win_rate:.1f}%  ({int((per_event_returns>0).sum())}/{n})")
    print(f"  Entradas/Salidas:   {n_entries} / {n_exits}  (menos vueltas = menos costo de transaccion pagado)")
    print(f"  Max drawdown real:  {dd:.4f} puntos porcentuales")
    print(f"  Sharpe anualizado (real, {n} eventos): {sharpe_annualized:.2f}")
    print(f"  Sharpe backtest (ref, 6 meses):        {cfg['bt_sharpe']:.2f}")
    print(f"  Peor ventana backtest (ref, 6 meses):   {cfg['bt_worst_window_pct']:+.2f}%")

    # Consistencia: compara las decisiones del ejecutor en vivo (dry-run,
    # via su log) contra las del paper watcher, evento por evento.
    log_files = sorted(glob.glob(cfg["log_glob"]))
    live_events = {}
    ev_re = re.compile(
        r"Evento (\S+)\s+rate=([\-\d\.]+)%\s+ma=\S+\s+in_position=(\w+)->(\w+)\s*(\S*)"
    )
    for lf in log_files:
        try:
            with open(lf, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    m = ev_re.search(line)
                    if m:
                        ts_str = m.group(1)
                        in_pos_after = m.group(4) == "True"
                        note = m.group(5)
                        live_events[ts_str] = (in_pos_after, note)
        except FileNotFoundError:
            continue

    if not live_events:
        print("  Consistencia live vs paper: SIN DATOS (no se encontraron logs del ejecutor en vivo para este simbolo)")
    else:
        matched = 0
        mismatched = 0
        for _, row in df.iterrows():
            ts_key = row["funding_time"].isoformat()
            # el timestamp del log puede diferir en microsegundos/formato; buscar por prefijo
            candidates = [k for k in live_events if k.startswith(ts_key[:19])]
            if not candidates:
                continue
            live_in_pos, _ = live_events[candidates[0]]
            paper_in_pos = bool(row["in_position"])
            if live_in_pos == paper_in_pos:
                matched += 1
            else:
                mismatched += 1
        total_checked = matched + mismatched
        if total_checked == 0:
            print("  Consistencia live vs paper: SIN EVENTOS EN COMUN (logs no cubren el mismo rango de tiempo)")
        else:
            print(f"  Consistencia live vs paper: {matched}/{total_checked} eventos coinciden "
                  f"({matched/total_checked*100:.1f}%) -- {mismatched} discrepancias"
                  + (" -- OJO, REVISAR" if mismatched > 0 else ""))
        print(f"  (nota: {len(log_files)} archivo(s) de log disponibles, puede no cubrir el mes completo por rotacion)")


print("ANALISIS RIGUROSO -- viabilidad Etapa 3 (Funding Carry)")
print(f"Generado: {datetime.utcnow().isoformat()}Z")

for symbol, cfg in SYMBOLS.items():
    analyze_symbol(symbol, cfg)

print(f"\n{'='*70}\n  CRITERIOS EXPLICITOS DEFINIDOS ANTES DE ARRANCAR (no inventados ahora)\n{'='*70}")
print("""
  PARAR (ninguno deberia estar roto):
    - PnL acumulado <= -1% en menos de 1 mes
    - Tendencia negativa sostenida (no un evento puntual)
    - Heartbeat/reconciliacion con errores por dias sin notarse

  PROCEDER (los 3 deberian cumplirse):
    - 4-6 semanas de datos limpios y continuos
    - PnL dentro del rango que predice el backtest (no rompe el umbral de arriba)
    - El ejecutor en vivo (dry-run) decide igual que el paper watcher, sin bugs
""")
