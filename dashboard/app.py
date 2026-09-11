"""
BOT-NAZI Dashboard - Streamlit App.
Ejecutar con: streamlit run dashboard/app.py
"""

import hmac
import sys
import time
import traceback
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
try:
    from streamlit_autorefresh import st_autorefresh
    _HAS_AUTOREFRESH = True
except ImportError:
    _HAS_AUTOREFRESH = False

try:
    import requests as _requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.storage.db import Database
from src.features.indicators import TechnicalIndicators

st.set_page_config(
    page_title="BOT-NAZI Monitor",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── CSS compacto ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
[data-testid="metric-container"] { background:#1e1e2e; border-radius:8px; padding:10px; }
.stAlert { border-radius:8px; }
div[data-testid="stHorizontalBlock"] { gap:8px; }
</style>
""", unsafe_allow_html=True)

REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_DB_PATH = REPO_ROOT / "data" / "bot_trading.db"
_TESTNET_REST = "https://testnet.binancefuture.com"
_REFRESH_INTERVAL_MS = 10_000  # 10 s — auto-refresh global
_ENV_DB_PATH = os.getenv("DB_PATH")
if _ENV_DB_PATH:
    _p = Path(_ENV_DB_PATH)
    DB_PATH = (REPO_ROOT / _p).resolve() if not _p.is_absolute() else _p
else:
    DB_PATH = _DEFAULT_DB_PATH
# Simbolos con Funding Carry corriendo -- ver scripts/funding_carry_paper_watch.py
# y funding_carry_live.py (parametrizados por FUNDING_CARRY_SYMBOL). BTCUSDT
# sumado 09/09/2026 tras confirmar que el edge no es especifico de ETH.
FUNDING_CARRY_SYMBOLS = ["ETHUSDT", "BTCUSDT"]
FUNDING_CARRY_HEARTBEAT_MAX_AGE_S = 1200  # igual que los healthcheck scripts (900 + 300 margen)


def _funding_carry_symbol_suffix(symbol: str) -> str:
    return "" if symbol == "ETHUSDT" else f"_{symbol.lower()}"


def _funding_carry_paper_db_path(symbol: str) -> Path:
    return REPO_ROOT / "data" / f"funding_carry_paper{_funding_carry_symbol_suffix(symbol)}.db"


def _funding_carry_live_db_path(symbol: str) -> Path:
    return REPO_ROOT / "data" / f"funding_carry_live{_funding_carry_symbol_suffix(symbol)}.db"
CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"
ENV_PATH = Path(__file__).parent.parent / ".env"
STACK_INFO_PATH = Path(__file__).parent.parent / "data" / "stack_pids.json"
LOG_PATH = Path(__file__).parent.parent / "logs" / "bot.log"
BOT_STDOUT_PATH = Path(__file__).parent.parent / "logs" / "bot.stdout.log"
WATCHDOG_STDOUT_PATH = Path(__file__).parent.parent / "logs" / "watchdog.stdout.log"
HEARTBEAT_WARNING_SECONDS = 60
HEARTBEAT_CRITICAL_SECONDS = 180

# Colores
C_GREEN  = "#00d26a"
C_RED    = "#ff4d6d"
C_YELLOW = "#ffd166"
C_BLUE   = "#4cc9f0"
C_GRAY   = "#6c757d"
C_BG     = "#0e1117"


# ═══════════════════════════════════════════════════════════════════════════════
# LIVE DATA — Binance testnet REST (público, sin auth)
# ═══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=3, show_spinner=False)
def fetch_live_price(symbol: str) -> Optional[float]:
    """Precio tick en tiempo real desde Binance testnet (TTL=3s)."""
    if not _HAS_REQUESTS:
        return None
    try:
        r = _requests.get(
            f"{_TESTNET_REST}/fapi/v1/ticker/price",
            params={"symbol": symbol},
            timeout=3,
        )
        if r.status_code == 200:
            return float(r.json()["price"])
    except Exception:
        pass
    return None


@st.cache_data(ttl=5, show_spinner=False)
def fetch_live_kline(symbol: str, interval: str = "15m") -> Optional[Dict]:
    """Vela en curso (no cerrada) desde Binance testnet (TTL=5s)."""
    if not _HAS_REQUESTS:
        return None
    try:
        r = _requests.get(
            f"{_TESTNET_REST}/fapi/v1/klines",
            params={"symbol": symbol, "interval": interval, "limit": 1},
            timeout=3,
        )
        if r.status_code == 200:
            k = r.json()[0]
            return {
                "open_time":   int(k[0]),
                "close_time":  int(k[6]),
                "open_price":  float(k[1]),
                "high_price":  float(k[2]),
                "low_price":   float(k[3]),
                "close_price": float(k[4]),
                "volume":      float(k[5]),
                "is_live":     True,
            }
    except Exception:
        pass
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def init_session_state() -> None:
    if "monitor_auto_refresh" not in st.session_state:
        st.session_state.monitor_auto_refresh = True
    if "monitor_refresh_seconds" not in st.session_state:
        st.session_state.monitor_refresh_seconds = 15
    if "last_manual_refresh_ts" not in st.session_state:
        st.session_state.last_manual_refresh_ts = 0.0


def get_database() -> Database:
    return Database(str(DB_PATH))


def db_query(db: Database, sql: str, params=()):
    if hasattr(db, "query"):
        return db.query(sql, params)
    conn = db.conn
    cur = conn.cursor()
    cur.execute(sql, params or ())
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


@st.cache_data(ttl=5, show_spinner=False)
def cached_query(sql: str, params: Sequence = ()) -> List[Dict]:
    db = Database(str(DB_PATH))
    return db.query(sql, params)


@st.cache_data(ttl=10, show_spinner=False)
def query_funding_carry_paper(symbol: str, sql: str, params: Sequence = ()) -> List[Dict]:
    """funding_carry_paper[_symbol].db usa sqlite3 crudo (no la clase Database)
    -- ver scripts/funding_carry_paper_watch.py. Modo solo lectura para no
    competir con el proceso que escribe."""
    import sqlite3
    db_path = _funding_carry_paper_db_path(symbol)
    if not db_path.exists():
        return []
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


@st.cache_data(ttl=10, show_spinner=False)
def query_funding_carry_live(symbol: str, sql: str, params: Sequence = ()) -> List[Dict]:
    """Modo solo lectura, igual que query_funding_carry_paper -- a diferencia
    de esa, esta DB SI usa la clase Database (tablas orders/positions/trades
    reales), pero instanciarla acá llama _ensure_tables_exist() (8 CREATE
    TABLE, 8 ALTER TABLE, commits) en CADA render con auto-refresh de 10s,
    compitiendo por el write-lock con el ejecutor en vivo justo cuando esta
    guardando un fill real. Con dinero real de por medio, un choque ahi puede
    hacer que el executor pierda el registro de que ya abrio posicion y la
    vuelva a abrir en el proximo evento. Se abre con sqlite3 crudo en
    mode=ro, sin pasar por Database ni sus migraciones."""
    import sqlite3
    db_path = _funding_carry_live_db_path(symbol)
    if not db_path.exists():
        return []
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def _heartbeat_age_seconds(iso_ts: Optional[str]) -> Optional[float]:
    if not iso_ts:
        return None
    try:
        clean = str(iso_ts).replace("Z", "").replace("+00:00", "")
        last = datetime.fromisoformat(clean)
        return (datetime.utcnow() - last).total_seconds()
    except Exception:
        return None


@st.cache_data(ttl=2, show_spinner=False)
def read_log_lines(path: str, n_lines: int) -> List[str]:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()
    return lines[-n_lines:]


def load_config() -> Dict:
    try:
        import yaml
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def load_stack_info() -> Dict:
    try:
        if not STACK_INFO_PATH.exists():
            return {}
        return json.loads(STACK_INFO_PATH.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def load_environment_value() -> str:
    try:
        if not ENV_PATH.exists():
            return "unknown"
        for raw_line in ENV_PATH.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip().upper() == "ENVIRONMENT":
                return value.strip().strip("'\"").lower()
    except Exception:
        return "unknown"
    return "unknown"


def _load_env_value(key: str) -> str:
    """Lee un valor de .env sin depender de variables de entorno del proceso
    (el dashboard normalmente se lanza sin heredar el .env)."""
    try:
        if not ENV_PATH.exists():
            return ""
        for raw_line in ENV_PATH.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, value = line.split("=", 1)
            if k.strip().upper() == key.upper():
                return value.strip().strip("'\"")
    except Exception:
        return ""
    return ""


def _controls_authenticated() -> bool:
    """Gate de acceso a Stack Controls — esta pestaña puede apagar/reiniciar
    el bot en vivo, y el dashboard es accesible desde toda la red local
    (Streamlit escucha en 0.0.0.0). Sin contraseña configurada, se bloquea
    por completo (fail-secure) en vez de dejar pasar."""
    if st.session_state.get("controls_unlocked"):
        return True

    configured_password = _load_env_value("DASHBOARD_CONTROLS_PASSWORD")
    if not configured_password:
        st.warning(
            "Stack Controls está bloqueado: falta configurar "
            "`DASHBOARD_CONTROLS_PASSWORD` en `.env`. Esta pestaña puede "
            "apagar/reiniciar el bot en vivo y el dashboard es accesible "
            "desde la red local, así que se bloquea hasta que definas una "
            "contraseña."
        )
        return False

    st.info("Esta pestaña puede apagar o reiniciar el bot en vivo. Ingresá la contraseña para continuar.")
    entered = st.text_input("Contraseña", type="password", key="controls_password_input")
    if st.button("Desbloquear"):
        if hmac.compare_digest(entered, configured_password):
            st.session_state.controls_unlocked = True
            st.rerun()
        else:
            st.error("Contraseña incorrecta.")
    return False


def load_runtime_environment() -> str:
    stack_info = load_stack_info()
    mode_hint = str(stack_info.get("mode", "")).strip().lower()
    if mode_hint in {"paper_demo", "paper", "testnet", "live"}:
        return mode_hint
    log_files = [BOT_STDOUT_PATH, LOG_PATH]
    for path in log_files:
        if not path.exists():
            continue
        lines = read_log_lines(str(path), n_lines=120)
        for line in reversed(lines):
            marker = "Environment="
            idx = line.find(marker)
            if idx == -1:
                continue
            value = line[idx + len(marker):].strip().lower()
            for sep in (" ", "\"", "'", ",", "}"):
                if sep in value:
                    value = value.split(sep, 1)[0]
            if value in {"paper", "testnet", "live"}:
                return value
    return load_environment_value()


def get_symbol(config: Dict) -> str:
    return config.get("bot", {}).get("symbol", "ETHUSDT")


def get_signal_timeframe(config: Dict) -> str:
    return config.get("timeframes", {}).get("signal", "4h")


def get_regime_timeframe(config: Dict) -> str:
    return config.get("timeframes", {}).get("regime", "1d")


_TIMEFRAME_SECONDS = {
    "1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400, "1d": 86400,
}


def timeframe_to_seconds(tf: str, default: int = 900) -> int:
    return _TIMEFRAME_SECONDS.get(str(tf), default)


def parse_utc_datetime(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    text = str(raw).strip().replace("Z", "")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _process_alive(pid: Optional[int]) -> bool:
    if not pid:
        return False
    try:
        import psutil
        return psutil.pid_exists(int(pid))
    except Exception:
        return False


def _count_watchdog_restarts() -> Dict[str, int]:
    stats = {"dashboard_restarts": 0, "bot_restarts": 0}
    try:
        if not WATCHDOG_STDOUT_PATH.exists():
            return stats
        lines = read_log_lines(str(WATCHDOG_STDOUT_PATH), n_lines=2000)
        for line in lines:
            low = line.lower()
            if "dashboard restarted" in low:
                stats["dashboard_restarts"] += 1
            if "bot restarted" in low:
                stats["bot_restarts"] += 1
    except Exception:
        pass
    return stats


def parse_log_json_line(line: str) -> Dict:
    try:
        outer = json.loads(line)
        msg = outer.get("message")
        if isinstance(msg, str) and msg.strip().startswith("{"):
            try:
                inner = json.loads(msg)
                outer["_message_json"] = inner
            except Exception:
                pass
        return outer
    except Exception:
        return {}


def _get_latest_runtime_telemetry(db: Database) -> Optional[Dict]:
    try:
        if hasattr(db, "get_latest_runtime_telemetry"):
            return db.get_latest_runtime_telemetry()
    except Exception:
        pass
    try:
        rows = cached_query("SELECT * FROM runtime_telemetry ORDER BY id DESC LIMIT 1")
        return rows[0] if rows else None
    except Exception:
        return None


def _get_recent_runtime_telemetry(db: Database, limit: int = 120) -> List[Dict]:
    try:
        return cached_query(
            "SELECT * FROM runtime_telemetry ORDER BY id DESC LIMIT ?", (limit,)
        )
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════════════════════════
# DATOS DE MERCADO E INDICADORES
# ═══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=5, show_spinner=False)
def load_klines(symbol: str, timeframe: str = "15m", limit: int = 150) -> pd.DataFrame:
    """Carga klines cerradas de DB + vela en curso de la API. TTL=5s."""
    try:
        rows = cached_query(
            """
            SELECT * FROM (
                SELECT open_time, close_time, open_price, high_price, low_price,
                       close_price, volume
                FROM klines WHERE symbol = ? AND timeframe = ?
                ORDER BY close_time DESC LIMIT ?
            ) ORDER BY close_time ASC
            """,
            (symbol, timeframe, limit),
        )
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)

        # Añadir vela en curso (no cerrada) desde la API REST
        if timeframe in ("15m", "1h", "4h", "1d"):
            live_kline = fetch_live_kline(symbol, timeframe)
            if live_kline:
                live_close_ms = live_kline["close_time"]
                # Solo añadir si es más reciente que la última vela en DB
                last_db_close = df["close_time"].max() if not df.empty else 0
                if live_close_ms > last_db_close:
                    live_row = {k: v for k, v in live_kline.items() if k != "is_live"}
                    df = pd.concat([df, pd.DataFrame([live_row])], ignore_index=True)

        df["time"] = pd.to_datetime(df["close_time"], unit="ms", errors="coerce")
        if df["time"].isna().all():
            df["time"] = pd.to_datetime(df["close_time"], errors="coerce")
        df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
        for col in ["open_price", "high_price", "low_price", "close_price", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["close_price"])
        if len(df) < 20:
            return df

        close = df["close_price"].values
        high  = df["high_price"].values
        low   = df["low_price"].values
        period = 20

        df["EMA"]      = TechnicalIndicators.ema(close, period)
        df["ATR"]      = TechnicalIndicators.atr(high, low, close, 14)
        df["ATR_Pct"]  = TechnicalIndicators.atr_percent(high, low, close, 14)
        df["ADX"]      = TechnicalIndicators.adx(high, low, close, 14)
        df["RSI"]      = TechnicalIndicators.rsi(close, 14)
        df["ZScore"]   = TechnicalIndicators.zscore(close, period)
        bb_u, bb_m, bb_l = TechnicalIndicators.bollinger_bands(close, period)
        df["BB_Upper"] = bb_u
        df["BB_Mid"]   = bb_m
        df["BB_Lower"] = bb_l
        return df
    except Exception:
        return pd.DataFrame()


def load_trades_df(symbol: str, limit: int = 500) -> pd.DataFrame:
    try:
        rows = cached_query(
            "SELECT * FROM trades WHERE symbol = ? ORDER BY id DESC LIMIT ?",
            (symbol, limit),
        )
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        for col in ["entry_time", "exit_time"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        for col in ["pnl_usdt", "pnl_percent", "entry_price", "exit_price", "quantity"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.sort_values("entry_time") if "entry_time" in df.columns else df
    except Exception:
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════════
# COMPONENTES VISUALES
# ═══════════════════════════════════════════════════════════════════════════════

def render_mode_badge() -> None:
    stack_info = load_stack_info()
    env = load_runtime_environment()
    mode_hint = str(stack_info.get("mode", "")).strip().lower()
    if mode_hint == "paper_demo":
        st.warning("MODO: DEMO PAPER (simulación)")
        return
    if env == "paper":
        st.warning("MODO: PAPER (simulación)")
        return
    if env == "testnet":
        st.info("MODO: TESTNET REAL (sin dinero real)")
        return
    if env == "live":
        st.error("MODO: LIVE (dinero real)")
        return
    st.info("MODO: DESCONOCIDO")


def regime_badge(regime: str) -> str:
    r = (regime or "").upper()
    if r == "LATERAL":
        return f"<span style='background:{C_GREEN};color:#000;padding:3px 10px;border-radius:5px;font-weight:bold'>LATERAL ✓</span>"
    if r == "TREND":
        return f"<span style='background:{C_RED};color:#fff;padding:3px 10px;border-radius:5px;font-weight:bold'>TREND ✗</span>"
    return f"<span style='background:{C_GRAY};color:#fff;padding:3px 10px;border-radius:5px;font-weight:bold'>{r}</span>"


def render_strategy_gauges(telemetry: Optional[Dict], df_signal: pd.DataFrame, df_regime: pd.DataFrame, config: Dict) -> None:
    """Semáforos y gauges de estrategia en tiempo real.

    df_signal: velas del timeframe de ejecución (config.timeframes.signal, ej. 4h) — ZScore/RSI (para MR).
    df_regime: velas del timeframe de régimen (config.timeframes.regime, ej. 1d) — ADX/ATR% (para RegimeDetector),
               el mismo timeframe que usa el motor real para decidir LATERAL/TREND — no el de ejecución.
    """
    zscore_thresh = float(config.get("mean_reversion", {}).get("zscore_threshold", 1.5))
    adx_thresh    = float(config.get("regime", {}).get("adx_threshold", 25.0))
    atr_thresh    = float(config.get("regime", {}).get("atr_percent_min", 1.0))

    # Valores actuales
    zscore_now = float(df_signal["ZScore"].iloc[-1]) if not df_signal.empty and "ZScore" in df_signal.columns and not pd.isna(df_signal["ZScore"].iloc[-1]) else None
    adx_now    = float(df_regime["ADX"].iloc[-1])    if not df_regime.empty and "ADX" in df_regime.columns    and not pd.isna(df_regime["ADX"].iloc[-1])    else None
    atr_pct    = float(df_regime["ATR_Pct"].iloc[-1]) if not df_regime.empty and "ATR_Pct" in df_regime.columns and not pd.isna(df_regime["ATR_Pct"].iloc[-1]) else None
    rsi_now    = float(df_signal["RSI"].iloc[-1])    if not df_signal.empty and "RSI" in df_signal.columns    and not pd.isna(df_signal["RSI"].iloc[-1])    else None
    regime     = str((telemetry or {}).get("last_regime") or "UNKNOWN").upper()

    cols = st.columns(5)
    with cols[0]:
        st.markdown("**Régimen**")
        st.markdown(regime_badge(regime), unsafe_allow_html=True)
    with cols[1]:
        delta_z = f"{'+' if (zscore_now or 0) >= 0 else ''}{zscore_now:.2f}" if zscore_now is not None else "N/A"
        color_z = C_RED if zscore_now is not None and abs(zscore_now) >= zscore_thresh else C_GREEN
        st.metric("ZScore", delta_z, help=f"Umbral: ±{zscore_thresh}")
        if zscore_now is not None:
            pct = min(abs(zscore_now) / zscore_thresh, 2.0) / 2.0
            st.progress(pct, text=f"{'⚠️ ZONA SEÑAL' if abs(zscore_now) >= zscore_thresh else 'zona neutra'}")
    with cols[2]:
        adx_str = f"{adx_now:.1f}" if adx_now is not None else "N/A"
        adx_delta = "TREND" if adx_now is not None and adx_now > adx_thresh else "lateral"
        st.metric("ADX", adx_str, delta=adx_delta,
                  delta_color="inverse" if adx_now is not None and adx_now > adx_thresh else "normal",
                  help=f"Umbral tendencia: {adx_thresh}")
    with cols[3]:
        atr_str = f"{atr_pct:.2f}%" if atr_pct is not None else "N/A"
        atr_ok = atr_pct is not None and atr_pct >= atr_thresh
        st.metric("ATR%", atr_str, delta="ok" if atr_ok else "bajo",
                  delta_color="normal" if atr_ok else "inverse",
                  help=f"Min volatilidad: {atr_thresh}%")
    with cols[4]:
        rsi_str = f"{rsi_now:.1f}" if rsi_now is not None else "N/A"
        rsi_zone = "sobrecomprado" if rsi_now and rsi_now > 70 else ("sobrevendido" if rsi_now and rsi_now < 30 else "neutro")
        st.metric("RSI", rsi_str, delta=rsi_zone,
                  delta_color="inverse" if rsi_zone != "neutro" else "off")


def render_smart_alerts(telemetry: Optional[Dict], df_signal: pd.DataFrame,
                        state: str, last_update_raw: Optional[str], config: Dict) -> None:
    """Sistema de alertas inteligentes con prioridad visual."""
    now = datetime.utcnow()
    alerts = []
    mr_enabled    = bool(config.get("mean_reversion", {}).get("enabled", False))
    trend_enabled = bool(config.get("trend", {}).get("enabled", False))

    # Heartbeat
    last_update_dt = parse_utc_datetime(last_update_raw)
    if last_update_dt:
        lag = int(max(0, (now - last_update_dt).total_seconds()))
        if lag >= HEARTBEAT_CRITICAL_SECONDS:
            alerts.append(("error", f"🔴 CRÍTICO: Bot sin heartbeat por {lag}s"))
        elif lag >= HEARTBEAT_WARNING_SECONDS:
            alerts.append(("warning", f"🟡 Bot: heartbeat lento ({lag}s)"))

    # Estado error
    if state == "ERROR_SAFE":
        alerts.append(("error", "🔴 CRÍTICO: Bot en ERROR_SAFE — revisar Logs"))

    if telemetry:
        # WebSocket
        if not telemetry.get("ws_healthy"):
            alerts.append(("warning", "🟡 WebSocket reportado como no saludable"))

        # Evaluación de señal — la señal solo se re-evalúa cuando cierra una vela
        # del timeframe configurado (p.ej. cada 4h), no continuamente. El umbral
        # tiene que ser relativo a ese timeframe + margen, si no dispara una falsa
        # alarma la mayor parte de cada ciclo (con 4h fijo en 900s alertaba ~94%
        # del tiempo, todos los días, sin que hubiera ningún problema real).
        last_eval = parse_utc_datetime(str(telemetry.get("last_signal_eval_time") or ""))
        if last_eval:
            since = int((now - last_eval).total_seconds())
            signal_threshold = timeframe_to_seconds(get_signal_timeframe(config)) + 900
            if since > signal_threshold:
                alerts.append(("warning", f"🟡 Sin evaluación de señal en {since//60}min ({since}s)"))

        # Bloqueo persistente
        block = str(telemetry.get("last_block_reason") or "").strip()
        if block and block not in ("NO_SETUP", ""):
            alerts.append(("warning", f"🟠 Bloqueo: {block}"))

        # Régimen y si hay o no una estrategia activa para ese régimen.
        regime = str(telemetry.get("last_regime") or "").upper()
        if regime == "LATERAL":
            if mr_enabled:
                alerts.append(("success", "✅ Régimen LATERAL activo — Mean Reversion evaluando setups"))
            else:
                alerts.append(("info", "⏸️ Régimen LATERAL — Mean Reversion está DESHABILITADA (mean_reversion.enabled=false), el bot no opera en este régimen"))
        elif regime == "TREND":
            if trend_enabled:
                alerts.append(("success", "✅ Régimen TREND activo — Trend evaluando setups"))
            else:
                alerts.append(("info", "⏸️ Régimen TREND — Trend está DESHABILITADA, el bot no opera en este régimen"))

    # ZScore en zona señal — solo relevante si Mean Reversion está habilitada;
    # si no, es un número informativo sin ningún efecto en el trading real.
    if mr_enabled and not df_signal.empty and "ZScore" in df_signal.columns:
        zscore_thresh = float(config.get("mean_reversion", {}).get("zscore_threshold", 1.5))
        z = df_signal["ZScore"].iloc[-1]
        if pd.notna(z):
            if abs(z) >= zscore_thresh:
                direction = "LONG (sobrevendido)" if z < 0 else "SHORT (sobrecomprado)"
                alerts.append(("success", f"🎯 ZScore={z:.2f} en zona señal → {direction}"))
            elif abs(z) >= zscore_thresh * 0.8:
                alerts.append(("info", f"📊 ZScore={z:.2f} acercándose al umbral ±{zscore_thresh}"))
    elif not mr_enabled and not df_signal.empty and "ZScore" in df_signal.columns:
        z = df_signal["ZScore"].iloc[-1]
        if pd.notna(z) and abs(z) >= float(config.get("mean_reversion", {}).get("zscore_threshold", 1.5)):
            alerts.append(("info", f"📊 ZScore={z:.2f} en zona extrema, pero Mean Reversion está deshabilitada — no genera operación"))

    if not alerts:
        st.success("✅ Sin alertas — sistema operativo y mercado sin setup")
        return

    for level, msg in alerts:
        if level == "error":
            st.error(msg)
        elif level == "warning":
            st.warning(msg)
        elif level == "success":
            st.success(msg)
        else:
            st.info(msg)


def render_equity_with_drawdown(df_trades: pd.DataFrame, initial_capital: float = 10000.0) -> None:
    """Curva de equity con drawdown sombreado."""
    if df_trades.empty or "pnl_usdt" not in df_trades.columns:
        st.info("Sin trades para mostrar curva de equity.")
        return

    df = df_trades.dropna(subset=["pnl_usdt"]).copy()
    if df.empty:
        return

    df["equity"] = initial_capital + df["pnl_usdt"].cumsum()
    df["peak"]   = df["equity"].cummax()
    df["dd"]     = (df["equity"] - df["peak"]) / df["peak"] * 100
    x = df["exit_time"] if "exit_time" in df.columns else df.index

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        row_heights=[0.7, 0.3],
                        vertical_spacing=0.05)

    fig.add_trace(go.Scatter(
        x=x, y=df["equity"], mode="lines", name="Equity",
        line=dict(color=C_BLUE, width=2),
        fill="tozeroy", fillcolor="rgba(76,201,240,0.1)"
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=x, y=df["dd"], mode="lines", name="Drawdown %",
        line=dict(color=C_RED, width=1.5),
        fill="tozeroy", fillcolor="rgba(255,77,109,0.2)"
    ), row=2, col=1)

    fig.update_layout(
        height=350, margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor=C_BG, plot_bgcolor=C_BG,
        legend=dict(orientation="h", y=1.05),
        font=dict(color="#cdd6f4"),
    )
    fig.update_yaxes(gridcolor="#2a2a3e")
    fig.update_xaxes(gridcolor="#2a2a3e")
    st.plotly_chart(fig, use_container_width=True)


def render_pnl_histogram(df_trades: pd.DataFrame) -> None:
    if df_trades.empty or "pnl_usdt" not in df_trades.columns:
        return
    df = df_trades.dropna(subset=["pnl_usdt"])
    colors = [C_GREEN if v >= 0 else C_RED for v in df["pnl_usdt"]]
    fig = go.Figure(go.Bar(
        x=list(range(len(df))), y=df["pnl_usdt"].values,
        marker_color=colors, name="PnL por trade"
    ))
    fig.update_layout(
        height=220, margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor=C_BG, plot_bgcolor=C_BG,
        font=dict(color="#cdd6f4"),
        xaxis=dict(title="Trade #", gridcolor="#2a2a3e"),
        yaxis=dict(title="PnL USDT", gridcolor="#2a2a3e"),
    )
    st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# GRÁFICO DE MERCADO COMPLETO
# ═══════════════════════════════════════════════════════════════════════════════

def render_market_chart(df: pd.DataFrame, config: Dict, live_price: Optional[float] = None) -> None:
    if df.empty:
        st.info("Sin datos de klines para el gráfico.")
        return

    zscore_thresh = float(config.get("mean_reversion", {}).get("zscore_threshold", 1.5))
    adx_thresh    = float(config.get("optimized_params", {}).get("adx_threshold", 37.5))
    atr_thresh    = float(config.get("regime", {}).get("atr_percent_min", 1.0))

    fig = make_subplots(
        rows=4, cols=1,
        shared_xaxes=True,
        row_heights=[0.45, 0.2, 0.2, 0.15],
        vertical_spacing=0.03,
        subplot_titles=("Precio + BB + EMA", "ZScore", "ADX / ATR%", "Volumen"),
    )

    x = df["time"]

    # ── Fila 1: Candlestick + BB + EMA ────────────────────────────────────────
    # Velas cerradas (todas excepto la última si hay vela en curso)
    df_closed = df.iloc[:-1] if live_price is not None and len(df) > 1 else df
    df_live   = df.iloc[[-1]] if live_price is not None and len(df) > 1 else pd.DataFrame()

    fig.add_trace(go.Candlestick(
        x=df_closed["time"],
        open=df_closed["open_price"], high=df_closed["high_price"],
        low=df_closed["low_price"],   close=df_closed["close_price"],
        name="OHLC",
        increasing_line_color=C_GREEN, decreasing_line_color=C_RED,
        increasing_fillcolor=C_GREEN, decreasing_fillcolor=C_RED,
    ), row=1, col=1)

    # Vela en curso (color diferenciado — azul claro)
    if not df_live.empty:
        fig.add_trace(go.Candlestick(
            x=df_live["time"],
            open=df_live["open_price"], high=df_live["high_price"],
            low=df_live["low_price"],   close=df_live["close_price"],
            name="En curso",
            increasing_line_color=C_BLUE, decreasing_line_color=C_YELLOW,
            increasing_fillcolor="rgba(76,201,240,0.5)",
            decreasing_fillcolor="rgba(255,209,102,0.5)",
        ), row=1, col=1)

    if "BB_Upper" in df.columns:
        fig.add_trace(go.Scatter(
            x=x, y=df["BB_Upper"], mode="lines", name="BB Upper",
            line=dict(color="rgba(255,209,102,0.6)", width=1, dash="dot"),
            showlegend=True,
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=x, y=df["BB_Lower"], mode="lines", name="BB Lower",
            line=dict(color="rgba(255,209,102,0.6)", width=1, dash="dot"),
            fill="tonexty", fillcolor="rgba(255,209,102,0.04)",
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=x, y=df["BB_Mid"], mode="lines", name="BB Mid",
            line=dict(color="rgba(255,209,102,0.3)", width=1),
        ), row=1, col=1)

    if "EMA" in df.columns:
        fig.add_trace(go.Scatter(
            x=x, y=df["EMA"], mode="lines", name="EMA20",
            line=dict(color=C_BLUE, width=1.5),
        ), row=1, col=1)

    # ── Fila 2: ZScore ────────────────────────────────────────────────────────
    if "ZScore" in df.columns:
        z = df["ZScore"]
        colors_z = [C_RED if v >= zscore_thresh else (C_GREEN if v <= -zscore_thresh else C_GRAY) for v in z]
        fig.add_trace(go.Bar(
            x=x, y=z, name="ZScore",
            marker_color=colors_z, opacity=0.8,
        ), row=2, col=1)
        for sign, label in [(zscore_thresh, f"+{zscore_thresh}"), (-zscore_thresh, f"-{zscore_thresh}")]:
            fig.add_hline(y=sign, line_color=C_YELLOW, line_dash="dash", line_width=1, row=2, col=1)

    # ── Fila 3: ADX y ATR% ────────────────────────────────────────────────────
    if "ADX" in df.columns:
        fig.add_trace(go.Scatter(
            x=x, y=df["ADX"], mode="lines", name="ADX",
            line=dict(color="#a78bfa", width=2),
        ), row=3, col=1)
        fig.add_hline(y=adx_thresh, line_color=C_RED, line_dash="dash", line_width=1, row=3, col=1)

    if "ATR_Pct" in df.columns:
        fig.add_trace(go.Scatter(
            x=x, y=df["ATR_Pct"], mode="lines", name="ATR%",
            line=dict(color="#f9a8d4", width=1.5),
        ), row=3, col=1)
        fig.add_hline(y=atr_thresh, line_color=C_GREEN, line_dash="dot", line_width=1, row=3, col=1)

    # ── Fila 4: Volumen ───────────────────────────────────────────────────────
    if "volume" in df.columns:
        vol_colors = [C_GREEN if c >= o else C_RED
                      for c, o in zip(df["close_price"], df["open_price"])]
        fig.add_trace(go.Bar(
            x=x, y=df["volume"], name="Volumen",
            marker_color=vol_colors, opacity=0.6,
        ), row=4, col=1)

    # ── Línea de precio en vivo ───────────────────────────────────────────────
    if live_price is not None:
        fig.add_hline(
            y=live_price,
            line_color="#ffffff", line_dash="dash", line_width=1.5,
            annotation_text=f" VIVO ${live_price:.2f}",
            annotation_position="right",
            annotation_font=dict(color="#ffffff", size=11),
            row=1, col=1,
        )

    fig.update_layout(
        height=700,
        margin=dict(l=0, r=0, t=30, b=0),
        paper_bgcolor=C_BG, plot_bgcolor=C_BG,
        font=dict(color="#cdd6f4", size=11),
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", y=1.02, x=0, font=dict(size=10)),
        hovermode="x unified",
    )
    for i in range(1, 5):
        fig.update_xaxes(gridcolor="#2a2a3e", row=i, col=1)
        fig.update_yaxes(gridcolor="#2a2a3e", row=i, col=1)

    st.plotly_chart(fig, use_container_width=True)


def render_zscore_detail(df: pd.DataFrame, config: Dict) -> None:
    if df.empty or "ZScore" not in df.columns:
        return
    zscore_thresh = float(config.get("mean_reversion", {}).get("zscore_threshold", 1.5))
    df_z = df.dropna(subset=["ZScore"]).tail(60)
    if df_z.empty:
        return

    z = df_z["ZScore"].values
    x = df_z["time"]
    fig = go.Figure()
    fig.add_hrect(y0=zscore_thresh, y1=max(z.max(), zscore_thresh + 0.5),
                  fillcolor="rgba(255,77,109,0.1)", line_width=0)
    fig.add_hrect(y0=min(z.min(), -zscore_thresh - 0.5), y1=-zscore_thresh,
                  fillcolor="rgba(0,210,106,0.1)", line_width=0)
    fig.add_trace(go.Scatter(x=x, y=z, mode="lines+markers", name="ZScore",
                             line=dict(color=C_BLUE, width=2),
                             marker=dict(size=4)))
    fig.add_hline(y=zscore_thresh,  line_color=C_RED,    line_dash="dash", line_width=1.5,
                  annotation_text=f"SHORT >{zscore_thresh}", annotation_position="right")
    fig.add_hline(y=-zscore_thresh, line_color=C_GREEN,  line_dash="dash", line_width=1.5,
                  annotation_text=f"LONG <-{zscore_thresh}", annotation_position="right")
    fig.add_hline(y=0, line_color=C_GRAY, line_dash="dot", line_width=1)
    fig.update_layout(
        height=280, margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor=C_BG, plot_bgcolor=C_BG,
        font=dict(color="#cdd6f4"),
        xaxis=dict(gridcolor="#2a2a3e"),
        yaxis=dict(gridcolor="#2a2a3e", title="ZScore"),
    )
    st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# COMPONENTES HEREDADOS (preservados)
# ═══════════════════════════════════════════════════════════════════════════════

def render_heartbeat(last_update_raw: Optional[str]) -> None:
    if not last_update_raw:
        st.error("Heartbeat: sin datos de estado del bot")
        return
    last_dt = parse_utc_datetime(last_update_raw)
    if last_dt is None:
        st.warning(f"Heartbeat: formato no reconocido ({last_update_raw})")
        return
    lag_s = max(0, int((datetime.utcnow() - last_dt).total_seconds()))
    if lag_s >= HEARTBEAT_CRITICAL_SECONDS:
        st.error(f"Heartbeat CRITICAL: {lag_s}s sin actualización")
    elif lag_s >= HEARTBEAT_WARNING_SECONDS:
        st.warning(f"Heartbeat WARNING: {lag_s}s sin actualización")
    else:
        st.success(f"Heartbeat OK: {lag_s}s de latencia")


def get_last_entry_block_reason() -> Optional[Dict[str, str]]:
    try:
        rows = cached_query(
            """SELECT event_type, level, message, timestamp FROM events
               WHERE event_type IN ('ENTRY_BLOCKED_RISK', 'ENTRY_BLOCKED_GUARDRAIL')
               ORDER BY id DESC LIMIT 1"""
        )
        if rows:
            row = rows[0]
            msg = str(row.get("message", "")).strip()
            reason = "N/A"
            symbol = ""
            if msg.startswith("{"):
                try:
                    payload = json.loads(msg)
                    reason = str(payload.get("reason", "N/A"))
                    symbol = str(payload.get("symbol", ""))
                except Exception:
                    pass
            return {"timestamp": str(row.get("timestamp", "")),
                    "type": str(row.get("event_type", "")),
                    "reason": reason, "symbol": symbol}
    except Exception:
        pass
    if not LOG_PATH.exists():
        return None
    lines = read_log_lines(str(LOG_PATH), n_lines=400)
    for line in reversed(lines):
        row = parse_log_json_line(line)
        msg_json = row.get("_message_json")
        if not isinstance(msg_json, dict):
            continue
        if str(msg_json.get("event", "")).upper() != "RISK":
            continue
        risk_type = str(msg_json.get("type", "")).upper()
        if risk_type not in {"ENTRY_BLOCKED_RISK", "ENTRY_BLOCKED_GUARDRAIL"}:
            continue
        return {"timestamp": str(row.get("timestamp", "")), "type": risk_type,
                "reason": str(msg_json.get("reason", "N/A")),
                "symbol": str(msg_json.get("symbol", ""))}
    return None


def render_runtime_telemetry(db: Database, state: str) -> None:
    st.subheader("Runtime Telemetry")
    latest = _get_latest_runtime_telemetry(db)
    if not latest:
        st.info("Aún no hay telemetría runtime persistida.")
        return
    recent = _get_recent_runtime_telemetry(db, limit=180)
    now = datetime.utcnow()
    cycles_last_min = sum(
        1 for r in recent
        if (ts := parse_utc_datetime(str(r.get("timestamp")))) and (now - ts).total_seconds() <= 60
    )
    last_eval_dt = parse_utc_datetime(str(latest.get("last_signal_eval_time") or ""))
    last_exec_dt = parse_utc_datetime(str(latest.get("last_executable_signal_time") or ""))
    since_eval_s = int((now - last_eval_dt).total_seconds()) if last_eval_dt else None
    since_exec_min = int((now - last_exec_dt).total_seconds() // 60) if last_exec_dt else None

    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("Last Signal", str(latest.get("last_signal") or "UNKNOWN"))
    with c2: st.metric("Last Regime", str(latest.get("last_regime") or "UNKNOWN"))
    with c3: st.metric("Cycles/min", str(cycles_last_min))
    with c4: st.metric("Since Last Eval", f"{since_eval_s}s" if since_eval_s is not None else "N/A")

    block_reason = str(latest.get("last_block_reason") or "").strip()
    if block_reason:
        st.warning(f"Last Block Reason: {block_reason}")
    else:
        st.success("Sin bloqueo reciente de entrada.")

    if state == "IDLE" and since_exec_min is not None and since_exec_min >= 10:
        st.info(f"Bot saludable pero sin setup ejecutable en {since_exec_min} min.")


def render_global_semaphore(state, last_update_raw, telemetry) -> None:
    now = datetime.utcnow()
    level = "GREEN"
    reason = "Sistema operativo y saludable."
    last_update_dt = parse_utc_datetime(last_update_raw)
    lag_s = int(max(0, (now - last_update_dt).total_seconds())) if last_update_dt else None
    if lag_s is None or lag_s >= HEARTBEAT_CRITICAL_SECONDS:
        level = "RED"; reason = "Sin actualización reciente de estado del bot."
    elif lag_s >= HEARTBEAT_WARNING_SECONDS:
        level = "YELLOW"; reason = "Latencia elevada en actualización."
    if state == "ERROR_SAFE":
        level = "RED"; reason = "Bot en ERROR_SAFE."
    if telemetry:
        if not telemetry.get("ws_healthy") and level != "RED":
            level = "YELLOW"; reason = "WebSocket no saludable."
    if level == "GREEN":   st.success(f"SEMAFORO: {level} | {reason}")
    elif level == "YELLOW": st.warning(f"SEMAFORO: {level} | {reason}")
    else:                  st.error(f"SEMAFORO: {level} | {reason}")


def render_rtos_live_panel(last_update_raw, telemetry) -> None:
    st.subheader("RTOS Live")
    stack = load_stack_info()
    bot_pid = stack.get("bot_pid")
    dashboard_pid = stack.get("dashboard_pid")
    watchdog_pid = stack.get("watchdog_pid")
    bot_alive   = _process_alive(int(bot_pid))       if bot_pid else False
    dash_alive  = _process_alive(int(dashboard_pid)) if dashboard_pid else False
    watch_alive = _process_alive(int(watchdog_pid))  if watchdog_pid else False
    now = datetime.utcnow()
    last_update_dt = parse_utc_datetime(last_update_raw)
    heartbeat_age = int((now - last_update_dt).total_seconds()) if last_update_dt else None
    rt_ts  = parse_utc_datetime(str((telemetry or {}).get("timestamp") or ""))
    rt_age = int((now - rt_ts).total_seconds()) if rt_ts else None
    restarts = _count_watchdog_restarts()
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: st.metric("Bot PID",       str(bot_pid or "N/A"),       delta="UP" if bot_alive  else "DOWN")
    with c2: st.metric("Dashboard PID", str(dashboard_pid or "N/A"), delta="UP" if dash_alive else "DOWN")
    with c3: st.metric("Watchdog PID",  str(watchdog_pid or "N/A"),  delta="UP" if watch_alive else "DOWN")
    with c4: st.metric("Telemetry Age", f"{rt_age}s"        if rt_age        is not None else "N/A")
    with c5: st.metric("Heartbeat Age", f"{heartbeat_age}s" if heartbeat_age is not None else "N/A")
    c6, c7 = st.columns(2)
    with c6: st.metric("Watchdog Dashboard Restarts", str(restarts["dashboard_restarts"]))
    with c7: st.metric("Watchdog Bot Restarts",       str(restarts["bot_restarts"]))
    if rt_age is not None and rt_age > HEARTBEAT_WARNING_SECONDS:
        st.warning(f"RTOS: telemetría desfasada ({rt_age}s).")
    elif rt_age is not None:
        st.success("RTOS: telemetría en tiempo real.")


# ═══════════════════════════════════════════════════════════════════════════════
# PÁGINAS
# ═══════════════════════════════════════════════════════════════════════════════

def page_monitor() -> None:
    st.title("Monitor")
    render_mode_badge()

    if not DB_PATH.exists():
        st.error(f"Base de datos no encontrada: {DB_PATH}")
        return

    # Botón de refresh manual (el auto-refresh global ya corre cada 10s)
    col_ref, col_ts = st.columns([1, 3])
    with col_ref:
        if st.button("Refresh ahora"):
            st.cache_data.clear()
            st.rerun()
    with col_ts:
        st.caption(f"Auto-refresh cada {_REFRESH_INTERVAL_MS//1000}s | {datetime.utcnow().strftime('%H:%M:%S')} UTC")

    db = get_database()
    config = load_config()
    symbol = get_symbol(config)
    state_row  = db.get_bot_state() or {}
    state      = state_row.get("bot_state", "UNKNOWN")
    last_update = state_row.get("last_update")
    telemetry  = _get_latest_runtime_telemetry(db)

    render_global_semaphore(state=state, last_update_raw=last_update, telemetry=telemetry)

    # ── KPIs principales ──────────────────────────────────────────────────────
    df_trades = load_trades_df(symbol, limit=500)
    today     = datetime.utcnow().strftime("%Y-%m-%d")
    today_pnl = float(df_trades[df_trades["exit_time"].dt.strftime("%Y-%m-%d") == today]["pnl_usdt"].sum()) \
                if not df_trades.empty and "exit_time" in df_trades.columns else 0.0
    total_pnl = float(df_trades["pnl_usdt"].sum()) if not df_trades.empty else 0.0
    wins      = int((df_trades["pnl_usdt"] > 0).sum()) if not df_trades.empty else 0
    total_t   = len(df_trades)
    wr        = wins / total_t * 100 if total_t else 0.0

    # Sharpe simplificado
    sharpe = 0.0
    if not df_trades.empty and "pnl_usdt" in df_trades.columns and len(df_trades) > 2:
        r = df_trades["pnl_usdt"].dropna()
        if r.std() > 0:
            sharpe = float((r.mean() / r.std()) * (365 ** 0.5))

    # Max drawdown
    max_dd = 0.0
    if not df_trades.empty:
        eq = 10000.0 + df_trades["pnl_usdt"].cumsum()
        pk = eq.cummax()
        dd = (eq - pk) / pk * 100
        max_dd = float(dd.min())

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    with k1: st.metric("Bot State", state)
    with k2: st.metric("Symbol",    symbol)
    with k3: st.metric("Today P&L", f"${today_pnl:+.2f}")
    with k4: st.metric("Total P&L", f"${total_pnl:+.2f}")
    with k5: st.metric("Win Rate",  f"{wr:.1f}% ({wins}/{total_t})")
    with k6: st.metric("Max DD",    f"{max_dd:.1f}%")

    # Posición abierta
    position = db.get_position(symbol)
    if position and float(position.get("quantity", 0) or 0) > 0:
        side = position.get("side", "FLAT")
        qty  = abs(float(position.get("quantity", 0)))
        entry = float(position.get("entry_price", 0) or 0)
        st.info(f"Posición abierta: **{side}** {qty:.4f} @ ${entry:.2f}")

    st.divider()

    # ── Gauges de estrategia ──────────────────────────────────────────────────
    live_price = fetch_live_price(symbol)
    if live_price:
        st.metric(f"{symbol} precio vivo", f"${live_price:.2f}",
                  help="Desde API Binance testnet, TTL=3s")
    st.subheader("Estado de Estrategia en Tiempo Real")
    signal_tf = get_signal_timeframe(config)
    regime_tf = get_regime_timeframe(config)
    df_signal = load_klines(symbol, signal_tf, 150)
    df_regime = load_klines(symbol, regime_tf, 150)
    st.caption(f"Ejecución: velas {signal_tf}  |  Régimen: velas {regime_tf}")
    render_strategy_gauges(telemetry, df_signal, df_regime, config)

    st.divider()

    # ── Alertas inteligentes ──────────────────────────────────────────────────
    st.subheader("Alertas")
    render_smart_alerts(telemetry, df_signal, state, last_update, config)

    st.divider()

    # ── Equity curve + RTOS ───────────────────────────────────────────────────
    col_eq, col_rtos = st.columns([3, 2])
    with col_eq:
        st.subheader("Equity + Drawdown")
        render_equity_with_drawdown(df_trades)
    with col_rtos:
        render_rtos_live_panel(last_update_raw=last_update, telemetry=telemetry)

    st.divider()
    render_runtime_telemetry(db, state)

    # ── Posición abierta (detalle) ────────────────────────────────────────────
    st.subheader("Posición Abierta")
    pos_detail = db.get_position(symbol)
    if pos_detail and float(pos_detail.get("quantity", 0) or 0) > 0:
        _side  = pos_detail.get("side", "?")
        _qty   = abs(float(pos_detail.get("quantity", 0)))
        _entry = float(pos_detail.get("entry_price", 0) or 0)
        _sl    = pos_detail.get("stop_loss")
        _tp    = pos_detail.get("take_profit")
        _etime = pos_detail.get("entry_time") or pos_detail.get("updated_at") or "—"
        _mark  = float(pos_detail.get("mark_price", 0) or _entry)
        _pnl_u = (_mark - _entry) * _qty if _side == "LONG" else (_entry - _mark) * _qty

        _color = "🟢" if _side == "LONG" else "🔴"
        pc1, pc2, pc3, pc4, pc5 = st.columns(5)
        with pc1: st.metric("Side", f"{_color} {_side}")
        with pc2: st.metric("Cantidad", f"{_qty:.4f} ETH")
        with pc3: st.metric("Entry", f"${_entry:.2f}")
        with pc4: st.metric("Stop Loss", f"${float(_sl):.2f}" if _sl else "—")
        with pc5: st.metric("Take Profit", f"${float(_tp):.2f}" if _tp else "—")
        st.caption(f"Entrada: {str(_etime)[:19]}  |  PnL estimado: ${_pnl_u:+.4f} USDT")
    else:
        st.info("Sin posición abierta")

    # ── Órdenes abiertas ─────────────────────────────────────────────────────
    st.subheader("Open Orders  (órdenes pendientes de ejecución)")
    orders = db.get_open_orders(symbol)
    if orders:
        st.dataframe(pd.DataFrame(orders)[[c for c in ["client_order_id","side","quantity","price","status","created_at"] if c in pd.DataFrame(orders).columns]], use_container_width=True)
    else:
        st.caption("Sin órdenes pendientes — el bot usa órdenes MARKET que se ejecutan al instante.")

    st.subheader("Últimos 10 Trades")
    if not df_trades.empty:
        disp = df_trades.tail(10)[["entry_price","exit_price","quantity","pnl_percent","pnl_usdt","exit_time"]].copy() if all(c in df_trades.columns for c in ["entry_price","exit_price","pnl_percent","pnl_usdt"]) else df_trades.tail(10)
        def _color_pnl(val):
            try:
                return f"color: {C_GREEN}" if float(val) >= 0 else f"color: {C_RED}"
            except Exception:
                return ""
        if "pnl_usdt" in disp.columns:
            st.dataframe(disp.style.map(_color_pnl, subset=["pnl_usdt"]), use_container_width=True)
        else:
            st.dataframe(disp, use_container_width=True)
    else:
        st.info("Sin trades completados")


def page_market() -> None:
    st.title("Mercado en Tiempo Real")
    render_mode_badge()

    config = load_config()
    symbol = get_symbol(config)
    signal_tf = get_signal_timeframe(config)
    regime_tf = get_regime_timeframe(config)
    # Opciones = timeframes que el bot realmente usa hoy (ejecución + régimen),
    # no valores fijos — "15m"/"1h" quedaron sin datos frescos desde el pivote a 4h.
    tf_options = list(dict.fromkeys([signal_tf, regime_tf, "1h"]))

    col_tf, col_bars, col_ref = st.columns([1, 1, 2])
    with col_tf:
        timeframe = st.selectbox("Timeframe", tf_options, index=0)
    with col_bars:
        n_bars = st.slider("Velas", 50, 300, 100, 10)
    with col_ref:
        if st.button("Refresh"):
            st.cache_data.clear()
            st.rerun()

    df = load_klines(symbol, timeframe, n_bars)

    if df.empty:
        st.warning("Sin datos de klines en la DB. El bot debe estar corriendo para acumular datos.")
        return

    # Precio en vivo desde API REST (se actualiza cada 3s vía cache)
    live_price = fetch_live_price(symbol)
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else last

    # Usar precio vivo si está disponible, si no el último cierre
    display_price = live_price if live_price else float(last["close_price"])
    ref_price = float(prev["close_price"])
    price_change_pct = (display_price - ref_price) / ref_price * 100

    p1, p2, p3, p4, p5, p6 = st.columns(6)
    with p1:
        label = "Precio VIVO" if live_price else "Precio"
        st.metric(label, f"${display_price:.2f}", delta=f"{price_change_pct:+.2f}%")
    with p2: st.metric("High (vela)",  f"${float(last['high_price']):.2f}")
    with p3: st.metric("Low (vela)",   f"${float(last['low_price']):.2f}")
    with p4:
        z = last.get("ZScore")
        st.metric("ZScore", f"{z:.3f}" if pd.notna(z) else "N/A")
    with p5:
        adx = last.get("ADX")
        st.metric("ADX", f"{adx:.1f}" if pd.notna(adx) else "N/A")
    with p6:
        rsi = last.get("RSI")
        st.metric("RSI", f"{rsi:.1f}" if pd.notna(rsi) else "N/A")

    st.caption(f"Precio vivo: {datetime.utcnow().strftime('%H:%M:%S')} UTC | {len(df)} velas cerradas + vela en curso")
    st.divider()

    # Gráfico principal (incluye vela en curso + línea de precio vivo)
    st.subheader(f"{symbol} {timeframe}")
    render_market_chart(df, config, live_price=live_price)

    st.divider()

    # ZScore detallado
    st.subheader("ZScore — últimas 60 velas")
    render_zscore_detail(df, config)

    # Tabla resumen indicadores recientes
    with st.expander("Indicadores recientes (últimas 10 velas)"):
        cols_show = [c for c in ["time", "close_price", "EMA", "ZScore", "BB_Upper", "BB_Lower",
                                  "ADX", "ATR_Pct", "RSI"] if c in df.columns]
        st.dataframe(df[cols_show].tail(10).round(4), use_container_width=True)


def page_trades() -> None:
    st.title("Análisis de Trades")
    db = get_database()
    config = load_config()
    symbol = get_symbol(config)

    col1, col2, col3 = st.columns(3)
    with col1: days_back = st.slider("Days back", 1, 365, 90)
    with col2: min_pnl   = st.number_input("Min P&L %", value=-100.0, step=1.0)
    with col3: max_pnl   = st.number_input("Max P&L %", value=100.0,  step=1.0)

    df = load_trades_df(symbol, limit=2000)
    if df.empty:
        st.info("Sin trades en la base de datos.")
        return

    if "exit_time" in df.columns:
        cutoff = datetime.utcnow() - timedelta(days=days_back)
        df = df[df["exit_time"] >= cutoff]
    if "pnl_percent" in df.columns:
        df = df[(df["pnl_percent"] >= min_pnl) & (df["pnl_percent"] <= max_pnl)]

    if df.empty:
        st.info("Sin trades para los filtros seleccionados.")
        return

    wins    = int((df["pnl_usdt"] > 0).sum()) if "pnl_usdt" in df.columns else 0
    losses  = int((df["pnl_usdt"] <= 0).sum()) if "pnl_usdt" in df.columns else 0
    total   = len(df)
    wr      = wins / total * 100 if total else 0
    total_p = float(df["pnl_usdt"].sum()) if "pnl_usdt" in df.columns else 0
    avg_w   = float(df[df["pnl_usdt"] > 0]["pnl_usdt"].mean()) if wins else 0
    avg_l   = float(df[df["pnl_usdt"] <= 0]["pnl_usdt"].mean()) if losses else 0
    profit_factor = abs(avg_w * wins / (avg_l * losses)) if losses and avg_l != 0 else float("inf")

    max_dd = 0.0
    if "pnl_usdt" in df.columns:
        eq = 10000.0 + df["pnl_usdt"].cumsum()
        pk = eq.cummax()
        max_dd = float(((eq - pk) / pk * 100).min())

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    with k1: st.metric("Trades",   str(total))
    with k2: st.metric("Win Rate", f"{wr:.1f}%")
    with k3: st.metric("Total P&L",f"${total_p:+.2f}")
    with k4: st.metric("Avg Win",  f"${avg_w:.2f}")
    with k5: st.metric("Avg Loss", f"${avg_l:.2f}")
    with k6: st.metric("Profit Factor", f"{profit_factor:.2f}" if profit_factor != float("inf") else "∞")

    st.divider()

    col_eq, col_hist = st.columns([3, 2])
    with col_eq:
        st.subheader("Equity + Drawdown")
        render_equity_with_drawdown(df)
    with col_hist:
        st.subheader("PnL por Trade")
        render_pnl_histogram(df)

    st.divider()
    st.subheader("Tabla de Trades")
    cols_show = [c for c in ["entry_time","exit_time","side","entry_price","exit_price",
                              "quantity","pnl_percent","pnl_usdt","reason_exit"] if c in df.columns]
    disp = df[cols_show].sort_values("entry_time", ascending=False) if "entry_time" in df.columns else df[cols_show]

    def _color_row(row):
        try:
            v = float(row.get("pnl_usdt", 0))
            color = "rgba(0,210,106,0.15)" if v > 0 else "rgba(255,77,109,0.15)"
            return [f"background-color: {color}"] * len(row)
        except Exception:
            return [""] * len(row)

    st.dataframe(disp.style.apply(_color_row, axis=1), use_container_width=True)


def _run_stack(action: str, mode: str = "auto", extra: str = "") -> str:
    import subprocess
    cmd = ["powershell", "-ExecutionPolicy", "Bypass",
           "-File", str(REPO_ROOT / "scripts" / "stack.ps1"),
           action, "-Mode", mode]
    if extra:
        cmd += extra.split()
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120, cwd=str(REPO_ROOT))
        return (r.stdout + r.stderr).strip()
    except Exception as exc:
        return f"Error: {exc}"


def _stack_status() -> Dict:
    pid_file = REPO_ROOT / "data" / "stack_pids.json"
    if not pid_file.exists():
        return {}
    try:
        # utf-8-sig strips BOM that PowerShell sometimes writes
        return json.loads(pid_file.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}


def page_controls() -> None:
    import subprocess
    st.title("Stack Controls")
    if not _controls_authenticated():
        return

    if os.getenv("RUNNING_IN_DOCKER") == "1":
        st.info(
            "Corriendo en Docker — esta pestaña no gestiona los contenedores "
            "(no le damos al dashboard acceso al socket de Docker: sería un "
            "riesgo de seguridad real para un servicio expuesto a la red). "
            "Usá estos comandos desde una terminal en el host:"
        )
        st.code(
            "docker compose ps                    # estado de los contenedores\n"
            "docker compose logs -f bot            # logs del bot en vivo\n"
            "docker compose logs -f dashboard      # logs del dashboard en vivo\n"
            "docker compose restart bot            # reiniciar solo el bot\n"
            "docker compose restart dashboard      # reiniciar solo el dashboard\n"
            "docker compose down                   # apagar todo\n"
            "docker compose up -d                  # levantar todo",
            language="bash",
        )
        return

    config = load_config()
    stack  = _stack_status()
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("Symbol",   get_symbol(config))
    with c2: st.metric("Leverage", f"{config.get('position', {}).get('leverage_default', 1)}x")
    with c3: st.metric("Mode",     stack.get("mode", "—"))
    with c4: st.metric("Dashboard", stack.get("dashboard_url", "http://localhost:8501"))

    if stack:
        bot_pid  = stack.get("bot_pid")
        dash_pid = stack.get("dashboard_pid")
        started  = stack.get("started_at_utc", "—")
        sim_db   = stack.get("sim_db")
        alive_bot  = _process_alive(int(bot_pid))  if bot_pid  else False
        alive_dash = _process_alive(int(dash_pid)) if dash_pid else False
        cols = st.columns(3)
        cols[0].metric("Bot PID",       str(bot_pid  or "—"), delta="alive" if alive_bot  else "dead", delta_color="normal" if alive_bot  else "inverse")
        cols[1].metric("Dashboard PID", str(dash_pid or "—"), delta="alive" if alive_dash else "dead", delta_color="normal" if alive_dash else "inverse")
        cols[2].metric("Started", started[:19].replace("T", " ") if started != "—" else "—")
        if sim_db:
            st.info(f"Simulation DB: `{sim_db}`")
    else:
        st.warning("Stack no iniciado.")

    st.divider()
    st.subheader("Quick Actions")
    cola, colb, colc, cold = st.columns(4)
    if cola.button("Start Testnet",   use_container_width=True):
        with st.spinner("Starting..."): out = _run_stack("start", "testnet")
        st.code(out)
    if colb.button("Start Simulation", use_container_width=True):
        with st.spinner("Starting..."): out = _run_stack("start", "sim")
        st.code(out)
    if colc.button("Status", use_container_width=True):
        with st.spinner("..."): out = _run_stack("status")
        st.code(out)
    if cold.button("Stop All", use_container_width=True, type="primary"):
        with st.spinner("Stopping..."): out = _run_stack("stop")
        st.code(out)

    st.divider()
    st.subheader("Custom Simulation")
    col_d, col_s, col_c = st.columns(3)
    sim_days    = col_d.number_input("Days",             min_value=7,     max_value=730,       value=90,     step=7)
    sim_speed   = col_s.number_input("Speed (s/candle)", min_value=0.05,  max_value=5.0,       value=0.3,    step=0.05)
    sim_capital = col_c.number_input("Capital (USDT)",   min_value=1000,  max_value=1_000_000, value=10_000, step=1000)
    sim_from    = st.text_input("From date (optional, YYYY-MM-DD)", value="")
    sim_norisk  = st.checkbox("Disable risk filters (stress-test)")
    if st.button("Launch Simulation", type="primary", use_container_width=True):
        extra = f"-SimDays {sim_days} -SimSpeed {sim_speed} -SimCapital {sim_capital}"
        if sim_from.strip(): extra += f" -SimFrom {sim_from.strip()}"
        if sim_norisk:       extra += " -SimNoRisk"
        with st.spinner(f"Launching {sim_days}-day simulation..."):
            out = _run_stack("start", "sim", extra)
        st.code(out)


def render_funding_carry_pnl_chart(df: pd.DataFrame) -> None:
    if df.empty or "cum_pnl_pct" not in df.columns:
        st.info("Sin eventos para graficar.")
        return
    fig = go.Figure(go.Scatter(
        x=np.array(df["funding_time"].dt.to_pydatetime()), y=df["cum_pnl_pct"] * 100, mode="lines", name="PnL acumulado %",
        line=dict(color=C_BLUE, width=2), fill="tozeroy", fillcolor="rgba(76,201,240,0.1)",
    ))
    fig.update_layout(
        height=300, margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor=C_BG, plot_bgcolor=C_BG, font=dict(color="#cdd6f4"),
        xaxis=dict(gridcolor="#2a2a3e"),
        yaxis=dict(title="PnL % acumulado", gridcolor="#2a2a3e"),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_heartbeat_metric(heartbeat_age: Optional[float]) -> None:
    if heartbeat_age is None:
        st.metric("Heartbeat", "sin datos")
        return
    ok = heartbeat_age <= FUNDING_CARRY_HEARTBEAT_MAX_AGE_S
    st.metric(
        "Heartbeat",
        f"{heartbeat_age / 60:.0f} min",
        delta="OK" if ok else "VIEJO — revisar",
        delta_color="normal" if ok else "inverse",
    )


def page_funding_carry() -> None:
    st.title("Funding Carry")
    st.caption(
        "Spot largo + perp corto, delta-neutral — cobra el funding rate. "
        "Regla: entry=0.000% exit=-0.020% sobre MA(9) del funding "
        "(src/signals/funding_carry_rule.py, fuente única compartida por ambos)."
    )

    symbol = st.radio("Símbolo", FUNDING_CARRY_SYMBOLS, horizontal=True, key="funding_carry_symbol")

    tab_paper, tab_live = st.tabs(["Paper (simulado)", "Live (ejecutor real)"])

    # ── PAPER ────────────────────────────────────────────────────────────
    with tab_paper:
        state_rows = query_funding_carry_paper(symbol, "SELECT key, value FROM state")
        state = {r["key"]: r["value"] for r in state_rows}
        heartbeat_age = _heartbeat_age_seconds(state.get("last_checked_at"))

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            cum_pnl = float(state.get("cum_pnl_pct", 0) or 0) * 100
            st.metric("PnL acumulado", f"{cum_pnl:+.4f}%")
        with c2:
            in_pos = state.get("in_position") == "1"
            st.metric("Posición", "DENTRO" if in_pos else "FUERA")
        with c3:
            _render_heartbeat_metric(heartbeat_age)
        with c4:
            n_events = query_funding_carry_paper(symbol, "SELECT COUNT(*) as n FROM funding_events")
            st.metric("Eventos registrados", str(n_events[0]["n"]) if n_events else "0")

        if heartbeat_age is not None and heartbeat_age > FUNDING_CARRY_HEARTBEAT_MAX_AGE_S:
            st.warning(
                f"Sin heartbeat hace {heartbeat_age / 60:.0f} min — revisar si "
                f"`funding-carry-watch` ({symbol}) sigue corriendo (`docker compose ps`)."
            )

        st.divider()
        rows = query_funding_carry_paper(
            symbol,
            "SELECT funding_time, funding_rate, funding_ma, in_position, pnl_pct, cum_pnl_pct, note "
            "FROM funding_events ORDER BY funding_time ASC"
        )
        df = pd.DataFrame(rows)
        if df.empty:
            st.info("Todavía no hay eventos registrados.")
        else:
            # format="ISO8601" (no un formato fijo): funding_time mezcla timestamps
            # con y sin microsegundos, ver funding_carry_paper_watch.py -- un formato
            # fijo rompe apenas aparece una fila con distinta precision.
            df["funding_time"] = pd.to_datetime(df["funding_time"], format="ISO8601")
            for col in ("funding_rate", "funding_ma", "pnl_pct", "cum_pnl_pct"):
                df[col] = pd.to_numeric(df[col], errors="coerce")

            st.subheader("PnL acumulado en el tiempo")
            render_funding_carry_pnl_chart(df)

            st.subheader("Últimos eventos")
            disp = df.tail(30).sort_values("funding_time", ascending=False).copy()
            disp["funding_rate"] = (disp["funding_rate"] * 100).map(lambda v: f"{v:+.4f}%")
            disp["funding_ma"] = (disp["funding_ma"] * 100).map(lambda v: f"{v:+.4f}%" if pd.notna(v) else "n/a")
            disp["pnl_pct"] = (disp["pnl_pct"] * 100).map(lambda v: f"{v:+.4f}%")
            disp["cum_pnl_pct"] = (disp["cum_pnl_pct"] * 100).map(lambda v: f"{v:+.4f}%")
            disp["in_position"] = disp["in_position"].map({1: "DENTRO", 0: "FUERA"})
            st.dataframe(
                disp[["funding_time", "funding_rate", "funding_ma", "in_position", "pnl_pct", "cum_pnl_pct", "note"]],
                use_container_width=True, hide_index=True,
            )

    # ── LIVE ─────────────────────────────────────────────────────────────
    with tab_live:
        state_rows = query_funding_carry_live(symbol, "SELECT key, value FROM carry_state")
        state = {r["key"]: r["value"] for r in state_rows}
        heartbeat_age = _heartbeat_age_seconds(state.get("last_checked_at"))

        positions = query_funding_carry_live(symbol, "SELECT * FROM positions")
        trades = query_funding_carry_live(symbol, "SELECT * FROM trades ORDER BY id DESC LIMIT 20")
        orders = query_funding_carry_live(symbol, "SELECT * FROM orders ORDER BY id DESC LIMIT 20")

        c1, c2, c3 = st.columns(3)
        with c1:
            has_position = len(positions) > 0
            st.metric("Posición real abierta", "SÍ" if has_position else "NO")
        with c2:
            _render_heartbeat_metric(heartbeat_age)
        with c3:
            ever_traded = len(orders) > 0
            st.metric("Modo", "En vivo (ya operó)" if ever_traded else "Dry-run (nunca ordenó real)")

        if heartbeat_age is not None and heartbeat_age > FUNDING_CARRY_HEARTBEAT_MAX_AGE_S:
            st.warning(
                f"Sin heartbeat hace {heartbeat_age / 60:.0f} min — revisar si "
                f"`funding-carry-live` ({symbol}) sigue corriendo (`docker compose ps`)."
            )

        if not ever_traded:
            st.info(
                "Nunca colocó una orden real — sigue en dry-run "
                "(requiere `--live` + `FUNDING_CARRY_LIVE_CONFIRM=yes` para operar de verdad)."
            )

        st.divider()
        if positions:
            st.subheader("Posiciones abiertas (reales)")
            st.dataframe(pd.DataFrame(positions), use_container_width=True, hide_index=True)
        else:
            st.caption("Sin posiciones reales abiertas.")

        st.subheader("Últimas órdenes")
        if orders:
            st.dataframe(pd.DataFrame(orders), use_container_width=True, hide_index=True)
        else:
            st.caption("Sin órdenes reales todavía.")

        st.subheader("Últimos trades cerrados")
        if trades:
            st.dataframe(pd.DataFrame(trades), use_container_width=True, hide_index=True)
        else:
            st.caption("Sin trades cerrados todavía.")


def page_logs() -> None:
    st.title("Logs")
    if not LOG_PATH.exists():
        st.warning("Log file not found")
        return
    col1, col2 = st.columns([2, 1])
    with col1: n_lines = st.slider("Lines", 10, 3000, 200, 10)
    with col2: refresh_logs = st.checkbox("Auto-refresh", value=False)
    refresh_seconds = st.slider("Refresh (s)", 2, 60, 5, 1)
    levels = st.multiselect("Levels", ["DEBUG","INFO","WARNING","ERROR","CRITICAL"],
                             default=["INFO","WARNING","ERROR","CRITICAL"])
    text_filter = st.text_input("Text contains", value="").strip().lower()
    if refresh_logs and _HAS_AUTOREFRESH:
        st_autorefresh(interval=refresh_seconds * 1000, key="logs_refresh")
    lines = read_log_lines(str(LOG_PATH), n_lines=n_lines)
    filtered = [l for l in lines
                if (not levels or any(f" {lv} " in l.upper() for lv in levels))
                and (not text_filter or text_filter in l.lower())]
    if not filtered:
        st.info("Sin líneas que coincidan.")
        return
    if st.checkbox("Newest first", value=False):
        filtered = list(reversed(filtered))
    st.code("".join(filtered), language="text")
    st.caption(f"{len(filtered)} líneas de {n_lines}.")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def run_page(page_callable) -> None:
    try:
        page_callable()
    except Exception as exc:
        st.error(f"Error renderizando página: {exc}")
        with st.expander("Traceback"):
            st.code(traceback.format_exc(), language="text")


def main() -> None:
    init_session_state()

    # Auto-refresh global — siempre activo en todas las páginas
    if _HAS_AUTOREFRESH:
        st_autorefresh(interval=_REFRESH_INTERVAL_MS, key="global_refresh")

    st.sidebar.title("BOT-NAZI")
    st.sidebar.caption("Trading Bot — ETHUSDT Futures")

    # Precio en vivo en sidebar (API REST Binance testnet, TTL=3s)
    try:
        config = load_config()
        symbol = get_symbol(config)
        live_price = fetch_live_price(symbol)
        if live_price:
            # Comparar con último cierre en DB para mostrar delta
            rows = cached_query(
                "SELECT close_price FROM klines WHERE symbol=? ORDER BY close_time DESC LIMIT 1",
                (symbol,)
            )
            last_close = float(rows[0]["close_price"]) if rows else live_price
            delta_pct = (live_price - last_close) / last_close * 100
            st.sidebar.metric(
                f"{symbol} (vivo)",
                f"${live_price:.2f}",
                delta=f"{delta_pct:+.2f}%",
            )
        else:
            rows = cached_query(
                "SELECT close_price FROM klines WHERE symbol=? ORDER BY close_time DESC LIMIT 1",
                (symbol,)
            )
            if rows:
                st.sidebar.metric(f"{symbol}", f"${float(rows[0]['close_price']):.2f}")
        st.sidebar.caption(f"Actualizado: {datetime.utcnow().strftime('%H:%M:%S')} UTC")
    except Exception:
        pass

    page = st.sidebar.radio("Navegación",
                             ["Monitor", "Mercado", "Trades", "Funding Carry", "Controls", "Logs"],
                             index=0)

    if   page == "Monitor":       run_page(page_monitor)
    elif page == "Mercado":       run_page(page_market)
    elif page == "Trades":        run_page(page_trades)
    elif page == "Funding Carry": run_page(page_funding_carry)
    elif page == "Controls":      run_page(page_controls)
    elif page == "Logs":          run_page(page_logs)


if __name__ == "__main__":
    main()



