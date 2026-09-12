"""
Cliente REST para Binance Spot.
Hermano de src/exchange/rest.py (Futuros) — mismo esquema de firma y manejo
de errores/retry, pero apuntando a la API de Spot (base URL, endpoints y
formas de respuesta distintas: sin positionSide/reduceOnly, cuenta con
balances free/locked en vez de assets/positions).

Duplicacion deliberada de _sign_request/_request/_sync_time_offset respecto
a BinanceRestClient: son dos clientes con formas de API realmente distintas
(create_order, parseo de cuenta) y hoy solo hay estos dos call-sites — forzar
una base compartida seria over-engineering. Si aparece un tercer mercado,
ahi si vale la pena extraer una base comun.
"""
from __future__ import annotations

import hashlib
import hmac
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests

from src.observability.logger import main_logger


def _fmt_decimal(value: float) -> str:
    """Ver misma funcion en src/exchange/rest.py -- duplicada a proposito
    (mismo criterio de duplicacion deliberada que el resto de este archivo).
    Evita que urlencode serialice qty/price muy chicos en notacion
    cientifica (ej. 1e-05), que Binance no espera en la query string."""
    return format(Decimal(str(value)), "f")


class BinanceSpotRestClient:
    """Cliente REST para Binance Spot API."""

    MAINNET_BASE_URL = "https://api.binance.com"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        environment: str = "paper",
        timeout: int = 10,
    ):
        """
        Args:
            api_key: API Key (Spot)
            api_secret: API Secret (Spot)
            environment: "live" o "paper". No hay testnet de Spot cableado
                aca (Binance Spot testnet vive en un dominio separado,
                testnet.binance.vision, fuera de alcance de la primera
                version — dry-run cubre la verificacion sin necesitarlo).
            timeout: Timeout en segundos
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.environment = environment.lower()
        self.timeout = timeout
        self.time_offset_ms = 0
        self.base_url = self.MAINNET_BASE_URL

        if self.environment != "paper":
            self.session = requests.Session()
            self.session.headers.update({"X-MBX-APIKEY": api_key})
            try:
                self._sync_time_offset()
            except Exception:
                self.time_offset_ms = 0
        else:
            self.session = None

        main_logger.info(f"BinanceSpotRestClient initialized. Environment={environment}")

    def is_paper(self) -> bool:
        return self.environment == "paper"

    def _sign_request(self, params: Dict[str, Any]) -> str:
        query_string = urlencode(params)
        signature = hmac.new(
            self.api_secret.encode(), query_string.encode(), hashlib.sha256
        ).hexdigest()
        return f"{query_string}&signature={signature}"

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = False,
    ) -> Dict[str, Any]:
        if self.is_paper():
            main_logger.debug(f"[PAPER] {method} {endpoint} {params}")
            return {}

        url = f"{self.base_url}{endpoint}"
        params = params or {}

        if signed:
            params["timestamp"] = int(time.time() * 1000 + self.time_offset_ms)
            params["recvWindow"] = 5000
            signed_params = self._sign_request(params)
            url = f"{url}?{signed_params}"
            response = self.session.request(method, url, timeout=self.timeout)
        else:
            response = self.session.request(method, url, params=params, timeout=self.timeout)

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            body = ""
            try:
                body = response.text[:500]
            except Exception:
                body = "<no-body>"

            status = response.status_code if response is not None else 0

            if status in (429, 418):
                retry_after = int(response.headers.get("Retry-After", 0))
                for attempt in range(1, 4):
                    wait = retry_after if retry_after > 0 else min(5 * (2 ** (attempt - 1)), 60)
                    main_logger.warning(
                        "Rate-limited (%s) on %s %s — waiting %ds (attempt %d/3)",
                        status, method, endpoint, wait, attempt,
                    )
                    time.sleep(wait)
                    retry_r = self.session.request(
                        method,
                        url if not signed else f"{self.base_url}{endpoint}?{self._sign_request({**params, 'timestamp': int(time.time()*1000 + self.time_offset_ms)})}",
                        **({"params": params} if not signed else {}),
                        timeout=self.timeout,
                    )
                    if retry_r.status_code not in (429, 418):
                        retry_r.raise_for_status()
                        return retry_r.json()
                raise requests.HTTPError(f"Rate limit persists after 3 retries | {exc}", response=response) from exc

            if signed and '"code":-1021' in body:
                try:
                    self._sync_time_offset()
                    retry_params = dict(params)
                    retry_params["timestamp"] = int(time.time() * 1000 + self.time_offset_ms)
                    retry_signed = self._sign_request(retry_params)
                    retry_url = f"{self.base_url}{endpoint}?{retry_signed}"
                    retry_response = self.session.request(method, retry_url, timeout=self.timeout)
                    retry_response.raise_for_status()
                    return retry_response.json()
                except Exception:
                    pass

            raise requests.HTTPError(f"{exc} | response={body}", response=response) from exc
        return response.json()

    def _sync_time_offset(self) -> None:
        if self.is_paper() or self.session is None:
            self.time_offset_ms = 0
            return
        try:
            resp = self.session.get(f"{self.base_url}/api/v3/time", timeout=self.timeout)
            resp.raise_for_status()
            server_time = int(resp.json().get("serverTime", 0))
            if server_time > 0:
                self.time_offset_ms = server_time - int(time.time() * 1000)
        except Exception:
            self.time_offset_ms = 0

    # ===== INFORMACIÓN DE EXCHANGE =====

    def get_exchange_info(self, symbol: str) -> Dict[str, Any]:
        """Obtiene informacion del simbolo (filters: LOT_SIZE, NOTIONAL, etc)."""
        data = self._request("GET", "/api/v3/exchangeInfo", {"symbol": symbol})
        symbols = data.get("symbols", [])
        if not symbols:
            raise ValueError(f"Symbol {symbol} not found")
        return symbols[0]

    def get_avg_price(self, symbol: str) -> Dict[str, Any]:
        """Precio promedio actual (publico, sin firma) — util para estimar notional en dry-run."""
        return self._request("GET", "/api/v3/avgPrice", {"symbol": symbol})

    # ===== CUENTA =====

    def get_account(self) -> Dict[str, Any]:
        return self._request("GET", "/api/v3/account", signed=True)

    def get_balances(self) -> Dict[str, Dict[str, float]]:
        """Balances no nulos: {asset: {"free": x, "locked": y}}. Distinto del
        shape de Futuros (assets[]/walletBalance) — Spot es free/locked."""
        account = self.get_account()
        balances: Dict[str, Dict[str, float]] = {}
        for b in account.get("balances", []):
            free = float(b.get("free", 0))
            locked = float(b.get("locked", 0))
            if free > 0 or locked > 0:
                balances[b["asset"]] = {"free": free, "locked": locked}
        return balances

    # ===== ÓRDENES =====

    def create_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: Optional[float] = None,
        quote_order_qty: Optional[float] = None,
        price: Optional[float] = None,
        client_order_id: Optional[str] = None,
        time_in_force: str = "GTC",
    ) -> Dict[str, Any]:
        """
        Crea una orden Spot. A diferencia de Futuros, NO existe positionSide
        ni reduceOnly — una venta simplemente reduce el balance del activo.

        Args:
            symbol: ETHUSDT
            side: BUY, SELL
            order_type: MARKET, LIMIT
            quantity: cantidad del activo base (excluyente con quote_order_qty)
            quote_order_qty: cantidad en el activo cotizado (USDT) — util para
                comprar "por $X" exacto en MARKET sin calcular qty a mano
            price: precio (si es LIMIT)
            client_order_id: ID unico (idempotencia)
            time_in_force: GTC, IOC, FOK
        """
        params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
        }
        if quantity is not None:
            params["quantity"] = _fmt_decimal(quantity)
        if quote_order_qty is not None:
            params["quoteOrderQty"] = _fmt_decimal(quote_order_qty)
        if str(order_type).upper() != "MARKET":
            params["timeInForce"] = time_in_force
        if price:
            params["price"] = _fmt_decimal(price)
        if client_order_id:
            params["newClientOrderId"] = client_order_id

        return self._request("POST", "/api/v3/order", params, signed=True)

    def cancel_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"symbol": symbol}
        if order_id:
            params["orderId"] = order_id
        elif client_order_id:
            params["origClientOrderId"] = client_order_id
        return self._request("DELETE", "/api/v3/order", params, signed=True)

    def get_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"symbol": symbol}
        if order_id:
            params["orderId"] = order_id
        elif client_order_id:
            params["origClientOrderId"] = client_order_id
        return self._request("GET", "/api/v3/order", params, signed=True)

    def get_open_orders(self, symbol: str) -> List[Dict[str, Any]]:
        return self._request("GET", "/api/v3/openOrders", {"symbol": symbol}, signed=True)

    def close(self) -> None:
        if self.session:
            self.session.close()
