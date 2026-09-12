"""
Cliente REST para Binance Futures.
Conecta con los endpoints principales.
"""

import hashlib
import hmac
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests

from src.observability.logger import main_logger


class BinanceRestClient:
    """Cliente REST para Binance Futures API."""
    
    # URLs
    MAINNET_BASE_URL = "https://fapi.binance.com"
    TESTNET_BASE_URL = "https://testnet.binancefuture.com"
    
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        environment: str = "paper",
        timeout: int = 10
    ):
        """
        Inicializa el cliente.
        
        Args:
            api_key: API Key
            api_secret: API Secret
            environment: "live", "testnet", o "paper"
            timeout: Timeout en segundos
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.environment = environment.lower()
        self.timeout = timeout
        self.time_offset_ms = 0
        
        # Seleccionar URL según ambiente
        if self.environment == "testnet":
            self.base_url = self.TESTNET_BASE_URL
        elif self.environment == "live":
            self.base_url = self.MAINNET_BASE_URL
        else:  # paper
            self.base_url = self.MAINNET_BASE_URL  # Para simulación usamos URLs reales pero sin conectar
        
        # Session solo si no es paper trading
        if self.environment != "paper":
            self.session = requests.Session()
            self.session.headers.update({
                "X-MBX-APIKEY": api_key
            })
            try:
                self._sync_time_offset()
            except Exception:
                self.time_offset_ms = 0
        else:
            self.session = None
        
        main_logger.info(f"BinanceRestClient initialized. Environment={environment}")
    
    def is_paper(self) -> bool:
        """¿Es modo paper trading?"""
        return self.environment == "paper"
    
    def _sign_request(self, params: Dict[str, Any]) -> str:
        """Firma una solicitud."""
        query_string = urlencode(params)
        signature = hmac.new(
            self.api_secret.encode(),
            query_string.encode(),
            hashlib.sha256
        ).hexdigest()
        return f"{query_string}&signature={signature}"
    
    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = False
    ) -> Dict[str, Any]:
        """
        Realiza una solicitud HTTP.
        
        Args:
            method: GET, POST, DELETE
            endpoint: Ruta del endpoint
            params: Parámetros
            signed: ¿Necesita firma?
        
        Returns:
            Respuesta JSON
        
        Raises:
            Exception si falla
        """
        # Paper trading no hace requests reales
        if self.is_paper():
            main_logger.debug(f"[PAPER] {method} {endpoint} {params}")
            return {}
        
        url = f"{self.base_url}{endpoint}"
        params = params or {}
        
        # Agregar timestamp si es signed
        if signed:
            params["timestamp"] = int(time.time() * 1000 + self.time_offset_ms)
            params["recvWindow"] = 5000
            signed_params = self._sign_request(params)
            url = f"{url}?{signed_params}"
            response = self.session.request(
                method,
                url,
                timeout=self.timeout
            )
        else:
            response = self.session.request(
                method,
                url,
                params=params,
                timeout=self.timeout
            )
        
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            body = ""
            try:
                body = response.text[:500]
            except Exception:
                body = "<no-body>"

            status = response.status_code if response is not None else 0

            # 429 / 418: rate-limited — backoff exponencial y reintentar
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

            # -1021: timestamp drift — resincronizar y reintentar una vez
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
        """Sincroniza offset local contra server time de Binance."""
        if self.is_paper() or self.session is None:
            self.time_offset_ms = 0
            return
        try:
            resp = self.session.get(f"{self.base_url}/fapi/v1/time", timeout=self.timeout)
            resp.raise_for_status()
            payload = resp.json()
            server_time = int(payload.get("serverTime", 0))
            if server_time > 0:
                self.time_offset_ms = server_time - int(time.time() * 1000)
        except Exception:
            # Si falla la sincronización, usamos offset 0 y continuamos.
            self.time_offset_ms = 0
    
    def get_server_time(self) -> int:
        """Return Binance server time and verify REST connectivity.

        Unlike ``_sync_time_offset``, errors propagate so recovery paths do not
        treat a failed request as a healthy connection.
        """
        payload = self._request("GET", "/fapi/v1/time")
        server_time = int(payload.get("serverTime", 0))
        if server_time <= 0:
            raise ValueError("Binance returned an invalid serverTime")
        self.time_offset_ms = server_time - int(time.time() * 1000)
        return server_time

    # ===== INFORMACIÓN DE EXCHANGE =====
    
    def get_exchange_info(self, symbol: str) -> Dict[str, Any]:
        """Obtiene información del símbolo."""
        data = self._request("GET", "/fapi/v1/exchangeInfo")
        
        # Buscar el símbolo
        for s in data.get("symbols", []):
            if s["symbol"] == symbol:
                return s
        
        raise ValueError(f"Symbol {symbol} not found")
    
    # ===== KLINES (VELAS) =====
    
    def get_klines(
        self,
        symbol: str,
        interval: str,
        limit: int = 100,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None
    ) -> List[List[Any]]:
        """
        Obtiene velas históricas.
        
        Retorna lista de velas:
        [open_time, open, high, low, close, volume, close_time, quote_asset_volume, ...]
        """
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        
        return self._request("GET", "/fapi/v1/klines", params)
    
    # ===== CUENTA =====
    
    def get_account(self) -> Dict[str, Any]:
        """Obtiene información de la cuenta."""
        return self._request("GET", "/fapi/v2/account", signed=True)
    
    def get_balance(self) -> Dict[str, float]:
        """Obtiene balance de USDT."""
        account = self.get_account()
        balance = {}
        
        for asset in account.get("assets", []):
            if float(asset.get("walletBalance", 0)) > 0:
                balance[asset["asset"]] = float(asset["walletBalance"])
        
        return balance
    
    # ===== POSICIONES =====
    
    def get_position(self, symbol: str) -> Dict[str, Any]:
        """Obtiene información de la posición."""
        account = self.get_account()
        
        for pos in account.get("positions", []):
            if pos["symbol"] == symbol:
                return pos
        
        return {}  # Sin posición
    
    # ===== ÓRDENES =====
    
    def create_order(
        self,
        symbol: str,
        side: str,
        position_side: str,
        order_type: str,
        quantity: float,
        price: Optional[float] = None,
        client_order_id: Optional[str] = None,
        reduce_only: bool = False,
        time_in_force: str = "GTC"
    ) -> Dict[str, Any]:
        """
        Crea una orden.
        
        Args:
            symbol: ETHUSDT
            side: BUY, SELL
            position_side: LONG, SHORT
            order_type: MARKET, LIMIT
            quantity: Cantidad
            price: Precio (si es LIMIT)
            client_order_id: ID único (idempotencia)
            reduce_only: ¿Orden de cierre?
            time_in_force: GTC, IOC, FOK
        
        Returns:
            Respuesta de la orden
        """
        params = {
            "symbol": symbol,
            "side": side,
            "positionSide": position_side,
            "type": order_type,
            "quantity": quantity,
        }

        # Binance rechaza reduceOnly=false en entradas; solo enviar cuando aplica.
        if reduce_only:
            params["reduceOnly"] = True

        # timeInForce aplica a tipos limit/stop-limit, no a MARKET.
        if str(order_type).upper() != "MARKET":
            params["timeInForce"] = time_in_force
        
        if price:
            params["price"] = price
        if client_order_id:
            # "newClientOrderId" es el nombre real que exige la API de
            # Binance Futures (POST /fapi/v1/order) -- "clientOrderId" (sin
            # el prefijo "new") no es un parametro reconocido y Binance lo
            # ignora en silencio, generando su propio ID aleatorio para la
            # orden. Efecto real (incidente del 12/09/2026): get_order_status
            # nunca podia encontrar la orden por el ID que este codigo
            # trackeaba (porque Binance nunca la guardo con ese ID), asi que
            # confirm_fill jamas veia "FILLED" aunque la orden ya se hubiera
            # llenado -- el codigo reintentaba y colocaba una orden real
            # NUEVA cada vez, apilando hasta 3x el tamaño de posicion
            # pretendido sin que la DB se enterara de ninguna.
            params["newClientOrderId"] = client_order_id
        
        def _is_err(payload: str, code: str) -> bool:
            return f'"code":{code}' in payload or f'"code":"{code}"' in payload

        last_exc: Optional[Exception] = None
        attempts = [dict(params)]

        # Reintento 1: cuentas one-way suelen requerir BOTH en vez de LONG/SHORT.
        if params.get("positionSide") != "BOTH":
            retry_both = dict(params)
            retry_both["positionSide"] = "BOTH"
            attempts.append(retry_both)

        # Reintento 2: algunos endpoints rechazan reduceOnly aun cuando se envía true.
        if "reduceOnly" in params:
            retry_no_reduce = dict(params)
            retry_no_reduce.pop("reduceOnly", None)
            attempts.append(retry_no_reduce)

            retry_both_no_reduce = dict(retry_no_reduce)
            retry_both_no_reduce["positionSide"] = "BOTH"
            attempts.append(retry_both_no_reduce)

        for i, attempt_params in enumerate(attempts, start=1):
            try:
                return self._request("POST", "/fapi/v1/order", attempt_params, signed=True)
            except requests.HTTPError as exc:
                last_exc = exc
                body = str(exc)
                retriable = (
                    _is_err(body, "-4061")   # position mode mismatch
                    or _is_err(body, "-1106")  # unexpected parameter
                    or _is_err(body, "-2022")  # reduceOnly rejected — retry without it
                )
                if retriable and i < len(attempts):
                    continue
                raise

        if last_exc:
            raise last_exc
        raise RuntimeError("Order placement failed unexpectedly")
    
    def cancel_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Cancela una orden."""
        params = {"symbol": symbol}
        
        if order_id:
            params["orderId"] = order_id
        elif client_order_id:
            params["origClientOrderId"] = client_order_id
        else:
            raise ValueError("order_id or client_order_id required")
        
        return self._request("DELETE", "/fapi/v1/order", params, signed=True)
    
    def get_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Obtiene el estado de una orden."""
        params = {"symbol": symbol}
        
        if order_id:
            params["orderId"] = order_id
        elif client_order_id:
            params["origClientOrderId"] = client_order_id
        else:
            raise ValueError("order_id or client_order_id required")
        
        return self._request("GET", "/fapi/v1/order", params, signed=True)
    
    def get_open_orders(self, symbol: str) -> List[Dict[str, Any]]:
        """Obtiene todas las órdenes abiertas."""
        params = {"symbol": symbol}
        response = self._request("GET", "/fapi/v1/openOrders", params, signed=True)
        return response if isinstance(response, list) else []
    
    # ===== MARK PRICE & FUNDING =====
    
    def get_mark_price(self, symbol: str) -> Dict[str, Any]:
        """Obtiene el mark price actual."""
        params = {"symbol": symbol}
        return self._request("GET", "/fapi/v1/premiumIndex", params)
    
    def get_funding_rate(self, symbol: str) -> Dict[str, Any]:
        """Obtiene el funding rate actual."""
        params = {"symbol": symbol}
        return self._request("GET", "/fapi/v1/fundingRate", params)
    
    def get_funding_rate_history(
        self,
        symbol: str,
        limit: int = 100,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Obtiene histórico de funding rates."""
        params = {
            "symbol": symbol,
            "limit": limit
        }
        
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time
        
        return self._request("GET", "/fapi/v1/fundingRate", params)
    
    # ===== STREAM DATA =====
    
    def get_listen_key(self) -> str:
        """Obtiene una listen key para user data stream.
        USER_STREAM endpoint — solo necesita X-MBX-APIKEY header, no signature."""
        response = self._request("POST", "/fapi/v1/listenKey", signed=False)
        return response.get("listenKey")

    def keep_alive_listen_key(self, listen_key: str) -> None:  # noqa: ARG002
        """Extiende la validez de una listen key 60 minutos.
        USER_STREAM endpoint — solo necesita X-MBX-APIKEY header, no params ni signature."""
        self._request("PUT", "/fapi/v1/listenKey", signed=False)
    
    def close(self) -> None:
        """Cierra la sesión."""
        if self.session is not None:
            self.session.close()
