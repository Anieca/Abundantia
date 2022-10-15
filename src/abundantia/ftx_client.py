import json
import time
import hmac
import math
from datetime import datetime
from typing import Any, Literal

import requests
from loguru import logger
from pydantic import BaseModel


class FTXKline(BaseModel):
    startTime: datetime
    time: float
    open: float
    high: float
    low: float
    close: float
    volume: float


class FTXOrderRequestParams(BaseModel, frozen=True):
    market: str
    side: Literal["sell", "buy"]
    price: float
    type: Literal["limit", "market"]
    size: float
    reduceOnly: bool = False
    ioc: bool = False
    postOnly: bool = False
    clientId: str | None = None


class FTXOrderResponse(BaseModel, frozen=True):
    id: int
    clientId: str | None
    market: str
    type: Literal["limit", "market"]
    side: Literal["sell", "buy"]
    status: Literal["new", "open", "closed"]
    filledSize: float
    remainingSize: float
    reduceOnly: bool
    liquidation: bool | None
    avgFillPrice: float | None
    postOnly: bool
    ioc: bool
    createdAt: datetime
    future: str


class FTXClient:
    HTTP_URL = "https://ftx.com/api"
    MAX_RECORD_PER_REQUEST = 1500

    def __init__(self, api_key: str | None = None, api_secret: str | None = None) -> None:
        self.api_key = api_key
        self.api_secret = api_secret

    def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        auth: bool = False,
    ) -> Any:
        return self._request("GET", endpoint, params, headers, auth)

    def post(self, endpoint: str, params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None) -> Any:
        return self._request("POST", endpoint, params, headers, True)

    def delete(self, endpoint: str, params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None) -> Any:
        return self._request("DELETE", endpoint, params, headers, True)

    def _request(
        self,
        method: Literal["GET", "POST", "DELETE"],
        endpoint: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        auth: bool = False,
    ) -> Any:

        headers = {} if headers is None else headers
        if auth:
            auth_headers = self._generate_auth_headers(method, endpoint, params)
            headers.update(auth_headers)

        params = {} if params is None else params

        if method in ("GET"):
            response = requests.request(method, self.HTTP_URL + endpoint, params=params, headers=headers)
        else:
            response = requests.request(method, self.HTTP_URL + endpoint, data=json.dumps(params), headers=headers)

        response.raise_for_status()
        result = response.json()
        assert result.get("success", False), result
        return result.get("result")

    def _generate_auth_headers(
        self, method: Literal["GET", "POST", "DELETE"], endpoint: str, params: dict[str, Any] | None = None
    ) -> dict[str, str]:
        assert self.api_key is not None and self.api_secret is not None
        assert endpoint.startswith("/")

        ts = str(int(time.time() * 1000))
        signature_payload = f"{ts}{method}/api{endpoint}".encode()
        if params is not None:
            signature_payload += json.dumps(params).encode("utf-8")

        signature = hmac.new(self.api_secret.encode(), signature_payload, "sha256").hexdigest()
        return {"FTX-KEY": self.api_key, "FTX-SIGN": signature, "FTX-TS": ts}

    def cancel_all_orders(self, market: str) -> None:
        result = self.delete("/orders", params={"market": market})
        logger.debug(result)

    def place_order(self, params: FTXOrderRequestParams) -> FTXOrderResponse:
        result = self.post("/orders", params=params.dict())
        logger.debug(result)
        return FTXOrderResponse.parse_obj(result)

    def get_open_orders(self) -> tuple[FTXOrderResponse, ...]:
        result = self.get("/orders", auth=True)
        logger.debug(result)
        return tuple(FTXOrderResponse.parse_obj(r) for r in result)

    def get_klines(
        self, symbol: str, interval: int, start: datetime, end: datetime, max_try: int = 100
    ) -> tuple[FTXKline, ...]:

        assert self.is_aware(start) and self.is_aware(end)
        assert self._estimate_num_request(start, end, interval) <= max_try

        endpoint = f"/markets/{symbol}/candles"

        n_try = 0
        klines: list[FTXKline] = []
        while start != end and n_try < max_try:
            n_try += 1
            params = {
                "resolution": interval,
                "start_time": int(start.timestamp()),
                "end_time": int(end.timestamp()) - 1,
            }
            result = self.get(endpoint=endpoint, params=params)
            klines_chunk = tuple(FTXKline.parse_obj(r) for r in result)
            klines += reversed(klines_chunk)

            if len(klines_chunk) == 0:
                break
            s, *_, e = klines_chunk
            logger.debug(f"Request: [{start} -> {end}] Current: [{s.startTime} -> {e.startTime}]")
            end = s.startTime

        return tuple(reversed(klines))

    @staticmethod
    def is_aware(d: datetime) -> bool:
        return d.tzinfo is not None and d.tzinfo.utcoffset(d) is not None

    @classmethod
    def _estimate_num_request(cls, start: datetime, end: datetime, interval: int) -> int:
        num_records = int((end - start).total_seconds() / interval)
        return math.ceil(num_records / cls.MAX_RECORD_PER_REQUEST)
