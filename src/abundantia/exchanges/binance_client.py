from __future__ import annotations

from datetime import datetime

import pandas as pd
import pandera as pa
from pandera.typing import DataFrame

from abundantia.exchanges.base import BaseClient, Symbols
from abundantia.logger import setup_logger
from abundantia.schema.binance import BinanceKline, BinanceSymbols
from abundantia.schema.common import CommonKlineSchema

logger = setup_logger(__name__)


class BinanceClient(BaseClient):
    NAME: str = "Binance"
    HTTP_URL: str = "https://api1.binance.com"
    SYMBOLS = BinanceSymbols

    INTERVALS: dict[int, str] = {
        60: "1m",
        180: "3m",
        300: "5m",
        900: "15m",
        1800: "30m",
        3600: "1h",
        7200: "2h",
        14400: "4h",
        21600: "6h",
        28800: "8h",
        43200: "12h",
        86400: "1d",
        259200: "3d",
        604800: "1w",
    }

    @classmethod
    def get_klines_by_http(
        cls, symbol: BinanceSymbols, interval: int, start_date: datetime, end_date: datetime, limit: int = 1000
    ) -> list[BinanceKline]:

        klines: list[BinanceKline] = []
        params = {
            "symbol": symbol.name,
            "interval": cls.convert_interval_to_specific(interval),
            "startTime": int(start_date.timestamp() * 1000),
            "endTime": int(end_date.timestamp() * 1000),
            "limit": limit,
        }

        result = cls.get("/api/v3/klines", params)

        if result is None:
            return klines

        klines = [BinanceKline(*k) for k in result]
        return klines

    @classmethod
    @pa.check_types
    def convert_klines_to_common_klines(
        cls,
        symbol: BinanceSymbols,
        interval: int,
        start_date: datetime,
        end_date: datetime,
        binance_klines: list[BinanceKline],
    ) -> DataFrame[CommonKlineSchema]:

        sub_klines = pd.DataFrame(binance_klines)
        sub_klines["time"] = pd.to_datetime(sub_klines["open_time"], unit="ms", utc=True).dt.tz_convert(cls.TZ)
        sub_klines = sub_klines.set_index("time").sort_index()
        del sub_klines["open_time"]

        klines = cls._create_common_klines(symbol.name, interval, start_date, end_date, sub_klines)
        return klines

    @classmethod
    def convert_interval_to_specific(cls, interval: int) -> str:
        return cls.INTERVALS[interval]

    def get_klines(
        self, symbol: Symbols, interval: int, start_date: datetime, end_date: datetime
    ) -> DataFrame[CommonKlineSchema]:

        self._check_invalid_datetime(start_date, end_date)

        limit = 1000
        date = start_date
        binance_klines = []
        while date < end_date:
            klines_chunk = self.get_klines_by_http(symbol, interval, date, end_date)
            binance_klines += klines_chunk

            if len(klines_chunk) != limit:
                break

            *_, latest_kline = klines_chunk
            date = datetime.fromtimestamp(latest_kline.open_time / 1000 + interval)

        klines = self.convert_klines_to_common_klines(symbol, interval, start_date, end_date, binance_klines)
        return klines
