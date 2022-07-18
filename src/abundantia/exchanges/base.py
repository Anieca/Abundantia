from __future__ import annotations

from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import Any, TypeVar

import pandas as pd
import pandera as pa
import requests
from dateutil.tz import gettz
from pandera.typing import DataFrame

from abundantia.logger import setup_logger
from abundantia.schema.binance import BinanceSymbols
from abundantia.schema.bitflyer import BitFlyerSymbols
from abundantia.schema.bybit import BybitInversePerpetualSymbols
from abundantia.schema.common import CommonKlineSchema
from abundantia.schema.ftx import FTXSymbols
from abundantia.schema.gmocoin import GMOCoinSymbols

logger = setup_logger(__name__)

Symbols = TypeVar("Symbols", BinanceSymbols, BybitInversePerpetualSymbols, FTXSymbols, BitFlyerSymbols, GMOCoinSymbols)


class BaseClient(metaclass=ABCMeta):
    NAME: str = ""
    HTTP_URL: str = ""
    WS_URL: str = ""

    TZ = gettz()
    FREQ_INTERVAL_MAP: dict[str, int] = {"S": 1, "T": 60, "H": 3600, "D": 86400}
    INTERVAL_FREQ_MAP: dict[int, str] = {v: k for k, v in FREQ_INTERVAL_MAP.items()}

    def __init__(self, duration: int = 1) -> None:
        self.duration = duration

    @classmethod
    def get(cls, url: str, params: dict[str, Any] | None = None) -> Any | None:
        logger.info(params)
        result: Any | None = None

        try:
            response = requests.get(cls.HTTP_URL + url, params=params)
            result = response.json()
        except Exception:
            logger.exception("requests error.")

        return result

    @abstractmethod
    def get_klines(
        self, symbol: Symbols, interval: int, start_date: datetime, end_date: datetime
    ) -> DataFrame[CommonKlineSchema]:
        pass

    @classmethod
    def convert_freq_to_interval(cls, freq: str) -> int:

        unit_str = freq[-1]
        num_str = freq[:-1]

        num = int(num_str) if len(num_str) > 0 else 1
        unit = cls.FREQ_INTERVAL_MAP[unit_str]

        return num * unit

    @classmethod
    def convert_interval_to_freq(cls, interval: int) -> str:
        unit_num: int | None = None
        for unit_candidate in sorted(cls.FREQ_INTERVAL_MAP.values(), reverse=True):
            if interval % unit_candidate == 0:
                unit_num = unit_candidate
                break

        if unit_num is None:
            raise ValueError

        num = interval // unit_num
        unit = cls.INTERVAL_FREQ_MAP[unit_num]
        return f"{num}{unit}"

    @classmethod
    @pa.check_types
    def _create_common_klines(
        cls, symbol_str: str, interval: int, start_date: datetime, end_date: datetime, sub_klines: pd.DataFrame
    ) -> DataFrame[CommonKlineSchema]:
        start_date = cls.convert_aware_datetime(start_date)
        end_date = cls.convert_aware_datetime(end_date)
        index = pd.date_range(
            start_date, end_date, freq=cls.convert_interval_to_freq(interval), inclusive="left", name="time", tz=cls.TZ
        )
        klines = pd.DataFrame(index=index).join(sub_klines).reset_index()
        klines["exchange"] = cls.NAME
        klines["symbol"] = symbol_str
        klines["interval"] = interval
        klines["open_time"] = klines["time"].map(datetime.timestamp).mul(1000).astype(int)

        klines = klines[list(CommonKlineSchema.to_schema().columns)]

        return klines

    @classmethod
    def _check_invalid_datetime(cls, start_date: datetime, end_date: datetime) -> None:
        if cls.is_aware(start_date) or cls.is_aware(end_date):
            raise ValueError

        if start_date >= end_date:
            logger.error(f"Must be start_date < end_date. start_date={start_date}, end_date={end_date}.")
            raise ValueError

    @staticmethod
    def is_aware(d: datetime) -> bool:
        return d.tzinfo is not None and d.tzinfo.utcoffset(d) is not None

    @classmethod
    def convert_aware_datetime(cls, d: datetime) -> datetime:
        return d if cls.is_aware(d) else d.replace(tzinfo=cls.TZ)
