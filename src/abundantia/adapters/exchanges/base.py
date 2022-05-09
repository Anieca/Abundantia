from __future__ import annotations

from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import Any, Literal

import pandas as pd
import pandera as pa
import requests
from dateutil.tz import gettz
from pandera.typing import DataFrame

from abundantia.logger import setup_logger
from abundantia.schema.common import CommonKlineSchema

logger = setup_logger(__name__)


class BaseClient(metaclass=ABCMeta):
    NAME = ""
    HTTP_URL = ""
    WS_URL = ""

    TZ = gettz()
    FREQ_INTERVAL_MAP: dict[str, int] = {"S": 1, "T": 60, "H": 3600}
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
        self, symbol: Any, interval: int, start_date: datetime, end_date: datetime
    ) -> DataFrame[CommonKlineSchema]:
        pass

    @staticmethod
    def get_date_range(time_index: pd.Index, freq: str, inclusive: Literal["both", "neither"]) -> pd.DatetimeIndex:
        start: pd.Timestamp = time_index.min().floor(freq=freq)
        end: pd.Timestamp = time_index.max().floor(freq=freq)
        return pd.date_range(start, end, name="open_time", freq=freq, inclusive=inclusive)

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
        start_date = start_date.replace(tzinfo=cls.TZ)
        end_date = end_date.replace(tzinfo=cls.TZ)
        index = pd.date_range(
            start_date, end_date, freq=cls.convert_interval_to_freq(interval), inclusive="left", name="time", tz=cls.TZ
        )
        klines = pd.DataFrame(index=index).join(sub_klines).reset_index()
        klines["exchange"] = cls.NAME
        klines["symbol"] = symbol_str
        klines["interval"] = interval  # overwrite response
        klines["open_time"] = klines["time"].map(datetime.timestamp).mul(1000).astype(int)

        klines = klines[list(CommonKlineSchema.to_schema().columns)]

        return klines
