from __future__ import annotations

from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import Any, Literal

import pandas as pd
import requests
from dateutil.tz import gettz
from pandera.typing import DataFrame

from abundantia.schema.common import CommonKlineSchema
from abundantia.utils import setup_logger

logger = setup_logger(__name__)


class BaseClient(metaclass=ABCMeta):
    tz = gettz()
    http_url = ""

    def __init__(self, duration: int = 1) -> None:
        self.duration = duration

    def get(self, url: str, params: dict[str, Any] | None = None) -> Any | None:
        logger.info(params)
        result: Any | None = None

        try:
            response = requests.get(self.http_url + url, params=params)
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
