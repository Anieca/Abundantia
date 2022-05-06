from __future__ import annotations

from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import Any

import requests
from dateutil.tz import gettz
from pandera.typing import DataFrame

from abundantia.schema import CommonKlineSchema
from abundantia.utils import setup_logger


class BaseClient(metaclass=ABCMeta):
    def __init__(self, duration: int = 1, log_level: str = "DEBUG") -> None:
        self.duration = duration
        self.logger = setup_logger(self.__class__.__name__, log_level)
        self.tz = gettz()

    def get(self, url: str, params: dict[str, Any]) -> Any | None:
        self.logger.info(params)
        result: Any | None = None

        try:
            response = requests.get(url, params=params)
            result = response.json()
        except Exception:
            self.logger.exception("requests error.")

        return result

    @abstractmethod
    def get_klines(
        self, symbol: Any, interval: int, start_date: datetime, end_date: datetime
    ) -> DataFrame[CommonKlineSchema]:
        pass
