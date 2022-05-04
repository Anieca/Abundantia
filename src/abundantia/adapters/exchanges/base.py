from __future__ import annotations

import traceback
from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import Any

import requests
from pandera.typing import DataFrame

from abundantia.schema import CommonKlineSchema
from abundantia.utils import setup_logger


class BaseClient(metaclass=ABCMeta):
    def __init__(self, log_level: str = "DEBUG") -> None:
        self.logger = setup_logger(__name__, log_level)

    def get(self, url: str, params: dict[str, str]) -> Any | None:
        self.logger.info(params)
        result: Any | None = None

        try:
            response = requests.get(url, params=params)
            result = response.json()
        except Exception:
            self.logger.error(traceback.format_exc())

        return result

    @abstractmethod
    def get_klines(
        self, symbol: Any, interval: int, start_date: datetime, end_date: datetime
    ) -> DataFrame[CommonKlineSchema]:
        pass
