import traceback
from typing import Any

import requests

from abundantia.utils import setup_logger


class BaseClient:
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
