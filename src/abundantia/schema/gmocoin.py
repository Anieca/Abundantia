from typing import Literal

from pydantic.dataclasses import dataclass


@dataclass
class GMOCoinExecution:
    price: float
    side: Literal["SELL", "BUY"]
    size: float
    timestamp: str


@dataclass
class GMOCoinKline:
    openTime: int
    open: float
    high: float
    low: float
    close: float
    volume: float
