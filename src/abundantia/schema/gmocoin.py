from enum import Enum, auto
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


class GMOCoinSymbols(Enum):
    BTC = auto()
    ETH = auto()
    BCH = auto()
    LTC = auto()
    XRP = auto()
    XEM = auto()
    XLM = auto()
    XYM = auto()
    MONA = auto()
    BAT = auto()
    QTUM = auto()
    BTC_JPY = auto()
    ETH_JPY = auto()
    BCH_JPY = auto()
    LTC_JPY = auto()
    XRP_JPY = auto()
