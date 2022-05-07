from enum import Enum, auto

from pydantic.dataclasses import dataclass


class BybitInversePerpetualSymbols(Enum):
    BTCUSD = auto()
    ETHUSD = auto()
    EOSUSD = auto()
    XRPUSD = auto()
    DOTUSD = auto()


@dataclass(frozen=True)
class BybitKline:
    symbol: str
    interval: str
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: float
