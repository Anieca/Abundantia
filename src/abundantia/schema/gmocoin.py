from __future__ import annotations

from dataclasses import dataclass


@dataclass
class GMOCoinExecution:
    price: float | str
    side: str
    size: float | str
    timestamp: str

    def __post_init__(self):
        self.price = float(self.price)
        self.size = float(self.size)


@dataclass
class GMOCoinKline:
    openTime: str | int
    open: str | float
    high: str | float
    low: str | float
    close: str | float
    volume: str | float

    def __post_init__(self):
        self.open = float(self.open)
        self.high = float(self.high)
        self.low = float(self.low)
        self.close = float(self.close)
        self.volume = float(self.volume)
