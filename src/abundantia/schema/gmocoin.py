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
