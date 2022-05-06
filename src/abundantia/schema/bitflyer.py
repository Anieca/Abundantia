from enum import Enum, auto
from typing import Literal

from pydantic.dataclasses import dataclass


@dataclass(frozen=True)
class BitFlyerExecution:
    id: int
    side: Literal["SELL", "BUY", ""]
    price: float
    size: float
    exec_date: str
    buy_child_order_acceptance_id: str
    sell_child_order_acceptance_id: str


class BitFlyerSymbols(Enum):
    BTC_JPY = auto()
    XRP_JPY = auto()
    ETH_JPY = auto()
    XLM_JPY = auto()
    MONA_JPY = auto()
    ETH_BTC = auto()
    BCH_BTC = auto()
    FX_BTC_JPY = auto()
