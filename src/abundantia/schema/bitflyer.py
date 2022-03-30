from dataclasses import dataclass


@dataclass
class BitFlyerExecution:
    id: int
    side: str
    price: float
    size: float
    exec_date: str
    buy_child_order_acceptance_id: str
    sell_child_order_acceptance_id: str
