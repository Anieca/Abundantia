from dataclasses import dataclass, field


@dataclass
class BitFlyerExecution:
    id: int
    side: str
    price: float
    size: float
    exec_date: str
    buy_child_order_acceptance_id: str
    sell_child_order_acceptance_id: str


@dataclass
class BitFlyer:
    http_url: str = "https://api.bitflyer.com"
    ws_url: str = "wss"
    fx_btc_jpy: str = "FX_BTC_JPY"
    btc_jpy: str = "BTC_JPY"
    symbols: list[str] = field(default_factory=list)

    def __post_init__(self):
        self.symbols += [self.fx_btc_jpy, self.btc_jpy]
