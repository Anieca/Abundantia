from pydantic import BaseModel


class BitFlyerExecution(BaseModel):
    id: int
    side: str
    price: float
    size: float
    exec_date: str
    buy_child_order_acceptance_id: str
    sell_child_order_acceptance_id: str


class BitFlyer(BaseModel):
    http_url: str = "https://api.bitflyer.com"
    ws_url: str = "wss"
    fx_btc_jpy: str = "FX_BTC_JPY"
    btc_jpy: str = "BTC_JPY"
    symbols: list[str] = [fx_btc_jpy, btc_jpy]
