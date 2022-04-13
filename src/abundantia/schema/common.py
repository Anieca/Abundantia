from pydantic.dataclasses import dataclass
from sqlalchemy import Column, Float, Integer, String, Table

from abundantia.adaptors import mapper_registry


@dataclass
class CommonKline:
    exchange: str
    symbol: str
    interval: int
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float


@mapper_registry.mapped
class CommonKlineModel(CommonKline):

    __table__ = Table(
        "common_kline",
        mapper_registry.metadata,
        Column("exchange", String(128), primary_key=True),
        Column("symbol", String(128), primary_key=True),
        Column("interval", Integer, primary_key=True),
        Column("open_time", Integer, primary_key=True),
        Column("open", Float),
        Column("high", Float),
        Column("low", Float),
        Column("close", Float),
        Column("volume", Float),
    )
