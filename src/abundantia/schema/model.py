from sqlalchemy import Column, DateTime, Float, Integer, String, Table

from abundantia.databases.mapper import mapper_registry


@mapper_registry.mapped
class CommonKlineModel:

    __table__ = Table(
        "common_kline",
        mapper_registry.metadata,
        Column("exchange", String(128), primary_key=True),
        Column("symbol", String(128), primary_key=True),
        Column("interval", Integer, primary_key=True),
        Column("open_time", Integer, primary_key=True),
        Column("time", DateTime),
        Column("open", Float),
        Column("high", Float),
        Column("low", Float),
        Column("close", Float),
        Column("volume", Float),
    )
