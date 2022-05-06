import pandera as pa
from pandera.typing import Series
from pydantic.dataclasses import dataclass


@dataclass(frozen=True)
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


class CommonKlineSchema(pa.SchemaModel):
    exchange: Series[str] = pa.Field(nullable=False)
    symbol: Series[str] = pa.Field(nullable=False)
    interval: Series[int] = pa.Field(gt=0, nullable=False)
    open_time: Series[int] = pa.Field(gt=0, nullable=False)
    open: Series[float] = pa.Field(nullable=True)
    high: Series[float] = pa.Field(nullable=True)
    low: Series[float] = pa.Field(nullable=True)
    close: Series[float] = pa.Field(nullable=True)
    volume: Series[float] = pa.Field(nullable=True)

    class Config:
        strict = True
