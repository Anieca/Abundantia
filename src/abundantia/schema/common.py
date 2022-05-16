import pandas as pd
import pandera as pa
from dateutil.tz import gettz
from pandera.typing import Series


class CommonKlineSchema(pa.SchemaModel):
    exchange: Series[str] = pa.Field(nullable=False)
    symbol: Series[str] = pa.Field(nullable=False)
    interval: Series[int] = pa.Field(gt=0, nullable=False)
    open_time: Series[int] = pa.Field(gt=0, nullable=False)
    time: Series[pd.DatetimeTZDtype] = pa.Field(nullable=False, dtype_kwargs={"tz": gettz()})
    open: Series[float] = pa.Field(nullable=True)
    high: Series[float] = pa.Field(nullable=True)
    low: Series[float] = pa.Field(nullable=True)
    close: Series[float] = pa.Field(nullable=True)
    volume: Series[float] = pa.Field(nullable=True)

    class Config:
        strict = True
