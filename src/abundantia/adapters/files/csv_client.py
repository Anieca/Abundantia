from typing import Any

import pandas as pd


class CSVClient:
    @classmethod
    def dump(cls, filename: str, obj: Any, **kwargs: Any) -> None:
        if isinstance(obj, list):
            pd.DataFrame(obj).to_csv(filename, **kwargs)
        elif isinstance(obj, pd.DataFrame):
            obj.to_csv(filename, **kwargs)
        else:
            raise TypeError

    @classmethod
    def load(cls, filename: str, **kwargs: Any) -> pd.DataFrame:
        return pd.read_csv(filename, **kwargs)
