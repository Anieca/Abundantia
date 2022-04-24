from typing import Any

import pandas as pd


class CSVClient:
    @classmethod
    def dump(cls, filename: str, obj: Any, **kwargs: Any) -> None:
        if not isinstance(obj, list):
            raise TypeError
        pd.DataFrame(obj).to_csv(filename, **kwargs)

    @classmethod
    def load(cls, filename: str) -> pd.DataFrame:
        return pd.read_csv(filename)
