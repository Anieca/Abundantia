from typing import Any

import pandas as pd


class CSVClient:
    @classmethod
    def dump(cls, filename: str, obj: Any, **kwargs: dict[str, Any]) -> None:
        if not isinstance(obj, list):
            raise TypeError
        pd.DataFrame(obj).to_csv(filename, **kwargs)

    @classmethod
    def load(cls, filename: str, **kwargs: dict[str, Any]) -> pd.DataFrame:
        return pd.read_csv(filename, **kwargs)
