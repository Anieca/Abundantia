import pickle
from typing import Any


class PickleClient:
    @classmethod
    def dump(cls, filename: str, obj: Any) -> None:
        with open(filename, "wb") as f:
            pickle.dump(obj, f)

    @classmethod
    def load(cls, filename: str) -> Any:
        with open(filename, "rb") as f:
            return pickle.load(f)
