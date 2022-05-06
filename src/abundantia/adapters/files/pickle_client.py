import pickle
from typing import Any


class PickleClient:
    @staticmethod
    def dump(filename: str, obj: Any) -> None:
        with open(filename, "wb") as f:
            pickle.dump(obj, f)

    @staticmethod
    def load(filename: str) -> Any:
        with open(filename, "rb") as f:
            return pickle.load(f)
