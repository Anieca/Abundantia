import pandas as pd


class CSVClient:
    @classmethod
    def dump(self, filename, obj):
        if not any([isinstance(obj, list), isinstance(obj, pd.DataFrame)]):
            raise TypeError
        pd.DataFrame(obj).to_csv(filename, index=False)

    @classmethod
    def load(self, filename):
        return pd.read_csv(filename)
