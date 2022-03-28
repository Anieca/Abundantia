import pandas as pd


class CSVClient:
    @classmethod
    def dump(self, filename, obj):
        if not isinstance(obj, list):
            raise TypeError
        pd.DataFrame(obj).to_csv(filename)
