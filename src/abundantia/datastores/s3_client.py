from typing import Any

import pandas as pd


class S3Client:
    def __init__(self, bucket_name: str = "crypto-bucket-ishiirub") -> None:
        self.bucket_name = bucket_name

    def dump(self, filename: str, obj: Any, **kwargs: Any) -> None:
        if isinstance(obj, pd.DataFrame):
            obj.to_csv(f"s3://{self.bucket_name}/{filename}", **kwargs)
        elif isinstance(obj, list):
            pd.DataFrame(obj).to_csv(f"s3://{self.bucket_name}/{filename}", **kwargs)
        else:
            raise TypeError

    def load(self, filename: str) -> pd.DataFrame:
        return pd.read_csv(f"s3://{self.bucket_name}/{filename}")
