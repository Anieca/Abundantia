import pandas as pd
import sqlalchemy
from sqlalchemy.orm import Session

from abundantia.adaptors import mapper_registry
from abundantia.schema.common import CommonKlineModel


class SQLiteClient:
    def __init__(self, file_path: str = "sqlite:///resources/db.sqlite3") -> None:
        self.engine = sqlalchemy.create_engine(file_path, echo=True)
        # self.session = sessionmaker(bind=self.engine)

    def create_tables(self) -> None:
        mapper_registry.metadata.create_all(bind=self.engine)

    def drop_common_kline_table(self) -> None:
        CommonKlineModel.__table__.drop(bind=self.engine)

    def insert_common_klines(self, klines: pd.DataFrame) -> None:

        with Session(self.engine) as sess:
            for kline in klines.to_dict("records"):
                sess.add(CommonKlineModel(**kline))
            sess.commit()
