import traceback

import pandera as pa
import sqlalchemy
from pandera.typing import DataFrame
from sqlalchemy import insert
from sqlalchemy.engine.row import Row
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from abundantia.adapters.databases.mapper import mapper_registry
from abundantia.schema import CommonKlineSchema
from abundantia.schema.model import CommonKlineModel
from abundantia.utils import setup_logger


class SQLiteClient:
    def __init__(self, file_path: str = "sqlite:///resources/db.sqlite3", log_level: str = "DEBUG") -> None:
        self.engine = sqlalchemy.create_engine(file_path, echo=False)
        self.logger = setup_logger(self.__class__.__name__, log_level)

    def create_tables(self) -> None:
        mapper_registry.metadata.create_all(bind=self.engine)

    def drop_common_kline_table(self) -> None:
        CommonKlineModel.__table__.drop(bind=self.engine)

    @pa.check_types
    def insert_common_klines(self, klines: DataFrame[CommonKlineSchema]) -> None:
        duplicates = 0
        with Session(self.engine) as sess:
            for kline in klines.to_dict("records"):
                try:
                    statement = insert(CommonKlineModel).values(**kline)
                    sess.execute(statement)
                except IntegrityError:
                    duplicates += 1
                except Exception:
                    self.logger.error(traceback.format_exc())
            sess.commit()

        if duplicates > 0:
            self.logger.warning(f"Insertion were skipped for {duplicates} duplicated rows.")

    def select_common_klines(self) -> list[Row]:
        with Session(self.engine) as sess:
            query = sess.query(CommonKlineModel)
            rows = query.all()
        return rows
