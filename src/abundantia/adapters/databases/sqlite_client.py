import pandera as pa
import sqlalchemy
from pandera.typing import DataFrame
from sqlalchemy import insert
from sqlalchemy.engine.row import Row
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from abundantia.adapters.databases.mapper import mapper_registry
from abundantia.logging import setup_logger
from abundantia.schema.common import CommonKlineSchema
from abundantia.schema.model import CommonKlineModel

logger = setup_logger(__name__)


class SQLiteClient:
    def __init__(self, file_path: str = "sqlite:///resources/db.sqlite3") -> None:
        self.engine = sqlalchemy.create_engine(file_path, echo=False)

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
                    logger.exception("Insertion error.")
            sess.commit()

        if duplicates > 0:
            logger.warning(f"Insertion were skipped for {duplicates} duplicated rows.")

    def select_common_klines(self) -> list[Row]:
        with Session(self.engine) as sess:
            query = sess.query(CommonKlineModel)
            rows = query.all()
        return rows
