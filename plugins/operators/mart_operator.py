"""Mart operator: thin wrapper around etl.load.mart_builder."""

from airflow.models import BaseOperator

from db.engine import get_engine
from etl.load.mart_builder import build_daily_mart


class MartBuildOperator(BaseOperator):
    def __init__(self, *, database_url: str | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.database_url = database_url

    def execute(self, context) -> int:
        engine = get_engine(self.database_url)
        days = build_daily_mart(engine)
        self.log.info("Rebuilt daily mart for %d day(s)", days)
        return days
