"""Load operators: thin wrappers around etl.load.*."""

from datetime import UTC, datetime

import pandas as pd
from airflow.models import BaseOperator

from db.engine import get_engine
from etl.load.fact_loader import upsert_fact_sales
from etl.load.scd_loader import upsert_dim_product


class ScdLoadOperator(BaseOperator):
    template_fields = ("input_path",)

    def __init__(self, *, input_path: str, database_url: str | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.input_path = input_path
        self.database_url = database_url

    def execute(self, context) -> int:
        df = pd.read_parquet(self.input_path)
        engine = get_engine(self.database_url)
        touched = upsert_dim_product(engine, df)
        self.log.info("Upserted SCD2 versions for %d products", touched)
        return touched


class FactLoadOperator(BaseOperator):
    template_fields = ("input_path",)

    def __init__(self, *, input_path: str, database_url: str | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.input_path = input_path
        self.database_url = database_url

    def execute(self, context) -> int:
        df = pd.read_parquet(self.input_path)
        engine = get_engine(self.database_url)
        loaded = upsert_fact_sales(engine, df, loaded_at=datetime.now(UTC))
        self.log.info("Upserted %d fact_sales rows", loaded)
        return loaded
