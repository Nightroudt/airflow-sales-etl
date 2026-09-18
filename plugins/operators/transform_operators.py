"""Transform operators: thin wrappers around etl.transform.*."""

from pathlib import Path

import pandas as pd
from airflow.models import BaseOperator

from etl.transform.cleaning import clean_transactions
from etl.transform.enrichment import shape_products


class CleanTransactionsOperator(BaseOperator):
    template_fields = ("input_path", "output_path")

    def __init__(self, *, input_path: str, output_path: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.input_path = input_path
        self.output_path = output_path

    def execute(self, context) -> str:
        df = pd.read_parquet(self.input_path)
        cleaned = clean_transactions(df)
        self.log.info("Cleaned transactions: %d -> %d rows", len(df), len(cleaned))

        out_path = Path(self.output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        cleaned.to_parquet(out_path, index=False)
        return str(out_path)


class EnrichProductsOperator(BaseOperator):
    template_fields = ("input_path", "output_path")

    def __init__(self, *, input_path: str, output_path: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.input_path = input_path
        self.output_path = output_path

    def execute(self, context) -> str:
        df = pd.read_parquet(self.input_path)
        shaped = shape_products(df)
        self.log.info("Shaped products: %d -> %d rows", len(df), len(shaped))

        out_path = Path(self.output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        shaped.to_parquet(out_path, index=False)
        return str(out_path)
