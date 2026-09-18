"""Extract operators: thin Airflow wrappers around etl.extract.* — all the
real logic lives there so it's testable without Airflow at all.
"""

from pathlib import Path

from airflow.models import BaseOperator

from etl.extract.csv_source import read_transactions_dir
from plugins.hooks.supplier_api_hook import SupplierApiHook


class CsvExtractOperator(BaseOperator):
    """Reads every CSV currently in `incoming_dir`, writes the concatenated
    result to `output_path`, and returns that path (pushed to XCom
    automatically) for downstream tasks to read — keeps the actual
    transaction data out of XCom, which isn't meant for bulk payloads.
    """

    template_fields = ("incoming_dir", "output_path")

    def __init__(self, *, incoming_dir: str, output_path: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.incoming_dir = incoming_dir
        self.output_path = output_path

    def execute(self, context) -> str:
        df = read_transactions_dir(self.incoming_dir)
        self.log.info("Read %d transaction rows from %s", len(df), self.incoming_dir)

        out_path = Path(self.output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out_path, index=False)
        return str(out_path)


class ApiExtractOperator(BaseOperator):
    """Fetches product data for `product_ids` from the supplier API stub in
    one batched call and writes the result to `output_path`.
    """

    template_fields = ("output_path",)

    def __init__(
        self,
        *,
        product_ids: list[int],
        output_path: str,
        api_base_url: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.product_ids = product_ids
        self.output_path = output_path
        self.api_base_url = api_base_url

    def execute(self, context) -> str:
        with SupplierApiHook(base_url=self.api_base_url) as hook:
            df = hook.get_products(self.product_ids)

        self.log.info("Fetched %d products from supplier API", len(df))

        out_path = Path(self.output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out_path, index=False)
        return str(out_path)
