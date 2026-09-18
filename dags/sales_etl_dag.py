"""Sales ETL DAG: CSV + supplier API -> clean/enrich -> SCD2 warehouse ->
daily mart. Runs every 6 hours.

Data flows between tasks as parquet file paths pushed through XCom (not the
DataFrames themselves — XCom isn't meant for bulk payloads), on a shared
volume under DATA_DIR.
"""

from datetime import UTC, datetime, timedelta

import structlog
from airflow import DAG

from plugins.operators.extract_operators import ApiExtractOperator, CsvExtractOperator
from plugins.operators.load_operators import FactLoadOperator, ScdLoadOperator
from plugins.operators.mart_operator import MartBuildOperator
from plugins.operators.transform_operators import CleanTransactionsOperator, EnrichProductsOperator

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ]
)
logger = structlog.get_logger()

DATA_DIR = "/opt/airflow/data"
INCOMING_DIR = f"{DATA_DIR}/incoming"
PROCESSED_DIR = f"{DATA_DIR}/processed"

# Stand-in for "our product catalog" — which SKUs to sync from the supplier
# on every run, independently of which ones happen to appear in today's
# transactions. Matches csv_source's default num_products so the demo data
# lines up. A real system would read this from a catalog table instead.
KNOWN_PRODUCT_IDS = list(range(1, 11))


def alert_on_failure(context: dict) -> None:
    """Demo alerting: a structured log line instead of real email — this
    setup has no SMTP server. To switch to real email with no code change,
    set default_args={"email_on_failure": True, "email": [...]} plus the
    AIRFLOW__SMTP__* environment variables (see README)."""
    ti = context["task_instance"]
    logger.error(
        "airflow_task_failed",
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=context["run_id"],
        exception=str(context.get("exception")),
    )


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="sales_etl",
    description="CSV + supplier API -> SCD2 warehouse -> daily sales mart",
    default_args=default_args,
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    schedule="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["sales", "etl"],
) as dag:
    extract_transactions_csv = CsvExtractOperator(
        task_id="extract_transactions_csv",
        incoming_dir=INCOMING_DIR,
        output_path=f"{PROCESSED_DIR}/transactions_raw.parquet",
    )

    extract_products_api = ApiExtractOperator(
        task_id="extract_products_api",
        product_ids=KNOWN_PRODUCT_IDS,
        output_path=f"{PROCESSED_DIR}/products_raw.parquet",
    )

    transform_clean_dedup = CleanTransactionsOperator(
        task_id="transform_clean_dedup",
        input_path="{{ ti.xcom_pull(task_ids='extract_transactions_csv') }}",
        output_path=f"{PROCESSED_DIR}/transactions_clean.parquet",
    )

    transform_enrich_products = EnrichProductsOperator(
        task_id="transform_enrich_products",
        input_path="{{ ti.xcom_pull(task_ids='extract_products_api') }}",
        output_path=f"{PROCESSED_DIR}/products_clean.parquet",
    )

    load_dim_product_scd2 = ScdLoadOperator(
        task_id="load_dim_product_scd2",
        input_path="{{ ti.xcom_pull(task_ids='transform_enrich_products') }}",
    )

    load_fact_sales = FactLoadOperator(
        task_id="load_fact_sales",
        input_path="{{ ti.xcom_pull(task_ids='transform_clean_dedup') }}",
    )

    build_daily_mart_task = MartBuildOperator(task_id="build_daily_mart")

    extract_transactions_csv >> transform_clean_dedup >> load_fact_sales
    extract_products_api >> transform_enrich_products >> load_dim_product_scd2
    [load_fact_sales, load_dim_product_scd2] >> build_daily_mart_task
