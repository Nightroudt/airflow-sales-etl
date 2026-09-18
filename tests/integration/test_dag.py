"""Full-DAG integration test via dag.test() — Airflow's own mechanism for
running an entire DAG synchronously without a scheduler. Exercises the real
operator chain end to end: real Postgres, a real running api_stub server,
and actual CSV files on disk.
"""

import importlib
from datetime import UTC, datetime

import pytest
from sqlalchemy import text

from etl.extract.csv_source import generate_sample_transactions_csv
from tests.integration.conftest import warehouse_database_url


@pytest.fixture
def sales_etl_dag(tmp_path, monkeypatch, api_stub_server, engine):
    incoming_dir = tmp_path / "incoming"
    incoming_dir.mkdir()
    generate_sample_transactions_csv(incoming_dir / "sample.csv", n=30, num_products=5, seed=7)

    monkeypatch.setenv("SALES_ETL_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("SALES_ETL_PRODUCT_IDS", "1,2,3,4,5")
    monkeypatch.setenv("API_STUB_BASE_URL", api_stub_server)
    monkeypatch.setenv("WAREHOUSE_DATABASE_URL", warehouse_database_url())

    # The DAG module reads SALES_ETL_DATA_DIR at *import* time to build its
    # operators, so a fresh reload is needed to pick up the tmp_path above —
    # setting the env var after an earlier import wouldn't retroactively
    # change already-constructed operator instances.
    import dags.sales_etl_dag as dag_module

    importlib.reload(dag_module)
    return dag_module.dag


def test_full_pipeline_runs_end_to_end(sales_etl_dag, engine):
    sales_etl_dag.test()

    with engine.connect() as conn:
        product_count = conn.execute(text("SELECT COUNT(*) FROM dim_product")).scalar()
        fact_count = conn.execute(text("SELECT COUNT(*) FROM fact_sales")).scalar()
        mart_count = conn.execute(text("SELECT COUNT(*) FROM mart_daily_sales")).scalar()

    assert product_count == 5  # KNOWN_PRODUCT_IDS default is 1..10, but only 5 fetched here
    assert fact_count > 0
    assert mart_count > 0


def test_rerunning_the_dag_does_not_duplicate_rows(sales_etl_dag, engine):
    """The concrete idempotency proof the spec asked for: running the same
    pipeline twice against the same source data must not double row counts.
    """
    sales_etl_dag.test(execution_date=datetime(2026, 1, 1, tzinfo=UTC))

    with engine.connect() as conn:
        fact_count_1 = conn.execute(text("SELECT COUNT(*) FROM fact_sales")).scalar()
        product_count_1 = conn.execute(text("SELECT COUNT(*) FROM dim_product")).scalar()

    sales_etl_dag.test(execution_date=datetime(2026, 1, 1, 6, tzinfo=UTC))

    with engine.connect() as conn:
        fact_count_2 = conn.execute(text("SELECT COUNT(*) FROM fact_sales")).scalar()
        # A second run legitimately adds new dim_product *versions* (the API
        # stub returns a fresh updated_at every call — that's the point of
        # the demo) — versions growing isn't a duplication bug, but the
        # *count of underlying products* must not exceed the id's synced.
        product_count_2 = conn.execute(
            text("SELECT COUNT(DISTINCT product_id) FROM dim_product")
        ).scalar()

    assert fact_count_2 == fact_count_1
    assert product_count_2 == 5
    assert product_count_1 == 5
