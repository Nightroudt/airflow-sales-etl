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


def _counts(engine) -> dict:
    with engine.connect() as conn:
        return {
            "fact_rows": conn.execute(text("SELECT COUNT(*) FROM fact_sales")).scalar(),
            "mart_rows": conn.execute(text("SELECT COUNT(*) FROM mart_daily_sales")).scalar(),
            "dim_product_rows": conn.execute(text("SELECT COUNT(*) FROM dim_product")).scalar(),
            "distinct_products": conn.execute(
                text("SELECT COUNT(DISTINCT product_id) FROM dim_product")
            ).scalar(),
        }


def test_rerunning_the_dag_does_not_duplicate_rows(sales_etl_dag, engine):
    """The concrete idempotency proof the spec asked for: running the same
    pipeline twice against the same source data must not double row counts
    in fact_sales or mart_daily_sales.

    dim_product's raw row *count* is deliberately excluded from that
    guarantee: SCD Type 2 means each run legitimately appends a new
    *version* per product (the API stub hands back a fresh updated_at every
    call), so dim_product grows run over run by design — what must stay
    stable instead is the *distinct* product_id count (the catalog itself
    isn't changing, only its version history).
    """
    sales_etl_dag.test(execution_date=datetime(2026, 1, 1, tzinfo=UTC))
    first = _counts(engine)

    sales_etl_dag.test(execution_date=datetime(2026, 1, 1, 6, tzinfo=UTC))
    second = _counts(engine)

    assert second["fact_rows"] == first["fact_rows"]
    assert second["mart_rows"] == first["mart_rows"]
    assert second["distinct_products"] == first["distinct_products"] == 5

    # Not a bug: this is the one metric that's *supposed* to grow — a
    # second run that didn't add new dim_product versions would mean SCD2
    # versioning silently stopped working.
    assert second["dim_product_rows"] > first["dim_product_rows"]
