from datetime import UTC, datetime

import pandas as pd
from sqlalchemy import text

from etl.load.fact_loader import upsert_fact_sales


def txn_row(transaction_id: str, **overrides) -> dict:
    row = {
        "transaction_id": transaction_id,
        "product_id": 1,
        "customer_id": 5,
        "quantity": 2,
        "unit_price": 10.0,
        "transaction_date": pd.Timestamp(datetime(2026, 1, 1, tzinfo=UTC)),
    }
    row.update(overrides)
    return row


def fetch(engine, transaction_id: str):
    with engine.connect() as conn:
        return conn.execute(
            text("SELECT * FROM fact_sales WHERE transaction_id = :tid"), {"tid": transaction_id}
        ).fetchone()


def test_inserts_new_transaction(engine):
    df = pd.DataFrame([txn_row("t1")])

    n = upsert_fact_sales(engine, df)

    assert n == 1
    assert fetch(engine, "t1") is not None


def test_reloading_same_transaction_does_not_duplicate(engine):
    df = pd.DataFrame([txn_row("t1")])

    upsert_fact_sales(engine, df)
    upsert_fact_sales(engine, df)

    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM fact_sales")).scalar()
    assert count == 1


def test_reloading_with_changed_data_updates_in_place(engine):
    upsert_fact_sales(engine, pd.DataFrame([txn_row("t1", quantity=1)]))
    upsert_fact_sales(engine, pd.DataFrame([txn_row("t1", quantity=99)]))

    row = fetch(engine, "t1")
    assert row.quantity == 99


def test_a_duplicate_transaction_id_within_one_batch_does_not_crash(engine):
    """Postgres raises CardinalityViolation if ON CONFLICT DO UPDATE would
    touch the same row twice in one INSERT. clean_transactions() already
    dedupes upstream, but this loader must not depend on that — it should
    survive unclean input by deduping itself (keeping the last occurrence).
    """
    df = pd.DataFrame([txn_row("t1", quantity=1), txn_row("t1", quantity=99)])

    n = upsert_fact_sales(engine, df)

    assert n == 1
    assert fetch(engine, "t1").quantity == 99


def test_empty_dataframe_is_a_no_op(engine):
    from etl.extract.csv_source import TRANSACTION_COLUMNS

    empty = pd.DataFrame(columns=TRANSACTION_COLUMNS)

    n = upsert_fact_sales(engine, empty)

    assert n == 0
