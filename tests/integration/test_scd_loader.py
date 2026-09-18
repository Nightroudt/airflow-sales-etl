from datetime import UTC, datetime

import pandas as pd
from sqlalchemy import text

from etl.load.scd_loader import upsert_dim_product


def product_row(product_id: int, updated_at: datetime, **overrides) -> dict:
    row = {
        "product_id": product_id,
        "name": "Widget",
        "category": "Tools",
        "supplier": "Acme",
        "unit_cost": 10.0,
        "updated_at": pd.Timestamp(updated_at),
    }
    row.update(overrides)
    return row


def fetch_versions(engine, product_id: int) -> list:
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT source_updated_at, valid_to, is_current FROM dim_product "
                "WHERE product_id = :pid ORDER BY source_updated_at"
            ),
            {"pid": product_id},
        ).fetchall()


def test_first_version_is_current_with_no_valid_to(engine):
    t1 = datetime(2026, 1, 1, tzinfo=UTC)
    df = pd.DataFrame([product_row(1, t1)])

    upsert_dim_product(engine, df)

    versions = fetch_versions(engine, 1)
    assert len(versions) == 1
    assert versions[0].valid_to is None
    assert versions[0].is_current is True


def test_newer_version_closes_out_the_previous_one(engine):
    t1 = datetime(2026, 1, 1, tzinfo=UTC)
    t2 = datetime(2026, 1, 2, tzinfo=UTC)

    upsert_dim_product(engine, pd.DataFrame([product_row(1, t1)]))
    upsert_dim_product(engine, pd.DataFrame([product_row(1, t2, unit_cost=12.0)]))

    versions = fetch_versions(engine, 1)
    assert len(versions) == 2
    assert versions[0].valid_to == t2
    assert versions[0].is_current is False
    assert versions[1].valid_to is None
    assert versions[1].is_current is True


def test_reloading_the_same_version_is_idempotent(engine):
    t1 = datetime(2026, 1, 1, tzinfo=UTC)
    df = pd.DataFrame([product_row(1, t1)])

    upsert_dim_product(engine, df)
    upsert_dim_product(engine, df)
    upsert_dim_product(engine, df)

    assert len(fetch_versions(engine, 1)) == 1


def test_different_products_are_independent(engine):
    t1 = datetime(2026, 1, 1, tzinfo=UTC)

    upsert_dim_product(engine, pd.DataFrame([product_row(1, t1), product_row(2, t1)]))

    assert len(fetch_versions(engine, 1)) == 1
    assert len(fetch_versions(engine, 2)) == 1


def test_empty_dataframe_is_a_no_op(engine):
    from etl.transform.enrichment import REQUIRED_COLUMNS

    empty = pd.DataFrame(columns=REQUIRED_COLUMNS)

    touched = upsert_dim_product(engine, empty)

    assert touched == 0
