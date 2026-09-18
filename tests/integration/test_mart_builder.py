from datetime import UTC, date, datetime

import pandas as pd
from sqlalchemy import text

from etl.load.fact_loader import upsert_fact_sales
from etl.load.mart_builder import build_daily_mart
from etl.load.scd_loader import upsert_dim_product


def seed_products(engine, *product_ids: int) -> None:
    t = datetime(2026, 1, 1, tzinfo=UTC)
    df = pd.DataFrame(
        [
            {
                "product_id": pid,
                "name": f"Product {pid}",
                "category": "Tools",
                "supplier": "Acme",
                "unit_cost": 1.0,
                "updated_at": pd.Timestamp(t),
            }
            for pid in product_ids
        ]
    )
    upsert_dim_product(engine, df)


def txn(
    transaction_id: str, product_id: int, quantity: int, unit_price: float, day: datetime
) -> dict:
    return {
        "transaction_id": transaction_id,
        "product_id": product_id,
        "customer_id": 1,
        "quantity": quantity,
        "unit_price": unit_price,
        "transaction_date": pd.Timestamp(day),
    }


def fetch_mart_row(engine, sale_date: date):
    with engine.connect() as conn:
        return conn.execute(
            text("SELECT * FROM mart_daily_sales WHERE sale_date = :d"), {"d": sale_date}
        ).fetchone()


def test_aggregates_revenue_and_quantity_for_a_day(engine):
    seed_products(engine, 1, 2)
    day = datetime(2026, 3, 1, tzinfo=UTC)
    df = pd.DataFrame(
        [
            txn("a1", 1, quantity=2, unit_price=10.0, day=day),  # revenue 20
            txn("a2", 2, quantity=1, unit_price=50.0, day=day),  # revenue 50
        ]
    )
    upsert_fact_sales(engine, df)

    days = build_daily_mart(engine)

    assert days == 1
    row = fetch_mart_row(engine, date(2026, 3, 1))
    assert row.total_revenue == 70
    assert row.total_quantity == 3


def test_top_product_is_the_highest_revenue_product_that_day(engine):
    seed_products(engine, 1, 2)
    day = datetime(2026, 3, 1, tzinfo=UTC)
    df = pd.DataFrame(
        [
            txn("a1", 1, quantity=1, unit_price=10.0, day=day),  # revenue 10
            txn("a2", 2, quantity=1, unit_price=50.0, day=day),  # revenue 50 -> top
        ]
    )
    upsert_fact_sales(engine, df)

    build_daily_mart(engine)

    row = fetch_mart_row(engine, date(2026, 3, 1))
    assert row.top_product_id == 2
    assert row.top_product_name == "Product 2"


def test_separate_days_get_separate_rows(engine):
    seed_products(engine, 1)
    day1 = datetime(2026, 3, 1, tzinfo=UTC)
    day2 = datetime(2026, 3, 2, tzinfo=UTC)
    df = pd.DataFrame(
        [
            txn("a1", 1, quantity=1, unit_price=10.0, day=day1),
            txn("a2", 1, quantity=1, unit_price=10.0, day=day2),
        ]
    )
    upsert_fact_sales(engine, df)

    days = build_daily_mart(engine)

    assert days == 2


def test_rebuilding_is_idempotent(engine):
    seed_products(engine, 1)
    day = datetime(2026, 3, 1, tzinfo=UTC)
    upsert_fact_sales(engine, pd.DataFrame([txn("a1", 1, quantity=1, unit_price=10.0, day=day)]))

    build_daily_mart(engine)
    build_daily_mart(engine)
    build_daily_mart(engine)

    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM mart_daily_sales")).scalar()
    assert count == 1


def test_rebuild_reflects_newly_loaded_transactions(engine):
    seed_products(engine, 1)
    day = datetime(2026, 3, 1, tzinfo=UTC)
    upsert_fact_sales(engine, pd.DataFrame([txn("a1", 1, quantity=1, unit_price=10.0, day=day)]))
    build_daily_mart(engine)

    upsert_fact_sales(engine, pd.DataFrame([txn("a2", 1, quantity=1, unit_price=10.0, day=day)]))
    build_daily_mart(engine)

    row = fetch_mart_row(engine, date(2026, 3, 1))
    assert row.total_revenue == 20
    assert row.total_quantity == 2
