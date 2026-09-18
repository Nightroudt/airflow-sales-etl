"""Idempotent upsert into fact_sales, keyed on the natural transaction_id
from the source CSV — reprocessing the same file merges into the same
rows instead of duplicating them."""

from datetime import UTC, datetime

import pandas as pd
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine

from db.schema import fact_sales

_UPDATE_COLUMNS = (
    "product_id",
    "customer_id",
    "quantity",
    "unit_price",
    "transaction_date",
    "loaded_at",
)


def upsert_fact_sales(engine: Engine, df: pd.DataFrame, loaded_at: datetime | None = None) -> int:
    if df.empty:
        return 0

    loaded_at = loaded_at or datetime.now(UTC)

    records = [
        {
            "transaction_id": row.transaction_id,
            "product_id": int(row.product_id),
            "customer_id": int(row.customer_id),
            "quantity": int(row.quantity),
            "unit_price": float(row.unit_price),
            "transaction_date": row.transaction_date.to_pydatetime(),
            "loaded_at": loaded_at,
        }
        for row in df.itertuples()
    ]

    stmt = pg_insert(fact_sales).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=["transaction_id"],
        set_={col: stmt.excluded[col] for col in _UPDATE_COLUMNS},
    )

    with engine.begin() as conn:
        conn.execute(stmt)

    return len(records)
