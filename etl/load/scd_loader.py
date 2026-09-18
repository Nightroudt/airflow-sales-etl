"""SCD Type 2 upsert for the dim_product dimension.

Two steps, both idempotent:
1. Insert any new (product_id, source_updated_at) versions this batch
   contains — ON CONFLICT DO NOTHING means re-running against the same
   supplier snapshot inserts nothing twice.
2. Recompute valid_to/is_current for every affected product via a window
   function over all of its versions — a pure function of the data already
   in the table, so re-running it is always safe and always converges to
   the same result.
"""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Connection, Engine

from db.schema import dim_product

_RECOMPUTE_VALIDITY_SQL = text("""
    UPDATE dim_product AS dp
    SET
        valid_to = sub.next_updated_at,
        is_current = (sub.next_updated_at IS NULL)
    FROM (
        SELECT
            id,
            LEAD(source_updated_at) OVER (
                PARTITION BY product_id ORDER BY source_updated_at
            ) AS next_updated_at
        FROM dim_product
        WHERE product_id = ANY(:product_ids)
    ) AS sub
    WHERE dp.id = sub.id
""")


def _insert_new_versions(conn: Connection, df: pd.DataFrame) -> None:
    records = [
        {
            "product_id": int(row.product_id),
            "name": row.name,
            "category": row.category,
            "supplier": row.supplier,
            "unit_cost": float(row.unit_cost),
            "source_updated_at": row.updated_at.to_pydatetime(),
            "valid_from": row.updated_at.to_pydatetime(),
            "valid_to": None,
            "is_current": True,  # provisional — _RECOMPUTE_VALIDITY_SQL fixes this up
        }
        for row in df.itertuples()
    ]
    stmt = pg_insert(dim_product).values(records)
    stmt = stmt.on_conflict_do_nothing(index_elements=["product_id", "source_updated_at"])
    conn.execute(stmt)


def upsert_dim_product(engine: Engine, df: pd.DataFrame) -> int:
    """Insert new product versions and recompute validity windows for every
    product touched. Returns the number of distinct products in this batch.
    """
    if df.empty:
        return 0

    product_ids = [int(pid) for pid in df["product_id"].unique()]

    with engine.begin() as conn:
        _insert_new_versions(conn, df)
        conn.execute(_RECOMPUTE_VALIDITY_SQL, {"product_ids": product_ids})

    return len(product_ids)
