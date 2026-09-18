"""Daily sales mart: revenue, quantity, and top product per day.

Rebuilt idempotently (ON CONFLICT DO UPDATE) straight from fact_sales
joined to the *current* dim_product version — always a pure function of
what's already in the warehouse, so re-running it never drifts.
"""

from datetime import UTC, date, datetime

from sqlalchemy import text
from sqlalchemy.engine import Engine

_BUILD_MART_SQL = text("""
    WITH daily_totals AS (
        SELECT
            DATE(transaction_date) AS sale_date,
            SUM(quantity * unit_price) AS total_revenue,
            SUM(quantity) AS total_quantity
        FROM fact_sales
        WHERE (:date_from IS NULL OR DATE(transaction_date) >= :date_from)
          AND (:date_to IS NULL OR DATE(transaction_date) <= :date_to)
        GROUP BY DATE(transaction_date)
    ),
    product_revenue AS (
        SELECT
            DATE(transaction_date) AS sale_date,
            product_id,
            SUM(quantity * unit_price) AS revenue,
            ROW_NUMBER() OVER (
                PARTITION BY DATE(transaction_date) ORDER BY SUM(quantity * unit_price) DESC
            ) AS rn
        FROM fact_sales
        WHERE (:date_from IS NULL OR DATE(transaction_date) >= :date_from)
          AND (:date_to IS NULL OR DATE(transaction_date) <= :date_to)
        GROUP BY DATE(transaction_date), product_id
    ),
    top_products AS (
        SELECT sale_date, product_id AS top_product_id
        FROM product_revenue
        WHERE rn = 1
    )
    INSERT INTO mart_daily_sales
        (sale_date, total_revenue, total_quantity, top_product_id, top_product_name, computed_at)
    SELECT
        dt.sale_date,
        dt.total_revenue,
        dt.total_quantity,
        tp.top_product_id,
        dp.name,
        :computed_at
    FROM daily_totals dt
    LEFT JOIN top_products tp ON tp.sale_date = dt.sale_date
    LEFT JOIN dim_product dp ON dp.product_id = tp.top_product_id AND dp.is_current
    ON CONFLICT (sale_date) DO UPDATE SET
        total_revenue = EXCLUDED.total_revenue,
        total_quantity = EXCLUDED.total_quantity,
        top_product_id = EXCLUDED.top_product_id,
        top_product_name = EXCLUDED.top_product_name,
        computed_at = EXCLUDED.computed_at
    RETURNING sale_date
""")


def build_daily_mart(
    engine: Engine,
    date_from: date | None = None,
    date_to: date | None = None,
    computed_at: datetime | None = None,
) -> int:
    """Recompute mart_daily_sales over [date_from, date_to] (or the whole
    table when both are None). Returns the number of days upserted."""
    computed_at = computed_at or datetime.now(UTC)

    with engine.begin() as conn:
        result = conn.execute(
            _BUILD_MART_SQL,
            {"date_from": date_from, "date_to": date_to, "computed_at": computed_at},
        )
        return len(result.fetchall())
