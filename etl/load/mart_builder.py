"""Daily sales mart: revenue, quantity, and top product per day.

Rebuilt idempotently (ON CONFLICT DO UPDATE, plus a cleanup DELETE for days
that no longer have any facts) straight from fact_sales joined to the
*current* dim_product version — always a pure function of what's already
in the warehouse, so re-running it never drifts.
"""

import logging
from datetime import UTC, date, datetime

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# `AT TIME ZONE 'UTC'` is explicit on every bucketing expression here:
# fact_sales.transaction_date is stored UTC-aware (etl.transform.cleaning
# parses it with utc=True), but plain DATE(timestamptz) converts using the
# *session's* timezone setting, not necessarily UTC. Without the explicit
# conversion, a session/connection with a non-UTC default timezone would
# silently bucket transactions near midnight into the wrong sale_date.
_DELETE_ORPHANED_DAYS_SQL = text("""
    DELETE FROM mart_daily_sales
    WHERE (:date_from IS NULL OR sale_date >= :date_from)
      AND (:date_to IS NULL OR sale_date <= :date_to)
      AND sale_date NOT IN (
          SELECT DISTINCT DATE(transaction_date AT TIME ZONE 'UTC') FROM fact_sales
      )
""")

_BUILD_MART_SQL = text("""
    WITH daily_totals AS (
        SELECT
            DATE(transaction_date AT TIME ZONE 'UTC') AS sale_date,
            SUM(quantity * unit_price) AS total_revenue,
            SUM(quantity) AS total_quantity
        FROM fact_sales
        WHERE (:date_from IS NULL OR DATE(transaction_date AT TIME ZONE 'UTC') >= :date_from)
          AND (:date_to IS NULL OR DATE(transaction_date AT TIME ZONE 'UTC') <= :date_to)
        GROUP BY DATE(transaction_date AT TIME ZONE 'UTC')
    ),
    product_revenue AS (
        SELECT
            DATE(transaction_date AT TIME ZONE 'UTC') AS sale_date,
            product_id,
            SUM(quantity * unit_price) AS revenue,
            ROW_NUMBER() OVER (
                PARTITION BY DATE(transaction_date AT TIME ZONE 'UTC')
                -- product_id tiebreak makes the pick deterministic when two
                -- products tie on revenue for the same day — otherwise
                -- Postgres may pick a different "top product" on every
                -- rebuild even though the underlying data hasn't changed.
                ORDER BY SUM(quantity * unit_price) DESC, product_id ASC
            ) AS rn
        FROM fact_sales
        WHERE (:date_from IS NULL OR DATE(transaction_date AT TIME ZONE 'UTC') >= :date_from)
          AND (:date_to IS NULL OR DATE(transaction_date AT TIME ZONE 'UTC') <= :date_to)
        GROUP BY DATE(transaction_date AT TIME ZONE 'UTC'), product_id
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
    RETURNING sale_date, top_product_id, top_product_name
""")


def build_daily_mart(
    engine: Engine,
    date_from: date | None = None,
    date_to: date | None = None,
    computed_at: datetime | None = None,
) -> int:
    """Recompute mart_daily_sales over [date_from, date_to] (or the whole
    table when both are None). Returns the number of days upserted.

    Days that no longer have any fact_sales rows in the considered range
    (e.g. their transactions were all re-dated elsewhere) are deleted
    instead of being left with stale totals — an INSERT/UPDATE-only upsert
    would otherwise never revisit a day that dropped out of fact_sales.
    """
    computed_at = computed_at or datetime.now(UTC)
    params = {"date_from": date_from, "date_to": date_to, "computed_at": computed_at}

    with engine.begin() as conn:
        conn.execute(_DELETE_ORPHANED_DAYS_SQL, params)
        rows = conn.execute(_BUILD_MART_SQL, params).fetchall()

    for row in rows:
        if row.top_product_id is not None and row.top_product_name is None:
            # top_product_id came from fact_sales but has no matching
            # *current* dim_product row — usually means a product was sold
            # before (or without) ever being synced from the supplier.
            logger.warning(
                "mart_daily_sales.%s: top_product_id=%s has no current dim_product row",
                row.sale_date,
                row.top_product_id,
            )

    return len(rows)
