"""Warehouse schema as SQLAlchemy Core Table objects.

Core, not ORM entity classes: the load layer writes raw upsert SQL
(``INSERT ... ON CONFLICT``) against these tables, not CRUD over mapped
objects — Core is the honest representation of what actually happens here.
"""

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    UniqueConstraint,
)

metadata = MetaData()

# SCD Type 2 dimension: one row per known *version* of a product, as seen
# from the supplier API. (product_id, source_updated_at) is the natural key
# the spec asked for — it's also what makes re-inserting the same supplier
# snapshot a no-op (ON CONFLICT DO NOTHING) instead of a duplicate version.
dim_product = Table(
    "dim_product",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("product_id", Integer, nullable=False),
    Column("name", String(255), nullable=False),
    Column("category", String(100), nullable=False),
    Column("supplier", String(100), nullable=False),
    Column("unit_cost", Numeric(12, 2), nullable=False),
    Column("source_updated_at", DateTime(timezone=True), nullable=False),
    Column("valid_from", DateTime(timezone=True), nullable=False),
    Column("valid_to", DateTime(timezone=True), nullable=True),
    Column("is_current", Boolean, nullable=False, server_default="true"),
    UniqueConstraint("product_id", "source_updated_at", name="uq_dim_product_version"),
)

# Fact table: one row per sales transaction. transaction_id is the natural
# key from the source CSV — the idempotency anchor for ON CONFLICT DO UPDATE.
fact_sales = Table(
    "fact_sales",
    metadata,
    Column("transaction_id", String(64), primary_key=True),
    Column("product_id", Integer, nullable=False),
    Column("customer_id", Integer, nullable=False),
    Column("quantity", Integer, nullable=False),
    Column("unit_price", Numeric(12, 2), nullable=False),
    Column("transaction_date", DateTime(timezone=True), nullable=False),
    Column("loaded_at", DateTime(timezone=True), nullable=False),
)

# Daily aggregate mart, rebuilt idempotently (ON CONFLICT DO UPDATE) from
# fact_sales joined to the *current* dim_product version.
mart_daily_sales = Table(
    "mart_daily_sales",
    metadata,
    Column("sale_date", Date, primary_key=True),
    Column("total_revenue", Numeric(14, 2), nullable=False),
    Column("total_quantity", BigInteger, nullable=False),
    Column("top_product_id", Integer, nullable=True),
    Column("top_product_name", String(255), nullable=True),
    Column("computed_at", DateTime(timezone=True), nullable=False),
)
