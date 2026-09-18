"""initial schema

Revision ID: 3be4c3d690aa
Revises:
Create Date: 2026-09-18
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "3be4c3d690aa"
down_revision: str | None = None
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "dim_product",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("product_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("category", sa.String(length=100), nullable=False),
        sa.Column("supplier", sa.String(length=100), nullable=False),
        sa.Column("unit_cost", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("source_updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_current", sa.Boolean(), server_default="true", nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("product_id", "source_updated_at", name="uq_dim_product_version"),
    )
    # Speeds up the mart's join to "the current version of this product" —
    # the query pattern load_operators/mart_builder actually run.
    op.create_index(
        "ix_dim_product_current",
        "dim_product",
        ["product_id"],
        postgresql_where=sa.text("is_current"),
    )

    op.create_table(
        "fact_sales",
        sa.Column("transaction_id", sa.String(length=64), nullable=False),
        sa.Column("product_id", sa.Integer(), nullable=False),
        sa.Column("customer_id", sa.Integer(), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("unit_price", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("transaction_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("loaded_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("transaction_id"),
    )
    # mart_builder aggregates fact_sales grouped by day.
    op.create_index("ix_fact_sales_transaction_date", "fact_sales", ["transaction_date"])

    op.create_table(
        "mart_daily_sales",
        sa.Column("sale_date", sa.Date(), nullable=False),
        sa.Column("total_revenue", sa.Numeric(precision=14, scale=2), nullable=False),
        sa.Column("total_quantity", sa.BigInteger(), nullable=False),
        sa.Column("top_product_id", sa.Integer(), nullable=True),
        sa.Column("top_product_name", sa.String(length=255), nullable=True),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("sale_date"),
    )


def downgrade() -> None:
    op.drop_table("mart_daily_sales")
    op.drop_index("ix_fact_sales_transaction_date", table_name="fact_sales")
    op.drop_table("fact_sales")
    op.drop_index("ix_dim_product_current", table_name="dim_product")
    op.drop_table("dim_product")
