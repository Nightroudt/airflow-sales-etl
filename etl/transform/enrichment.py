"""Normalizing the supplier API's product payload before it reaches the
SCD Type 2 load — validation happens here so bad upstream data can't create
a bogus dimension version."""

import pandas as pd

from etl.extract.api_source import PRODUCT_COLUMNS


def shape_products(df: pd.DataFrame) -> pd.DataFrame:
    """Drop incomplete rows, coerce types, dedupe by (product_id,
    updated_at) — defense-in-depth alongside the DB's own ON CONFLICT, in
    case the API returned the same version twice in one batch."""
    df = df.copy()
    df = df.dropna(subset=PRODUCT_COLUMNS)

    df["product_id"] = pd.to_numeric(df["product_id"], errors="coerce").astype("Int64")
    df["unit_cost"] = pd.to_numeric(df["unit_cost"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce", utc=True)
    df["name"] = df["name"].astype(str).str.strip()
    df["category"] = df["category"].astype(str).str.strip()
    df["supplier"] = df["supplier"].astype(str).str.strip()

    df = df.dropna(subset=PRODUCT_COLUMNS)
    df = df[df["unit_cost"] >= 0]
    df = df.drop_duplicates(subset=["product_id", "updated_at"], keep="last")

    df["product_id"] = df["product_id"].astype(int)

    return df.reset_index(drop=True)
