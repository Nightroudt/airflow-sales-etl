"""Cleaning + deduplication for raw transaction rows."""

import pandas as pd

REQUIRED_COLUMNS = [
    "transaction_id",
    "product_id",
    "customer_id",
    "quantity",
    "unit_price",
    "transaction_date",
]


def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Drop invalid rows, coerce types, dedupe by transaction_id.

    Never mutates the input DataFrame.
    """
    df = df.copy()
    df = df.dropna(subset=REQUIRED_COLUMNS)

    df["transaction_id"] = df["transaction_id"].astype(str)
    df["product_id"] = pd.to_numeric(df["product_id"], errors="coerce").astype("Int64")
    df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce").astype("Int64")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce", utc=True)

    # A failed coercion above shows up as null in that column — drop those
    # rows the same way as originally-missing data.
    df = df.dropna(subset=REQUIRED_COLUMNS)

    df = df[(df["quantity"] > 0) & (df["unit_price"] >= 0)]
    df = df.drop_duplicates(subset="transaction_id", keep="first")

    df["product_id"] = df["product_id"].astype(int)
    df["customer_id"] = df["customer_id"].astype(int)
    df["quantity"] = df["quantity"].astype(int)

    return df.reset_index(drop=True)
