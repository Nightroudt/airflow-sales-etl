"""Fetching product data from the supplier API — pure HTTP + pandas, no
Airflow dependency, so it's directly unit-testable with any httpx.Client
(including FastAPI's in-process TestClient)."""

import httpx
import pandas as pd

PRODUCT_COLUMNS = ["product_id", "name", "category", "supplier", "unit_cost", "updated_at"]


def fetch_products(client: httpx.Client, product_ids: list[int]) -> pd.DataFrame:
    """One batched call for all ids — avoids N+1 requests against the
    supplier. `client` is expected to already be configured with the right
    base_url (a real httpx.Client for prod, FastAPI's TestClient for tests)."""
    if not product_ids:
        return pd.DataFrame(columns=PRODUCT_COLUMNS)

    ids_param = ",".join(str(pid) for pid in product_ids)
    response = client.get("/products", params={"ids": ids_param})
    response.raise_for_status()

    df = pd.DataFrame(response.json(), columns=PRODUCT_COLUMNS)
    # utc=True to match every other datetime coercion in this codebase
    # (cleaning.py, enrichment.py) — without it, a naive timestamp from a
    # real (non-stub) supplier would later get *localized* rather than
    # *converted* by enrichment's utc=True parse, silently mis-recording
    # wall-clock time as UTC.
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)
    return df
