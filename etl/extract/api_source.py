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
    df["updated_at"] = pd.to_datetime(df["updated_at"])
    return df
