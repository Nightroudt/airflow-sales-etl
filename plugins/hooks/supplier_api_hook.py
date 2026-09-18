"""Custom Airflow hook wrapping the supplier API stub.

Thin on purpose: the actual HTTP + parsing logic lives in
etl.extract.api_source (Airflow-independent, directly unit-testable) — this
hook just manages the client lifecycle and where its base_url comes from,
which is the part that's genuinely Airflow-flavored.
"""

import os

import httpx
import pandas as pd
from airflow.hooks.base import BaseHook

from etl.extract.api_source import fetch_products


class SupplierApiHook(BaseHook):
    def __init__(self, base_url: str | None = None, client: httpx.Client | None = None) -> None:
        super().__init__()
        # An injected client (e.g. FastAPI's TestClient, an in-process ASGI
        # client) is owned by the caller — we only close clients we created
        # ourselves.
        self._owns_client = client is None
        self._client = client or httpx.Client(base_url=base_url or os.environ["API_STUB_BASE_URL"])

    def get_products(self, product_ids: list[int]) -> pd.DataFrame:
        return fetch_products(self._client, product_ids)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "SupplierApiHook":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
