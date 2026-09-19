"""FastAPI stand-in for a supplier's product-catalog API.

Returns randomized product data on every call, with updated_at set to
"now" each time — this is deliberate: it simulates the supplier's catalog
genuinely changing between pipeline runs, which is what makes the SCD Type 2
load downstream actually have something to version.
"""

import random
from datetime import UTC, datetime

from fastapi import FastAPI, HTTPException, Query

from api_stub.schemas import ProductOut

app = FastAPI(title="Supplier API Stub")

_NAMES = [
    "Wireless Mouse",
    "USB-C Cable",
    "Mechanical Keyboard",
    "27-inch Monitor",
    "Webcam HD",
    "Desk Lamp",
    "Laptop Stand",
    "Noise-Cancelling Headphones",
    "Portable SSD 1TB",
    "Bluetooth Speaker",
]
_CATEGORIES = ["Electronics", "Accessories", "Office", "Audio"]
_SUPPLIERS = ["Acme Supply Co", "Globex Distribution", "Initech Wholesale"]


def _random_product(product_id: int) -> ProductOut:
    return ProductOut(
        product_id=product_id,
        name=_NAMES[product_id % len(_NAMES)],
        category=random.choice(_CATEGORIES),
        supplier=random.choice(_SUPPLIERS),
        unit_cost=round(random.uniform(5.0, 500.0), 2),
        updated_at=datetime.now(UTC),
    )


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/products", response_model=list[ProductOut])
def get_products(
    ids: str = Query(..., description="Comma-separated product ids"),
) -> list[ProductOut]:
    """Batch endpoint — one call for many ids, so extract doesn't do N+1
    requests against the supplier."""
    try:
        product_ids = [int(x) for x in ids.split(",") if x.strip()]
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail="ids must be a comma-separated list of integers"
        ) from exc
    return [_random_product(pid) for pid in product_ids]
