"""Reading raw transaction CSVs, and generating sample ones for demo/tests."""

import random
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd

TRANSACTION_COLUMNS = [
    "transaction_id",
    "product_id",
    "customer_id",
    "quantity",
    "unit_price",
    "transaction_date",
]


def generate_sample_transactions_csv(
    path: str | Path,
    n: int = 500,
    num_products: int = 10,
    seed: int | None = None,
    now: datetime | None = None,
) -> Path:
    """Write a CSV of synthetic transactions — the "CSV source" stand-in
    the spec asked for, and also reused directly as a test fixture.

    `now` defaults to the real current time; pass a fixed value together
    with `seed` for byte-for-byte reproducible output in tests.
    """
    rng = random.Random(seed)
    now = now or datetime.now(UTC)

    rows = [
        {
            # Derived from the seeded RNG (not uuid.uuid4(), which draws from
            # os.urandom() and would ignore `seed` entirely) so the same
            # seed reproduces the exact same ids — needed for deterministic
            # test fixtures.
            "transaction_id": str(uuid.UUID(int=rng.getrandbits(128))),
            "product_id": rng.randint(1, num_products),
            "customer_id": rng.randint(1, 200),
            "quantity": rng.randint(1, 5),
            "unit_price": round(rng.uniform(5.0, 500.0), 2),
            "transaction_date": (now - timedelta(hours=rng.randint(0, 24 * 7))).isoformat(),
        }
        for _ in range(n)
    ]

    df = pd.DataFrame(rows, columns=TRANSACTION_COLUMNS)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return path


def read_transactions_dir(directory: str | Path) -> pd.DataFrame:
    """Read and concatenate every CSV currently sitting in `directory`.

    Extract is intentionally stateless — it doesn't track which files it
    has already seen. Idempotency across repeated runs is guaranteed at the
    load layer instead (ON CONFLICT on the natural key), not here.
    """
    directory = Path(directory)
    csv_files = sorted(directory.glob("*.csv"))
    if not csv_files:
        return pd.DataFrame(columns=TRANSACTION_COLUMNS)

    frames = [pd.read_csv(f, parse_dates=["transaction_date"]) for f in csv_files]
    return pd.concat(frames, ignore_index=True)
