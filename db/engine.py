import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def get_engine(database_url: str | None = None) -> Engine:
    """Create a warehouse-DB engine.

    database_url defaults to WAREHOUSE_DATABASE_URL so operators/tests can
    each point at their own database without hardcoding a connection string.
    """
    url = database_url or os.environ["WAREHOUSE_DATABASE_URL"]
    return create_engine(url)
