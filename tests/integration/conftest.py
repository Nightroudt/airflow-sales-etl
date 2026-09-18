import os
import threading
import time

# Must be imported before any dag.test() call: DagRunNote/TaskInstanceNote
# declare a FK to ab_user.id, but that table's model (Flask-AppBuilder's
# auth models) is only registered lazily. If SQLAlchemy's global mapper
# configuration fires before this import runs, it raises
# NoReferencedTableError trying to resolve that FK — an Airflow/SQLAlchemy
# quirk of driving dag.test() directly rather than through the webserver,
# unrelated to anything in this project's own code.
import airflow.providers.fab.auth_manager.models  # noqa: F401
import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from api_stub.main import app as api_stub_app
from db.engine import get_engine
from db.schema import metadata
from plugins.hooks.supplier_api_hook import SupplierApiHook


def warehouse_database_url() -> str:
    # Same default port a GitHub Actions `services: postgres:` container
    # publishes on — override locally if your own throwaway Postgres uses
    # a different port.
    return os.environ.get(
        "TEST_WAREHOUSE_DATABASE_URL",
        "postgresql+psycopg2://warehouse:warehouse@localhost:5432/warehouse",
    )


@pytest.fixture
def engine():
    eng = get_engine(warehouse_database_url())
    metadata.drop_all(eng)
    metadata.create_all(eng)
    yield eng
    metadata.drop_all(eng)
    eng.dispose()


@pytest.fixture
def api_stub_client() -> TestClient:
    """In-process ASGI client for the supplier API stub — no real server or
    port needed. Used for testing SupplierApiHook/ApiExtractOperator in
    isolation."""
    return TestClient(api_stub_app)


@pytest.fixture
def supplier_hook(api_stub_client: TestClient) -> SupplierApiHook:
    with SupplierApiHook(client=api_stub_client) as hook:
        yield hook


class _ServerThread(threading.Thread):
    def __init__(self, app, port: int) -> None:
        super().__init__(daemon=True)
        config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning")
        self.server = uvicorn.Server(config)

    def run(self) -> None:
        self.server.run()

    def stop(self) -> None:
        self.server.should_exit = True
        self.join(timeout=5)


@pytest.fixture
def api_stub_server():
    """A *real* running api_stub server — used by the full-DAG integration
    test, which exercises ApiExtractOperator exactly as it runs in
    production (real HTTP over a real socket), not through an injected
    test double."""
    port = 8199
    thread = _ServerThread(api_stub_app, port)
    thread.start()

    base_url = f"http://127.0.0.1:{port}"
    for _ in range(50):
        try:
            httpx.get(f"{base_url}/health", timeout=0.2)
            break
        except httpx.ConnectError:
            time.sleep(0.1)
    else:
        raise RuntimeError("api_stub server did not start in time")

    yield base_url

    thread.stop()
