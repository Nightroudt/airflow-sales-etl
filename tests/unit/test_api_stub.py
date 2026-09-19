from fastapi.testclient import TestClient

from api_stub.main import app

client = TestClient(app)


def test_health_returns_ok():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_products_returns_one_entry_per_id():
    response = client.get("/products", params={"ids": "1,2,3"})

    assert response.status_code == 200
    body = response.json()
    assert [p["product_id"] for p in body] == [1, 2, 3]


def test_products_rejects_non_integer_ids_with_422_instead_of_crashing():
    response = client.get("/products", params={"ids": "1,abc,3"})

    assert response.status_code == 422
