import pandas as pd

from etl.transform.enrichment import shape_products


def make_row(**overrides) -> dict:
    row = {
        "product_id": 1,
        "name": "Widget",
        "category": "Tools",
        "supplier": "Acme",
        "unit_cost": 10.0,
        "updated_at": "2026-01-01T00:00:00Z",
    }
    row.update(overrides)
    return row


def test_keeps_valid_rows():
    df = pd.DataFrame([make_row()])

    out = shape_products(df)

    assert len(out) == 1
    assert out.iloc[0]["name"] == "Widget"


def test_drops_rows_with_missing_fields():
    df = pd.DataFrame([make_row(name=None), make_row(product_id=2)])

    out = shape_products(df)

    assert len(out) == 1
    assert out.iloc[0]["product_id"] == 2


def test_drops_negative_unit_cost():
    df = pd.DataFrame([make_row(unit_cost=-1.0), make_row(product_id=2)])

    out = shape_products(df)

    assert list(out["product_id"]) == [2]


def test_dedupes_same_product_and_version_keeping_last():
    df = pd.DataFrame([make_row(name="Old"), make_row(name="New")])

    out = shape_products(df)

    assert len(out) == 1
    assert out.iloc[0]["name"] == "New"


def test_different_versions_of_same_product_both_kept():
    df = pd.DataFrame(
        [make_row(updated_at="2026-01-01T00:00:00Z"), make_row(updated_at="2026-01-02T00:00:00Z")]
    )

    out = shape_products(df)

    assert len(out) == 2


def test_strips_whitespace_from_text_fields():
    df = pd.DataFrame([make_row(name="  Widget  ", category=" Tools ")])

    out = shape_products(df)

    assert out.iloc[0]["name"] == "Widget"
    assert out.iloc[0]["category"] == "Tools"


def test_coerces_product_id_to_int():
    df = pd.DataFrame([make_row(product_id="1")])

    out = shape_products(df)

    assert pd.api.types.is_integer_dtype(out["product_id"])
