import pandas as pd

from etl.transform.cleaning import clean_transactions


def make_row(**overrides) -> dict:
    row = {
        "transaction_id": "t1",
        "product_id": 1,
        "customer_id": 5,
        "quantity": 2,
        "unit_price": 10.0,
        "transaction_date": "2026-01-01T00:00:00Z",
    }
    row.update(overrides)
    return row


def test_keeps_valid_rows_unchanged():
    df = pd.DataFrame([make_row()])

    out = clean_transactions(df)

    assert len(out) == 1
    assert out.iloc[0]["transaction_id"] == "t1"


def test_drops_exact_duplicate_transaction_ids():
    df = pd.DataFrame([make_row(), make_row()])

    out = clean_transactions(df)

    assert len(out) == 1


def test_keeps_first_of_duplicate_transaction_ids():
    df = pd.DataFrame([make_row(quantity=1), make_row(quantity=99)])

    out = clean_transactions(df)

    assert len(out) == 1
    assert out.iloc[0]["quantity"] == 1


def test_drops_rows_with_missing_required_fields():
    df = pd.DataFrame([make_row(customer_id=None), make_row(transaction_id="t2")])

    out = clean_transactions(df)

    assert len(out) == 1
    assert out.iloc[0]["transaction_id"] == "t2"


def test_drops_non_positive_quantity():
    df = pd.DataFrame([make_row(transaction_id="t2", quantity=0), make_row(transaction_id="t3")])

    out = clean_transactions(df)

    assert list(out["transaction_id"]) == ["t3"]


def test_drops_negative_unit_price():
    df = pd.DataFrame(
        [make_row(transaction_id="t2", unit_price=-5.0), make_row(transaction_id="t3")]
    )

    out = clean_transactions(df)

    assert list(out["transaction_id"]) == ["t3"]


def test_drops_rows_with_unparseable_types():
    df = pd.DataFrame(
        [make_row(transaction_id="t2", quantity="not-a-number"), make_row(transaction_id="t3")]
    )

    out = clean_transactions(df)

    assert list(out["transaction_id"]) == ["t3"]


def test_coerces_types_on_output():
    df = pd.DataFrame([make_row(product_id="1", customer_id="5", quantity="2")])

    out = clean_transactions(df)

    assert pd.api.types.is_integer_dtype(out["product_id"])
    assert pd.api.types.is_integer_dtype(out["customer_id"])
    assert pd.api.types.is_integer_dtype(out["quantity"])


def test_does_not_mutate_input_dataframe():
    df = pd.DataFrame([make_row()])
    original = df.copy()

    clean_transactions(df)

    pd.testing.assert_frame_equal(df, original)


def test_empty_input_returns_empty_output():
    from etl.transform.cleaning import REQUIRED_COLUMNS

    df = pd.DataFrame(columns=REQUIRED_COLUMNS)

    out = clean_transactions(df)

    assert len(out) == 0
