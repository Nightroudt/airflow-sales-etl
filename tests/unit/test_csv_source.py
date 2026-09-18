from datetime import UTC, datetime

import pandas as pd

from etl.extract.csv_source import (
    TRANSACTION_COLUMNS,
    generate_sample_transactions_csv,
    read_transactions_dir,
)


def test_generate_writes_requested_row_count(tmp_path):
    path = generate_sample_transactions_csv(tmp_path / "sample.csv", n=25, seed=1)

    df = pd.read_csv(path)

    assert len(df) == 25
    assert list(df.columns) == TRANSACTION_COLUMNS


def test_generate_is_deterministic_with_a_seed_and_fixed_now(tmp_path):
    fixed_now = datetime(2026, 1, 1, tzinfo=UTC)
    path_a = generate_sample_transactions_csv(tmp_path / "a.csv", n=10, seed=42, now=fixed_now)
    path_b = generate_sample_transactions_csv(tmp_path / "b.csv", n=10, seed=42, now=fixed_now)

    df_a = pd.read_csv(path_a)
    df_b = pd.read_csv(path_b)

    pd.testing.assert_frame_equal(df_a, df_b)


def test_generate_creates_missing_parent_directories(tmp_path):
    nested = tmp_path / "a" / "b" / "c.csv"

    path = generate_sample_transactions_csv(nested, n=1)

    assert path.exists()


def test_read_transactions_dir_concatenates_all_csvs(tmp_path):
    generate_sample_transactions_csv(tmp_path / "one.csv", n=3, seed=1)
    generate_sample_transactions_csv(tmp_path / "two.csv", n=4, seed=2)

    df = read_transactions_dir(tmp_path)

    assert len(df) == 7


def test_read_transactions_dir_returns_empty_frame_for_empty_dir(tmp_path):
    df = read_transactions_dir(tmp_path)

    assert len(df) == 0
    assert list(df.columns) == TRANSACTION_COLUMNS
