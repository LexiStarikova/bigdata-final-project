"""Tests for scripts/build_projectdb.py — Stage I data ingestion logic."""

import io
import os
import textwrap
from pathlib import Path
from unittest import mock

import pandas as pd
import numpy as np
import pytest

import build_projectdb as bp


# ── COPY_COLUMNS ──────────────────────────────────────────────────────────

EXPECTED_COLUMNS = [
    "vendorid",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "ratecodeid",
    "store_and_fwd_flag",
    "pulocationid",
    "dolocationid",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]


class TestCopyColumns:
    def test_column_count(self):
        assert len(bp.COPY_COLUMNS) == 19

    def test_column_names_match_expected(self):
        assert bp.COPY_COLUMNS == EXPECTED_COLUMNS

    def test_all_lowercase(self):
        for col in bp.COPY_COLUMNS:
            assert col == col.lower(), f"Column '{col}' is not lowercase"

    def test_no_duplicates(self):
        assert len(bp.COPY_COLUMNS) == len(set(bp.COPY_COLUMNS))

    def test_trip_id_not_in_copy_columns(self):
        """trip_id is BIGSERIAL and auto-generated, must not be in COPY columns."""
        assert "trip_id" not in bp.COPY_COLUMNS

    def test_datetime_columns_present(self):
        assert "tpep_pickup_datetime" in bp.COPY_COLUMNS
        assert "tpep_dropoff_datetime" in bp.COPY_COLUMNS

    def test_target_variable_present(self):
        """tip_amount is the target for the regression/classification task."""
        assert "tip_amount" in bp.COPY_COLUMNS

    def test_geospatial_columns_present(self):
        assert "pulocationid" in bp.COPY_COLUMNS
        assert "dolocationid" in bp.COPY_COLUMNS


class TestCopySQL:
    def test_starts_with_copy(self):
        assert bp.COPY_SQL.startswith("COPY yellow_taxi_trips (")

    def test_ends_with_format(self):
        assert bp.COPY_SQL.endswith("FROM STDIN WITH (FORMAT csv, NULL '\\N')")

    def test_all_columns_in_sql(self):
        for col in bp.COPY_COLUMNS:
            assert col in bp.COPY_SQL, f"Column '{col}' missing from COPY SQL"

    def test_column_order_matches(self):
        inside = bp.COPY_SQL.split("(")[1].split(")")[0]
        cols_in_sql = [c.strip() for c in inside.split(",")]
        assert cols_in_sql == bp.COPY_COLUMNS


# ── _repo_root ────────────────────────────────────────────────────────────

class TestRepoRoot:
    def test_returns_path(self):
        result = bp._repo_root()
        assert isinstance(result, Path)

    def test_repo_root_contains_scripts(self):
        root = bp._repo_root()
        assert (root / "scripts").is_dir()

    def test_repo_root_contains_sql(self):
        root = bp._repo_root()
        assert (root / "sql").is_dir()


# ── _load_password ────────────────────────────────────────────────────────

class TestLoadPassword:
    def test_reads_from_env_pgpassword(self, tmp_path):
        with mock.patch.dict(os.environ, {"PGPASSWORD": "secret123"}):
            assert bp._load_password(tmp_path) == "secret123"

    def test_strips_whitespace_from_env(self, tmp_path):
        with mock.patch.dict(os.environ, {"PGPASSWORD": "  secret123  "}):
            assert bp._load_password(tmp_path) == "secret123"

    def test_reads_from_psql_pass_file(self, tmp_path):
        secret_dir = tmp_path / "secrets"
        secret_dir.mkdir()
        (secret_dir / ".psql.pass").write_text("filepass\n")
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PGPASSWORD", None)
            assert bp._load_password(tmp_path) == "filepass"

    def test_reads_from_alt_psql_pass_file(self, tmp_path):
        secret_dir = tmp_path / "secrets"
        secret_dir.mkdir()
        (secret_dir / "psql.pass").write_text("altpass\n")
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PGPASSWORD", None)
            assert bp._load_password(tmp_path) == "altpass"

    def test_env_takes_precedence_over_file(self, tmp_path):
        secret_dir = tmp_path / "secrets"
        secret_dir.mkdir()
        (secret_dir / ".psql.pass").write_text("filepass\n")
        with mock.patch.dict(os.environ, {"PGPASSWORD": "envpass"}):
            assert bp._load_password(tmp_path) == "envpass"

    def test_exits_when_no_password(self, tmp_path):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PGPASSWORD", None)
            with pytest.raises(SystemExit):
                bp._load_password(tmp_path)

    def test_exits_on_empty_file(self, tmp_path):
        secret_dir = tmp_path / "secrets"
        secret_dir.mkdir()
        (secret_dir / ".psql.pass").write_text("   \n")
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PGPASSWORD", None)
            with pytest.raises(SystemExit):
                bp._load_password(tmp_path)


# ── _normalize_frame ──────────────────────────────────────────────────────

class TestNormalizeFrame:
    @pytest.fixture
    def sample_raw_df(self):
        """Mimics TLC Parquet column naming (mixed case)."""
        return pd.DataFrame({
            "VendorID": [1, 2],
            "tpep_pickup_datetime": pd.to_datetime(["2025-01-15 08:30:00", "2025-01-15 09:00:00"]),
            "tpep_dropoff_datetime": pd.to_datetime(["2025-01-15 08:45:00", "2025-01-15 09:20:00"]),
            "passenger_count": [1.0, 2.0],
            "trip_distance": [2.5, 5.0],
            "RatecodeID": [1.0, 1.0],
            "store_and_fwd_flag": ["N", "Y"],
            "PULocationID": [161, 236],
            "DOLocationID": [236, 161],
            "payment_type": [1.0, 2.0],
            "fare_amount": [12.5, 20.0],
            "extra": [1.0, 0.0],
            "mta_tax": [0.5, 0.5],
            "tip_amount": [3.0, 0.0],
            "tolls_amount": [0.0, 6.55],
            "improvement_surcharge": [1.0, 1.0],
            "total_amount": [18.0, 28.05],
            "congestion_surcharge": [2.5, 2.5],
            "airport_fee": [0.0, 1.75],
        })

    def test_output_has_all_columns(self, sample_raw_df):
        result = bp._normalize_frame(sample_raw_df)
        assert list(result.columns) == bp.COPY_COLUMNS

    def test_output_row_count_preserved(self, sample_raw_df):
        result = bp._normalize_frame(sample_raw_df)
        assert len(result) == len(sample_raw_df)

    def test_timestamps_are_tz_naive(self, sample_raw_df):
        result = bp._normalize_frame(sample_raw_df)
        assert result["tpep_pickup_datetime"].dt.tz is None
        assert result["tpep_dropoff_datetime"].dt.tz is None

    def test_numeric_columns_are_numeric(self, sample_raw_df):
        result = bp._normalize_frame(sample_raw_df)
        non_numeric = {"tpep_pickup_datetime", "tpep_dropoff_datetime", "store_and_fwd_flag"}
        for col in bp.COPY_COLUMNS:
            if col not in non_numeric:
                assert pd.api.types.is_numeric_dtype(result[col]), \
                    f"Column '{col}' should be numeric"

    def test_store_and_fwd_flag_truncated(self):
        raw = pd.DataFrame({
            "VendorID": [1],
            "tpep_pickup_datetime": pd.to_datetime(["2025-01-01 00:00:00"]),
            "tpep_dropoff_datetime": pd.to_datetime(["2025-01-01 00:10:00"]),
            "passenger_count": [1.0],
            "trip_distance": [1.0],
            "RatecodeID": [1.0],
            "store_and_fwd_flag": ["TOOLONGVALUE"],
            "PULocationID": [1],
            "DOLocationID": [2],
            "payment_type": [1.0],
            "fare_amount": [10.0],
            "extra": [0.0],
            "mta_tax": [0.5],
            "tip_amount": [2.0],
            "tolls_amount": [0.0],
            "improvement_surcharge": [0.3],
            "total_amount": [12.8],
            "congestion_surcharge": [2.5],
            "airport_fee": [0.0],
        })
        result = bp._normalize_frame(raw)
        assert len(result["store_and_fwd_flag"].iloc[0]) <= 8

    def test_handles_na_store_and_fwd_flag(self):
        raw = pd.DataFrame({
            "VendorID": [1],
            "tpep_pickup_datetime": pd.to_datetime(["2025-01-01 00:00:00"]),
            "tpep_dropoff_datetime": pd.to_datetime(["2025-01-01 00:10:00"]),
            "passenger_count": [1.0],
            "trip_distance": [1.0],
            "RatecodeID": [1.0],
            "store_and_fwd_flag": [None],
            "PULocationID": [1],
            "DOLocationID": [2],
            "payment_type": [1.0],
            "fare_amount": [10.0],
            "extra": [0.0],
            "mta_tax": [0.5],
            "tip_amount": [2.0],
            "tolls_amount": [0.0],
            "improvement_surcharge": [0.3],
            "total_amount": [12.8],
            "congestion_surcharge": [2.5],
            "airport_fee": [0.0],
        })
        result = bp._normalize_frame(raw)
        assert result["store_and_fwd_flag"].iloc[0] == ""

    def test_missing_airport_fee_column(self):
        """Older Parquet files may lack airport_fee entirely."""
        raw = pd.DataFrame({
            "VendorID": [1],
            "tpep_pickup_datetime": pd.to_datetime(["2025-01-01 00:00:00"]),
            "tpep_dropoff_datetime": pd.to_datetime(["2025-01-01 00:10:00"]),
            "passenger_count": [1.0],
            "trip_distance": [1.0],
            "RatecodeID": [1.0],
            "store_and_fwd_flag": ["N"],
            "PULocationID": [1],
            "DOLocationID": [2],
            "payment_type": [1.0],
            "fare_amount": [10.0],
            "extra": [0.0],
            "mta_tax": [0.5],
            "tip_amount": [2.0],
            "tolls_amount": [0.0],
            "improvement_surcharge": [0.3],
            "total_amount": [12.8],
            "congestion_surcharge": [2.5],
        })
        result = bp._normalize_frame(raw)
        assert "airport_fee" in result.columns
        assert pd.isna(result["airport_fee"].iloc[0])

    def test_airportfee_variant_mapped(self):
        """Some Parquet files have 'Airport_fee' instead of 'airport_fee'."""
        raw = pd.DataFrame({
            "VendorID": [1],
            "tpep_pickup_datetime": pd.to_datetime(["2025-01-01 00:00:00"]),
            "tpep_dropoff_datetime": pd.to_datetime(["2025-01-01 00:10:00"]),
            "passenger_count": [1.0],
            "trip_distance": [1.0],
            "RatecodeID": [1.0],
            "store_and_fwd_flag": ["N"],
            "PULocationID": [1],
            "DOLocationID": [2],
            "payment_type": [1.0],
            "fare_amount": [10.0],
            "extra": [0.0],
            "mta_tax": [0.5],
            "tip_amount": [2.0],
            "tolls_amount": [0.0],
            "improvement_surcharge": [0.3],
            "total_amount": [12.8],
            "congestion_surcharge": [2.5],
            "Airport_fee": [1.75],
        })
        result = bp._normalize_frame(raw)
        assert result["airport_fee"].iloc[0] == 1.75

    def test_csv_output_format(self, sample_raw_df):
        """Verify the normalized frame produces valid CSV for COPY."""
        result = bp._normalize_frame(sample_raw_df)
        buf = io.StringIO()
        result.to_csv(
            buf, index=False, header=False,
            na_rep="\\N",
            date_format="%Y-%m-%d %H:%M:%S",
            columns=bp.COPY_COLUMNS,
        )
        buf.seek(0)
        csv_text = buf.read()
        lines = csv_text.strip().split("\n")
        assert len(lines) == 2
        for line in lines:
            fields = line.split(",")
            assert len(fields) == len(bp.COPY_COLUMNS)


# ── _parquet_paths ────────────────────────────────────────────────────────

class TestParquetPaths:
    def test_finds_matching_files(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        for m in ["01", "02", "03"]:
            (data_dir / f"yellow_tripdata_2025-{m}.parquet").write_bytes(b"dummy")
        paths = bp._parquet_paths(tmp_path)
        assert len(paths) == 3
        assert all(p.suffix == ".parquet" for p in paths)

    def test_paths_are_sorted(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        for m in ["03", "01", "02"]:
            (data_dir / f"yellow_tripdata_2025-{m}.parquet").write_bytes(b"dummy")
        paths = bp._parquet_paths(tmp_path)
        names = [p.name for p in paths]
        assert names == sorted(names)

    def test_ignores_non_matching_files(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "yellow_tripdata_2025-01.parquet").write_bytes(b"dummy")
        (data_dir / "green_tripdata_2025-01.parquet").write_bytes(b"not this")
        (data_dir / "yellow_tripdata_2024-01.parquet").write_bytes(b"wrong year")
        paths = bp._parquet_paths(tmp_path)
        assert len(paths) == 1

    def test_exits_when_no_files(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        with pytest.raises(SystemExit):
            bp._parquet_paths(tmp_path)


# ── _connect ──────────────────────────────────────────────────────────────

class TestConnect:
    def test_exits_when_no_user(self, tmp_path):
        secret_dir = tmp_path / "secrets"
        secret_dir.mkdir()
        (secret_dir / ".psql.pass").write_text("pass\n")
        env = {"PGPASSWORD": "pass"}
        for k in ("PGUSER", "USER", "USERNAME"):
            env[k] = ""
        with mock.patch.dict(os.environ, env, clear=True):
            with pytest.raises(SystemExit):
                bp._connect(tmp_path)

    def test_derives_dbname_from_user(self, tmp_path):
        secret_dir = tmp_path / "secrets"
        secret_dir.mkdir()
        (secret_dir / ".psql.pass").write_text("pass\n")
        with mock.patch.dict(os.environ, {
            "PGUSER": "team35",
            "PGPASSWORD": "pass",
        }):
            with mock.patch("build_projectdb.psql.connect") as mock_connect:
                mock_connect.return_value = mock.MagicMock()
                bp._connect(tmp_path)
                call_kwargs = mock_connect.call_args[1]
                assert call_kwargs["dbname"] == "team35_projectdb"
                assert call_kwargs["user"] == "team35"
                assert call_kwargs["host"] == "hadoop-04.uni.innopolis.ru"
                assert call_kwargs["port"] == 5432
