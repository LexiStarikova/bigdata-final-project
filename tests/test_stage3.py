"""Tests for scripts/stage3.py — Stage III Spark ML regression helpers.

Only pure-Python functions are tested directly; Spark-dependent functions are
tested via mocking where feasible.
"""

import csv
import json
import math
import os
import textwrap
from pathlib import Path
from unittest import mock

import pytest

import stage3 as s3
import stage3_helpers as s3h


# ── _remote_data_uri ─────────────────────────────────────────────────────

class TestRemoteDataUri:
    def test_hdfs_triple_slash(self):
        assert s3h._remote_data_uri("hdfs:///user/team35/data") is True

    def test_hdfs_double_slash(self):
        assert s3h._remote_data_uri("hdfs://namenode/data") is True

    def test_s3_uri(self):
        assert s3h._remote_data_uri("s3://bucket/path") is True

    def test_s3a_uri(self):
        assert s3h._remote_data_uri("s3a://bucket/path") is True

    def test_wasb_uri(self):
        assert s3h._remote_data_uri("wasb://container@account/path") is True

    def test_abfs_uri(self):
        assert s3h._remote_data_uri("abfs://container@account/path") is True

    def test_viewfs_uri(self):
        assert s3h._remote_data_uri("viewfs://cluster/data") is True

    def test_local_absolute_path(self):
        assert s3h._remote_data_uri("/home/user/data") is False

    def test_relative_path(self):
        assert s3h._remote_data_uri("data/parquet") is False

    def test_file_scheme(self):
        assert s3h._remote_data_uri("file:///tmp/data") is False

    def test_whitespace_stripped(self):
        assert s3h._remote_data_uri("  hdfs:///data  ") is True

    def test_case_insensitive_scheme(self):
        assert s3h._remote_data_uri("HDFS:///data") is True


# ── _normalize_hdfs_uri_for_spark ────────────────────────────────────────

class TestNormalizeHdfsUri:
    def test_triple_slash_unchanged(self):
        assert s3h._normalize_hdfs_uri_for_spark("hdfs:///user/data") == "hdfs:///user/data"

    def test_single_slash_gets_double(self):
        assert s3h._normalize_hdfs_uri_for_spark("hdfs:/user/data") == "hdfs:///user/data"

    def test_non_hdfs_passthrough(self):
        assert s3h._normalize_hdfs_uri_for_spark("s3://bucket/key") == "s3://bucket/key"

    def test_local_path_passthrough(self):
        assert s3h._normalize_hdfs_uri_for_spark("/home/data") == "/home/data"

    def test_double_slash_unchanged(self):
        assert s3h._normalize_hdfs_uri_for_spark("hdfs://nn:8020/data") == "hdfs://nn:8020/data"


# ── resolve_filesystem_data_dir ──────────────────────────────────────────

class TestResolveFilesystemDataDir:
    def test_remote_uri_untouched(self):
        result = s3h.resolve_filesystem_data_dir("hdfs:///user/data/")
        assert result == "hdfs:///user/data"

    def test_local_path_becomes_absolute(self):
        result = s3h.resolve_filesystem_data_dir("data")
        assert os.path.isabs(result)

    def test_trailing_slash_stripped(self):
        result = s3h.resolve_filesystem_data_dir("/tmp/data/")
        assert not result.endswith("/")


# ── hive_table_qualifier ─────────────────────────────────────────────────

class TestHiveTableQualifier:
    def test_none_when_no_table(self):
        assert s3h.hive_table_qualifier("mydb", None) is None

    def test_empty_table_returns_none(self):
        assert s3h.hive_table_qualifier("mydb", "") is None

    def test_db_and_short_table(self):
        assert s3h.hive_table_qualifier("team35_projectdb", "yellow_taxi") == \
            "team35_projectdb.yellow_taxi"

    def test_qualified_table_without_db(self):
        assert s3h.hive_table_qualifier(None, "mydb.mytable") == "mydb.mytable"

    def test_qualified_table_with_db_raises(self):
        with pytest.raises(ValueError, match="not both"):
            s3h.hive_table_qualifier("mydb", "otherdb.table")

    def test_strips_table_whitespace(self):
        assert s3h.hive_table_qualifier("db", "  tbl  ") == "db.tbl"

    def test_no_db_no_dot_returns_table(self):
        assert s3h.hive_table_qualifier(None, "simple_table") == "simple_table"

    def test_no_db_empty_string_db(self):
        assert s3h.hive_table_qualifier("", "simple_table") == "simple_table"


# ── scaler_use_mean ──────────────────────────────────────────────────────

class TestScalerUseMean:
    def test_cli_flag_true(self):
        assert s3h.scaler_use_mean(True) is True

    def test_cli_flag_false_env_unset(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("STAGE3_SCALER_WITH_MEAN", None)
            assert s3h.scaler_use_mean(False) is False

    def test_cli_flag_false_env_true(self):
        with mock.patch.dict(os.environ, {"STAGE3_SCALER_WITH_MEAN": "1"}):
            assert s3h.scaler_use_mean(False) is True

    def test_env_yes(self):
        with mock.patch.dict(os.environ, {"STAGE3_SCALER_WITH_MEAN": "yes"}):
            assert s3h.scaler_use_mean(False) is True

    def test_env_true_string(self):
        with mock.patch.dict(os.environ, {"STAGE3_SCALER_WITH_MEAN": "true"}):
            assert s3h.scaler_use_mean(False) is True

    def test_env_false_value(self):
        with mock.patch.dict(os.environ, {"STAGE3_SCALER_WITH_MEAN": "0"}):
            assert s3h.scaler_use_mean(False) is False


# ── _use_hdfs_for_writes ─────────────────────────────────────────────────

class TestUseHdfsForWrites:
    def test_local_only_env_disables(self):
        with mock.patch.dict(os.environ, {"STAGE3_LOCAL_WRITES_ONLY": "1"}):
            assert s3h._use_hdfs_for_writes() is False

    def test_local_only_true_string(self):
        with mock.patch.dict(os.environ, {"STAGE3_LOCAL_WRITES_ONLY": "true"}):
            assert s3h._use_hdfs_for_writes() is False

    def test_local_only_yes(self):
        with mock.patch.dict(os.environ, {"STAGE3_LOCAL_WRITES_ONLY": "yes"}):
            assert s3h._use_hdfs_for_writes() is False

    def test_no_env_checks_hdfs_binary(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("STAGE3_LOCAL_WRITES_ONLY", None)
            with mock.patch("stage3_helpers.shutil.which", return_value="/usr/bin/hdfs"):
                assert s3h._use_hdfs_for_writes() is True

    def test_no_hdfs_binary_returns_false(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("STAGE3_LOCAL_WRITES_ONLY", None)
            with mock.patch("stage3_helpers.shutil.which", return_value=None):
                assert s3h._use_hdfs_for_writes() is False


# ── _local_file_uri ──────────────────────────────────────────────────────

class TestLocalFileUri:
    def test_produces_file_uri(self):
        result = s3h._local_file_uri("/tmp/some/path")
        assert result.startswith("file://")

    def test_absolute_path_preserved(self):
        result = s3h._local_file_uri("/tmp/some/path")
        assert "/tmp/some/path" in result


# ── _hdfs_scratch_base ───────────────────────────────────────────────────

class TestHdfsScratchBase:
    def test_default_uses_user(self):
        with mock.patch.dict(os.environ, {"USER": "team35"}, clear=False):
            os.environ.pop("STAGE3_HDFS_SCRATCH", None)
            result = s3h._hdfs_scratch_base()
            assert "team35" in result
            assert result.startswith("hdfs:///")

    def test_env_override(self):
        with mock.patch.dict(os.environ, {"STAGE3_HDFS_SCRATCH": "hdfs:///custom/scratch"}):
            assert s3h._hdfs_scratch_base() == "hdfs:///custom/scratch"

    def test_trailing_slash_stripped(self):
        with mock.patch.dict(os.environ, {"STAGE3_HDFS_SCRATCH": "hdfs:///custom/"}):
            assert not s3h._hdfs_scratch_base().endswith("/")


# ── discover_parquet ─────────────────────────────────────────────────────

class TestDiscoverParquet:
    def test_finds_shallow_parquets(self, tmp_path):
        (tmp_path / "yellow_tripdata_2025-01.parquet").write_bytes(b"P")
        (tmp_path / "yellow_tripdata_2025-02.parquet").write_bytes(b"P")
        found = s3h.discover_parquet(str(tmp_path))
        assert len(found) == 2

    def test_finds_nested_parquets(self, tmp_path):
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "yellow_tripdata_2025-01.parquet").write_bytes(b"P")
        found = s3h.discover_parquet(str(tmp_path))
        assert len(found) == 1

    def test_returns_sorted(self, tmp_path):
        for m in ("03", "01", "02"):
            (tmp_path / f"yellow_tripdata_2025-{m}.parquet").write_bytes(b"P")
        found = s3h.discover_parquet(str(tmp_path))
        assert found == sorted(found)

    def test_empty_falls_through_to_fuzzy(self, tmp_path):
        (tmp_path / "yellow_trip_something.parquet").write_bytes(b"P")
        found = s3h.discover_parquet(str(tmp_path))
        assert len(found) == 1

    def test_returns_empty_list_when_nothing(self, tmp_path):
        found = s3h.discover_parquet(str(tmp_path))
        assert found == []

    def test_deduplicates_shallow_and_nested(self, tmp_path):
        (tmp_path / "yellow_tripdata_2025-01.parquet").write_bytes(b"P")
        found = s3h.discover_parquet(str(tmp_path))
        assert len(found) == 1


# ── compute_prediction_csv_metrics ───────────────────────────────────────

class TestComputePredictionCsvMetrics:
    def _write_prediction_csv(self, path, rows):
        with open(path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["label", "prediction"])
            writer.writeheader()
            writer.writerows(rows)

    def test_perfect_predictions(self, tmp_path):
        csv_path = str(tmp_path / "pred.csv")
        self._write_prediction_csv(csv_path, [
            {"label": "1.0", "prediction": "1.0"},
            {"label": "2.0", "prediction": "2.0"},
            {"label": "3.0", "prediction": "3.0"},
        ])
        metrics = s3h.compute_prediction_csv_metrics(csv_path)
        assert metrics["rmse"] == pytest.approx(0.0)
        assert metrics["mae"] == pytest.approx(0.0)
        assert metrics["r2"] == pytest.approx(1.0)
        assert metrics["within_1_dollar"] == pytest.approx(1.0)
        assert metrics["within_2_dollars"] == pytest.approx(1.0)

    def test_known_error_values(self, tmp_path):
        csv_path = str(tmp_path / "pred.csv")
        self._write_prediction_csv(csv_path, [
            {"label": "0.0", "prediction": "1.0"},
            {"label": "0.0", "prediction": "1.0"},
        ])
        metrics = s3h.compute_prediction_csv_metrics(csv_path)
        assert metrics["rmse"] == pytest.approx(1.0)
        assert metrics["mae"] == pytest.approx(1.0)
        assert metrics["within_1_dollar"] == pytest.approx(1.0)

    def test_within_2_dollars(self, tmp_path):
        csv_path = str(tmp_path / "pred.csv")
        self._write_prediction_csv(csv_path, [
            {"label": "0.0", "prediction": "1.5"},
            {"label": "0.0", "prediction": "3.0"},
        ])
        metrics = s3h.compute_prediction_csv_metrics(csv_path)
        assert metrics["within_1_dollar"] == pytest.approx(0.0)
        assert metrics["within_2_dollars"] == pytest.approx(0.5)

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            s3h.compute_prediction_csv_metrics(str(tmp_path / "nonexistent.csv"))

    def test_empty_csv_raises(self, tmp_path):
        csv_path = str(tmp_path / "empty.csv")
        with open(csv_path, "w", encoding="utf-8") as fh:
            fh.write("label,prediction\n")
        with pytest.raises(RuntimeError, match="empty"):
            s3h.compute_prediction_csv_metrics(csv_path)

    def test_mean_error(self, tmp_path):
        csv_path = str(tmp_path / "pred.csv")
        self._write_prediction_csv(csv_path, [
            {"label": "5.0", "prediction": "6.0"},
            {"label": "5.0", "prediction": "4.0"},
        ])
        metrics = s3h.compute_prediction_csv_metrics(csv_path)
        assert metrics["mean_error"] == pytest.approx(0.0)

    def test_median_abs_error(self, tmp_path):
        csv_path = str(tmp_path / "pred.csv")
        self._write_prediction_csv(csv_path, [
            {"label": "0.0", "prediction": "1.0"},
            {"label": "0.0", "prediction": "2.0"},
            {"label": "0.0", "prediction": "3.0"},
        ])
        metrics = s3h.compute_prediction_csv_metrics(csv_path)
        assert metrics["median_abs_error"] == pytest.approx(2.0)

    def test_r2_single_value_labels(self, tmp_path):
        csv_path = str(tmp_path / "pred.csv")
        self._write_prediction_csv(csv_path, [
            {"label": "5.0", "prediction": "5.0"},
            {"label": "5.0", "prediction": "5.0"},
        ])
        metrics = s3h.compute_prediction_csv_metrics(csv_path)
        assert metrics["r2"] == pytest.approx(0.0)


# ── write_json_local ─────────────────────────────────────────────────────

class TestWriteJsonLocal:
    def test_creates_file(self, tmp_path):
        out = str(tmp_path / "test.json")
        s3h.write_json_local({"key": "value"}, out)
        assert os.path.isfile(out)

    def test_valid_json_content(self, tmp_path):
        out = str(tmp_path / "test.json")
        payload = {"model": "LR", "rmse": 1.23}
        s3h.write_json_local(payload, out)
        with open(out, encoding="utf-8") as fh:
            loaded = json.load(fh)
        assert loaded == payload

    def test_creates_parent_dirs(self, tmp_path):
        out = str(tmp_path / "deep" / "nested" / "test.json")
        s3h.write_json_local({"x": 1}, out)
        assert os.path.isfile(out)

    def test_indent_formatting(self, tmp_path):
        out = str(tmp_path / "test.json")
        s3h.write_json_local({"a": 1}, out)
        with open(out, encoding="utf-8") as fh:
            content = fh.read()
        assert "\n" in content


# ── summarize_params ─────────────────────────────────────────────────────

class TestSummarizeParams:
    def test_basic_extraction(self):
        model = mock.MagicMock()
        param1 = mock.MagicMock()
        param1.name = "regParam"
        param2 = mock.MagicMock()
        param2.name = "maxIter"
        model.extractParamMap.return_value = {param1: 0.01, param2: 100}
        result = s3h.summarize_params(model)
        assert result["regParam"] == 0.01
        assert result["maxIter"] == 100

    def test_numpy_item_conversion(self):
        import numpy as np
        model = mock.MagicMock()
        param = mock.MagicMock()
        param.name = "regParam"
        model.extractParamMap.return_value = {param: np.float64(0.01)}
        result = s3h.summarize_params(model)
        assert isinstance(result["regParam"], float)


# ── load_saved_best_params ───────────────────────────────────────────────

class TestLoadSavedBestParams:
    def test_returns_none_when_missing(self, tmp_path):
        result = s3h.load_saved_best_params(str(tmp_path), "model1")
        assert result is None

    def test_loads_params_from_file(self, tmp_path):
        payload = {
            "model": "LinearRegression",
            "best_params_from_cv": {"regParam": 0.01, "maxIter": 100},
        }
        path = tmp_path / "model1_best_params.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        result = s3h.load_saved_best_params(str(tmp_path), "model1")
        assert result == {"regParam": 0.01, "maxIter": 100}


# ── STAGE2_TO_TLC_COLUMNS constant ──────────────────────────────────────

class TestStage2Constants:
    def test_stage2_column_mapping_count(self):
        assert len(s3h.STAGE2_TO_TLC_COLUMNS) == 4

    def test_all_mappings_are_tuples(self):
        for mapping in s3h.STAGE2_TO_TLC_COLUMNS:
            assert isinstance(mapping, tuple)
            assert len(mapping) == 2

    def test_lowercase_sources(self):
        for src, _ in s3h.STAGE2_TO_TLC_COLUMNS:
            assert src == src.lower()

    def test_categorical_features_constant(self):
        assert len(s3h.CATEGORICAL_FEATURES) == 4
        for feat in s3h.CATEGORICAL_FEATURES:
            assert isinstance(feat, str)


# ── parse_args ───────────────────────────────────────────────────────────

class TestParseArgs:
    def test_defaults(self):
        with mock.patch("sys.argv", ["stage3.py"]):
            with mock.patch.dict(os.environ, {}, clear=False):
                for key in ("SPARK_MASTER", "STAGE3_HIVE_DATABASE",
                            "STAGE3_HIVE_TABLE"):
                    os.environ.pop(key, None)
                args = s3.parse_args()
        assert args.data_dir == "data"
        assert args.models_dir == "models"
        assert args.output_dir == "output"
        assert args.cv_folds == 3
        assert args.random_seed == 42

    def test_custom_args(self):
        with mock.patch("sys.argv", [
            "stage3.py",
            "--data-dir", "/custom/data",
            "--cv-folds", "5",
            "--random-seed", "123",
        ]):
            args = s3.parse_args()
        assert args.data_dir == "/custom/data"
        assert args.cv_folds == 5
        assert args.random_seed == 123

    def test_hive_args(self):
        with mock.patch("sys.argv", [
            "stage3.py",
            "--hive-database", "mydb",
            "--hive-table", "mytable",
        ]):
            args = s3.parse_args()
        assert args.hive_database == "mydb"
        assert args.hive_table == "mytable"

    def test_sample_fraction(self):
        with mock.patch("sys.argv", ["stage3.py", "--sample-fraction", "0.1"]):
            args = s3.parse_args()
        assert args.sample_fraction == pytest.approx(0.1)

    def test_start_model_choices(self):
        for choice in (1, 2, 3):
            with mock.patch("sys.argv", [
                "stage3.py", "--start-model", str(choice),
            ]):
                args = s3.parse_args()
            assert args.start_model == choice
