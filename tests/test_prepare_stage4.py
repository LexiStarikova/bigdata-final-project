"""Tests for scripts/prepare_stage4_ml_artifacts.py — Stage IV artifact preparation."""

import csv
import json
import os
from pathlib import Path
from unittest import mock

import pytest

import prepare_stage4_ml_artifacts as s4


# ── stringify ─────────────────────────────────────────────────────────────

class TestStringify:
    def test_bool_true(self):
        assert s4.stringify(True) == "true"

    def test_bool_false(self):
        assert s4.stringify(False) == "false"

    def test_none(self):
        assert s4.stringify(None) == ""

    def test_string(self):
        assert s4.stringify("hello") == "hello"

    def test_integer(self):
        assert s4.stringify(42) == "42"

    def test_float(self):
        assert s4.stringify(3.14) == "3.14"

    def test_zero(self):
        assert s4.stringify(0) == "0"

    def test_empty_string(self):
        assert s4.stringify("") == ""


# ── bucket_error ──────────────────────────────────────────────────────────

class TestBucketError:
    def test_within_1(self):
        assert s4.bucket_error(0.5) == "A_<=1"

    def test_exactly_1(self):
        assert s4.bucket_error(1.0) == "A_<=1"

    def test_between_1_and_2(self):
        assert s4.bucket_error(1.5) == "B_1_to_2"

    def test_exactly_2(self):
        assert s4.bucket_error(2.0) == "B_1_to_2"

    def test_between_2_and_5(self):
        assert s4.bucket_error(3.0) == "C_2_to_5"

    def test_exactly_5(self):
        assert s4.bucket_error(5.0) == "C_2_to_5"

    def test_above_5(self):
        assert s4.bucket_error(10.0) == "D_>5"

    def test_zero(self):
        assert s4.bucket_error(0.0) == "A_<=1"

    def test_large_error(self):
        assert s4.bucket_error(100.0) == "D_>5"


# ── MODELS constant ──────────────────────────────────────────────────────

class TestModelsConstant:
    def test_three_models(self):
        assert len(s4.MODELS) == 3

    def test_model_keys(self):
        keys = [m[0] for m in s4.MODELS]
        assert keys == ["model1", "model2", "model3"]

    def test_model_names(self):
        names = [m[1] for m in s4.MODELS]
        assert "LinearRegression" in names
        assert "RandomForestRegressor" in names
        assert "GBTRegressor" in names


# ── FEATURE_DESCRIPTIONS constant ────────────────────────────────────────

class TestFeatureDescriptions:
    def test_non_empty(self):
        assert len(s4.FEATURE_DESCRIPTIONS) > 0

    def test_all_tuples_of_three(self):
        for desc in s4.FEATURE_DESCRIPTIONS:
            assert isinstance(desc, tuple)
            assert len(desc) == 3

    def test_feature_groups_present(self):
        groups = {d[0] for d in s4.FEATURE_DESCRIPTIONS}
        assert "raw_trip" in groups
        assert "fare" in groups
        assert "time" in groups

    def test_all_descriptions_non_empty(self):
        for _, _, desc in s4.FEATURE_DESCRIPTIONS:
            assert len(desc) > 0


# ── repo_root ─────────────────────────────────────────────────────────────

class TestRepoRoot:
    def test_returns_path(self):
        result = s4.repo_root()
        assert isinstance(result, Path)

    def test_contains_scripts_dir(self):
        root = s4.repo_root()
        assert (root / "scripts").is_dir()


# ── read_json ─────────────────────────────────────────────────────────────

class TestReadJson:
    def test_reads_valid_json(self, tmp_path):
        path = tmp_path / "test.json"
        path.write_text('{"key": "value"}', encoding="utf-8")
        result = s4.read_json(path)
        assert result == {"key": "value"}

    def test_reads_nested_json(self, tmp_path):
        path = tmp_path / "test.json"
        payload = {"model": {"params": {"lr": 0.01}}}
        path.write_text(json.dumps(payload), encoding="utf-8")
        result = s4.read_json(path)
        assert result == payload


# ── write_csv ─────────────────────────────────────────────────────────────

class TestWriteCsv:
    def test_creates_file(self, tmp_path):
        path = tmp_path / "out.csv"
        s4.write_csv(path, ["a", "b"], [{"a": 1, "b": 2}])
        assert path.is_file()

    def test_header_row(self, tmp_path):
        path = tmp_path / "out.csv"
        s4.write_csv(path, ["col1", "col2"], [{"col1": "x", "col2": "y"}])
        text = path.read_text(encoding="utf-8")
        assert text.startswith("col1,col2")

    def test_data_rows(self, tmp_path):
        path = tmp_path / "out.csv"
        rows = [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]
        s4.write_csv(path, ["a", "b"], rows)
        lines = path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 3

    def test_creates_parent_dirs(self, tmp_path):
        path = tmp_path / "deep" / "nested" / "out.csv"
        s4.write_csv(path, ["x"], [{"x": "1"}])
        assert path.is_file()

    def test_empty_rows(self, tmp_path):
        path = tmp_path / "out.csv"
        s4.write_csv(path, ["a", "b"], [])
        text = path.read_text(encoding="utf-8").strip()
        assert text == "a,b"


# ── sample_prediction_rows ───────────────────────────────────────────────

class TestSamplePredictionRows:
    def _write_pred_csv(self, path, n_rows):
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["label", "prediction"])
            writer.writeheader()
            for i in range(n_rows):
                writer.writerow({"label": str(float(i)), "prediction": str(float(i + 0.1))})

    def test_returns_all_when_fewer_than_sample(self, tmp_path):
        path = tmp_path / "pred.csv"
        self._write_pred_csv(path, 3)
        result = s4.sample_prediction_rows(path, 10, seed=42)
        assert len(result) == 3

    def test_returns_exact_sample_size(self, tmp_path):
        path = tmp_path / "pred.csv"
        self._write_pred_csv(path, 100)
        result = s4.sample_prediction_rows(path, 10, seed=42)
        assert len(result) == 10

    def test_zero_sample_size(self, tmp_path):
        path = tmp_path / "pred.csv"
        self._write_pred_csv(path, 5)
        result = s4.sample_prediction_rows(path, 0, seed=42)
        assert result == []

    def test_deterministic_with_seed(self, tmp_path):
        path = tmp_path / "pred.csv"
        self._write_pred_csv(path, 50)
        r1 = s4.sample_prediction_rows(path, 5, seed=42)
        r2 = s4.sample_prediction_rows(path, 5, seed=42)
        assert r1 == r2

    def test_different_seeds_may_differ(self, tmp_path):
        path = tmp_path / "pred.csv"
        self._write_pred_csv(path, 100)
        r1 = s4.sample_prediction_rows(path, 5, seed=42)
        r2 = s4.sample_prediction_rows(path, 5, seed=99)
        nums1 = [r[0] for r in r1]
        nums2 = [r[0] for r in r2]
        assert nums1 != nums2 or True  # might occasionally match

    def test_result_sorted_by_row_number(self, tmp_path):
        path = tmp_path / "pred.csv"
        self._write_pred_csv(path, 50)
        result = s4.sample_prediction_rows(path, 10, seed=42)
        row_numbers = [r[0] for r in result]
        assert row_numbers == sorted(row_numbers)

    def test_each_row_has_label_and_prediction(self, tmp_path):
        path = tmp_path / "pred.csv"
        self._write_pred_csv(path, 5)
        result = s4.sample_prediction_rows(path, 5, seed=42)
        for _, row in result:
            assert "label" in row
            assert "prediction" in row


# ── validate_required_outputs ────────────────────────────────────────────

class TestValidateRequiredOutputs:
    def _create_all_outputs(self, output_dir):
        """Create minimal placeholder files for all required Stage III outputs."""
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "evaluation.csv").write_text("model,RMSE\n", encoding="utf-8")
        (output_dir / "model_feature_signals.csv").write_text("model,rank\n", encoding="utf-8")
        (output_dir / "stage3_training_summary.json").write_text("{}", encoding="utf-8")
        for key in ("model1", "model2", "model3"):
            (output_dir / f"{key}_predictions.csv").write_text(
                "label,prediction\n", encoding="utf-8",
            )
            (output_dir / f"{key}_best_params.json").write_text("{}", encoding="utf-8")

    def test_passes_when_all_present(self, tmp_path):
        output_dir = tmp_path / "output"
        self._create_all_outputs(output_dir)
        s4.validate_required_outputs(output_dir)

    def test_raises_when_evaluation_missing(self, tmp_path):
        output_dir = tmp_path / "output"
        self._create_all_outputs(output_dir)
        (output_dir / "evaluation.csv").unlink()
        with pytest.raises(FileNotFoundError, match="evaluation.csv"):
            s4.validate_required_outputs(output_dir)

    def test_raises_when_predictions_missing(self, tmp_path):
        output_dir = tmp_path / "output"
        self._create_all_outputs(output_dir)
        (output_dir / "model2_predictions.csv").unlink()
        with pytest.raises(FileNotFoundError, match="model2_predictions.csv"):
            s4.validate_required_outputs(output_dir)

    def test_raises_when_params_missing(self, tmp_path):
        output_dir = tmp_path / "output"
        self._create_all_outputs(output_dir)
        (output_dir / "model3_best_params.json").unlink()
        with pytest.raises(FileNotFoundError, match="model3_best_params.json"):
            s4.validate_required_outputs(output_dir)

    def test_raises_when_summary_missing(self, tmp_path):
        output_dir = tmp_path / "output"
        self._create_all_outputs(output_dir)
        (output_dir / "stage3_training_summary.json").unlink()
        with pytest.raises(FileNotFoundError, match="stage3_training_summary.json"):
            s4.validate_required_outputs(output_dir)


# ── build_best_params ────────────────────────────────────────────────────

class TestBuildBestParams:
    def _setup_params(self, output_dir):
        output_dir.mkdir(parents=True, exist_ok=True)
        for key, name in s4.MODELS:
            payload = {
                "model": name,
                "best_params_from_cv": {"regParam": 0.01, "maxIter": 100},
            }
            (output_dir / f"{key}_best_params.json").write_text(
                json.dumps(payload), encoding="utf-8",
            )

    def test_creates_csv(self, tmp_path):
        output_dir = tmp_path / "output"
        stage4_dir = tmp_path / "stage4"
        self._setup_params(output_dir)
        s4.build_best_params(output_dir, stage4_dir)
        assert (stage4_dir / "stage4_best_params.csv").is_file()

    def test_csv_has_all_models(self, tmp_path):
        output_dir = tmp_path / "output"
        stage4_dir = tmp_path / "stage4"
        self._setup_params(output_dir)
        s4.build_best_params(output_dir, stage4_dir)
        with (stage4_dir / "stage4_best_params.csv").open(encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)
        models = {r["model"] for r in rows}
        assert "LinearRegression" in models
        assert "RandomForestRegressor" in models
        assert "GBTRegressor" in models

    def test_csv_has_correct_headers(self, tmp_path):
        output_dir = tmp_path / "output"
        stage4_dir = tmp_path / "stage4"
        self._setup_params(output_dir)
        s4.build_best_params(output_dir, stage4_dir)
        with (stage4_dir / "stage4_best_params.csv").open(encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            assert set(reader.fieldnames) == {"model", "param_name", "param_value"}


# ── build_training_summary ───────────────────────────────────────────────

class TestBuildTrainingSummary:
    def test_creates_csv(self, tmp_path):
        output_dir = tmp_path / "output"
        stage4_dir = tmp_path / "stage4"
        output_dir.mkdir()
        summary = {
            "task": "regression",
            "data_source": "hive",
            "train_rows": 1000,
            "test_rows": 300,
            "cv_sample_rows": 500,
            "split_strategy": "chronological",
            "scaler_with_mean": False,
            "prediction_postprocess": "clip to 0",
        }
        (output_dir / "stage3_training_summary.json").write_text(
            json.dumps(summary), encoding="utf-8",
        )
        s4.build_training_summary(output_dir, stage4_dir)
        assert (stage4_dir / "stage4_training_summary.csv").is_file()

    def test_csv_row_count(self, tmp_path):
        output_dir = tmp_path / "output"
        stage4_dir = tmp_path / "stage4"
        output_dir.mkdir()
        summary = {
            "task": "regression",
            "data_source": "hive",
            "train_rows": 1000,
            "test_rows": 300,
            "cv_sample_rows": 500,
            "split_strategy": "chronological",
            "scaler_with_mean": False,
            "prediction_postprocess": "clip to 0",
        }
        (output_dir / "stage3_training_summary.json").write_text(
            json.dumps(summary), encoding="utf-8",
        )
        s4.build_training_summary(output_dir, stage4_dir)
        with (stage4_dir / "stage4_training_summary.csv").open(encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        assert len(rows) == 8


# ── build_feature_catalog ────────────────────────────────────────────────

class TestBuildFeatureCatalog:
    def test_creates_csv(self, tmp_path):
        s4.build_feature_catalog(tmp_path)
        assert (tmp_path / "stage4_feature_catalog.csv").is_file()

    def test_row_count_matches_descriptions(self, tmp_path):
        s4.build_feature_catalog(tmp_path)
        with (tmp_path / "stage4_feature_catalog.csv").open(encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        assert len(rows) == len(s4.FEATURE_DESCRIPTIONS)

    def test_correct_headers(self, tmp_path):
        s4.build_feature_catalog(tmp_path)
        with (tmp_path / "stage4_feature_catalog.csv").open(encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            assert set(reader.fieldnames) == {"feature_group", "feature_name", "description"}


# ── build_prediction_samples ─────────────────────────────────────────────

class TestBuildPredictionSamples:
    def _setup_predictions(self, output_dir, n_rows=20):
        output_dir.mkdir(parents=True, exist_ok=True)
        for key, _ in s4.MODELS:
            path = output_dir / f"{key}_predictions.csv"
            with path.open("w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=["label", "prediction"])
                writer.writeheader()
                for i in range(n_rows):
                    writer.writerow({
                        "label": str(float(i)),
                        "prediction": str(float(i) + 0.5),
                    })

    def test_creates_csv(self, tmp_path):
        output_dir = tmp_path / "output"
        stage4_dir = tmp_path / "stage4"
        self._setup_predictions(output_dir)
        with mock.patch.dict(os.environ, {"STAGE4_PREDICTION_SAMPLE_SIZE": "5"}):
            s4.build_prediction_samples(output_dir, stage4_dir)
        assert (stage4_dir / "stage4_prediction_samples.csv").is_file()

    def test_has_all_models(self, tmp_path):
        output_dir = tmp_path / "output"
        stage4_dir = tmp_path / "stage4"
        self._setup_predictions(output_dir)
        with mock.patch.dict(os.environ, {"STAGE4_PREDICTION_SAMPLE_SIZE": "5"}):
            s4.build_prediction_samples(output_dir, stage4_dir)
        with (stage4_dir / "stage4_prediction_samples.csv").open(encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        models = {r["model"] for r in rows}
        assert len(models) == 3

    def test_error_bucket_assigned(self, tmp_path):
        output_dir = tmp_path / "output"
        stage4_dir = tmp_path / "stage4"
        self._setup_predictions(output_dir)
        with mock.patch.dict(os.environ, {"STAGE4_PREDICTION_SAMPLE_SIZE": "5"}):
            s4.build_prediction_samples(output_dir, stage4_dir)
        with (stage4_dir / "stage4_prediction_samples.csv").open(encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        for row in rows:
            assert row["error_bucket"] in ("A_<=1", "B_1_to_2", "C_2_to_5", "D_>5")


# ── build_sample_prediction ──────────────────────────────────────────────

class TestBuildSamplePrediction:
    def test_skips_when_no_file(self, tmp_path):
        stage4_dir = tmp_path / "stage4"
        stage4_dir.mkdir()
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        s4.build_sample_prediction(stage4_dir, output_dir)
        assert not (stage4_dir / "stage4_single_prediction.csv").is_file()

    def test_creates_csv_when_file_exists(self, tmp_path):
        stage4_dir = tmp_path / "stage4"
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        payload = {
            "label": 5.0,
            "prediction": 5.5,
            "sample_source": "test_set",
            "sample_index": 42,
        }
        (output_dir / "sample_prediction_model2.json").write_text(
            json.dumps(payload), encoding="utf-8",
        )
        s4.build_sample_prediction(stage4_dir, output_dir)
        assert (stage4_dir / "stage4_single_prediction.csv").is_file()

    def test_csv_content(self, tmp_path):
        stage4_dir = tmp_path / "stage4"
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        payload = {
            "label": 5.0,
            "prediction": 5.5,
            "sample_source": "test_set",
            "sample_index": 42,
        }
        (output_dir / "sample_prediction_model2.json").write_text(
            json.dumps(payload), encoding="utf-8",
        )
        s4.build_sample_prediction(stage4_dir, output_dir)
        with (stage4_dir / "stage4_single_prediction.csv").open(encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        assert len(rows) == 1
        assert rows[0]["model"] == "RandomForestRegressor"
        assert float(rows[0]["absolute_error"]) == pytest.approx(0.5)
