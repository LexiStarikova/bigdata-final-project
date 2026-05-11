"""Static validation of PySpark ML code against Stage III rubric.

Reads scripts/stage3.py and scripts/stage3_helpers.py as text and checks
for required patterns WITHOUT importing pyspark.
"""

import re
from pathlib import Path

import pytest

from static_validate import (
    ValidationResult,
    _gather_pyspark_sources,
    _read_text,
    check_evaluators,
    check_hive_support,
    check_model_save,
    check_model_types,
    check_no_local_master_python,
    check_pipeline,
    check_prediction_output,
    check_pyspark_ml_imports,
    check_python_compile,
    check_train_test_split,
)

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def result():
    return ValidationResult()


@pytest.fixture(scope="module")
def pyspark_sources():
    return _gather_pyspark_sources(REPO_ROOT)


@pytest.fixture(scope="module")
def stage3_py():
    return _read_text(REPO_ROOT / "scripts" / "stage3.py")


@pytest.fixture(scope="module")
def stage3_helpers():
    return _read_text(REPO_ROOT / "scripts" / "stage3_helpers.py")


# PySpark ML library usage

class TestPySparkImports:
    def test_pyspark_ml_used(self, result):
        check_pyspark_ml_imports(REPO_ROOT, result)
        assert result.success, f"PySpark ML errors: {result.errors}"

    def test_no_sklearn(self, pyspark_sources):
        assert not re.search(r"from\s+sklearn|import\s+sklearn", pyspark_sources), \
            "Should not use sklearn for distributed ML training"

    def test_pyspark_ml_imported(self, pyspark_sources):
        assert "pyspark.ml" in pyspark_sources


class TestNoLocalMaster:
    def test_no_local_master_python(self, result):
        check_no_local_master_python(REPO_ROOT, result)
        assert result.success, f"Local master errors: {result.errors}"


# Hive integration

class TestHiveIntegration:
    """Rubric: read Hive tables as dataframes."""

    def test_hive_support_enabled(self, result):
        check_hive_support(REPO_ROOT, result)
        assert result.success

    def test_enable_hive_support(self, stage3_py):
        assert ".enableHiveSupport()" in stage3_py

    def test_hive_metastore_uris(self, stage3_py):
        assert "hive.metastore.uris" in stage3_py

    def test_reads_hive_table(self, pyspark_sources):
        """Rubric: read the Hive tables as dataframes."""
        assert "spark.table(" in pyspark_sources or "load_hive_table" in pyspark_sources


# Feature pipeline

class TestFeaturePipeline:
    """Rubric: build and fit a feature extraction pipeline."""

    def test_pipeline(self, result):
        check_pipeline(REPO_ROOT, result)
        assert result.success

    def test_pipeline_import(self, stage3_py):
        assert "from pyspark.ml import Pipeline" in stage3_py

    def test_vector_assembler(self, pyspark_sources):
        assert "VectorAssembler" in pyspark_sources

    def test_imputer(self, pyspark_sources):
        assert "Imputer" in pyspark_sources

    def test_string_indexer(self, pyspark_sources):
        assert "StringIndexer" in pyspark_sources

    def test_one_hot_encoder(self, pyspark_sources):
        assert "OneHotEncoder" in pyspark_sources

    def test_standard_scaler(self, pyspark_sources):
        assert "StandardScaler" in pyspark_sources


# Train/test split

class TestTrainTestSplit:
    """Rubric: split into train and test, save as JSON to HDFS."""

    def test_split_exists(self, result):
        check_train_test_split(REPO_ROOT, result)
        assert result.success

    def test_train_ratio_gte_60(self, stage3_helpers):
        ratio_match = re.search(r"train_ratio\s*=\s*([\d.]+)", stage3_helpers)
        if ratio_match:
            ratio = float(ratio_match.group(1))
            assert ratio >= 0.6, f"Train ratio {ratio} < 0.6"

    def test_saves_train_test_json(self, stage3_helpers):
        """Rubric: save train/test as JSON to HDFS."""
        assert "save_train_test_json" in stage3_helpers
        assert "project/data/train" in stage3_helpers or "data/train" in stage3_helpers

    def test_json_write(self, stage3_helpers):
        assert ".write.mode" in stage3_helpers
        assert ".json(" in stage3_helpers


# Three model types

class TestModelTypes:
    """Rubric: select at least 2 (project uses 3) model types from pyspark.ml."""

    def test_model_types_check(self, result):
        check_model_types(REPO_ROOT, result)
        assert result.success

    def test_model1_linear_regression(self, stage3_py):
        assert "LinearRegression" in stage3_py

    def test_model2_random_forest(self, stage3_py):
        assert "RandomForestRegressor" in stage3_py

    def test_model3_gbt(self, stage3_py):
        assert "GBTRegressor" in stage3_py

    def test_three_model_pipelines(self, stage3_py):
        """Each model should have its own Pipeline."""
        pipeline_count = len(re.findall(r'Pipeline\(', stage3_py))
        assert pipeline_count >= 3, f"Only {pipeline_count} Pipeline definitions, need >=3"


# Hyperparameter tuning (per model)

class TestHyperparameterTuning:
    """Rubric: at least 2 hyperparameters per model, grid search + CV."""

    def test_param_grid_builder(self, pyspark_sources):
        assert "ParamGridBuilder" in pyspark_sources

    def test_cross_validator(self, pyspark_sources):
        assert "CrossValidator" in pyspark_sources

    def test_cv_folds_3_or_4(self, stage3_py):
        match = re.search(r'cv.folds.*?default\s*=\s*(\d+)', stage3_py)
        if match:
            k = int(match.group(1))
            assert k in (3, 4), f"CV folds default={k}, should be 3 or 4"

    def test_model1_at_least_2_hyperparams(self, stage3_py):
        """LR: regParam, elasticNetParam, tol → 3 hyperparams."""
        lr_section = stage3_py[stage3_py.find("lr_grid"):stage3_py.find(".build()", stage3_py.find("lr_grid"))]
        addgrid_count = len(re.findall(r"\.addGrid\(", lr_section))
        assert addgrid_count >= 2, f"Model1 (LR) has only {addgrid_count} hyperparams, need >=2"

    def test_model2_at_least_2_hyperparams(self, stage3_py):
        """RF: numTrees, maxDepth, minInstancesPerNode → 3 hyperparams."""
        rf_section = stage3_py[stage3_py.find("rf_grid"):stage3_py.find(".build()", stage3_py.find("rf_grid"))]
        addgrid_count = len(re.findall(r"\.addGrid\(", rf_section))
        assert addgrid_count >= 2, f"Model2 (RF) has only {addgrid_count} hyperparams, need >=2"

    def test_model3_at_least_2_hyperparams(self, stage3_py):
        """GBT: maxDepth, stepSize, minInstancesPerNode → 3 hyperparams."""
        gbt_section = stage3_py[stage3_py.find("gbt_grid"):stage3_py.find(".build()", stage3_py.find("gbt_grid"))]
        addgrid_count = len(re.findall(r"\.addGrid\(", gbt_section))
        assert addgrid_count >= 2, f"Model3 (GBT) has only {addgrid_count} hyperparams, need >=2"

    def test_addgrid_total_at_least_9(self, pyspark_sources):
        count = len(re.findall(r"\.addGrid\s*\(", pyspark_sources))
        assert count >= 9, f"Only {count} .addGrid() calls total, expected >=9"

    def test_hyperparams_have_3_values_each(self, stage3_py):
        """Each .addGrid should have a list with at least 3 values."""
        grids = re.findall(r"\.addGrid\([^,]+,\s*\[([^\]]+)\]", stage3_py)
        for grid_values in grids:
            values = [v.strip() for v in grid_values.split(",") if v.strip()]
            assert len(values) >= 3, \
                f"Grid values {grid_values} has only {len(values)} values, need >=3"

    def test_cv_on_training_data_only(self, stage3_helpers):
        """Rubric: optimize hyperparameters using CV on training data only."""
        assert "fit_cv_and_pick_best" in stage3_helpers
        assert "cv.fit(cv_train)" in stage3_helpers or "cv.fit(" in stage3_helpers

    def test_best_model_selection(self, stage3_helpers):
        """Select the best model from grid search."""
        assert "bestModel" in stage3_helpers or "best_index" in stage3_helpers


# Model saving

class TestModelSaving:
    """Rubric: save models to HDFS, save predictions, save evaluation."""

    def test_model_save(self, result):
        check_model_save(REPO_ROOT, result)
        assert result.success

    def test_write_overwrite_save(self, pyspark_sources):
        assert ".write().overwrite().save(" in pyspark_sources

    def test_model1_saved(self, stage3_py):
        assert "model1" in stage3_py

    def test_model2_saved(self, stage3_py):
        assert "model2" in stage3_py

    def test_model3_saved(self, stage3_py):
        assert "model3" in stage3_py

    def test_models_dir_used(self, stage3_py):
        assert "models_root" in stage3_py or "models_dir" in stage3_py


# Prediction saving

class TestPredictionSaving:
    """Rubric: save predictions as CSV, label+prediction columns, one partition."""

    def test_prediction_output(self, result):
        check_prediction_output(REPO_ROOT, result)
        assert result.success

    def test_evaluation_csv_reference(self, pyspark_sources):
        assert "evaluation.csv" in pyspark_sources

    def test_model_predictions_csv(self, pyspark_sources):
        for key in ("model1_predictions", "model2_predictions", "model3_predictions"):
            assert key in pyspark_sources, f"{key} not found"

    def test_single_partition_csv(self, pyspark_sources):
        """Rubric: save as one partition."""
        assert "coalesce(1)" in pyspark_sources

    def test_label_and_prediction_columns(self, pyspark_sources):
        """Rubric: keep only label and prediction columns."""
        assert "label" in pyspark_sources
        assert "prediction" in pyspark_sources


# Evaluation

class TestEvaluation:
    """Rubric: evaluate models, compare on test data, evaluation dataframe."""

    def test_evaluators(self, result):
        check_evaluators(REPO_ROOT, result)
        assert result.success

    def test_regression_evaluator(self, pyspark_sources):
        assert "RegressionEvaluator" in pyspark_sources

    def test_rmse_metric(self, pyspark_sources):
        assert "rmse" in pyspark_sources.lower()

    def test_mae_metric(self, pyspark_sources):
        assert "mae" in pyspark_sources.lower()

    def test_r2_metric(self, pyspark_sources):
        assert "r2" in pyspark_sources.lower()

    def test_evaluation_dataframe_created(self, stage3_py):
        """Rubric: create comparison dataframe and store as evaluation.csv."""
        assert "evaluation.csv" in stage3_py
        assert "_write_evaluation" in stage3_py or "write_single_partition_csv" in stage3_py

    def test_model_comparison(self, stage3_py):
        """All models' results are collected for comparison."""
        assert "results" in stage3_py
        assert "_metrics_row" in stage3_py or "metrics" in stage3_py


# Python compile

class TestPythonCompile:
    def test_python_compile(self, result):
        check_python_compile(REPO_ROOT, result)
        assert result.success

    def test_stage3_py_compiles(self):
        path = REPO_ROOT / "scripts" / "stage3.py"
        assert path.is_file()
        source = _read_text(path)
        compile(source, str(path), "exec")

    def test_stage3_helpers_compiles(self):
        path = REPO_ROOT / "scripts" / "stage3_helpers.py"
        assert path.is_file()
        source = _read_text(path)
        compile(source, str(path), "exec")

    def test_build_projectdb_compiles(self):
        path = REPO_ROOT / "scripts" / "build_projectdb.py"
        assert path.is_file()
        source = _read_text(path)
        compile(source, str(path), "exec")

    def test_prepare_stage4_compiles(self):
        path = REPO_ROOT / "scripts" / "prepare_stage4_ml_artifacts.py"
        assert path.is_file()
        source = _read_text(path)
        compile(source, str(path), "exec")
