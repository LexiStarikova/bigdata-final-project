"""Tests for shell scripts — structure, syntax, and configuration validation.

These tests verify that all shell scripts exist, have correct structure,
contain expected commands, and pass bash syntax checks.
"""

import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"


# ── Script existence ──────────────────────────────────────────────────────

class TestScriptExistence:
    @pytest.mark.parametrize("script", [
        "preprocess.sh",
        "stage1.sh",
        "stage2.sh",
        "stage3.sh",
        "stage4.sh",
        "postprocess.sh",
    ])
    def test_script_exists(self, script):
        assert (SCRIPTS_DIR / script).is_file(), f"scripts/{script} must exist"

    def test_main_sh_exists(self):
        assert (REPO_ROOT / "main.sh").is_file()


# ── Bash syntax check ────────────────────────────────────────────────────

class TestBashSyntax:
    @pytest.mark.parametrize("script", [
        "scripts/preprocess.sh",
        "scripts/stage1.sh",
        "scripts/stage2.sh",
        "scripts/stage3.sh",
        "scripts/stage4.sh",
        "scripts/postprocess.sh",
        "main.sh",
    ])
    def test_bash_syntax_valid(self, script):
        path = REPO_ROOT / script
        result = subprocess.run(
            ["bash", "-n", str(path)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            f"Bash syntax error in {script}:\n{result.stderr}"
        )


# ── Shebang lines ────────────────────────────────────────────────────────

class TestShebangLines:
    @pytest.mark.parametrize("script", [
        "preprocess.sh",
        "stage1.sh",
        "stage2.sh",
        "stage4.sh",
        "postprocess.sh",
    ])
    def test_has_bash_shebang(self, script):
        path = SCRIPTS_DIR / script
        first_line = path.read_text(encoding="utf-8").split("\n")[0]
        assert first_line.startswith("#!/"), (
            f"scripts/{script} should start with a shebang line"
        )
        assert "bash" in first_line.lower() or "sh" in first_line.lower()

    def test_stage3_sh_has_shebang(self):
        path = SCRIPTS_DIR / "stage3.sh"
        first_line = path.read_text(encoding="utf-8").split("\n")[0]
        assert first_line.startswith("#!")

    def test_main_sh_has_shebang(self):
        path = REPO_ROOT / "main.sh"
        first_line = path.read_text(encoding="utf-8").split("\n")[0]
        assert first_line.startswith("#!/")


# ── main.sh ──────────────────────────────────────────────────────────────

class TestMainSh:
    @pytest.fixture
    def main_content(self):
        return (REPO_ROOT / "main.sh").read_text(encoding="utf-8")

    def test_calls_preprocess(self, main_content):
        assert "preprocess.sh" in main_content

    def test_calls_stage1(self, main_content):
        assert "stage1.sh" in main_content

    def test_calls_stage2(self, main_content):
        assert "stage2.sh" in main_content

    def test_calls_stage3(self, main_content):
        assert "stage3.sh" in main_content

    def test_calls_stage4(self, main_content):
        assert "stage4.sh" in main_content

    def test_calls_postprocess(self, main_content):
        assert "postprocess.sh" in main_content

    def test_runs_pylint(self, main_content):
        assert "pylint" in main_content

    def test_pipeline_order(self, main_content):
        positions = {
            "preprocess": main_content.index("preprocess.sh"),
            "stage1": main_content.index("stage1.sh"),
            "stage2": main_content.index("stage2.sh"),
            "stage3": main_content.index("stage3.sh"),
            "stage4": main_content.index("stage4.sh"),
            "postprocess": main_content.index("postprocess.sh"),
        }
        assert positions["preprocess"] < positions["stage1"]
        assert positions["stage1"] < positions["stage2"]
        assert positions["stage2"] < positions["stage3"]
        assert positions["stage3"] < positions["stage4"]
        assert positions["stage4"] < positions["postprocess"]


# ── preprocess.sh ────────────────────────────────────────────────────────

class TestPreprocessSh:
    @pytest.fixture
    def content(self):
        return (SCRIPTS_DIR / "preprocess.sh").read_text(encoding="utf-8")

    def test_creates_data_dir(self, content):
        assert "mkdir -p data" in content

    def test_downloads_from_tlc(self, content):
        assert "d37ci6vzurychx.cloudfront.net" in content

    def test_downloads_parquet(self, content):
        assert ".parquet" in content

    def test_uses_curl(self, content):
        assert "curl" in content

    def test_skip_existing(self, content):
        assert "Skip existing" in content or "skip" in content.lower()

    def test_set_pipefail(self, content):
        assert "set -euo pipefail" in content

    def test_iterates_months(self, content):
        for m in ("01", "06", "12"):
            assert m in content


# ── stage1.sh ────────────────────────────────────────────────────────────

class TestStage1Sh:
    @pytest.fixture
    def content(self):
        return (SCRIPTS_DIR / "stage1.sh").read_text(encoding="utf-8")

    def test_runs_build_projectdb(self, content):
        assert "build_projectdb.py" in content

    def test_uses_sqoop_import(self, content):
        assert "sqoop import" in content

    def test_uses_sqoop_codegen(self, content):
        assert "sqoop codegen" in content

    def test_avro_format(self, content):
        assert "avrodatafile" in content.lower() or "avro" in content.lower()

    def test_snappy_codec(self, content):
        assert "snappy" in content

    def test_jdbc_connection(self, content):
        assert "jdbc:postgresql" in content

    def test_password_handling(self, content):
        assert "PGPASSWORD" in content or "PASSWORD" in content

    def test_set_pipefail(self, content):
        assert "set -euo pipefail" in content

    def test_creates_output_dir(self, content):
        assert "mkdir -p output" in content


# ── stage2.sh ────────────────────────────────────────────────────────────

class TestStage2Sh:
    @pytest.fixture
    def content(self):
        return (SCRIPTS_DIR / "stage2.sh").read_text(encoding="utf-8")

    def test_uses_beeline(self, content):
        assert "beeline" in content.lower()

    def test_runs_db_hql(self, content):
        assert "db.hql" in content

    def test_runs_eda_hql(self, content):
        assert "eda.hql" in content

    def test_exports_csv_files(self, content):
        for i in range(1, 13):
            assert f"q{i}" in content

    def test_hive_server_url(self, content):
        assert "hive2://" in content

    def test_hive_password_handling(self, content):
        assert "HIVE_PASS" in content or "hive.pass" in content

    def test_creates_output_dir(self, content):
        assert "mkdir -p output" in content

    def test_set_pipefail(self, content):
        assert "set -euo pipefail" in content

    def test_avsc_upload(self, content):
        assert "avsc" in content.lower() or "schema" in content.lower()


# ── stage3.sh ────────────────────────────────────────────────────────────

class TestStage3Sh:
    @pytest.fixture
    def content(self):
        return (SCRIPTS_DIR / "stage3.sh").read_text(encoding="utf-8")

    def test_spark_submit(self, content):
        assert "spark-submit" in content

    def test_references_stage3_py(self, content):
        assert "stage3.py" in content

    def test_yarn_master(self, content):
        assert "yarn" in content.lower()

    def test_local_mode_support(self, content):
        assert "local[" in content or "local" in content

    def test_hive_database_config(self, content):
        assert "STAGE3_HIVE_DATABASE" in content

    def test_hive_table_config(self, content):
        assert "STAGE3_HIVE_TABLE" in content

    def test_models_dir(self, content):
        assert "models" in content

    def test_output_dir(self, content):
        assert "output" in content

    def test_driver_memory_config(self, content):
        assert "STAGE3_DRIVER_MEM" in content

    def test_set_pipefail(self, content):
        assert "set -euo pipefail" in content

    def test_force_local_support(self, content):
        assert "STAGE3_FORCE_LOCAL" in content


# ── stage4.sh ────────────────────────────────────────────────────────────

class TestStage4Sh:
    @pytest.fixture
    def content(self):
        return (SCRIPTS_DIR / "stage4.sh").read_text(encoding="utf-8")

    def test_runs_prepare_script(self, content):
        assert "prepare_stage4_ml_artifacts.py" in content

    def test_hdfs_upload(self, content):
        assert "hdfs dfs" in content

    def test_runs_hql(self, content):
        assert "stage4_ml_tables.hql" in content

    def test_beeline_usage(self, content):
        assert "beeline" in content.lower() or "BEELINE" in content

    def test_hive_password_handling(self, content):
        assert "HIVE_PASS" in content or "hive.pass" in content

    def test_creates_stage4_dir(self, content):
        assert "mkdir -p output/stage4" in content

    def test_set_pipefail(self, content):
        assert "set -euo pipefail" in content

    def test_uploads_evaluation(self, content):
        assert "evaluation.csv" in content

    def test_uploads_feature_signals(self, content):
        assert "feature_signals" in content or "model_feature_signals" in content


# ── postprocess.sh ───────────────────────────────────────────────────────

class TestPostprocessSh:
    def test_has_shebang(self):
        path = SCRIPTS_DIR / "postprocess.sh"
        first_line = path.read_text(encoding="utf-8").split("\n")[0]
        assert first_line.startswith("#!/")


# ── SQL files existence ──────────────────────────────────────────────────

class TestSqlFiles:
    @pytest.mark.parametrize("sql_file", [
        "create_tables.sql",
        "db.hql",
        "eda.hql",
        "import_data.sql",
        "stage4_ml_tables.hql",
        "test_database.sql",
    ])
    def test_sql_file_exists(self, sql_file):
        assert (REPO_ROOT / "sql" / sql_file).is_file(), f"sql/{sql_file} must exist"

    def test_create_tables_has_yellow_taxi(self):
        content = (REPO_ROOT / "sql" / "create_tables.sql").read_text(encoding="utf-8")
        assert "yellow_taxi_trips" in content

    def test_create_tables_has_trip_id(self):
        content = (REPO_ROOT / "sql" / "create_tables.sql").read_text(encoding="utf-8")
        assert "trip_id" in content

    def test_test_database_has_count(self):
        content = (REPO_ROOT / "sql" / "test_database.sql").read_text(encoding="utf-8")
        assert "COUNT" in content.upper()


# ── requirements.txt ─────────────────────────────────────────────────────

class TestRequirements:
    @pytest.fixture
    def requirements(self):
        return (REPO_ROOT / "requirements.txt").read_text(encoding="utf-8")

    def test_file_exists(self):
        assert (REPO_ROOT / "requirements.txt").is_file()

    def test_pylint_listed(self, requirements):
        assert "pylint" in requirements

    def test_pyspark_listed(self, requirements):
        assert "pyspark" in requirements

    def test_pandas_listed(self, requirements):
        assert "pandas" in requirements

    def test_psycopg2_listed(self, requirements):
        assert "psycopg2" in requirements

    def test_pyarrow_listed(self, requirements):
        assert "pyarrow" in requirements
