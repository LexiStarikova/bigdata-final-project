"""Rubric-aligned static tests for stage shell scripts.

Complements test_shell_scripts.py (which covers existence, syntax, shebangs,
main.sh order, and per-script content keywords). This module focuses on
project-specification requirements NOT already covered there.
"""

from pathlib import Path

import pytest

from static_validate import (
    ValidationResult,
    _read_text,
    _stage_content,
    check_no_hardcoded_user_paths,
    check_no_local_master_in_scripts,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = REPO_ROOT / "scripts"


# Stage 1 rubric-specific

class TestStage1Rubric:
    """Rubric: schema, constraints, COPY load, Sqoop AVRO+Snappy, .avsc/.java."""

    @pytest.fixture
    def s1(self):
        return _stage_content(REPO_ROOT, "stage1.sh")

    def test_creates_postgresql_schema(self, s1):
        """Calls build_projectdb.py which runs create_tables.sql."""
        assert "build_projectdb" in s1 or "create_tables.sql" in s1

    def test_sqoop_import_single_table(self, s1):
        assert "sqoop import" in s1

    def test_sqoop_avro_format(self, s1):
        assert "--as-avrodatafile" in s1, "Sqoop must use --as-avrodatafile"

    def test_sqoop_snappy_compression(self, s1):
        assert "snappy" in s1.lower(), "Sqoop must use Snappy codec"
        assert "--compress" in s1 or "--compression-codec" in s1

    def test_delete_target_dir_before_import(self, s1):
        assert "--delete-target-dir" in s1 or "hdfs dfs -rm" in s1

    def test_sqoop_split_by(self, s1):
        """Parallel import needs --split-by on PK column."""
        assert "--split-by" in s1

    def test_sqoop_codegen_for_avsc_java(self, s1):
        """Rubric: put .avsc and .java files in output/."""
        assert "sqoop codegen" in s1

    def test_avsc_extraction(self, s1):
        """Stage 1 should extract or generate .avsc schema."""
        assert ".avsc" in s1

    def test_java_codegen_copied_to_output(self, s1):
        """Generated Java files copied to output/."""
        assert ".java" in s1

    def test_output_dir_created(self, s1):
        assert "mkdir -p output" in s1 or "mkdir -p output/" in s1

    def test_warehouse_hdfs_path(self, s1):
        """Sqoop target dir should be under project/warehouse."""
        assert "project/warehouse" in s1


# Stage 2 rubric-specific

class TestStage2Rubric:
    """Rubric: Hive DB, external tables, partitioning, bucketing, EDA, CSV export."""

    @pytest.fixture
    def s2(self):
        return _stage_content(REPO_ROOT, "stage2.sh")

    def test_uploads_avsc_to_hdfs(self, s2):
        """Rubric: if AVRO, put .avsc files to HDFS."""
        assert "avsc" in s2.lower()
        assert "hdfs dfs" in s2

    def test_runs_db_hql(self, s2):
        """db.hql creates database, external tables, partitioned+bucketed table."""
        assert "db.hql" in s2

    def test_runs_eda_hql(self, s2):
        assert "eda.hql" in s2

    def test_exports_at_least_6_csv(self, s2):
        """Rubric: export q*_results to output/q*.csv."""
        for i in range(1, 7):
            assert f"q{i}" in s2, f"stage2.sh must export q{i}"

    def test_exports_11_csv(self, s2):
        """Project has 11 EDA queries."""
        for i in range(1, 12):
            assert f"q{i}" in s2, f"stage2.sh should export q{i}"

    def test_csv_output_dir(self, s2):
        """Output CSVs go to output/."""
        assert "output/q" in s2

    def test_hive_password_from_secrets(self, s2):
        """Password from secrets/.hive.pass or env, never hardcoded."""
        assert ".hive.pass" in s2 or "HIVE_PASSWORD" in s2

    def test_database_location_separate_from_sqoop(self, s2):
        """Rubric: do not use same HDFS location as Sqoop."""
        db_hql = _read_text(REPO_ROOT / "sql" / "db.hql")
        assert "project/hive/warehouse" in db_hql, \
            "Hive DB should use project/hive/warehouse (separate from Sqoop's project/warehouse)"


# Stage 3 rubric-specific

class TestStage3ShRubric:
    """Rubric: spark-submit on YARN, Hive table, models dir, output dir."""

    @pytest.fixture
    def s3(self):
        return _stage_content(REPO_ROOT, "stage3.sh")

    def test_spark_submit_yarn(self, s3):
        assert "spark-submit" in s3
        assert "--master yarn" in s3

    def test_hive_database_env(self, s3):
        """Stage 3 should pass Hive database to stage3.py."""
        assert "STAGE3_HIVE_DATABASE" in s3 or "--hive-database" in s3

    def test_hive_table_env(self, s3):
        assert "STAGE3_HIVE_TABLE" in s3 or "--hive-table" in s3

    def test_models_dir_argument(self, s3):
        assert "--models-dir" in s3

    def test_output_dir_argument(self, s3):
        assert "--output-dir" in s3

    def test_executor_memory_config(self, s3):
        assert "--executor-memory" in s3 or "STAGE3_EXEC_MEM" in s3

    def test_num_executors_config(self, s3):
        assert "--num-executors" in s3 or "STAGE3_EXECUTORS" in s3

    def test_deploy_mode(self, s3):
        assert "--deploy-mode" in s3


# Cross-stage checks

class TestCrossStageChecks:
    def test_no_hardcoded_local_master(self):
        r = ValidationResult()
        check_no_local_master_in_scripts(REPO_ROOT, r)
        assert r.success, f"Hardcoded --master local: {r.errors}"

    def test_no_hardcoded_user_paths(self):
        r = ValidationResult()
        check_no_hardcoded_user_paths(REPO_ROOT, r)
        assert r.success, f"Hardcoded user paths: {r.errors}"
