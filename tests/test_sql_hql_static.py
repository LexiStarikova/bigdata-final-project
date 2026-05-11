"""Static validation of SQL and HQL files against project rubric.

SQL file existence is already checked in test_shell_scripts.py::TestSqlFiles.
This module validates content compliance with the rubric.
"""

import re
from pathlib import Path

import pytest

from static_validate import (
    ValidationResult,
    _read_text,
    check_sql_bulk_load,
)

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def result():
    return ValidationResult()


# Stage I: PostgreSQL schema (create_tables.sql)

class TestCreateTablesSql:
    @pytest.fixture
    def content(self):
        return _read_text(REPO_ROOT / "sql" / "create_tables.sql")

    def test_file_exists(self):
        assert (REPO_ROOT / "sql" / "create_tables.sql").is_file()

    def test_defines_yellow_taxi_trips(self, content):
        assert "yellow_taxi_trips" in content

    def test_has_primary_key(self, content):
        assert "PRIMARY KEY" in content.upper()

    def test_trip_id_serial(self, content):
        """trip_id should be BIGSERIAL (auto-increment PK)."""
        assert "BIGSERIAL" in content.upper() or "SERIAL" in content.upper()

    def test_drop_before_create(self, content):
        upper = content.upper()
        assert "DROP TABLE" in upper
        assert "IF EXISTS" in upper or "CASCADE" in upper

    def test_all_19_columns_defined(self, content):
        """Schema must define all 19 data columns + trip_id PK."""
        expected = [
            "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "passenger_count", "trip_distance", "ratecodeid",
            "store_and_fwd_flag", "pulocationid", "dolocationid",
            "payment_type", "fare_amount", "extra", "mta_tax",
            "tip_amount", "tolls_amount", "improvement_surcharge",
            "total_amount", "congestion_surcharge", "airport_fee",
        ]
        lower = content.lower()
        for col in expected:
            assert col in lower, f"Column '{col}' not found in create_tables.sql"

    def test_timestamp_columns(self, content):
        """Datetime columns should use TIMESTAMP type."""
        upper = content.upper()
        assert "TIMESTAMP" in upper

    def test_transaction_block(self, content):
        """Schema should use transaction for atomicity."""
        upper = content.upper()
        has_transaction = "BEGIN" in upper or "START TRANSACTION" in upper
        has_commit = "COMMIT" in upper
        assert has_transaction and has_commit, \
            "create_tables.sql should use BEGIN/COMMIT transaction"


class TestImportDataSql:
    def test_file_exists(self):
        assert (REPO_ROOT / "sql" / "import_data.sql").is_file()

    def test_documents_copy_command(self):
        """import_data.sql should document COPY or reference bulk load."""
        content = _read_text(REPO_ROOT / "sql" / "import_data.sql").upper()
        assert "COPY" in content

    def test_copy_from_stdin(self):
        content = _read_text(REPO_ROOT / "sql" / "import_data.sql")
        assert "FROM STDIN" in content.upper() or "csv" in content.lower()


class TestBulkLoad:
    def test_bulk_load_in_python(self, result):
        """build_projectdb.py uses COPY for bulk loading."""
        check_sql_bulk_load(REPO_ROOT, result)
        assert result.success


# Stage II: Hive (db.hql)

class TestDbHql:
    @pytest.fixture
    def content(self):
        return _read_text(REPO_ROOT / "sql" / "db.hql")

    def test_file_exists(self):
        assert (REPO_ROOT / "sql" / "db.hql").is_file()

    def test_drop_database_if_exists(self, content):
        assert "DROP DATABASE IF EXISTS" in content.upper()

    def test_create_database(self, content):
        assert "CREATE DATABASE" in content.upper()

    def test_database_location(self, content):
        """Rubric: Hive DB in project/hive/warehouse."""
        assert "project/hive/warehouse" in content

    def test_create_external_table(self, content):
        assert "CREATE EXTERNAL TABLE" in content.upper()

    def test_stored_as_avro(self, content):
        upper = content.upper()
        assert "STORED AS AVRO" in upper or "STORED AS PARQUET" in upper

    def test_avro_schema_url(self, content):
        """External table needs avro.schema.url pointing to HDFS .avsc."""
        assert "avro.schema.url" in content

    def test_partitioned_by(self, content):
        assert "PARTITIONED BY" in content.upper()

    def test_clustered_by_bucketing(self, content):
        assert "CLUSTERED BY" in content.upper()

    def test_buckets_defined(self, content):
        """CLUSTERED BY ... INTO N BUCKETS."""
        assert "BUCKETS" in content.upper()

    def test_dynamic_partitioning_enabled(self, content):
        """Rubric: enable dynamic partitioning for automatic partition creation."""
        assert "hive.exec.dynamic.partition" in content

    def test_insert_into_partitioned_table(self, content):
        """Data must be loaded into the partitioned/bucketed table."""
        upper = content.upper()
        assert "INSERT INTO" in upper or "INSERT OVERWRITE" in upper

    def test_show_partitions(self, content):
        """Verify partitions were created."""
        assert "SHOW PARTITIONS" in content.upper()

    def test_drops_unpartitioned_table(self, content):
        """Rubric: delete the unpartitioned Hive table, keep only partitioned."""
        lines = content.upper().split("\n")
        found_drop_original = False
        for line in lines:
            if "DROP TABLE" in line and "YELLOW_TAXI_TRIPS" in line:
                if "PART_BUCK" not in line:
                    found_drop_original = True
        assert found_drop_original, \
            "db.hql should DROP the original unpartitioned table"

    def test_separate_location_from_sqoop(self, content):
        """Hive DB location must differ from Sqoop import location."""
        has_hive_warehouse = "project/hive/warehouse" in content
        s1 = _read_text(REPO_ROOT / "scripts" / "stage1.sh")
        has_sqoop_warehouse = "project/warehouse" in s1
        assert has_hive_warehouse and has_sqoop_warehouse, \
            "Hive DB and Sqoop should use different HDFS locations"
        assert "project/hive/warehouse" != "project/warehouse"


# Stage II: EDA (eda.hql)

class TestEdaHql:
    @pytest.fixture
    def content(self):
        return _read_text(REPO_ROOT / "sql" / "eda.hql")

    def test_file_exists(self):
        assert (REPO_ROOT / "sql" / "eda.hql").is_file()

    def test_at_least_6_queries(self, content):
        q_tables = set(re.findall(r"q(\d+)_results", content, re.IGNORECASE))
        assert len(q_tables) >= 6, f"Only {len(q_tables)} EDA queries, need >=6"

    def test_12_queries(self, content):
        q_tables = set(re.findall(r"q(\d+)_results", content, re.IGNORECASE))
        assert len(q_tables) >= 12, f"Only {len(q_tables)} EDA queries, expected 12"

    def test_each_query_creates_results_table(self, content):
        """Rubric: store results of query qx in qx_results table."""
        upper = content.upper()
        for i in range(1, 7):
            assert f"Q{i}_RESULTS" in upper, f"q{i}_results table not found"

    def test_each_query_drops_before_create(self, content):
        """Each result table should be idempotent."""
        upper = content.upper()
        assert upper.count("DROP TABLE IF EXISTS") >= 6

    def test_external_tables_for_results(self, content):
        """Result tables should be EXTERNAL for HDFS export."""
        upper = content.upper()
        assert "CREATE EXTERNAL TABLE" in upper

    def test_results_have_location(self, content):
        """Results stored at HDFS location for export."""
        assert "LOCATION" in content.upper()

    def test_group_by(self, content):
        assert "GROUP BY" in content.upper()

    def test_aggregations(self, content):
        upper = content.upper()
        aggs = ["SUM(", "AVG(", "COUNT(", "MIN(", "MAX("]
        found = [a for a in aggs if a in upper]
        assert len(found) >= 3, f"Only {len(found)} aggregation types, need >=3"

    def test_order_by(self, content):
        assert "ORDER BY" in content.upper()

    def test_uses_partitioned_table(self, content):
        """Rubric: EDA should use only the partitioned table."""
        assert "yellow_taxi_trips_part_buck" in content

    def test_select_from_results(self, content):
        """Each query should SELECT * FROM q*_results for output."""
        assert content.upper().count("SELECT * FROM") >= 6


# Stage IV: Hive ML tables (stage4_ml_tables.hql)

class TestStage4MlTablesHql:
    @pytest.fixture
    def content(self):
        return _read_text(REPO_ROOT / "sql" / "stage4_ml_tables.hql")

    def test_file_exists(self):
        assert (REPO_ROOT / "sql" / "stage4_ml_tables.hql").is_file()

    def test_evaluation_table(self, content):
        assert "stage4_evaluation" in content

    def test_prediction_tables(self, content):
        for model in ("model1", "model2", "model3"):
            assert f"stage4_{model}_predictions" in content

    def test_external_tables(self, content):
        assert "CREATE EXTERNAL TABLE" in content.upper()
