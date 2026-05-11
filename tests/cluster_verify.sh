#!/bin/bash
# Full cluster verification script — checks that all rubric conditions are met.
# NOT run by GitHub Actions CI. Run manually on the cluster after running
# the pipeline (stage1.sh, stage2.sh, stage3.sh).
#
# Usage: bash tests/cluster_verify.sh
#
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

pass_count=0
warn_count=0
fail_count=0

ok()   { echo -e "${GREEN}[PASS]${NC} $1"; pass_count=$((pass_count + 1)); }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; warn_count=$((warn_count + 1)); }
fail() { echo -e "${RED}[FAIL]${NC} $1"; fail_count=$((fail_count + 1)); }
section() { echo -e "\n${CYAN}=== $1 ===${NC}"; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

HIVE_DB="${STAGE3_HIVE_DATABASE:-team35_projectdb}"
HIVE_TABLE="${STAGE3_HIVE_TABLE:-yellow_taxi_trips_part_buck}"
HDFS_USER="${STAGE3_HDFS_USER:-$USER}"

echo "Cluster Verification for Big Data Final Project"
echo "Repo root: $ROOT"
echo "Hive DB: $HIVE_DB"
echo "HDFS user: $HDFS_USER"

# ==========================================================================
section "1. Prerequisites — commands and secrets"
# ==========================================================================

for cmd in hdfs sqoop beeline spark-submit python3 pylint; do
    if command -v "$cmd" &>/dev/null; then
        ok "Command '$cmd' available"
    else
        fail "Command '$cmd' not found in PATH"
    fi
done

for secret in secrets/.psql.pass secrets/.hive.pass; do
    if [[ -f "$secret" && -s "$secret" ]]; then
        ok "$secret exists and is non-empty (contents hidden)"
    elif [[ -f "$secret" ]]; then
        fail "$secret exists but is empty"
    else
        fail "$secret not found"
    fi
done

# ==========================================================================
section "2. Stage I — Data collection and ingestion"
# ==========================================================================

# PostgreSQL connection
if command -v psql &>/dev/null; then
    PGPASSWORD="$(head -1 secrets/.psql.pass 2>/dev/null || echo '')"
    PG_USER="${PGUSER:-$USER}"
    PG_DB="${PG_USER}_projectdb"
    PG_HOST="${PGHOST:-hadoop-04.uni.innopolis.ru}"

    if PGPASSWORD="$PGPASSWORD" psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -c "SELECT 1" &>/dev/null; then
        ok "PostgreSQL connection to $PG_DB works"
    else
        fail "Cannot connect to PostgreSQL $PG_DB"
    fi

    # Check table exists
    TABLE_EXISTS=$(PGPASSWORD="$PGPASSWORD" psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -tAc \
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='yellow_taxi_trips')" 2>/dev/null || echo "f")
    if [[ "$TABLE_EXISTS" == "t" ]]; then
        ok "PostgreSQL table yellow_taxi_trips exists"
    else
        fail "PostgreSQL table yellow_taxi_trips not found"
    fi

    # Check primary key
    PK_EXISTS=$(PGPASSWORD="$PGPASSWORD" psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -tAc \
        "SELECT EXISTS(SELECT 1 FROM information_schema.table_constraints WHERE table_name='yellow_taxi_trips' AND constraint_type='PRIMARY KEY')" 2>/dev/null || echo "f")
    if [[ "$PK_EXISTS" == "t" ]]; then
        ok "PostgreSQL yellow_taxi_trips has PRIMARY KEY"
    else
        fail "PostgreSQL yellow_taxi_trips missing PRIMARY KEY"
    fi

    # Check row count
    ROW_COUNT=$(PGPASSWORD="$PGPASSWORD" psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -tAc \
        "SELECT COUNT(*) FROM yellow_taxi_trips" 2>/dev/null || echo "0")
    if [[ "$ROW_COUNT" -gt 0 ]]; then
        ok "PostgreSQL yellow_taxi_trips has $ROW_COUNT rows"
    else
        fail "PostgreSQL yellow_taxi_trips is empty"
    fi

    # Check column count
    COL_COUNT=$(PGPASSWORD="$PGPASSWORD" psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -tAc \
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_name='yellow_taxi_trips'" 2>/dev/null || echo "0")
    if [[ "$COL_COUNT" -ge 19 ]]; then
        ok "PostgreSQL yellow_taxi_trips has $COL_COUNT columns (>=19+PK)"
    else
        fail "PostgreSQL yellow_taxi_trips has only $COL_COUNT columns, expected >=19"
    fi
else
    warn "psql not available; skipping PostgreSQL checks"
fi

# HDFS: Sqoop import directory
if command -v hdfs &>/dev/null; then
    SQOOP_DIR="/user/$HDFS_USER/project/warehouse"
    if hdfs dfs -test -d "$SQOOP_DIR" 2>/dev/null; then
        ok "HDFS Sqoop directory exists: $SQOOP_DIR"
    else
        fail "HDFS Sqoop directory missing: $SQOOP_DIR"
    fi

    # Check AVRO files in Sqoop output
    AVRO_COUNT=$(hdfs dfs -ls "$SQOOP_DIR/yellow_taxi_trips/" 2>/dev/null | grep -c "\.avro" || echo "0")
    if [[ "$AVRO_COUNT" -gt 0 ]]; then
        ok "HDFS: $AVRO_COUNT AVRO files in Sqoop output"
    else
        warn "No .avro files found in $SQOOP_DIR/yellow_taxi_trips/"
    fi

    # Check .avsc files in HDFS
    AVSC_DIR="/user/$HDFS_USER/project/warehouse/avsc"
    if hdfs dfs -test -d "$AVSC_DIR" 2>/dev/null; then
        ok "HDFS AVSC directory exists: $AVSC_DIR"
    else
        warn "HDFS AVSC directory not found: $AVSC_DIR"
    fi
else
    warn "hdfs not available; skipping HDFS Stage I checks"
fi

# Local output: .avsc and .java
if ls output/*.avsc &>/dev/null; then
    ok "output/ contains .avsc files"
else
    fail "output/ missing .avsc files (Sqoop AVRO schema)"
fi

if ls output/*.java &>/dev/null; then
    ok "output/ contains .java files"
else
    fail "output/ missing .java files (Sqoop codegen)"
fi

# ==========================================================================
section "3. Stage II — Hive database, tables, and EDA"
# ==========================================================================

if command -v beeline &>/dev/null; then
    HIVE_PASS="$(head -1 secrets/.hive.pass 2>/dev/null || echo '')"
    BEELINE_URL="${BEELINE_URL:-jdbc:hive2://hadoop-04.uni.innopolis.ru:10001/${HIVE_DB}}"
    BEELINE_CMD="beeline -u '${BEELINE_URL}' -n $USER -p '${HIVE_PASS}' --silent=true --outputformat=csv2"

    # Check database exists
    DB_CHECK=$(eval $BEELINE_CMD -e "SHOW DATABASES" 2>/dev/null | grep -c "$HIVE_DB" || echo "0")
    if [[ "$DB_CHECK" -gt 0 ]]; then
        ok "Hive database $HIVE_DB exists"
    else
        fail "Hive database $HIVE_DB not found"
    fi

    # Check partitioned+bucketed table exists
    TABLE_CHECK=$(eval $BEELINE_CMD -e "SHOW TABLES IN $HIVE_DB" 2>/dev/null | grep -c "$HIVE_TABLE" || echo "0")
    if [[ "$TABLE_CHECK" -gt 0 ]]; then
        ok "Hive table $HIVE_DB.$HIVE_TABLE exists"
    else
        fail "Hive table $HIVE_DB.$HIVE_TABLE not found"
    fi

    # Check table has data
    ROW_CHECK=$(eval $BEELINE_CMD -e "SELECT COUNT(*) FROM $HIVE_DB.$HIVE_TABLE" 2>/dev/null | tail -1 || echo "0")
    if [[ "$ROW_CHECK" -gt 0 ]]; then
        ok "Hive table $HIVE_DB.$HIVE_TABLE has $ROW_CHECK rows"
    else
        warn "Hive table $HIVE_DB.$HIVE_TABLE appears empty"
    fi

    # Check unpartitioned table was deleted
    ORIG_TABLE=$(eval $BEELINE_CMD -e "SHOW TABLES IN $HIVE_DB" 2>/dev/null | grep -c "yellow_taxi_trips[^_]" || echo "0")
    if [[ "$ORIG_TABLE" -eq 0 ]]; then
        ok "Unpartitioned table yellow_taxi_trips was deleted"
    else
        fail "Unpartitioned table yellow_taxi_trips still exists (should be deleted)"
    fi

    # Check partitions
    PARTITION_COUNT=$(eval $BEELINE_CMD -e "SHOW PARTITIONS $HIVE_DB.$HIVE_TABLE" 2>/dev/null | grep -c "year=" || echo "0")
    if [[ "$PARTITION_COUNT" -gt 0 ]]; then
        ok "Table $HIVE_TABLE has $PARTITION_COUNT partition(s)"
    else
        warn "No partitions found for $HIVE_TABLE"
    fi

    # Check qX_results tables exist (at least 6)
    Q_TABLES=$(eval $BEELINE_CMD -e "SHOW TABLES IN $HIVE_DB" 2>/dev/null | grep -c "q[0-9]*_results" || echo "0")
    if [[ "$Q_TABLES" -ge 6 ]]; then
        ok "$Q_TABLES EDA result tables found (>=6)"
    else
        fail "Only $Q_TABLES q*_results tables found, need >=6"
    fi
else
    warn "beeline not available; skipping Hive checks"
fi

# Hive DB HDFS location (separate from Sqoop)
if command -v hdfs &>/dev/null; then
    HIVE_LOC="/user/$HDFS_USER/project/hive/warehouse"
    if hdfs dfs -test -d "$HIVE_LOC" 2>/dev/null; then
        ok "Hive DB HDFS location exists: $HIVE_LOC (separate from Sqoop)"
    else
        warn "Hive DB HDFS location not found: $HIVE_LOC"
    fi
fi

# Local EDA CSV outputs
for i in $(seq 1 12); do
    f="output/q${i}.csv"
    if [[ -s "$f" ]]; then
        ok "$f exists and is non-empty"
    elif [[ -f "$f" ]]; then
        warn "$f exists but is empty"
    else
        if [[ $i -le 6 ]]; then
            fail "$f missing (minimum 6 EDA queries required)"
        else
            warn "$f missing (optional EDA query)"
        fi
    fi
done

if [[ -s output/hive_results.txt ]]; then
    ok "output/hive_results.txt exists"
else
    warn "output/hive_results.txt missing or empty"
fi

# ==========================================================================
section "4. Stage III — Predictive Data Analytics (PySpark ML)"
# ==========================================================================

# HDFS models
if command -v hdfs &>/dev/null; then
    for m in model1 model2 model3; do
        MODEL_DIR="/user/$HDFS_USER/project/models/$m"
        if hdfs dfs -test -d "$MODEL_DIR" 2>/dev/null; then
            ok "HDFS model saved: $MODEL_DIR"
        else
            fail "HDFS model missing: $MODEL_DIR"
        fi
    done

    # Check train/test JSON data
    for ds in train test; do
        DATA_DIR="/user/$HDFS_USER/project/data/$ds"
        if hdfs dfs -test -d "$DATA_DIR" 2>/dev/null; then
            ok "HDFS $ds dataset exists: $DATA_DIR"
        else
            warn "HDFS $ds dataset not found: $DATA_DIR"
        fi
    done

    # Check prediction outputs on HDFS
    for m in model1 model2 model3; do
        PRED_DIR="/user/$HDFS_USER/project/output/${m}_predictions"
        if hdfs dfs -test -d "$PRED_DIR" 2>/dev/null; then
            ok "HDFS predictions: $PRED_DIR"
        else
            warn "HDFS predictions not found: $PRED_DIR"
        fi
    done

    # Check evaluation on HDFS
    EVAL_DIR="/user/$HDFS_USER/project/output/evaluation"
    if hdfs dfs -test -d "$EVAL_DIR" 2>/dev/null; then
        ok "HDFS evaluation output: $EVAL_DIR"
    else
        warn "HDFS evaluation not found: $EVAL_DIR"
    fi
fi

# Local models
for m in model1 model2 model3; do
    if [[ -d "models/$m" ]]; then
        ok "Local model directory: models/$m"
    else
        warn "Local model directory missing: models/$m"
    fi
done

# Local predictions
for m in model1 model2 model3; do
    f="output/${m}_predictions.csv"
    if [[ -s "$f" ]]; then
        ok "$f exists and is non-empty"
        # Check CSV has label and prediction columns
        HEADER=$(head -1 "$f" 2>/dev/null || echo "")
        if echo "$HEADER" | grep -qi "label" && echo "$HEADER" | grep -qi "prediction"; then
            ok "$f has label and prediction columns"
        else
            fail "$f missing label/prediction columns (header: $HEADER)"
        fi
        # Check single partition (should be one CSV file, not a directory)
        if [[ -f "$f" ]]; then
            ok "$f saved as single partition"
        fi
    else
        fail "$f missing (required prediction output)"
    fi
done

# Evaluation CSV
if [[ -s "output/evaluation.csv" ]]; then
    ok "output/evaluation.csv exists and is non-empty"
    EVAL_HEADER=$(head -1 output/evaluation.csv 2>/dev/null || echo "")
    if echo "$EVAL_HEADER" | grep -qi "model"; then
        ok "evaluation.csv has model column"
    else
        warn "evaluation.csv might be missing model column"
    fi
    # Check it contains results for all models
    for m in model1 model2 model3; do
        if grep -qi "$m\|LinearRegression\|RandomForest\|GBT" output/evaluation.csv 2>/dev/null; then
            ok "evaluation.csv references $m results"
        else
            warn "evaluation.csv might not have $m results"
        fi
    done
else
    fail "output/evaluation.csv missing"
fi

# Local train/test JSON
for ds in train test; do
    f="data/${ds}.json"
    if [[ -s "$f" ]]; then
        ok "$f exists"
    elif [[ -d "data/$ds" ]]; then
        ok "data/$ds directory exists (Spark JSON partitioned output)"
    else
        warn "$f / data/$ds not found"
    fi
done

# ==========================================================================
section "5. Pylint quality check"
# ==========================================================================

if command -v pylint &>/dev/null; then
    PYLINT_SCORE=$(pylint scripts/ --output-format=text 2>/dev/null | grep "Your code has been rated" | grep -oP '[\d.]+/10' | head -1 || echo "N/A")
    if [[ "$PYLINT_SCORE" != "N/A" ]]; then
        ok "Pylint score: $PYLINT_SCORE"
    else
        warn "Could not determine pylint score"
    fi
else
    warn "pylint not available"
fi

# ==========================================================================
section "Summary"
# ==========================================================================

echo ""
echo -e "${GREEN}Passed: ${pass_count}${NC}"
echo -e "${YELLOW}Warnings: ${warn_count}${NC}"
echo -e "${RED}Failures: ${fail_count}${NC}"
echo ""

if [[ $fail_count -gt 0 ]]; then
    echo -e "${RED}CLUSTER VERIFICATION FAILED${NC}"
    exit 1
else
    echo -e "${GREEN}CLUSTER VERIFICATION PASSED${NC}"
    exit 0
fi
