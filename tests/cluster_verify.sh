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

# Safe grep -c wrapper: trims whitespace and defaults to 0
count_grep() {
    local result
    result=$(grep -c "$@" 2>/dev/null || true)
    # Take only the last line and strip whitespace
    result=$(echo "$result" | tail -1 | tr -d '[:space:]')
    if [[ -z "$result" ]]; then
        echo "0"
    else
        echo "$result"
    fi
}

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

    TABLE_EXISTS=$(PGPASSWORD="$PGPASSWORD" psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -tAc \
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='yellow_taxi_trips')" 2>/dev/null || echo "f")
    if [[ "$TABLE_EXISTS" == "t" ]]; then
        ok "PostgreSQL table yellow_taxi_trips exists"
    else
        fail "PostgreSQL table yellow_taxi_trips not found"
    fi

    PK_EXISTS=$(PGPASSWORD="$PGPASSWORD" psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -tAc \
        "SELECT EXISTS(SELECT 1 FROM information_schema.table_constraints WHERE table_name='yellow_taxi_trips' AND constraint_type='PRIMARY KEY')" 2>/dev/null || echo "f")
    if [[ "$PK_EXISTS" == "t" ]]; then
        ok "PostgreSQL yellow_taxi_trips has PRIMARY KEY"
    else
        fail "PostgreSQL yellow_taxi_trips missing PRIMARY KEY"
    fi

    ROW_COUNT=$(PGPASSWORD="$PGPASSWORD" psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -tAc \
        "SELECT COUNT(*) FROM yellow_taxi_trips" 2>/dev/null | tr -d '[:space:]' || echo "0")
    if [[ "$ROW_COUNT" -gt 0 ]] 2>/dev/null; then
        ok "PostgreSQL yellow_taxi_trips has $ROW_COUNT rows"
    else
        fail "PostgreSQL yellow_taxi_trips is empty"
    fi

    COL_COUNT=$(PGPASSWORD="$PGPASSWORD" psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -tAc \
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_name='yellow_taxi_trips'" 2>/dev/null | tr -d '[:space:]' || echo "0")
    if [[ "$COL_COUNT" -ge 19 ]] 2>/dev/null; then
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

    AVRO_COUNT=$(hdfs dfs -ls "$SQOOP_DIR/yellow_taxi_trips/" 2>/dev/null | grep -c "\.avro" || echo "0")
    AVRO_COUNT=$(echo "$AVRO_COUNT" | tail -1 | tr -d '[:space:]')
    if [[ "$AVRO_COUNT" -gt 0 ]] 2>/dev/null; then
        ok "HDFS: $AVRO_COUNT AVRO files in Sqoop output"
    else
        warn "No .avro files found in $SQOOP_DIR/yellow_taxi_trips/"
    fi

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
    BEELINE_URL="jdbc:hive2://hadoop-04.uni.innopolis.ru:10001/${HIVE_DB}"

    run_beeline() {
        beeline -u "$BEELINE_URL" -n "$USER" -p "$HIVE_PASS" \
            --silent=true --outputformat=csv2 -e "$1" 2>/dev/null || true
    }

    # Check database exists
    DB_CHECK=$(run_beeline "SHOW DATABASES" | count_grep "$HIVE_DB")
    if [[ "$DB_CHECK" -gt 0 ]] 2>/dev/null; then
        ok "Hive database $HIVE_DB exists"
    else
        fail "Hive database $HIVE_DB not found"
    fi

    # Check partitioned+bucketed table exists
    TABLE_CHECK=$(run_beeline "SHOW TABLES IN $HIVE_DB" | count_grep "$HIVE_TABLE")
    if [[ "$TABLE_CHECK" -gt 0 ]] 2>/dev/null; then
        ok "Hive table $HIVE_DB.$HIVE_TABLE exists"
    else
        fail "Hive table $HIVE_DB.$HIVE_TABLE not found"
    fi

    # Check table has data
    ROW_CHECK=$(run_beeline "SELECT COUNT(*) AS cnt FROM $HIVE_DB.$HIVE_TABLE" | tail -1 | tr -d '[:space:]')
    if [[ "$ROW_CHECK" -gt 0 ]] 2>/dev/null; then
        ok "Hive table $HIVE_DB.$HIVE_TABLE has $ROW_CHECK rows"
    else
        warn "Hive table $HIVE_DB.$HIVE_TABLE appears empty"
    fi

    # Check unpartitioned table was deleted
    ORIG_TABLE=$(run_beeline "SHOW TABLES IN $HIVE_DB" | grep -v "part_buck" | count_grep "yellow_taxi_trips")
    if [[ "$ORIG_TABLE" -eq 0 ]] 2>/dev/null; then
        ok "Unpartitioned table yellow_taxi_trips was deleted"
    else
        fail "Unpartitioned table yellow_taxi_trips still exists (should be deleted)"
    fi

    # Check partitions
    PARTITION_COUNT=$(run_beeline "SHOW PARTITIONS $HIVE_DB.$HIVE_TABLE" | count_grep "year=")
    if [[ "$PARTITION_COUNT" -gt 0 ]] 2>/dev/null; then
        ok "Table $HIVE_TABLE has $PARTITION_COUNT partition(s)"
    else
        warn "No partitions found for $HIVE_TABLE"
    fi

    # Check qX_results tables exist (at least 6)
    Q_TABLES=$(run_beeline "SHOW TABLES IN $HIVE_DB" | count_grep "q[0-9]*_results")
    if [[ "$Q_TABLES" -ge 6 ]] 2>/dev/null; then
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

# Train/test JSON on HDFS (these are actually persisted, not deleted)
if command -v hdfs &>/dev/null; then
    for ds in train test; do
        DATA_DIR="/user/$HDFS_USER/project/data/$ds"
        if hdfs dfs -test -d "$DATA_DIR" 2>/dev/null; then
            ok "HDFS $ds dataset exists: $DATA_DIR"
        else
            warn "HDFS $ds dataset not found: $DATA_DIR"
        fi
    done
fi

# Local models (saved via hdfs dfs -get from temp staging, then staging deleted)
# Model suffixes are: model1_lr, model2_rf, model3_gbt
for m in model1_lr model2_rf model3_gbt; do
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
        HEADER=$(head -1 "$f" 2>/dev/null || echo "")
        if echo "$HEADER" | grep -qi "label" && echo "$HEADER" | grep -qi "prediction"; then
            ok "$f has label and prediction columns"
        else
            fail "$f missing label/prediction columns (header: $HEADER)"
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
    for label in "LinearRegression" "RandomForestRegressor" "GBTRegressor"; do
        if grep -q "$label" output/evaluation.csv 2>/dev/null; then
            ok "evaluation.csv contains $label results"
        else
            warn "evaluation.csv might not have $label results"
        fi
    done
else
    fail "output/evaluation.csv missing"
fi

# Local train/test JSON (pulled from HDFS via hdfs dfs -get)
for ds in train test; do
    f="data/${ds}.json"
    if [[ -s "$f" ]]; then
        ok "$f exists"
    elif [[ -d "data/$ds" ]]; then
        ok "data/$ds directory exists (Spark JSON partitioned output)"
    else
        warn "$f / data/$ds not found (pulled from HDFS at runtime)"
    fi
done

# Stage3 training summary
if [[ -s "output/stage3_training_summary.json" ]]; then
    ok "output/stage3_training_summary.json exists"
else
    warn "output/stage3_training_summary.json missing"
fi

# Best params JSONs
for m in model1 model2 model3; do
    f="output/${m}_best_params.json"
    if [[ -s "$f" ]]; then
        ok "$f exists"
    else
        warn "$f missing"
    fi
done

# ==========================================================================
section "5. Pylint quality check"
# ==========================================================================

if command -v pylint &>/dev/null; then
    PYLINT_SCORE=$(pylint scripts/ --ignore=.ipynb_checkpoints --output-format=text 2>/dev/null | grep "Your code has been rated" | grep -oP '[\d.]+/10' | head -1 || echo "N/A")
    if [[ "$PYLINT_SCORE" != "N/A" ]]; then
        ok "Pylint score: $PYLINT_SCORE (ignoring .ipynb_checkpoints)"
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
