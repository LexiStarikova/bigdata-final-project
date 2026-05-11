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


section "1. Prerequisites — commands and secrets"


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


section "2. Stage I — Data collection and ingestion"


# PostgreSQL via Python psycopg2 (psql CLI is not on this cluster)
PG_USER="${PGUSER:-$USER}"
PG_DB="${PG_USER}_projectdb"
PG_HOST="${PGHOST:-hadoop-04.uni.innopolis.ru}"
PG_PASS="$(head -1 secrets/.psql.pass 2>/dev/null | tr -d '\r\n' || echo '')"

PG_RESULT=$(python3 -c "
import sys
try:
    import psycopg2
except ImportError:
    print('NO_DRIVER'); sys.exit(0)
try:
    conn = psycopg2.connect(
        host='${PG_HOST}', port=5432,
        user='${PG_USER}', dbname='${PG_DB}',
        password='''${PG_PASS}''',
        connect_timeout=10,
    )
    cur = conn.cursor()
    # 1. Table exists?
    cur.execute(\"\"\"
        SELECT EXISTS(
            SELECT 1 FROM information_schema.tables
            WHERE table_name='yellow_taxi_trips'
        )
    \"\"\")
    table_exists = cur.fetchone()[0]
    # 2. Primary key?
    cur.execute(\"\"\"
        SELECT EXISTS(
            SELECT 1 FROM information_schema.table_constraints
            WHERE table_name='yellow_taxi_trips'
              AND constraint_type='PRIMARY KEY'
        )
    \"\"\")
    pk_exists = cur.fetchone()[0]
    # 3. Column count
    cur.execute(\"\"\"
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_name='yellow_taxi_trips'
    \"\"\")
    col_count = cur.fetchone()[0]
    # 4. Row count (fast estimate from pg_class)
    cur.execute(\"\"\"
        SELECT reltuples::bigint
        FROM pg_class WHERE relname='yellow_taxi_trips'
    \"\"\")
    row_estimate = cur.fetchone()[0]
    conn.close()
    print(f'OK|{table_exists}|{pk_exists}|{col_count}|{row_estimate}')
except Exception as exc:
    print(f'ERR|{exc}')
" 2>/dev/null || echo "ERR|python failed")

if [[ "$PG_RESULT" == "NO_DRIVER" ]]; then
    warn "psycopg2 not installed; skipping PostgreSQL checks"
elif [[ "$PG_RESULT" == ERR* ]]; then
    PG_ERR="${PG_RESULT#ERR|}"
    fail "PostgreSQL connection failed: $PG_ERR"
else
    IFS='|' read -r _status PG_TABLE PG_PK PG_COLS PG_ROWS <<< "$PG_RESULT"

    ok "PostgreSQL connection to $PG_DB works"

    if [[ "$PG_TABLE" == "True" ]]; then
        ok "PostgreSQL table yellow_taxi_trips exists"
    else
        fail "PostgreSQL table yellow_taxi_trips not found"
    fi

    if [[ "$PG_PK" == "True" ]]; then
        ok "PostgreSQL yellow_taxi_trips has PRIMARY KEY"
    else
        fail "PostgreSQL yellow_taxi_trips missing PRIMARY KEY"
    fi

    if [[ "$PG_COLS" -ge 19 ]] 2>/dev/null; then
        ok "PostgreSQL yellow_taxi_trips has $PG_COLS columns (>=19+PK)"
    else
        fail "PostgreSQL yellow_taxi_trips has only $PG_COLS columns, expected >=19"
    fi

    if [[ "$PG_ROWS" -gt 0 ]] 2>/dev/null; then
        ok "PostgreSQL yellow_taxi_trips has ~$PG_ROWS rows"
    else
        fail "PostgreSQL yellow_taxi_trips appears empty"
    fi
fi

# HDFS: Sqoop import directory
if command -v hdfs &>/dev/null; then
    SQOOP_DIR="/user/$HDFS_USER/project/warehouse"
    if hdfs dfs -test -d "$SQOOP_DIR" 2>/dev/null; then
        ok "HDFS Sqoop directory exists: $SQOOP_DIR"
    else
        fail "HDFS Sqoop directory missing: $SQOOP_DIR"
    fi

    AVRO_COUNT=$(hdfs dfs -ls "$SQOOP_DIR/yellow_taxi_trips/" 2>/dev/null \
        | grep -c "\.avro" || echo "0")
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


section "3. Stage II — Hive database, tables, and EDA"


# 3.1 Hive warehouse location in HDFS
if command -v hdfs &>/dev/null; then
    HIVE_LOC="/user/$HDFS_USER/project/hive/warehouse"

    if hdfs dfs -test -d "$HIVE_LOC" 2>/dev/null; then
        ok "Hive DB HDFS location exists: $HIVE_LOC"
    else
        fail "Hive DB HDFS location missing: $HIVE_LOC"
    fi

    # Find physical Hive table directory
    TABLE_HDFS_FOUND=0
    TABLE_HDFS_PATH=""

    for candidate in \
        "$HIVE_LOC/$HIVE_TABLE" \
        "$HIVE_LOC/${HIVE_TABLE,,}" \
        "$HIVE_LOC/${HIVE_DB}.db/$HIVE_TABLE" \
        "$HIVE_LOC/${HIVE_DB}.db/${HIVE_TABLE,,}"
    do
        if hdfs dfs -test -d "$candidate" 2>/dev/null; then
            TABLE_HDFS_FOUND=1
            TABLE_HDFS_PATH="$candidate"
            ok "Hive table HDFS directory exists: $candidate"
            break
        fi
    done

    if [[ "$TABLE_HDFS_FOUND" -eq 1 ]]; then
        # Check partition directories (year=XXXX)
        PARTITION_DIRS=$(hdfs dfs -ls "$TABLE_HDFS_PATH" 2>/dev/null \
            | grep "year=" | wc -l | tr -d '[:space:]' || echo "0")
        if [[ "$PARTITION_DIRS" -gt 0 ]] 2>/dev/null; then
            ok "Hive table has $PARTITION_DIRS partition directories (year=*)"
        else
            warn "No year=* partition directories found under $TABLE_HDFS_PATH"
        fi

        # Check data files inside partition dirs (look 2 levels deep)
        DATA_FILES=$(hdfs dfs -ls -R "$TABLE_HDFS_PATH" 2>/dev/null \
            | grep -v "^d" | head -20 | wc -l | tr -d '[:space:]' || echo "0")
        if [[ "$DATA_FILES" -gt 0 ]] 2>/dev/null; then
            ok "Hive table partition directories contain $DATA_FILES+ data files"
        else
            warn "No data files found in partition directories under $TABLE_HDFS_PATH"
        fi
    else
        warn "Could not find physical HDFS directory for $HIVE_TABLE"
    fi

    # Check that Sqoop warehouse and Hive warehouse are separate
    SQOOP_DIR="/user/$HDFS_USER/project/warehouse"
    if [[ "$HIVE_LOC" != "$SQOOP_DIR" ]]; then
        ok "Hive warehouse location is separate from Sqoop warehouse"
    else
        fail "Hive warehouse location equals Sqoop warehouse"
    fi
else
    warn "hdfs not available; skipping HDFS-based Hive checks"
fi

# 3.2 Local EDA CSV outputs
EDA_LOCAL_COUNT=0
for i in $(seq 1 12); do
    f="output/q${i}.csv"
    if [[ -s "$f" ]]; then
        ok "$f exists and is non-empty"
        EDA_LOCAL_COUNT=$((EDA_LOCAL_COUNT + 1))
    elif [[ -f "$f" ]]; then
        warn "$f exists but is empty"
    else
        if [[ "$i" -le 6 ]]; then
            fail "$f missing (minimum q1-q6 required)"
        fi
    fi
done

if [[ "$EDA_LOCAL_COUNT" -ge 6 ]]; then
    ok "$EDA_LOCAL_COUNT local EDA CSV files found (>=6)"
else
    fail "Only $EDA_LOCAL_COUNT local EDA CSV files found, need >=6"
fi

# 3.3 Local Hive result log
if [[ -s output/hive_results.txt ]]; then
    ok "output/hive_results.txt exists and is non-empty"
    if grep -qi "$HIVE_DB" output/hive_results.txt 2>/dev/null; then
        ok "output/hive_results.txt references Hive database $HIVE_DB"
    fi
    if grep -qi "$HIVE_TABLE" output/hive_results.txt 2>/dev/null; then
        ok "output/hive_results.txt references Hive table $HIVE_TABLE"
    fi
else
    warn "output/hive_results.txt missing or empty"
fi

# 3.4 SQL/script evidence
if compgen -G "sql/*.sql" >/dev/null || compgen -G "sql/*.hql" >/dev/null; then
    ok "sql/ contains SQL/HQL files"
fi

if grep -Rqi "partitioned by\|partition by" sql/ scripts/ 2>/dev/null; then
    ok "Repository scripts contain partitioning logic"
fi

if grep -Rqi "clustered by\|bucket" sql/ scripts/ 2>/dev/null; then
    ok "Repository scripts contain bucketing logic"
fi

# Count q*_results references in codebase
Q_SCRIPT_COUNT="$(
    grep -Rho "q[0-9]\+_results" scripts/ sql/ output/ 2>/dev/null \
    | sort -u | wc -l | tr -d '[:space:]' || echo "0"
)"
if [[ "${Q_SCRIPT_COUNT:-0}" =~ ^[0-9]+$ && "${Q_SCRIPT_COUNT:-0}" -ge 6 ]]; then
    ok "$Q_SCRIPT_COUNT q*_results references found in scripts/sql/output"
fi


section "4. Stage III — Predictive Data Analytics (PySpark ML)"


# Local models (model suffixes: model1_lr, model2_rf, model3_gbt)
for model_dir in model1_lr model2_rf model3_gbt; do
    if [[ -d "models/$model_dir" ]]; then
        ok "Local model directory: models/$model_dir"
    else
        fail "Local model directory missing: models/$model_dir"
    fi
done

# Local predictions
for model_key in model1 model2 model3; do
    f="output/${model_key}_predictions.csv"
    if [[ -s "$f" ]]; then
        ok "$f exists and is non-empty"
        HEADER=$(head -1 "$f" 2>/dev/null || echo "")
        if echo "$HEADER" | grep -qi "label" \
            && echo "$HEADER" | grep -qi "prediction"; then
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
    fi
    for label in "LinearRegression" "RandomForestRegressor" "GBTRegressor"; do
        if grep -q "$label" output/evaluation.csv 2>/dev/null; then
            ok "evaluation.csv contains $label results"
        else
            fail "evaluation.csv missing $label results"
        fi
    done
else
    fail "output/evaluation.csv missing"
fi

# Stage3 training summary
if [[ -s "output/stage3_training_summary.json" ]]; then
    ok "output/stage3_training_summary.json exists"
fi

# Best params JSONs
for model_key in model1 model2 model3; do
    f="output/${model_key}_best_params.json"
    if [[ -s "$f" ]]; then
        ok "$f exists"
    else
        warn "$f missing"
    fi
done


section "5. Pylint quality check"


if command -v pylint &>/dev/null; then
    PYLINT_SCORE=$(pylint scripts/ \
        --ignore=.ipynb_checkpoints \
        --ignore=stage3_sample_prediction.py \
        --output-format=text 2>/dev/null \
        | grep "Your code has been rated" \
        | grep -oP '[\d.]+/10' | head -1 || echo "N/A")
    if [[ "$PYLINT_SCORE" != "N/A" ]]; then
        ok "Pylint score: $PYLINT_SCORE"
    else
        warn "Could not determine pylint score"
    fi
else
    warn "pylint not available"
fi


section "Summary"

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
