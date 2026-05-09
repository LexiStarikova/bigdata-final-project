#!/bin/bash

set -euo pipefail

# ---------------------------------------------------------------------------
# 0. Paths & constants
# ---------------------------------------------------------------------------
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

HIVE_SERVER="jdbc:hive2://hadoop-03.uni.innopolis.ru:10001"
USER_NAME="${USER:-}"

if [[ -z "$USER_NAME" ]]; then
    echo "[ERROR] \$USER is not set. Run on the cluster as teamXX." >&2
    exit 1
fi

# Hive password
if [[ -n "${HIVE_PASSWORD:-}" ]]; then
    HIVE_PASS="${HIVE_PASSWORD}"
elif [[ -f "secrets/.hive.pass" ]]; then
    HIVE_PASS="$(head -n 1 secrets/.hive.pass | tr -d '\r\n')"
else
    echo "[ERROR] No Hive password. Create secrets/.hive.pass or set HIVE_PASSWORD." >&2
    exit 1
fi

if [[ -z "$HIVE_PASS" ]]; then
    echo "[ERROR] Hive password file is empty." >&2
    exit 1
fi

# Beeline base command (suppress INFO noise to stderr, keep clean stdout)
BEELINE="beeline -u ${HIVE_SERVER} -n ${USER_NAME} -p ${HIVE_PASS}"

mkdir -p output

echo " Stage II — Hive + EDA"
echo " User     : ${USER_NAME}"
echo " Hive URL : ${HIVE_SERVER}"

# 1. Upload AVRO schema files to HDFS
#    Sqoop generates yellow_taxi_trips.avsc in the local output/ folder.
#    Hive needs it in HDFS so the external table can read the schema.

echo ""
echo "[1/5] Uploading AVRO schema files to HDFS..."

AVSC_HDFS="project/warehouse/avsc"
hdfs dfs -mkdir -p "${AVSC_HDFS}"

if [[ -f "output/yellow_taxi_trips.avsc" ]]; then
    hdfs dfs -put -f "output/yellow_taxi_trips.avsc" "${AVSC_HDFS}/"
    echo "      Uploaded output/yellow_taxi_trips.avsc → HDFS ${AVSC_HDFS}/"
else
    echo "      [WARN] output/yellow_taxi_trips.avsc not found." \
         "Run stage1.sh first if this is a fresh setup."
fi


# 2. Create Hive database + tables

echo ""
echo "[2/5] Running sql/db.hql (create DB, tables, load data)..."

${BEELINE} -f sql/db.hql \
    > output/hive_results.txt 2>/dev/null

echo "      Done. Output saved to output/hive_results.txt"


# 3. Run EDA queries (eda.hql)
#    - q1  : Dataset overview (total trips, date range, revenue)
#    - q2  : Data quality check (NULLs, zeros, outliers)
#    - q3  : Passenger count distribution
#    - q4  : Trip distance distribution
#    - q5  : Revenue component breakdown (fare vs tax vs tip etc.)
#    - q6  : Payment type breakdown
#    - q7  : Peak hours (hourly demand)
#    - q8  : Airport vs standard trips (ratecodeid)
#    - q9  : Revenue per mile by distance bucket
#    - q10 : Day-of-week revenue pattern
#    - q11 : Top 10 busiest pickup locations
#    - q12 : Late-night tipping (credit card only)

echo ""
echo "[3/5] Running sql/eda.hql (12 EDA queries)..."

${BEELINE} -f sql/eda.hql \
    > output/eda_results.txt 2>/dev/null

echo "Done. Output saved to output/eda_results.txt"


# 4. Export each result table to a local CSV file

echo ""
echo "[4/5] Exporting result tables to output/q1.csv ... output/q10.csv ..."

DB="team35_projectdb"

export_table() {
    local TABLE="$1"   # e.g.  q1_results
    local FILE="$2"    # e.g.  output/q1.csv
    echo "      Exporting ${DB}.${TABLE} → ${FILE}"
    ${BEELINE} \
        --hiveconf hive.resultset.use.unique.column.names=false \
        -e "USE ${DB}; SELECT * FROM ${TABLE};" \
        > "${FILE}" 2>/dev/null
}

export_table "q1_results"  "output/q1.csv"
export_table "q2_results"  "output/q2.csv"
export_table "q3_results"  "output/q3.csv"
export_table "q4_results"  "output/q4.csv"
export_table "q5_results"  "output/q5.csv"
export_table "q6_results"  "output/q6.csv"
export_table "q7_results"  "output/q7.csv"
export_table "q8_results"  "output/q8.csv"
export_table "q9_results"  "output/q9.csv"
export_table "q10_results" "output/q10.csv"
export_table "q11_results" "output/q11.csv"
export_table "q12_results" "output/q12.csv"

echo "      All CSV files saved."


# 5. Quick sanity check — print first 3 lines of each CSV
echo ""
echo "[5/5] Sanity check — first result row per query:"
for i in 1 2 3 4 5 6 7 8 9 10 11 12; do
    FILE="output/q${i}.csv"
    if [[ -s "$FILE" ]]; then
        # Print last non-empty data line (skip beeline header/separator lines)
        SAMPLE=$(grep '|' "$FILE" | grep -v '^\+' | tail -1 || true)
        printf "  q%2d | %s\n" "$i" "$SAMPLE"
    else
        printf "  q%2d | [empty or missing]\n" "$i"
    fi
done


echo " Stage II complete."
