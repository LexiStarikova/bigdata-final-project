#!/bin/bash
# Stage I — PostgreSQL load + Sqoop import to HDFS (AVRO + Snappy).
# Matches: BS - Stage I - Data collection and Ingestion (IU Big Data).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Same password as Python/psycopg2: secrets/.psql.pass or secrets/psql.pass or env PGPASSWORD
if [[ -n "${PGPASSWORD:-}" ]]; then
  PASSWORD="${PGPASSWORD}"
elif [[ -f "secrets/.psql.pass" ]]; then
  PASSWORD="$(head -n 1 secrets/.psql.pass | tr -d '\r\n')"
elif [[ -f "secrets/psql.pass" ]]; then
  PASSWORD="$(head -n 1 secrets/psql.pass | tr -d '\r\n')"
else
  echo "Set PGPASSWORD or create secrets/.psql.pass (see secrets/README.txt)." >&2
  exit 1
fi
if [[ -z "$PASSWORD" ]]; then
  echo "Password file exists but is empty; put the DB password on one line." >&2
  exit 1
fi

USER_NAME="${USER:-}"
if [[ -z "$USER_NAME" ]]; then
  echo "USER is not set; run on the cluster as teamXX." >&2
  exit 1
fi

DB="${USER_NAME}_projectdb"
JDBC="jdbc:postgresql://hadoop-04.uni.innopolis.ru/${DB}"
WAREHOUSE="project/warehouse"

echo "Loading Parquet into PostgreSQL (${DB}) …"
python3 scripts/build_projectdb.py

echo "Clearing HDFS warehouse ${WAREHOUSE} if present …"
hdfs dfs -rm -r -f "${WAREHOUSE}" || true

echo "Sqoop: import yellow_taxi_trips as Avro + Snappy → HDFS ${WAREHOUSE}/yellow_taxi_trips …"
# Parallel mappers on trip_id (BIGINT PK). For smoke tests: STAGE1_SQOOP_MAPPERS=1
MAPS="${STAGE1_SQOOP_MAPPERS:-8}"
sqoop import \
  --connect "${JDBC}" \
  --username "${USER_NAME}" \
  --password "${PASSWORD}" \
  --table yellow_taxi_trips \
  --compression-codec snappy \
  --compress \
  --as-avrodatafile \
  --target-dir "${WAREHOUSE}/yellow_taxi_trips" \
  --delete-target-dir \
  --split-by trip_id \
  -m "${MAPS}"

mkdir -p output output/sqoop_codegen

echo "Sqoop codegen → output/sqoop_codegen (Java sources) …"
sqoop codegen \
  --connect "${JDBC}" \
  --username "${USER_NAME}" \
  --password "${PASSWORD}" \
  --table yellow_taxi_trips \
  --outdir output/sqoop_codegen \
  --package-name "bigdata.${USER_NAME}.taxi"

echo "Extract Avro schema from first part file (requires fastavro) …"
PART_FILE=$(hdfs dfs -ls "${WAREHOUSE}/yellow_taxi_trips" 2>/dev/null | grep '\.avro$' | head -1 | awk '{print $NF}' || true)
if [[ -n "${PART_FILE}" ]]; then
  SAMPLE="output/_sample_yellow_taxi_trips.avro"
  hdfs dfs -get "${PART_FILE}" "${SAMPLE}"
  python3 - <<'PY'
import json
import sys
from pathlib import Path

sample = Path("output/_sample_yellow_taxi_trips.avro")
if not sample.is_file():
    sys.exit(0)
try:
    import fastavro
except ImportError:
    print("fastavro not installed; skip writing .avsc (pip install fastavro).")
    sys.exit(0)
with sample.open("rb") as fh:
    r = fastavro.reader(fh)
    schema = r.writer_schema
out = Path("output/yellow_taxi_trips.avsc")
out.write_text(json.dumps(schema, indent=2), encoding="utf-8")
print("Wrote", out)
PY
else
  echo "No .avro parts found under ${WAREHOUSE}/yellow_taxi_trips; skip schema extract."
fi

echo "Copy generated Java files to output/ root for checklist visibility …"
find output/sqoop_codegen -maxdepth 1 -name '*.java' -exec cp -t output/ {} + 2>/dev/null || true

echo "Stage 1 finished. HDFS: hdfs://${WAREHOUSE}/yellow_taxi_trips/"
echo "Report: justify Avro + Snappy vs Parquet in your Stage I write-up (see course PDF)."