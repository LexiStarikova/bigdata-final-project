#!/usr/bin/bash
#
# Stage 3 — Predictive Analytics (Spark ML regression for tip_amount).
#
# When TLC Parquet shards are present locally under ./data or ./data/stage3_sample,
# submits with local[*] (requires a JDK).
# Otherwise targets the course YARN layout (hdfs:///user/$USER/taxi/data).
#

set -euo pipefail
shopt -s nullglob

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${ROOT_DIR}/scripts/stage3.py"
REMOTE_USER="${STAGE3_HDFS_USER:-$USER}"

local_parquets=(
  "${ROOT_DIR}/data"/yellow_tripdata_*.parquet
  "${ROOT_DIR}/data/stage3_sample"/yellow_tripdata_*.parquet
  "${ROOT_DIR}/data/stage3_sample/"*.parquet
)

if [[ -n "${STAGE3_FORCE_LOCAL:-}" || ${#local_parquets[@]} -gt 0 ]]; then

  LOCAL_MASTER="${STAGE3_SPARK_MASTER:-local[*]}"
  EXTRA=( "$@" )

  exec spark-submit \
    --master "${LOCAL_MASTER}" \
    --driver-memory "${STAGE3_DRIVER_MEM:-4g}" \
    "${SCRIPT}" \
    --data-dir "${STAGE3_DATA_DIR_OVERRIDE:-${ROOT_DIR}/data}" \
    --models-dir "${ROOT_DIR}/models" \
    --output-dir "${ROOT_DIR}/output" \
    --master "${LOCAL_MASTER}" \
    "${EXTRA[@]}"
fi

spark-submit \
  --master yarn \
  --deploy-mode "${STAGE3_DEPLOY:-client}" \
  --num-executors "${STAGE3_EXECUTORS:-4}" \
  --executor-memory "${STAGE3_EXEC_MEM:-4g}" \
  --executor-cores "${STAGE3_CORES:-2}" \
  --driver-memory "${STAGE3_DRIVER_MEM:-4g}" \
  "${SCRIPT}" \
  --data-dir "hdfs:///user/${REMOTE_USER}/taxi/data" \
  --models-dir "${ROOT_DIR}/models" \
  --output-dir "${ROOT_DIR}/output" \
  "$@"
