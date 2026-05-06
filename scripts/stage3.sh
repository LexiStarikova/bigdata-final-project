#!/usr/bin/bash
#
# Stage 3 — Predictive Analytics (Spark ML regression for tip_amount).
#
# Cluster (hostname *hadoop*): submits to YARN and by default reads Stage II Hive table
#   team35_projectdb.yellow_taxi_trips_part_buck (sql/db.hql). Override DB/table with env or
#   extra args appended after scripts/stage3.sh -- ...
#
# Parquet TLC path fallback: STAGE3_USE_PARQUET_DIR=1 (uses STAGE3_HDFS_DATA_DIR or taxi/data).
#
# Warehouse aligns with CREATE DATABASE LOCATION in sql/db.hql (project/hive/warehouse → HDFS).
#
# Laptop/offline: local Parquet under ./data/stage3_sample or FORCE_LOCAL — no Yarn.
#

set -euo pipefail
shopt -s nullglob

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="${ROOT_DIR}/scripts/stage3.py"
REMOTE_USER="${STAGE3_HDFS_USER:-$USER}"

export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-$PYSPARK_PYTHON}"

local_parquets=(
  "${ROOT_DIR}/data"/yellow_tripdata_*.parquet
  "${ROOT_DIR}/data/stage3_sample"/yellow_tripdata_*.parquet
  "${ROOT_DIR}/data/stage3_sample/"*.parquet
)

PREFER_YARN=no
case "${STAGE3_USE_YARN:-}" in 1 | yes | true | TRUE) PREFER_YARN=yes ;; esac
case "${HOSTNAME:-}" in *hadoop* | *[Hh]adoop*) PREFER_YARN=yes ;; esac

use_local=no
[[ -n "${STAGE3_FORCE_LOCAL:-}" ]] && use_local=yes
[[ "${use_local}" == no && "${PREFER_YARN}" == no && ${#local_parquets[@]} -gt 0 ]] && \
  use_local=yes

if [[ "${use_local}" == yes ]]; then
  LOCAL_MASTER="${STAGE3_SPARK_MASTER:-local[*]}"

  exec spark-submit \
    --master "${LOCAL_MASTER}" \
    --driver-memory "${STAGE3_DRIVER_MEM:-8g}" \
    "${SCRIPT}" \
    --data-dir "${STAGE3_DATA_DIR_OVERRIDE:-${ROOT_DIR}/data}" \
    --models-dir "${ROOT_DIR}/models" \
    --output-dir "${ROOT_DIR}/output" \
    --master "${LOCAL_MASTER}" \
    "$@"
fi

# spark-submit --master yarn needs Hadoop/YARN client XML on classpath.
if [[ -z "${HADOOP_CONF_DIR:-}" && -z "${YARN_CONF_DIR:-}" ]]; then
  # Inno cluster gateways (and similar) ship config here; avoids SparkSubmit failure
  # when Hadoop was installed without HADOOP_HOME in the user's shell profile.
  if [[ -f /etc/hadoop/conf/yarn-site.xml || -f /etc/hadoop/conf/core-site.xml ]]; then
    export HADOOP_CONF_DIR=/etc/hadoop/conf
  fi
fi
if [[ -z "${HADOOP_CONF_DIR:-}" && -z "${YARN_CONF_DIR:-}" ]]; then
  for d in "${HADOOP_HOME:+$HADOOP_HOME/etc/hadoop}" /etc/hadoop/conf "/usr/lib/hadoop/etc/hadoop"; do
    [[ -z "$d" || ! -d "$d" ]] && continue
    if [[ -f "$d/yarn-site.xml" || -f "$d/core-site.xml" ]]; then
      export HADOOP_CONF_DIR="$d"
      break
    fi
  done
fi
if [[ -z "${HADOOP_CONF_DIR:-}" && -z "${YARN_CONF_DIR:-}" ]]; then
  echo >&2 "stage3.sh: For Yarn, set HADOOP_CONF_DIR or YARN_CONF_DIR (Spark requires one)."
  echo >&2 "  Example: export HADOOP_CONF_DIR=/etc/hadoop/conf"
  echo >&2 "  Or:      export YARN_CONF_DIR=/etc/hadoop/conf"
  echo >&2 "  Typical cluster layout: \$HADOOP_HOME/etc/hadoop or /etc/hadoop/conf."
  exit 2
fi

STAGE3_HIVE_DATABASE="${STAGE3_HIVE_DATABASE:-team35_projectdb}"
STAGE3_HIVE_TABLE="${STAGE3_HIVE_TABLE:-yellow_taxi_trips_part_buck}"

PYTHON_ARGS=(
  "${SCRIPT}"
  --models-dir "${ROOT_DIR}/models"
  --output-dir "${ROOT_DIR}/output"
)

if [[ -z "${STAGE3_USE_PARQUET_DIR:-}" ]]; then
  export SPARK_SQL_WAREHOUSE_DIR="${SPARK_SQL_WAREHOUSE_DIR:-hdfs:///user/${REMOTE_USER}/project/hive/warehouse}"
  PYTHON_ARGS+=(
    --hive-database "${STAGE3_HIVE_DATABASE}"
    --hive-table "${STAGE3_HIVE_TABLE}"
  )
else
  PYTHON_ARGS+=(
    --data-dir "${STAGE3_HDFS_DATA_DIR:-hdfs:///user/${REMOTE_USER}/taxi/data}"
  )
fi

export STAGE3_CV_PARALLELISM="${STAGE3_CV_PARALLELISM:-1}"

exec spark-submit \
  --master yarn \
  --deploy-mode "${STAGE3_DEPLOY:-client}" \
  --num-executors "${STAGE3_EXECUTORS:-4}" \
  --executor-memory "${STAGE3_EXEC_MEM:-8g}" \
  --executor-cores "${STAGE3_CORES:-2}" \
  --driver-memory "${STAGE3_DRIVER_MEM:-12g}" \
  "${PYTHON_ARGS[@]}" \
  "$@"
