#!/bin/bash
# Stage 3 — Spark ML: train 3 classifiers on NYC Taxi 2025, tune with CV, save models & results
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/etc/hadoop/conf}"
export YARN_CONF_DIR="${YARN_CONF_DIR:-/etc/hadoop/conf}"

spark-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors 4 \
  --executor-memory 4g \
  --executor-cores 2 \
  --driver-memory 4g \
  scripts/stage3.py \
  --data-dir hdfs:///user/"$USER"/taxi/data \
  --models-dir hdfs:///user/"$USER"/taxi/models \
  --output-dir output
