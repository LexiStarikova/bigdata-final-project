#!/bin/bash
#
# Stage IV - Publish Stage III ML artifacts to Hive for Apache Superset.
#
# This script does not create Superset charts automatically. It prepares all Hive
# datasets that Superset needs for the ML Modeling dashboard section.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

USER_NAME="${USER:-}"
if [[ -z "$USER_NAME" ]]; then
    echo "[ERROR] USER is not set. Run on the cluster as teamXX." >&2
    exit 1
fi

HIVE_SERVER="${STAGE4_HIVE_SERVER:-jdbc:hive2://hadoop-03.uni.innopolis.ru:10001}"
DB="${STAGE4_HIVE_DATABASE:-team35_projectdb}"
HDFS_ROOT="${STAGE4_HDFS_ROOT:-/user/${USER_NAME}/project/stage4/ml}"

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

BEELINE=(beeline -u "${HIVE_SERVER}/${DB}" -n "${USER_NAME}" -p "${HIVE_PASS}")

mkdir -p output/stage4

echo "Stage IV - Hive tables for Superset ML dashboard"
echo "User      : ${USER_NAME}"
echo "Database  : ${DB}"
echo "HDFS root : ${HDFS_ROOT}"

echo ""
echo "[1/4] Preparing small tabular Stage III artifacts..."
python3 scripts/prepare_stage4_ml_artifacts.py

upload_one_file_dir() {
    local src_file="$1"
    local dst_dir="$2"

    if [[ ! -f "$src_file" ]]; then
        echo "[ERROR] Missing file: ${src_file}" >&2
        exit 1
    fi

    hdfs dfs -rm -r -f "$dst_dir" >/dev/null 2>&1 || true
    hdfs dfs -mkdir -p "$dst_dir"
    hdfs dfs -put -f "$src_file" "${dst_dir}/"
}

echo ""
echo "[2/4] Uploading ML dashboard files to HDFS..."
hdfs dfs -mkdir -p "${HDFS_ROOT}"

upload_one_file_dir "output/evaluation.csv" "${HDFS_ROOT}/evaluation"
upload_one_file_dir "output/model_feature_signals.csv" "${HDFS_ROOT}/feature_signals"
upload_one_file_dir "output/stage4/stage4_best_params.csv" "${HDFS_ROOT}/best_params"
upload_one_file_dir "output/stage4/stage4_training_summary.csv" "${HDFS_ROOT}/training_summary"
upload_one_file_dir "output/stage4/stage4_feature_catalog.csv" "${HDFS_ROOT}/feature_catalog"
upload_one_file_dir "output/stage4/stage4_prediction_samples.csv" "${HDFS_ROOT}/prediction_samples"

if [[ -f "output/stage4/stage4_single_prediction.csv" ]]; then
    upload_one_file_dir "output/stage4/stage4_single_prediction.csv" "${HDFS_ROOT}/single_prediction"
else
    hdfs dfs -rm -r -f "${HDFS_ROOT}/single_prediction" >/dev/null 2>&1 || true
    hdfs dfs -mkdir -p "${HDFS_ROOT}/single_prediction"
fi

if [[ "${STAGE4_UPLOAD_FULL_PREDICTIONS:-1}" == "1" ]]; then
    upload_one_file_dir "output/model1_predictions.csv" "${HDFS_ROOT}/model1_predictions"
    upload_one_file_dir "output/model2_predictions.csv" "${HDFS_ROOT}/model2_predictions"
    upload_one_file_dir "output/model3_predictions.csv" "${HDFS_ROOT}/model3_predictions"
else
    echo "Skipping full prediction CSV upload because STAGE4_UPLOAD_FULL_PREDICTIONS!=1."
    hdfs dfs -mkdir -p \
        "${HDFS_ROOT}/model1_predictions" \
        "${HDFS_ROOT}/model2_predictions" \
        "${HDFS_ROOT}/model3_predictions"
fi

echo ""
echo "[3/4] Creating Hive external tables and views..."
"${BEELINE[@]}" \
    --hivevar "stage4_db=${DB}" \
    --hivevar "stage4_hdfs_root=${HDFS_ROOT}" \
    -f sql/stage4_ml_tables.hql \
    > output/stage4_hive_results.txt 2>&1

echo ""
echo "[4/4] Stage IV ML tables are ready for Superset."
echo "Hive log: output/stage4_hive_results.txt"
echo "Main Superset datasets: stage4_evaluation, stage4_model_metric_long,"
echo "stage4_prediction_samples, stage4_prediction_sample_metrics,"
echo "stage4_error_bucket_summary, stage4_model_feature_signals,"
echo "stage4_best_params, stage4_training_summary, stage4_feature_catalog."
