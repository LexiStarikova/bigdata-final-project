#!/bin/bash
# Stage 3 — Spark ML: train 3 classifiers on NYC Taxi 2025, tune with CV, save models & results
# Run on YARN cluster:
#   spark-submit --master yarn --deploy-mode cluster scripts/stage3.py \
#     --data-dir hdfs:///user/$USER/taxi/data \
#     --models-dir hdfs:///user/$USER/taxi/models \
#     --output-dir output
#
# Run locally (for testing):
#   spark-submit scripts/stage3.py

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
