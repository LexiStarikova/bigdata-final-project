# Stage III contribution — Predictive Data Analytics

This document summarizes what was implemented for **Stage III (Predictive Data Analytics)** for the NYC Yellow Taxi tip study.


## Implementation map

| Path | Purpose |
| --- | --- |
| `scripts/stage3.py` | End-to-end regression: preprocessing, two estimators, CV, evaluation, CSV exports, model persistence. |
| `scripts/stage3.sh` | `spark-submit` wrapper switching between **local** data & **Yarn+HDFS** layouts. |
| `output/model1_predictions.csv`, `output/model2_predictions.csv` | Only `label,prediction`, effectively single partition (`coalesce` + merge). |
| `output/evaluation.csv` | Holds `model`, `RMSE`, `R2` for comparison. |
| `output/stage3_training_summary.json` | Metrics + serialized best-parameter map (`default=str` for safety). |
| `models/model1_lr`, `models/model2_rf` | Saved Spark `PipelineModel` folders. |

## How to run on a Cluster

1. Consume Hive-managed tables exactly as Stage II published (partitioning/bucketing per rubric).
2. Either keep using Parquet snapshots under `hdfs:///user/$USER/taxi/data` (wired in `stage3.sh`) or replace `load_raw()` with metastore-backed reads (`spark.table("team_db.yellow_trips_part")`).
3. Enforce Yarn submission flags from the syllabus template (`spark-submit --master yarn ...`).
4. After writing checklist paths on HDFS, `hdfs dfs -cat` merges into repo `output/*.csv`.