"""
Stage 3 — Predictive Data Analysis (PDA)
NYC Yellow Taxi 2023 — Generous Tip Binary Classification

Models:
  1. Random Forest Classifier
  2. Gradient-Boosted Trees (GBT) Classifier
  3. Logistic Regression

Each model is tuned with CrossValidator (k=3) over a 3×3×3 = 27-combination grid.
Metrics: Area Under ROC, Area Under PR (binary classification task).
Best model is saved to models/.  Results are written to output/.
"""

import os
import json
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, Imputer
from pyspark.ml.classification import (
    RandomForestClassifier,
    GBTClassifier,
    LogisticRegression,
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ── Argument parsing ───────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Stage 3: Spark ML pipeline")
parser.add_argument("--data-dir",   default="data",    help="Path to parquet files")
parser.add_argument("--models-dir", default="models",  help="Where to save trained models")
parser.add_argument("--output-dir", default="output",  help="Where to save results")
args = parser.parse_args()

DATA_DIR   = os.path.abspath(args.data_dir)
MODELS_DIR = os.path.abspath(args.models_dir)
OUTPUT_DIR = os.path.abspath(args.output_dir)

os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Spark Session ──────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("NYC_Taxi_Stage3_ML")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(f"Spark {spark.version} | data={DATA_DIR}")

# ── Load data ──────────────────────────────────────────────────────────────
raw_df = spark.read.parquet(os.path.join(DATA_DIR, "yellow_tripdata_2025-*.parquet"))
print(f"Raw records: {raw_df.count():,}")

# ── Feature engineering ────────────────────────────────────────────────────
FEATURE_COLS = [
    "VendorID", "passenger_count", "trip_distance", "RatecodeID",
    "PULocationID", "DOLocationID", "fare_amount", "extra", "mta_tax",
    "tolls_amount", "congestion_surcharge", "airport_fee",
    "trip_duration_min", "pickup_hour", "pickup_dayofweek",
    "pickup_month", "is_weekend", "avg_speed_mph",
]


def engineer_features(df):
    """Add derived columns and binary label (generous tip = tip > 20% of fare)."""
    return (
        df
        .filter(F.col("payment_type") == 1)
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("tip_amount") >= 0)
        .filter(F.col("fare_amount") < 500)
        .filter(F.col("trip_distance") < 200)
        .filter(F.col("passenger_count").between(1, 6))
        .withColumn(
            "trip_duration_min",
            (F.unix_timestamp("tpep_dropoff_datetime") -
             F.unix_timestamp("tpep_pickup_datetime")) / 60.0,
        )
        .filter(F.col("trip_duration_min").between(1, 180))
        .withColumn("pickup_hour",      F.hour("tpep_pickup_datetime").cast(DoubleType()))
        .withColumn("pickup_dayofweek", F.dayofweek("tpep_pickup_datetime").cast(DoubleType()))
        .withColumn("pickup_month",     F.month("tpep_pickup_datetime").cast(DoubleType()))
        .withColumn("is_weekend",
            F.when(F.dayofweek("tpep_pickup_datetime").isin([1, 7]), 1.0).otherwise(0.0))
        .withColumn("avg_speed_mph",
            F.col("trip_distance") / (F.col("trip_duration_min") / 60.0))
        .withColumn("tip_ratio", F.col("tip_amount") / F.col("fare_amount"))
        .withColumn("label",
            F.when(F.col("tip_ratio") > 0.20, 1.0).otherwise(0.0))
        .select(FEATURE_COLS + ["label"])
    )


ml_df = engineer_features(raw_df).cache()
total = ml_df.count()
pos   = ml_df.filter(F.col("label") == 1).count()
print(f"ML dataset: {total:,} rows | positive={pos:,} ({100*pos/total:.1f}%)")

# ── Train / test split ─────────────────────────────────────────────────────
train_df, test_df = ml_df.randomSplit([0.70, 0.30], seed=42)
train_df = train_df.cache()
test_df  = test_df.cache()
print(f"Train={train_df.count():,}  Test={test_df.count():,}")

# ── Preprocessing stages ───────────────────────────────────────────────────
imputer = Imputer(
    inputCols=FEATURE_COLS,
    outputCols=[c + "_imp" for c in FEATURE_COLS],
    strategy="median",
)
assembler = VectorAssembler(
    inputCols=[c + "_imp" for c in FEATURE_COLS],
    outputCol="features_raw",
    handleInvalid="skip",
)
scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withMean=True,
    withStd=True,
)
preprocessing = [imputer, assembler, scaler]

# ── Evaluators ─────────────────────────────────────────────────────────────
eval_roc = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
)
eval_pr = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR"
)

results = {}


def evaluate_and_save(cv_model, name, model_path):
    """Evaluate best CV model on test set, save it, return metrics."""
    best = cv_model.bestModel
    preds = best.transform(test_df)
    auroc = eval_roc.evaluate(preds)
    aupr  = eval_pr.evaluate(preds)
    best.write().overwrite().save(model_path)
    print(f"[{name}] AUROC={auroc:.4f}  AUPR={aupr:.4f}  saved→{model_path}")
    return best, auroc, aupr


# ── Model 1: Random Forest ─────────────────────────────────────────────────
print("\n--- Training Random Forest ---")
rf = RandomForestClassifier(labelCol="label", featuresCol="features", seed=42)
rf_grid = (
    ParamGridBuilder()
    .addGrid(rf.numTrees,            [50, 100, 200])
    .addGrid(rf.maxDepth,            [5, 10, 15])
    .addGrid(rf.minInstancesPerNode, [1, 5, 10])
    .build()
)
rf_cv = CrossValidator(
    estimator=Pipeline(stages=preprocessing + [rf]),
    estimatorParamMaps=rf_grid,
    evaluator=eval_roc,
    numFolds=3,
    seed=42,
    parallelism=4,
)
rf_best, rf_auroc, rf_aupr = evaluate_and_save(
    rf_cv.fit(train_df), "RandomForest",
    os.path.join(MODELS_DIR, "random_forest_model"),
)
rf_stage = rf_best.stages[-1]
results["RandomForest"] = {
    "AUROC": rf_auroc,
    "AUPR":  rf_aupr,
    "best_params": {
        "numTrees":            rf_stage.getNumTrees,
        "maxDepth":            rf_stage.getOrDefault(rf_stage.maxDepth),
        "minInstancesPerNode": rf_stage.getOrDefault(rf_stage.minInstancesPerNode),
    },
}

# Feature importance plot
fi = pd.DataFrame({"feature": FEATURE_COLS, "importance": rf_stage.featureImportances.toArray()})
fi = fi.sort_values("importance", ascending=False)
fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(fi["feature"][:15][::-1], fi["importance"][:15][::-1], color="steelblue")
ax.set_xlabel("Importance")
ax.set_title("Random Forest — Feature Importances")
plt.tight_layout()
fig.savefig(os.path.join(OUTPUT_DIR, "rf_feature_importance.png"), dpi=150)
plt.close(fig)

# ── Model 2: GBT ──────────────────────────────────────────────────────────
print("\n--- Training GBT ---")
gbt = GBTClassifier(labelCol="label", featuresCol="features", seed=42)
gbt_grid = (
    ParamGridBuilder()
    .addGrid(gbt.maxIter,  [10, 20, 30])
    .addGrid(gbt.maxDepth, [3, 5, 7])
    .addGrid(gbt.stepSize, [0.05, 0.10, 0.20])
    .build()
)
gbt_cv = CrossValidator(
    estimator=Pipeline(stages=preprocessing + [gbt]),
    estimatorParamMaps=gbt_grid,
    evaluator=eval_roc,
    numFolds=3,
    seed=42,
    parallelism=4,
)
gbt_best, gbt_auroc, gbt_aupr = evaluate_and_save(
    gbt_cv.fit(train_df), "GBT",
    os.path.join(MODELS_DIR, "gbt_model"),
)
gbt_stage = gbt_best.stages[-1]
results["GBT"] = {
    "AUROC": gbt_auroc,
    "AUPR":  gbt_aupr,
    "best_params": {
        "maxIter":  gbt_stage.getOrDefault(gbt_stage.maxIter),
        "maxDepth": gbt_stage.getOrDefault(gbt_stage.maxDepth),
        "stepSize": gbt_stage.getOrDefault(gbt_stage.stepSize),
    },
}

gbt_fi = pd.DataFrame({"feature": FEATURE_COLS, "importance": gbt_stage.featureImportances.toArray()})
gbt_fi = gbt_fi.sort_values("importance", ascending=False)
fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(gbt_fi["feature"][:15][::-1], gbt_fi["importance"][:15][::-1], color="darkorange")
ax.set_xlabel("Importance")
ax.set_title("GBT — Feature Importances")
plt.tight_layout()
fig.savefig(os.path.join(OUTPUT_DIR, "gbt_feature_importance.png"), dpi=150)
plt.close(fig)

# ── Model 3: Logistic Regression ───────────────────────────────────────────
print("\n--- Training Logistic Regression ---")
lr = LogisticRegression(labelCol="label", featuresCol="features")
lr_grid = (
    ParamGridBuilder()
    .addGrid(lr.regParam,        [0.001, 0.01, 0.1])
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
    .addGrid(lr.maxIter,         [50, 100, 200])
    .build()
)
lr_cv = CrossValidator(
    estimator=Pipeline(stages=preprocessing + [lr]),
    estimatorParamMaps=lr_grid,
    evaluator=eval_roc,
    numFolds=3,
    seed=42,
    parallelism=4,
)
lr_best, lr_auroc, lr_aupr = evaluate_and_save(
    lr_cv.fit(train_df), "LogisticRegression",
    os.path.join(MODELS_DIR, "logistic_regression_model"),
)
lr_stage = lr_best.stages[-1]
results["LogisticRegression"] = {
    "AUROC": lr_auroc,
    "AUPR":  lr_aupr,
    "best_params": {
        "regParam":        lr_stage.getOrDefault(lr_stage.regParam),
        "elasticNetParam": lr_stage.getOrDefault(lr_stage.elasticNetParam),
        "maxIter":         lr_stage.getOrDefault(lr_stage.maxIter),
    },
}

# ── Comparison ─────────────────────────────────────────────────────────────
comp = pd.DataFrame([
    {"Model": n, "AUROC": v["AUROC"], "AUPR": v["AUPR"]}
    for n, v in results.items()
]).sort_values("AUROC", ascending=False)

print("\n=== Model Comparison ===")
print(comp.to_string(index=False))

best_name = comp.iloc[0]["Model"]
print(f"\nBest model: {best_name}")

comp.to_csv(os.path.join(OUTPUT_DIR, "stage3_model_comparison.csv"), index=False)

colors = ["steelblue", "darkorange", "seagreen"]
fig, axes = plt.subplots(1, 2, figsize=(12, 5))
for i, metric in enumerate(["AUROC", "AUPR"]):
    axes[i].bar(comp["Model"], comp[metric], color=colors)
    axes[i].set_ylim(0.5, 1.0)
    axes[i].set_title(f"Area Under {'ROC' if metric=='AUROC' else 'PR'}")
    axes[i].tick_params(axis="x", rotation=15)
    for j, v in enumerate(comp[metric]):
        axes[i].text(j, v + 0.005, f"{v:.4f}", ha="center", fontsize=9)
plt.suptitle("NYC Taxi Tip Classification — Model Comparison", fontsize=13, fontweight="bold")
plt.tight_layout()
fig.savefig(os.path.join(OUTPUT_DIR, "model_comparison.png"), dpi=150)
plt.close(fig)

# ── Single-sample prediction ───────────────────────────────────────────────
best_pipeline = {"RandomForest": rf_best, "GBT": gbt_best, "LogisticRegression": lr_best}[best_name]

sample_data = [{
    "VendorID": 2.0, "passenger_count": 1.0, "trip_distance": 2.5, "RatecodeID": 1.0,
    "PULocationID": 161.0, "DOLocationID": 236.0, "fare_amount": 12.5, "extra": 1.0,
    "mta_tax": 0.5, "tolls_amount": 0.0, "congestion_surcharge": 2.5, "airport_fee": 0.0,
    "trip_duration_min": 10.0, "pickup_hour": 18.0, "pickup_dayofweek": 3.0,
    "pickup_month": 6.0, "is_weekend": 0.0, "avg_speed_mph": 15.0, "label": -1.0,
}]
sample_df = spark.createDataFrame(sample_data)
pred_row  = best_pipeline.transform(sample_df).select("prediction", "probability").collect()[0]
prob_gen  = float(pred_row["probability"][1])

print(f"\nSample prediction ({best_name}):")
print(f"  label={int(pred_row['prediction'])}  P(generous)={prob_gen:.4f}")

# ── Save JSON results ──────────────────────────────────────────────────────
out = {
    name: {
        "AUROC": float(v["AUROC"]),
        "AUPR":  float(v["AUPR"]),
        "best_params": {
            k: (int(p) if isinstance(p, int) else float(p) if isinstance(p, float) else str(p))
            for k, p in v["best_params"].items()
        },
    }
    for name, v in results.items()
}
out["best_model"] = best_name
out["sample_prediction"] = {"label": int(pred_row["prediction"]), "prob_generous_tip": prob_gen}

with open(os.path.join(OUTPUT_DIR, "stage3_results.json"), "w") as fh:
    json.dump(out, fh, indent=2)

print(f"\nAll outputs saved to {OUTPUT_DIR}/")
spark.stop()
