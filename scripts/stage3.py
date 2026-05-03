"""
Stage 3 — Predictive Data Analytics (Spark ML regression).

Predict NYC Yellow Taxi trip tip_amount (credit-card trips). Two regressors tuned
via CrossValidator + ParamGridBuilder on train only; metrics on held-out test.

Outputs (aligned with BS/MS Stage III checklist):
  output/model1_predictions.csv — label,prediction
  output/model2_predictions.csv — label,prediction
  output/evaluation.csv — model, RMSE, R2 on test data
"""
# Whole-stage script: keep sequential ML steps explicit for reproducibility reports.
# pylint: disable=too-many-locals,too-many-statements,too-many-arguments,too-many-positional-arguments,missing-function-docstring

import argparse
import glob
import json
import math
import os
import shutil

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import Imputer, StandardScaler, VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


def parse_args():
    parser = argparse.ArgumentParser(
        description="Stage 3: tip_amount regression via Spark ML",
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Directory containing trip parquet/csv files",
    )
    parser.add_argument(
        "--models-dir",
        default="models",
        help="Persisted Spark ML model roots",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Prediction CSV + evaluation artefacts",
    )
    parser.add_argument(
        "--master",
        default=os.environ.get("SPARK_MASTER"),
        help="Spark master (e.g. local[*]); SPARK_MASTER env if unset arg",
    )
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=None,
        help="Optional 0<s<=1 uniform sample BEFORE filters for quick debugging",
    )
    parser.add_argument("--cv-folds", type=int, default=3)
    parser.add_argument("--parallelism", type=int, default=2)
    parser.add_argument("--random-seed", type=int, default=42)
    return parser.parse_args()


def _remote_data_uri(data_dir: str) -> bool:
    """True for hdfs:, s3a:, wasb:, etc. Python glob cannot enumerate these."""
    if "://" not in data_dir:
        return False
    lowered = data_dir.lower()
    return not lowered.startswith("file:")


def discover_parquet(data_dir: str):
    shallow = sorted(glob.glob(os.path.join(data_dir, "yellow_tripdata_*.parquet")))
    nested = sorted(
        glob.glob(os.path.join(data_dir, "**", "yellow_tripdata_*.parquet"), recursive=True),
    )
    paths = sorted({*shallow, *nested})
    if paths:
        return paths

    fuzzy = sorted(
        glob.glob(os.path.join(data_dir, "**", "yellow*trip*.parquet"), recursive=True),
    )
    return fuzzy


def load_raw(spark, data_dir: str):
    data_dir = data_dir.rstrip("/")

    # Single directory/glob URI on HDFS/ObjectStore — bypass os glob (POSIX-only).
    if _remote_data_uri(data_dir):
        return spark.read.option("mergeSchema", "true").parquet(data_dir)

    parquet_paths = discover_parquet(data_dir)
    if parquet_paths:
        # PySpark expects path(s) as *args, not one list; passing a list triggers
        # ClassCastException (ArrayList vs String) inside HadoopConf setup on some Spark builds.
        return spark.read.option("mergeSchema", "true").parquet(*parquet_paths)

    csv_patterns = (
        os.path.join(data_dir, "yellow*.csv"),
        os.path.join(data_dir, "**", "*.csv"),
    )
    csv_paths = sorted({p for pat in csv_patterns for p in glob.glob(pat, recursive=True)})
    if csv_paths:
        acc = spark.read.option("header", True).option("inferSchema", True).csv(csv_paths[0])
        for extra in csv_paths[1:]:
            nxt = spark.read.option("header", True).option("inferSchema", True).csv(extra)
            acc = acc.unionByName(nxt, allowMissingColumns=True)
        return acc

    display_dir = (
        data_dir if _remote_data_uri(data_dir) else os.path.abspath(data_dir)
    )
    raise FileNotFoundError(
        f"No parquet/CSV trip files under {display_dir!r}. "
        "Place TLC Yellow files here, run scripts/make_stage3_sample_parquet.py "
        "(local), or use an HDFS/YARN path.",
    )


def engineer_features(raw_df):
    """Regression target tip_amount with cyclical time encodings."""
    req = {"tpep_pickup_datetime", "tpep_dropoff_datetime", "payment_type"}
    if req - set(raw_df.columns):
        raise ValueError(f"Missing columns: {sorted(req - set(raw_df.columns))}")

    tau = F.lit(2.0 * math.pi)

    df = (
        raw_df.filter(F.col("payment_type") == F.lit(1))
        .filter(F.col("fare_amount") > F.lit(0))
        .filter(F.col("trip_distance") > F.lit(0))
        .filter(F.col("tip_amount") >= F.lit(0))
        .filter(F.col("fare_amount") < F.lit(500))
        .filter(F.col("trip_distance") < F.lit(200))
        .filter(F.col("passenger_count").between(F.lit(1), F.lit(6)))
        .withColumn(
            "trip_duration_min",
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime"))
            / F.lit(60.0),
        )
        .filter(F.col("trip_duration_min").between(F.lit(1), F.lit(180)))
        .withColumn("pickup_year", F.year("tpep_pickup_datetime").cast(DoubleType()))
        .withColumn("_m", F.month("tpep_pickup_datetime").cast(DoubleType()))
        .withColumn("_hh", F.hour("tpep_pickup_datetime").cast(DoubleType()))
        .withColumn("_dow0", (F.dayofweek("tpep_pickup_datetime") - F.lit(1)).cast(DoubleType()))
        .withColumn("pickup_month_sin", F.sin(tau * F.col("_m") / F.lit(12.0)))
        .withColumn("pickup_month_cos", F.cos(tau * F.col("_m") / F.lit(12.0)))
        .withColumn("pickup_hour_sin", F.sin(tau * F.col("_hh") / F.lit(24.0)))
        .withColumn("pickup_hour_cos", F.cos(tau * F.col("_hh") / F.lit(24.0)))
        .withColumn(
            "pickup_dow_sin",
            F.sin(tau * F.col("_dow0") / F.lit(7.0)),
        )
        .withColumn(
            "pickup_dow_cos",
            F.cos(tau * F.col("_dow0") / F.lit(7.0)),
        )
        .withColumn(
            "is_weekend",
            F.when(
                F.dayofweek("tpep_pickup_datetime").isin(1, 7),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "avg_speed_mph",
            F.col("trip_distance") / (F.col("trip_duration_min") / F.lit(60.0)),
        )
        .drop("_m", "_hh", "_dow0")
    )

    cand = [
        "VendorID",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
        "extra",
        "mta_tax",
        "tolls_amount",
        "congestion_surcharge",
        "airport_fee",
        "improvement_surcharge",
        "pickup_year",
        "trip_duration_min",
        "pickup_month_sin",
        "pickup_month_cos",
        "pickup_hour_sin",
        "pickup_hour_cos",
        "pickup_dow_sin",
        "pickup_dow_cos",
        "is_weekend",
        "avg_speed_mph",
    ]
    use = [c for c in cand if c in df.columns]
    return df.select(*(use + ["tip_amount"]))


def write_single_partition_csv(frame, outfile: str):
    out_dir = outfile + "__tmp_parts"
    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir)
    frame.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_dir)
    parts = sorted(glob.glob(os.path.join(out_dir, "part-*.csv")))
    if not parts:
        shutil.rmtree(out_dir, ignore_errors=True)
        raise RuntimeError(f"No CSV part files emitted under {out_dir!r}")
    parent = os.path.dirname(os.path.abspath(outfile))
    if parent:
        os.makedirs(parent, exist_ok=True)
    if os.path.isfile(outfile):
        os.remove(outfile)
    shutil.move(parts[0], outfile)
    shutil.rmtree(out_dir, ignore_errors=True)


def summarize_params(model_stage):
    m = {}
    for k, v in model_stage.extractParamMap().items():
        name = str(k.name)
        if hasattr(v, "item"):
            v = v.item()
        elif isinstance(v, (list, tuple)) and hasattr(v[0], "item"):
            v = type(v)(x.item() if hasattr(x, "item") else x for x in v)
        m[name] = v
    return m


def fit_cv_and_pick_best(train, pipeline, grid, evaluator, folds, parallelism, seed):
    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=grid,
        evaluator=evaluator,
        numFolds=folds,
        parallelism=parallelism,
        seed=seed,
    )
    return cv.fit(train)


def evaluate_test(best_pipeline, test_df):
    pred = best_pipeline.transform(test_df)
    rmse_ev = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="rmse",
    )
    r2_ev = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="r2",
    )
    return pred.select("label", "prediction"), float(rmse_ev.evaluate(pred)), float(
        r2_ev.evaluate(pred),
    )


def make_preprocessing_stages(features):
    imp_in = features[:]
    imp_out = [c + "__imp" for c in imp_in]
    imputer = Imputer(inputCols=imp_in, outputCols=imp_out, strategy="median")
    assembler = VectorAssembler(
        inputCols=imp_out,
        outputCol="raw_features",
        handleInvalid="skip",
    )
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withMean=True,
        withStd=True,
    )
    return imputer, assembler, scaler


def main():
    args = parse_args()
    data_dir = os.path.abspath(args.data_dir)
    models_root = os.path.abspath(args.models_dir)
    output_root = os.path.abspath(args.output_dir)
    os.makedirs(models_root, exist_ok=True)
    os.makedirs(output_root, exist_ok=True)

    spark_builder = SparkSession.builder.appName("yellow_taxi_stage3_tip_reg").config(
        "spark.sql.shuffle.partitions",
        os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "200"),
    ).config(
        "spark.sql.adaptive.enabled",
        os.environ.get("SPARK_SQL_ADAPTIVE_ENABLED", "true"),
    )
    if args.master:
        spark_builder = spark_builder.master(args.master)
    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = load_raw(spark, data_dir)
    if args.sample_fraction is not None:
        s = args.sample_fraction
        if not 0 < s <= 1:
            raise ValueError("--sample-fraction must be in (0, 1]")
        raw = raw.sample(withReplacement=False, fraction=s, seed=args.random_seed)

    ml_ready = engineer_features(raw).withColumnRenamed("tip_amount", "label")
    feats = [c for c in ml_ready.columns if c != "label"]
    ml_ready = ml_ready.select(*feats, "label")

    # Avoid MEMORY_ONLY .cache() stacking on the driver (OOM / process "Killed"):
    # spill to disk when heap is tight; drop the pre-split frame once train/test exist.
    md = StorageLevel.MEMORY_AND_DISK
    ml_ready = ml_ready.persist(md)

    train_df, test_df = ml_ready.randomSplit([0.7, 0.3], seed=args.random_seed)
    train_df = train_df.persist(md)
    test_df = test_df.persist(md)

    train_n = train_df.count()
    test_n = test_df.count()
    n = train_n + test_n
    if n == 0:
        raise RuntimeError("Empty dataframe after preprocessing (check filters/input).")

    ml_ready.unpersist(blocking=True)
    print(f"[stage3] rows={n:,} train={train_n:,} test={test_n:,} features={len(feats)}")

    cv_evaluator = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="rmse",
    )

    # Model 1 — LinearRegression (optimize regParam & elasticNetParam; tuning maxIter discouraged)
    lr = LinearRegression(
        labelCol="label",
        featuresCol="features",
        maxIter=200,
        elasticNetParam=0.5,
        regParam=0.01,
    )
    lr_grid = (
        ParamGridBuilder()
        .addGrid(lr.regParam, [1e-3, 1e-2, 1e-1])
        .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
        .build()
    )
    imputer_lr, assembler_lr, scaler_lr = make_preprocessing_stages(feats)
    lr_pipe = Pipeline(stages=[imputer_lr, assembler_lr, scaler_lr, lr])

    lr_cv_model = fit_cv_and_pick_best(
        train_df,
        lr_pipe,
        lr_grid,
        cv_evaluator,
        folds=args.cv_folds,
        parallelism=args.parallelism,
        seed=args.random_seed,
    )
    lr_best = lr_cv_model.bestModel
    preds1, lr_rmse, lr_r2 = evaluate_test(lr_best, test_df)

    write_single_partition_csv(preds1, os.path.join(output_root, "model1_predictions.csv"))
    lr_best.write().overwrite().save(os.path.join(models_root, "model1_lr"))

    rf = RandomForestRegressor(
        labelCol="label",
        featuresCol="features",
        seed=args.random_seed,
        numTrees=100,
        maxDepth=10,
        minInstancesPerNode=1,
    )
    rf_grid = (
        ParamGridBuilder()
        .addGrid(rf.numTrees, [80, 120])
        .addGrid(rf.maxDepth, [8, 12])
        .addGrid(rf.minInstancesPerNode, [1, 10])
        .build()
    )
    imputer_rf, assembler_rf, scaler_rf = make_preprocessing_stages(feats)
    rf_pipe = Pipeline(stages=[imputer_rf, assembler_rf, scaler_rf, rf])

    rf_cv_model = fit_cv_and_pick_best(
        train_df,
        rf_pipe,
        rf_grid,
        cv_evaluator,
        folds=args.cv_folds,
        parallelism=args.parallelism,
        seed=args.random_seed,
    )
    rf_best = rf_cv_model.bestModel
    preds2, rf_rmse, rf_r2 = evaluate_test(rf_best, test_df)

    write_single_partition_csv(preds2, os.path.join(output_root, "model2_predictions.csv"))
    rf_best.write().overwrite().save(os.path.join(models_root, "model2_rf"))

    rows = (
        ("LinearRegression", lr_rmse, lr_r2),
        ("RandomForestRegressor", rf_rmse, rf_r2),
    )
    comp = spark.createDataFrame(rows, ["model", "RMSE", "R2"])
    write_single_partition_csv(comp, os.path.join(output_root, "evaluation.csv"))

    lr_stage = lr_best.stages[-1]
    rf_stage = rf_best.stages[-1]

    summary = {
        "task": "regression_tip_amount_credit_card_trips_only",
        "train_rows": int(train_n),
        "test_rows": int(test_n),
        "cross_validation_rmse_focus": True,
        "model1": {
            "name": "LinearRegression",
            "test_rmse": lr_rmse,
            "test_r2": lr_r2,
            "best_params_from_cv": summarize_params(lr_stage),
            "prediction_csv": os.path.join(output_root, "model1_predictions.csv"),
            "persisted_pipeline": os.path.join(models_root, "model1_lr"),
        },
        "model2": {
            "name": "RandomForestRegressor",
            "test_rmse": rf_rmse,
            "test_r2": rf_r2,
            "best_params_from_cv": summarize_params(rf_stage),
            "prediction_csv": os.path.join(output_root, "model2_predictions.csv"),
            "persisted_pipeline": os.path.join(models_root, "model2_rf"),
        },
    }

    summ_path = os.path.join(output_root, "stage3_training_summary.json")
    with open(summ_path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, default=str)

    print(json.dumps(summary, indent=2, default=str))
    spark.stop()


if __name__ == "__main__":
    main()
