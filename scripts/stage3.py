"""
Stage 3 — Predictive Data Analytics (Spark ML regression).

Predict NYC Yellow Taxi trip tip_amount (credit-card trips). Three regressors tuned
via CrossValidator + ParamGridBuilder on train only; metrics on held-out test.

Stage II compatibility (sql/db.hql):
  - Hive DB: team35_projectdb (default; override via --hive-database).
  - Table: yellow_taxi_trips_part_buck (partitioned Avro); base yellow_taxi_trips is dropped.
  - Timestamps stored as BIGINT epoch milliseconds — coerced before feature engineering.

On large Hive tables (>~10M rows): keep StandardScaler withMean=false (default). withMean=true
forces dense vectors and often OOMs the Spark driver during CV unless RAM is huge.
Use STAGE3_DRIVER_MEM / executor memory and STAGE3_CV_PARALLELISM if the gateway is tight.
On congested Yarn hosts, Executor Process Lost + ``Killed`` almost always indicates container RAM:
pass STAGE3_YARN_STABLE=1 / STAGE3_EXEC_MEMORY_OVERHEAD (see scripts/stage3.sh).

On YARN, default fs is usually HDFS — writes to UNIX paths such as /home/user/... are resolved on
HDFS and fail unless you explicitly use file:// staging. CSV and model dirs are staged under
hdfs:///user/$USER/project/stage3_scratch (override STAGE3_HDFS_SCRATCH), then pulled with
``hdfs dfs -getmerge`` / ``hdfs dfs -get`` for model dirs (recursive; no ``-r`` on ``-get``).
Set ``STAGE3_LOCAL_WRITES_ONLY=1`` when the ``hdfs`` CLI is unavailable.

Outputs (aligned with BS/MS Stage III checklist):
  output/model1_predictions.csv — label,prediction
  output/model2_predictions.csv — label,prediction
  output/model3_predictions.csv — label,prediction
  output/model{1,2,3}_best_params.json — selected CV params saved as soon as each CV fit finishes
  output/evaluation.csv — model-level accuracy and residual metrics on test data
  output/model_feature_signals.csv — top linear coefficients / tree feature importances
"""

import argparse
import json
import math
import os

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import (
    Imputer,
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.ml.tuning import ParamGridBuilder

from stage3_helpers import (
    CATEGORICAL_FEATURES,
    build_cv_sample,
    coerce_epoch_ms_pickup_dropoff,
    compute_prediction_csv_metrics,
    evaluate_test,
    fit_cv_and_pick_best,
    hive_table_qualifier,
    load_hive_table,
    load_raw,
    load_saved_best_params,
    resolve_filesystem_data_dir,
    save_best_params,
    save_ml_pipeline_local,
    save_train_test_json,
    scaler_use_mean,
    temporal_train_test_split,
    top_model_signal_rows,
    unify_stage2_columns,
    write_single_partition_csv,
)


def parse_args():
    """Build CLI argument parser for Stage 3."""
    parser = argparse.ArgumentParser(
        description="Stage 3: tip_amount regression via Spark ML",
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="POSIX or HDFS path with TLC Parquet/CSV (ignored when --hive-table is set).",
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
        help="Spark master (e.g. local[*]); SparkSubmit may omit this on Yarn.",
    )
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=None,
        help="Optional 0<s<=1 uniform sample BEFORE filters for quick debugging",
    )
    parser.add_argument("--cv-folds", type=int, default=3)
    parser.add_argument(
        "--cv-sample-size",
        type=int,
        default=int(os.environ.get("STAGE3_CV_SAMPLE_SIZE", "4800")),
        help=(
            "Maximum representative training rows used only for CV grid search. "
            "The selected params are refit on the full train split. "
            "Env STAGE3_CV_SAMPLE_SIZE."
        ),
    )
    parser.add_argument(
        "--start-model",
        type=int,
        choices=(1, 2, 3),
        default=int(os.environ.get("STAGE3_START_MODEL", "1")),
        help=(
            "Resume from this model number. Earlier models are not retrained; "
            "their existing prediction CSVs under --output-dir are used for metrics."
        ),
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=int(os.environ.get("STAGE3_CV_PARALLELISM", "1")),
        help=(
            "CrossValidator concurrent folds (1 = lowest peak memory). "
            "Env STAGE3_CV_PARALLELISM."
        ),
    )
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument(
        "--scaler-with-mean",
        action="store_true",
        help=(
            "StandardScaler centers features → dense vectors (~8 bytes × n_rows × dim). "
            "Do not enable on tens of millions of rows unless driver has ample RAM "
            "(or set STAGE3_SCALER_WITH_MEAN=1)."
        ),
    )
    parser.add_argument(
        "--hive-database",
        default=os.environ.get("STAGE3_HIVE_DATABASE"),
        help=(
            "Stage II Hive DB (team35_projectdb in sql/db.hql). Used with --hive-table."
        ),
    )
    parser.add_argument(
        "--hive-table",
        default=os.environ.get("STAGE3_HIVE_TABLE"),
        help=(
            "If set, read Hive table (default after Stage II: yellow_taxi_trips_part_buck)."
        ),
    )
    return parser.parse_args()


def build_spark_session(args):
    """Create or retrieve a SparkSession with optional Hive support."""
    builder = SparkSession.builder.appName("yellow_taxi_stage3_tip_reg").config(
        "spark.sql.shuffle.partitions",
        os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "200"),
    ).config(
        "spark.sql.adaptive.enabled",
        os.environ.get("SPARK_SQL_ADAPTIVE_ENABLED", "true"),
    )
    if args.master:
        builder = builder.master(args.master)
    if args.hive_table:
        metastore = os.environ.get(
            "HIVE_METASTORE_URIS",
            "thrift://hadoop-02.uni.innopolis.ru:9883",
        )
        builder = builder.enableHiveSupport().config("hive.metastore.uris", metastore)
        warehouse = os.environ.get("SPARK_SQL_WAREHOUSE_DIR")
        if warehouse:
            builder = builder.config("spark.sql.warehouse.dir", warehouse)
    return builder.getOrCreate()


def engineer_features(raw_df):
    """Regression target tip_amount with trip-context and pickup-time features."""
    df0 = unify_stage2_columns(raw_df)
    df0 = coerce_epoch_ms_pickup_dropoff(df0)

    req = {"tpep_pickup_datetime", "tpep_dropoff_datetime", "payment_type"}
    if req - set(df0.columns):
        raise ValueError(f"Missing columns after coercion: {sorted(req - set(df0.columns))}")

    tau = F.lit(2.0 * math.pi)

    filtered_df = (
        df0.filter(F.col("payment_type") == F.lit(1))
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
        .withColumn("_m", F.month("tpep_pickup_datetime").cast(DoubleType()))
        .withColumn("_hh", F.hour("tpep_pickup_datetime").cast(DoubleType()))
        .withColumn("_dow0", (F.dayofweek("tpep_pickup_datetime") - F.lit(1)).cast(DoubleType()))
        .withColumn("pickup_month_sin", F.sin(tau * F.col("_m") / F.lit(12.0)))
        .withColumn("pickup_month_cos", F.cos(tau * F.col("_m") / F.lit(12.0)))
        .withColumn("pickup_hour_sin", F.sin(tau * F.col("_hh") / F.lit(24.0)))
        .withColumn("pickup_hour_cos", F.cos(tau * F.col("_hh") / F.lit(24.0)))
        .withColumn("pickup_dow_sin", F.sin(tau * F.col("_dow0") / F.lit(7.0)))
        .withColumn("pickup_dow_cos", F.cos(tau * F.col("_dow0") / F.lit(7.0)))
        .withColumn(
            "is_weekend",
            F.when(
                F.dayofweek("tpep_pickup_datetime").isin(1, 7),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "rush_hour",
            F.when(F.col("_hh").between(F.lit(7.0), F.lit(9.0)), F.lit(1.0))
            .when(F.col("_hh").between(F.lit(16.0), F.lit(19.0)), F.lit(1.0))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "night_trip",
            F.when((F.col("_hh") >= F.lit(22.0)) | (F.col("_hh") <= F.lit(5.0)), F.lit(1.0))
            .otherwise(F.lit(0.0)),
        )
        .withColumn("__split_ts", F.unix_timestamp("tpep_pickup_datetime").cast(DoubleType()))
        .drop("_m", "_hh", "_dow0")
    )

    if "RatecodeID" in filtered_df.columns:
        filtered_df = filtered_df.withColumn(
            "airport_rate",
            F.when(F.col("RatecodeID").isin(2, 3), F.lit(1.0)).otherwise(F.lit(0.0)),
        )
    pre_tip_cols = [
        c
        for c in (
            "fare_amount",
            "extra",
            "mta_tax",
            "tolls_amount",
            "congestion_surcharge",
            "airport_fee",
            "improvement_surcharge",
        )
        if c in filtered_df.columns
    ]
    if pre_tip_cols:
        total_expr = sum((F.coalesce(F.col(c), F.lit(0.0)) for c in pre_tip_cols), F.lit(0.0))
        filtered_df = filtered_df.withColumn("pre_tip_amount", total_expr)

    cand = [
        "VendorID",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
        "pickup_month_sin",
        "pickup_month_cos",
        "pickup_hour_sin",
        "pickup_hour_cos",
        "pickup_dow_sin",
        "pickup_dow_cos",
        "is_weekend",
        "rush_hour",
        "night_trip",
        "airport_rate",
        "pre_tip_amount",
    ]
    use = [c for c in cand if c in filtered_df.columns]
    return filtered_df.select(*(use + ["__split_ts", "tip_amount"]))


def make_preprocessing_stages(
    features,
    categorical_features,
    with_mean_center: bool,
    scale_features: bool,
):
    """Build Imputer → StringIndexer → OHE → VectorAssembler → Scaler stages."""
    categorical = [c for c in categorical_features if c in features]
    numeric = [c for c in features if c not in categorical]
    stages = []
    assembler_inputs = []
    if numeric:
        imp_out = [c + "__imp" for c in numeric]
        stages.append(Imputer(inputCols=numeric, outputCols=imp_out, strategy="median"))
        assembler_inputs.extend(imp_out)
    if categorical:
        idx_out = [c + "__idx" for c in categorical]
        oh_out = [c + "__oh" for c in categorical]
        stages.append(
            StringIndexer(inputCols=categorical, outputCols=idx_out, handleInvalid="keep"),
        )
        stages.append(OneHotEncoder(inputCols=idx_out, outputCols=oh_out, dropLast=False))
        assembler_inputs.extend(oh_out)
    assembler = VectorAssembler(
        inputCols=assembler_inputs,
        outputCol="raw_features" if scale_features else "features",
        handleInvalid="skip",
    )
    stages.append(assembler)
    if scale_features:
        stages.append(
            StandardScaler(
                inputCol="raw_features",
                outputCol="features",
                withMean=with_mean_center,
                withStd=True,
            ),
        )
    return stages


def _load_source_data(args, spark):
    """Load raw data from Hive or filesystem, with optional sampling."""
    if args.hive_table:
        raw = load_hive_table(spark, args.hive_database, args.hive_table)
        data_note = hive_table_qualifier(args.hive_database, args.hive_table)
    else:
        data_dir_resolved = resolve_filesystem_data_dir(args.data_dir)
        raw = load_raw(spark, data_dir_resolved)
        data_note = data_dir_resolved
    if args.sample_fraction is not None:
        sample_frac = args.sample_fraction
        if not 0 < sample_frac <= 1:
            raise ValueError("--sample-fraction must be in (0, 1]")
        raw = raw.sample(withReplacement=False, fraction=sample_frac, seed=args.random_seed)
    return raw, data_note


def _validate_splits(train_n, test_n, cv_sample_n, cv_folds):
    """Raise RuntimeError if any data split is empty or too small for CV."""
    total_rows = train_n + test_n
    if total_rows == 0:
        raise RuntimeError("Empty dataframe after preprocessing (check filters/input).")
    if train_n == 0 or test_n == 0:
        raise RuntimeError(
            "Temporal train/test split produced an empty side; check pickup timestamps.",
        )
    if cv_sample_n == 0:
        raise RuntimeError("CV sample is empty; increase --cv-sample-size.")
    if cv_sample_n < cv_folds:
        raise RuntimeError(
            f"CV sample has {cv_sample_n} rows, fewer than --cv-folds={cv_folds}.",
        )


def _train_single_model(spec, ctx):
    """Train or load a single model, returning (best_pipeline, params, metrics).

    *spec*: dict with key, name, pipeline, grid, suffix.
    *ctx*: dict with args, train_df, cv_train_df, cv_evaluator, test_df,
           output_root, models_root.
    """
    key, name = spec["key"], spec["name"]
    args, output_root = ctx["args"], ctx["output_root"]
    prediction_csv = os.path.join(output_root, f"{key}_predictions.csv")
    model_num = int(key[-1])

    if args.start_model <= model_num:
        best = fit_cv_and_pick_best(
            ctx["train_df"], ctx["cv_train_df"], spec["pipeline"], spec["grid"],
            ctx["cv_evaluator"],
            numFolds=args.cv_folds, parallelism=args.parallelism, seed=args.random_seed,
        )
        params = save_best_params(output_root, key, name, best)
        preds, metrics = evaluate_test(best, ctx["test_df"])
        write_single_partition_csv(preds, prediction_csv)
        save_ml_pipeline_local(best, ctx["models_root"], spec["suffix"])
        return best, params, metrics

    print(f"[stage3] skipping {key}; using existing predictions {prediction_csv}")
    params = load_saved_best_params(output_root, key)
    metrics = compute_prediction_csv_metrics(prediction_csv)
    return None, params, metrics


def _metrics_row(model_name, metrics):
    """Build a single evaluation row tuple from a metrics dict."""
    keys = [
        "rmse", "mae", "r2", "explained_variance", "mean_error",
        "median_abs_error", "p90_abs_error", "p95_abs_error",
        "within_1_dollar", "within_2_dollars",
    ]
    return tuple([model_name] + [metrics[k] for k in keys])


_EVAL_COLUMNS = [
    "model", "RMSE", "MAE", "R2", "ExplainedVariance", "MeanError",
    "MedianAbsError", "P90AbsError", "P95AbsError", "Within1Dollar", "Within2Dollars",
]


def _write_evaluation(spark, model_results, output_root):
    """Write evaluation.csv from model results list."""
    rows = tuple(_metrics_row(r["name"], r["metrics"]) for r in model_results)
    comp = spark.createDataFrame(rows, _EVAL_COLUMNS)
    write_single_partition_csv(comp, os.path.join(output_root, "evaluation.csv"))


def _write_feature_signals(spark, model_results, test_df, output_root):
    """Write model_feature_signals.csv from model results list."""
    signal_rows = []
    for result in model_results:
        if result["best"] is not None:
            signal_rows += top_model_signal_rows(result["name"], result["best"], test_df)
    if signal_rows:
        signal_df = spark.createDataFrame(
            signal_rows,
            ["model", "rank", "feature", "signal_type", "signed_value", "absolute_value"],
        )
        write_single_partition_csv(
            signal_df, os.path.join(output_root, "model_feature_signals.csv"),
        )


def _write_summary(model_results, ctx):
    """Write stage3_training_summary.json.

    *ctx*: dict with output_root, models_root, data_note, args,
           train_n, test_n, cv_sample_n, split_cutoff, scale_center.
    """
    output_root = ctx["output_root"]
    summary = {
        "task": "regression_tip_amount_credit_card_trips_only",
        "data_source": str(ctx["data_note"]),
        "train_rows": int(ctx["train_n"]),
        "test_rows": int(ctx["test_n"]),
        "cv_sample_rows": int(ctx["cv_sample_n"]),
        "cv_sample_max_rows": int(ctx["args"].cv_sample_size),
        "split_strategy": "chronological_70_30_by_pickup_time",
        "split_cutoff_unix_seconds": float(ctx["split_cutoff"]),
        "scaler_with_mean": bool(ctx["scale_center"]),
        "cross_validation_rmse_focus": True,
        "prediction_postprocess": "negative predictions clipped to 0.0 for metrics and CSV exports",
        "feature_signal_csv": os.path.join(output_root, "model_feature_signals.csv"),
    }
    for result in model_results:
        summary[result["key"]] = {
            "name": result["name"],
            "test_metrics": result["metrics"],
            "best_params_from_cv": result["params"],
            "prediction_csv": os.path.join(output_root, f"{result['key']}_predictions.csv"),
            "persisted_pipeline": os.path.join(ctx["models_root"], result["suffix"]),
        }
    summ_path = os.path.join(output_root, "stage3_training_summary.json")
    with open(summ_path, "w", encoding="utf-8") as file_handle:
        json.dump(summary, file_handle, indent=2, default=str)
    print(json.dumps(summary, indent=2, default=str))


def _prepare_data(args, spark):
    """Load, engineer features, split, and build CV sample."""
    raw, data_note = _load_source_data(args, spark)
    ml_ready = engineer_features(raw).withColumnRenamed("tip_amount", "label")
    feats = [c for c in ml_ready.columns if c not in ("label", "__split_ts")]
    ml_ready = ml_ready.select(*feats, "__split_ts", "label")

    storage_level = StorageLevel.MEMORY_AND_DISK
    ml_ready = ml_ready.persist(storage_level)

    train_df, test_df, split_cutoff = temporal_train_test_split(ml_ready)
    train_df = train_df.persist(storage_level)
    test_df = test_df.persist(storage_level)

    train_n = train_df.count()
    test_n = test_df.count()

    cv_train_df, cv_sample_n = build_cv_sample(
        train_df, train_n, args.cv_sample_size, args.random_seed,
    )
    cv_train_df = cv_train_df.persist(storage_level)
    cv_sample_n = cv_train_df.count() if cv_sample_n is None else cv_sample_n
    _validate_splits(train_n, test_n, cv_sample_n, args.cv_folds)

    ml_ready.unpersist(blocking=True)

    save_train_test_json(train_df, test_df)

    return {
        "feats": feats, "data_note": data_note, "split_cutoff": split_cutoff,
        "train_df": train_df, "test_df": test_df, "cv_train_df": cv_train_df,
        "train_n": train_n, "test_n": test_n, "cv_sample_n": cv_sample_n,
    }


def _build_model_specs(feats, scale_center, seed):
    """Define the three model pipelines and their CV grids."""
    linear_reg = LinearRegression(
        labelCol="label", featuresCol="features",
        maxIter=200, elasticNetParam=0.5, regParam=0.01,
    )
    lr_grid = (
        ParamGridBuilder()
        .addGrid(linear_reg.regParam, [1e-4, 1e-3, 1e-2])
        .addGrid(linear_reg.elasticNetParam, [0.0, 0.5, 1.0])
        .addGrid(linear_reg.tol, [1e-4, 1e-5, 1e-6])
        .build()
    )
    random_forest = RandomForestRegressor(
        labelCol="label", featuresCol="features", seed=seed,
    )
    rf_grid = (
        ParamGridBuilder()
        .addGrid(random_forest.numTrees, [20, 50, 100])
        .addGrid(random_forest.maxDepth, [6, 8, 10])
        .addGrid(random_forest.minInstancesPerNode, [1, 5, 10])
        .build()
    )
    gbt = GBTRegressor(
        labelCol="label", featuresCol="features", seed=seed,
        maxIter=100, maxDepth=7, stepSize=0.05, minInstancesPerNode=10,
    )
    gbt_grid = (
        ParamGridBuilder()
        .addGrid(gbt.maxDepth, [4, 6, 8])
        .addGrid(gbt.stepSize, [0.03, 0.05, 0.08])
        .addGrid(gbt.minInstancesPerNode, [5, 10, 20])
        .build()
    )
    pre = make_preprocessing_stages
    return [
        {"key": "model1", "name": "LinearRegression", "suffix": "model1_lr",
         "pipeline": Pipeline(stages=pre(feats, CATEGORICAL_FEATURES, scale_center, True) + [linear_reg]),
         "grid": lr_grid},
        {"key": "model2", "name": "RandomForestRegressor", "suffix": "model2_rf",
         "pipeline": Pipeline(stages=pre(feats, CATEGORICAL_FEATURES, scale_center, False) + [random_forest]),
         "grid": rf_grid},
        {"key": "model3", "name": "GBTRegressor", "suffix": "model3_gbt",
         "pipeline": Pipeline(
             stages=pre(feats, CATEGORICAL_FEATURES, scale_center, False) + [gbt]),
         "grid": gbt_grid},
    ]


def main():
    """Run the full Stage 3 ML pipeline."""
    args = parse_args()
    models_root = os.path.abspath(args.models_dir)
    output_root = os.path.abspath(args.output_dir)
    os.makedirs(models_root, exist_ok=True)
    os.makedirs(output_root, exist_ok=True)

    spark = build_spark_session(args)
    spark.sparkContext.setLogLevel("WARN")

    ctx = _prepare_data(args, spark)
    scale_center = scaler_use_mean(args.scaler_with_mean)
    total_rows = ctx["train_n"] + ctx["test_n"]
    print(
        f"[stage3] source={ctx['data_note']} rows={total_rows:,} train={ctx['train_n']:,} "
        f"test={ctx['test_n']:,} cv_sample={ctx['cv_sample_n']:,} "
        f"features={len(ctx['feats'])} scaler_with_mean={scale_center} "
        f"cv_parallelism={args.parallelism} split_cutoff={ctx['split_cutoff']}",
    )

    cv_evaluator = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="rmse",
    )

    specs = _build_model_specs(ctx["feats"], scale_center, args.random_seed)
    train_ctx = {
        "args": args, "train_df": ctx["train_df"],
        "cv_train_df": ctx["cv_train_df"],
        "cv_evaluator": cv_evaluator, "test_df": ctx["test_df"],
        "output_root": output_root, "models_root": models_root,
    }
    results = []
    for spec in specs:
        best, params, metrics = _train_single_model(spec, train_ctx)
        results.append({
            "key": spec["key"], "name": spec["name"], "suffix": spec["suffix"],
            "best": best, "params": params, "metrics": metrics,
        })

    _write_evaluation(spark, results, output_root)
    _write_feature_signals(spark, results, ctx["test_df"], output_root)
    _write_summary(results, {
        "output_root": output_root, "models_root": models_root,
        "data_note": ctx["data_note"], "args": args,
        "train_n": ctx["train_n"], "test_n": ctx["test_n"],
        "cv_sample_n": ctx["cv_sample_n"], "split_cutoff": ctx["split_cutoff"],
        "scale_center": scale_center,
    })
    spark.stop()


if __name__ == "__main__":
    main()
