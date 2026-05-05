"""
Stage 3 — Predictive Data Analytics (Spark ML regression).

Predict NYC Yellow Taxi trip tip_amount (credit-card trips). Two regressors tuned
via CrossValidator + ParamGridBuilder on train only; metrics on held-out test.

Stage II compatibility (sql/db.hql):
  - Hive DB: team35_projectdb (default; override via --hive-database).
  - Table: yellow_taxi_trips_part_buck (partitioned Avro); base yellow_taxi_trips is dropped.
  - Timestamps stored as BIGINT epoch milliseconds — coerced before feature engineering.

On large Hive tables (>~10M rows): keep StandardScaler withMean=false (default). withMean=true
forces dense vectors and often OOMs the Spark driver during CV unless RAM is huge.
Use STAGE3_DRIVER_MEM / executor memory and STAGE3_CV_PARALLELISM if the gateway is tight.
On congested Yarn hosts, Executor Process Lost + ``Killed`` almost always indicates container RAM:
pass STAGE3_YARN_STABLE=1 / STAGE3_EXEC_MEMORY_OVERHEAD (see scripts/stage3.sh) and optional
``STAGE3_GBT_LITE=1`` for a smaller Gradient-Boosted Tree CV grid without changing LR.

On YARN, default fs is usually HDFS — writes to UNIX paths such as /home/user/... are resolved on
HDFS and fail unless you explicitly use file:// staging. CSV and model dirs are staged under
hdfs:///user/$USER/project/stage3_scratch (override STAGE3_HDFS_SCRATCH), then pulled with
``hdfs dfs -getmerge`` / ``hdfs dfs -get`` for model dirs (recursive; no ``-r`` on ``-get``).
Set ``STAGE3_LOCAL_WRITES_ONLY=1`` when the ``hdfs`` CLI is unavailable.

Outputs (aligned with BS/MS Stage III checklist):
  output/model1_predictions.csv — label,prediction
  output/model2_predictions.csv — label,prediction
  output/evaluation.csv — model-level accuracy and residual metrics on test data
  output/model_feature_signals.csv — top linear coefficients / tree feature importances
"""
# Whole-stage script: keep sequential ML steps explicit for reproducibility reports.
# pylint: disable=too-many-locals,too-many-statements,too-many-arguments,too-many-positional-arguments,missing-function-docstring

import argparse
import glob
import json
import math
import os
import shutil
import subprocess
import uuid

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import Imputer, OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Stage II hive column aliases (snake/lowercase → TLC-style names engineer_features expects)
STAGE2_TO_TLC_COLUMNS = (
    ("vendorid", "VendorID"),
    ("ratecodeid", "RatecodeID"),
    ("pulocationid", "PULocationID"),
    ("dolocationid", "DOLocationID"),
)

CATEGORICAL_FEATURES = ("VendorID", "RatecodeID", "PULocationID", "DOLocationID")


def parse_args():
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


def _remote_data_uri(path: str) -> bool:
    trimmed = path.strip().lower()
    if trimmed.startswith("file:"):
        return False
    if "://" in path:
        return True
    return any(trimmed.startswith(s) for s in ("hdfs:", "s3:", "s3a:", "wasb:", "abfs:", "viewfs:"))


def _normalize_hdfs_uri_for_spark(uri: str) -> str:
    if not uri.startswith("hdfs:"):
        return uri
    tail = uri[len("hdfs:") :]
    if tail.startswith("/") and not tail.startswith("//"):
        return "hdfs://" + tail
    return uri


def resolve_filesystem_data_dir(arg: str) -> str:
    p = arg.rstrip("/")
    return p if _remote_data_uri(p) else os.path.abspath(p)


def hive_table_qualifier(database, table):
    if not table:
        return None
    db = database
    tbl = table
    if db:
        if "." in tbl:
            raise ValueError(
                "Use (--hive-database + short table) or qualified --hive-table, not both.",
            )
        return f"{db}.{tbl.strip()}"
    if "." in tbl:
        parts = tbl.split(".", 1)
        return f"{parts[0].strip()}.{parts[1].strip()}"
    return tbl.strip()


def build_spark_session(args):
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

    if _remote_data_uri(data_dir):
        hdfs_uri = _normalize_hdfs_uri_for_spark(data_dir)
        return spark.read.option("mergeSchema", "true").parquet(hdfs_uri)

    parquet_paths = discover_parquet(data_dir)
    if parquet_paths:
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

    display = data_dir if _remote_data_uri(data_dir) else os.path.abspath(data_dir)
    raise FileNotFoundError(
        f"No parquet/CSV under {display!r}. For Stage II Hive use "
        "--hive-database + --hive-table yellow_taxi_trips_part_buck.",
    )


def unify_stage2_columns(raw_df):
    """Rename Stage II lowercase Avro cols to TLC-style names."""
    lowers = {c.lower(): c for c in raw_df.columns}
    out = raw_df
    for stage2_low, tlc_name in STAGE2_TO_TLC_COLUMNS:
        if tlc_name in out.columns:
            continue
        if stage2_low in lowers and lowers[stage2_low] != tlc_name:
            out = out.withColumnRenamed(lowers[stage2_low], tlc_name)
    return out


def coerce_epoch_ms_pickup_dropoff(raw_df):
    """sql/db.hql stores pickup/dropoff as BIGINT millis; parquet TLC uses TIMESTAMP."""

    def needs_ms_to_ts(typ: str) -> bool:
        t = typ.lower().split("(")[0].strip()
        if t == "timestamp":
            return False
        return t in ("bigint", "long", "int", "smallint", "short", "double", "float", "decimal")

    dtypes = dict(raw_df.dtypes)
    out = raw_df
    lowmap = {c.lower(): c for c in out.columns}

    def pick(alias):
        for key in alias:
            if key in lowmap:
                return lowmap[key]
        return None

    pu_col = pick(("tpep_pickup_datetime",))
    do_col = pick(("tpep_dropoff_datetime",))
    if not pu_col or not do_col:
        raise ValueError("Expected tpep_pickup_datetime and tpep_dropoff_datetime columns.")

    pu_type = dtypes.get(pu_col, "")
    if needs_ms_to_ts(pu_type):
        out = out.withColumn(
            pu_col,
            F.to_timestamp(F.col(pu_col).cast("double") / F.lit(1000.0)),
        )

    do_type = dtypes.get(do_col, "")
    if needs_ms_to_ts(do_type):
        out = out.withColumn(
            do_col,
            F.to_timestamp(F.col(do_col).cast("double") / F.lit(1000.0)),
        )

    return out


def engineer_features(raw_df):
    """Regression target tip_amount with cyclical time encodings."""
    df0 = unify_stage2_columns(raw_df)
    df0 = coerce_epoch_ms_pickup_dropoff(df0)

    req = {"tpep_pickup_datetime", "tpep_dropoff_datetime", "payment_type"}
    if req - set(df0.columns):
        raise ValueError(f"Missing columns after coercion: {sorted(req - set(df0.columns))}")

    tau = F.lit(2.0 * math.pi)

    df = (
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
        .drop("_m", "_hh", "_dow0")
    )

    if "RatecodeID" in df.columns:
        df = df.withColumn(
            "airport_rate",
            F.when(F.col("RatecodeID").isin(2, 3), F.lit(1.0)).otherwise(F.lit(0.0)),
        )
    if {"fare_amount", "trip_distance"} <= set(df.columns):
        df = df.withColumn(
            "fare_per_mile",
            F.col("fare_amount") / F.greatest(F.col("trip_distance"), F.lit(0.1)),
        )
        df = df.withColumn(
            "distance_fare_interaction",
            F.col("trip_distance") * F.col("fare_amount"),
        )
    if {"fare_amount", "trip_duration_min"} <= set(df.columns):
        df = df.withColumn(
            "fare_per_minute",
            F.col("fare_amount") / F.greatest(F.col("trip_duration_min"), F.lit(1.0)),
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
        if c in df.columns
    ]
    if pre_tip_cols:
        total_expr = sum((F.coalesce(F.col(c), F.lit(0.0)) for c in pre_tip_cols), F.lit(0.0))
        df = df.withColumn("pre_tip_amount", total_expr)

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
        "rush_hour",
        "night_trip",
        "avg_speed_mph",
        "airport_rate",
        "fare_per_mile",
        "fare_per_minute",
        "distance_fare_interaction",
        "pre_tip_amount",
    ]
    use = [c for c in cand if c in df.columns]
    return df.select(*(use + ["tip_amount"]))


def load_hive_table(spark, database, table):
    qual = hive_table_qualifier(database, table)
    if not qual:
        raise ValueError("Hive loading needs --hive-table (and optional --hive-database).")
    return spark.table(qual)


def _hdfs_scratch_base():
    linux_user = os.environ.get("USER", os.environ.get("LOGNAME", "user"))
    default_scratch = f"hdfs:///user/{linux_user}/project/stage3_scratch"
    return os.environ.get("STAGE3_HDFS_SCRATCH", default_scratch).rstrip("/")


def _use_hdfs_for_writes():
    if os.environ.get("STAGE3_LOCAL_WRITES_ONLY", "").lower() in ("1", "true", "yes"):
        return False
    return shutil.which("hdfs") is not None


def _hdfs_dfs(args):
    subprocess.check_call(["hdfs", "dfs"] + list(args))


def _local_file_uri(abs_path):
    """Force local FS when cluster default FS is hdfs (client-side paths only)."""
    return "file://" + os.path.abspath(abs_path)


def write_single_partition_csv(frame, outfile: str):
    """
    Yarn + fs.defaultFS=hdfs: unix paths resolve on HDFS and break (Permission denied /).
    Write to hdfs:///user/... staging, then hdfs dfs -getmerge to repo output/.
    Fallback: plain local dirs + file:/// if STAGE3_LOCAL_WRITES_ONLY or no hdfs CLI.
    """
    final_path = os.path.abspath(outfile)
    parent_dir = os.path.dirname(final_path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)

    if _use_hdfs_for_writes():
        staging_dir = f"{_hdfs_scratch_base()}/csv_{uuid.uuid4().hex}"
        frame.coalesce(1).write.mode("overwrite").option("header", "true").csv(staging_dir)
        if os.path.isfile(final_path):
            os.remove(final_path)
        try:
            _hdfs_dfs(["-getmerge", staging_dir, final_path])
        finally:
            try:
                _hdfs_dfs(["-rm", "-r", "-f", staging_dir])
            except subprocess.CalledProcessError:
                pass
        return

    tmp_parts = final_path + "__tmp_parts"
    shutil.rmtree(tmp_parts, ignore_errors=True)
    frame.coalesce(1).write.mode("overwrite").option(
        "header", "true"
    ).csv(_local_file_uri(tmp_parts))
    globbed = sorted(glob.glob(os.path.join(tmp_parts, "part-*.csv")))
    if not globbed:
        shutil.rmtree(tmp_parts, ignore_errors=True)
        raise RuntimeError(f"No CSV part files under {_local_file_uri(tmp_parts)!r}")
    if os.path.isfile(final_path):
        os.remove(final_path)
    shutil.move(globbed[0], final_path)
    shutil.rmtree(tmp_parts, ignore_errors=True)


def save_ml_pipeline_local(pipeline_model, models_root: str, name: str):
    """
    Persist PipelineModel under models_root/name on the gateway filesystem.
    On Yarn-default-HDFS clusters, stage under hdfs:///user/... then hdfs dfs -get (recursive dirs).
    """
    models_root_abs = os.path.abspath(models_root)
    local_dest = os.path.join(models_root_abs, name)

    if _use_hdfs_for_writes():
        staging = f"{_hdfs_scratch_base()}/ml_{uuid.uuid4().hex}"
        pipeline_model.write().overwrite().save(staging)
        try:
            if os.path.isdir(local_dest):
                shutil.rmtree(local_dest)
            elif os.path.isfile(local_dest):
                os.remove(local_dest)
            parent = os.path.dirname(local_dest.rstrip(os.sep))
            if parent:
                os.makedirs(parent, exist_ok=True)
            _hdfs_dfs(["-get", staging, local_dest])
        finally:
            try:
                _hdfs_dfs(["-rm", "-r", "-f", staging])
            except subprocess.CalledProcessError:
                pass
        return

    os.makedirs(models_root_abs, exist_ok=True)
    pipeline_model.write().overwrite().save(_local_file_uri(local_dest))


def summarize_params(model_stage):
    m = {}
    for k, v in model_stage.extractParamMap().items():
        name = str(k.name)
        if hasattr(v, "item"):
            v = v.item()
        elif isinstance(v, (list, tuple)) and v and hasattr(v[0], "item"):
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


def compute_regression_metrics(pred):
    scored = pred.withColumn("error", F.col("prediction") - F.col("label")).withColumn(
        "abs_error",
        F.abs(F.col("prediction") - F.col("label")),
    )
    evaluators = {
        "rmse": RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse"),
        "mae": RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae"),
        "r2": RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2"),
        "explained_variance": RegressionEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="var",
        ),
    }
    metrics = {name: float(ev.evaluate(pred)) for name, ev in evaluators.items()}
    residuals = scored.agg(
        F.avg("error").alias("mean_error"),
        F.avg(F.when(F.col("abs_error") <= F.lit(1.0), F.lit(1.0)).otherwise(F.lit(0.0))).alias(
            "within_1_dollar",
        ),
        F.avg(F.when(F.col("abs_error") <= F.lit(2.0), F.lit(1.0)).otherwise(F.lit(0.0))).alias(
            "within_2_dollars",
        ),
    ).first()
    q50, q90, q95 = scored.approxQuantile("abs_error", [0.5, 0.9, 0.95], 0.01)
    metrics.update(
        {
            "mean_error": float(residuals["mean_error"]),
            "median_abs_error": float(q50),
            "p90_abs_error": float(q90),
            "p95_abs_error": float(q95),
            "within_1_dollar": float(residuals["within_1_dollar"]),
            "within_2_dollars": float(residuals["within_2_dollars"]),
        },
    )
    return metrics


def evaluate_test(best_pipeline, test_df):
    pred = best_pipeline.transform(test_df).select(
        "label",
        F.greatest(F.col("prediction"), F.lit(0.0)).alias("prediction"),
    )
    return pred, compute_regression_metrics(pred)


def scaler_use_mean(cli_flag: bool) -> bool:
    if cli_flag:
        return True
    return os.environ.get("STAGE3_SCALER_WITH_MEAN", "").lower() in ("1", "true", "yes")


def strong_model_lite_requested() -> bool:
    return os.environ.get("STAGE3_GBT_LITE", "").lower() in ("1", "true", "yes") or os.environ.get(
        "STAGE3_RF_LITE",
        "",
    ).lower() in ("1", "true", "yes")


def make_preprocessing_stages(features, categorical_features, with_mean_center: bool, scale_features: bool):
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
        stages.append(StringIndexer(inputCols=categorical, outputCols=idx_out, handleInvalid="keep"))
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


def feature_names_from_pipeline(pipeline_model, ref_df):
    schema = pipeline_model.transform(ref_df.limit(1)).schema
    metadata = schema["features"].metadata.get("ml_attr", {})
    attrs = metadata.get("attrs", {})
    indexed = {}
    for attr_group in attrs.values():
        for attr in attr_group:
            if "idx" in attr and "name" in attr:
                indexed[int(attr["idx"])] = attr["name"]
    size = metadata.get("num_attrs", metadata.get("numAttrs", len(indexed)))
    return [indexed.get(i, f"feature_{i}") for i in range(int(size))]


def top_model_signal_rows(model_name, pipeline_model, ref_df, limit=30):
    model_stage = pipeline_model.stages[-1]
    names = feature_names_from_pipeline(pipeline_model, ref_df)
    rows = []
    if hasattr(model_stage, "coefficients"):
        values = model_stage.coefficients.toArray().tolist()
        ranked = sorted(
            enumerate(values),
            key=lambda item: abs(float(item[1])),
            reverse=True,
        )[:limit]
        for rank, (idx, value) in enumerate(ranked, start=1):
            rows.append(
                (
                    model_name,
                    rank,
                    names[idx] if idx < len(names) else f"feature_{idx}",
                    "coefficient",
                    float(value),
                    float(abs(value)),
                ),
            )
    elif hasattr(model_stage, "featureImportances"):
        values = model_stage.featureImportances.toArray().tolist()
        ranked = sorted(
            enumerate(values),
            key=lambda item: float(item[1]),
            reverse=True,
        )[:limit]
        for rank, (idx, value) in enumerate(ranked, start=1):
            rows.append(
                (
                    model_name,
                    rank,
                    names[idx] if idx < len(names) else f"feature_{idx}",
                    "importance",
                    float(value),
                    float(value),
                ),
            )
    return rows


def main():
    args = parse_args()
    models_root = os.path.abspath(args.models_dir)
    output_root = os.path.abspath(args.output_dir)
    os.makedirs(models_root, exist_ok=True)
    os.makedirs(output_root, exist_ok=True)

    spark = build_spark_session(args)
    spark.sparkContext.setLogLevel("WARN")

    if args.hive_table:
        raw = load_hive_table(spark, args.hive_database, args.hive_table)
        data_note = hive_table_qualifier(args.hive_database, args.hive_table)
    else:
        data_dir_resolved = resolve_filesystem_data_dir(args.data_dir)
        raw = load_raw(spark, data_dir_resolved)
        data_note = data_dir_resolved

    if args.sample_fraction is not None:
        s = args.sample_fraction
        if not 0 < s <= 1:
            raise ValueError("--sample-fraction must be in (0, 1]")
        raw = raw.sample(withReplacement=False, fraction=s, seed=args.random_seed)

    ml_ready = engineer_features(raw).withColumnRenamed("tip_amount", "label")
    feats = [c for c in ml_ready.columns if c != "label"]
    ml_ready = ml_ready.select(*feats, "label")

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
    scale_center = scaler_use_mean(args.scaler_with_mean)
    print(
        f"[stage3] source={data_note} rows={n:,} train={train_n:,} "
        f"test={test_n:,} features={len(feats)} scaler_with_mean={scale_center} "
        f"cv_parallelism={args.parallelism}",
    )

    cv_evaluator = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="rmse",
    )

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
    lr_pipe = Pipeline(
        stages=make_preprocessing_stages(feats, CATEGORICAL_FEATURES, scale_center, True) + [lr],
    )

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
    preds1, lr_metrics = evaluate_test(lr_best, test_df)

    write_single_partition_csv(preds1, os.path.join(output_root, "model1_predictions.csv"))
    save_ml_pipeline_local(lr_best, models_root, "model1_lr")

    gbt_lite = strong_model_lite_requested()
    if gbt_lite:
        gbt = GBTRegressor(
            labelCol="label",
            featuresCol="features",
            seed=args.random_seed,
            maxIter=48,
            maxDepth=5,
            stepSize=0.05,
            minInstancesPerNode=20,
        )
        gbt_grid = (
            ParamGridBuilder()
            .addGrid(gbt.maxIter, [40, 60])
            .addGrid(gbt.maxDepth, [4, 6])
            .addGrid(gbt.minInstancesPerNode, [20])
            .build()
        )
        print("[stage3] STAGE3_GBT_LITE=1: using reduced GBT grid/trees")
    else:
        gbt = GBTRegressor(
            labelCol="label",
            featuresCol="features",
            seed=args.random_seed,
            maxIter=100,
            maxDepth=7,
            stepSize=0.05,
            minInstancesPerNode=10,
        )
        gbt_grid = (
            ParamGridBuilder()
            .addGrid(gbt.maxIter, [80, 120])
            .addGrid(gbt.maxDepth, [5, 7])
            .addGrid(gbt.stepSize, [0.03, 0.05])
            .addGrid(gbt.minInstancesPerNode, [10])
            .build()
        )
    gbt_pipe = Pipeline(
        stages=make_preprocessing_stages(feats, CATEGORICAL_FEATURES, scale_center, False) + [gbt],
    )

    gbt_cv_model = fit_cv_and_pick_best(
        train_df,
        gbt_pipe,
        gbt_grid,
        cv_evaluator,
        folds=args.cv_folds,
        parallelism=args.parallelism,
        seed=args.random_seed,
    )
    gbt_best = gbt_cv_model.bestModel
    preds2, gbt_metrics = evaluate_test(gbt_best, test_df)

    write_single_partition_csv(preds2, os.path.join(output_root, "model2_predictions.csv"))
    save_ml_pipeline_local(gbt_best, models_root, "model2_gbt")

    rows = (
        (
            "LinearRegression",
            lr_metrics["rmse"],
            lr_metrics["mae"],
            lr_metrics["r2"],
            lr_metrics["explained_variance"],
            lr_metrics["mean_error"],
            lr_metrics["median_abs_error"],
            lr_metrics["p90_abs_error"],
            lr_metrics["p95_abs_error"],
            lr_metrics["within_1_dollar"],
            lr_metrics["within_2_dollars"],
        ),
        (
            "GBTRegressor",
            gbt_metrics["rmse"],
            gbt_metrics["mae"],
            gbt_metrics["r2"],
            gbt_metrics["explained_variance"],
            gbt_metrics["mean_error"],
            gbt_metrics["median_abs_error"],
            gbt_metrics["p90_abs_error"],
            gbt_metrics["p95_abs_error"],
            gbt_metrics["within_1_dollar"],
            gbt_metrics["within_2_dollars"],
        ),
    )
    comp = spark.createDataFrame(
        rows,
        [
            "model",
            "RMSE",
            "MAE",
            "R2",
            "ExplainedVariance",
            "MeanError",
            "MedianAbsError",
            "P90AbsError",
            "P95AbsError",
            "Within1Dollar",
            "Within2Dollars",
        ],
    )
    write_single_partition_csv(comp, os.path.join(output_root, "evaluation.csv"))

    lr_stage = lr_best.stages[-1]
    gbt_stage = gbt_best.stages[-1]
    signal_rows = top_model_signal_rows("LinearRegression", lr_best, test_df) + top_model_signal_rows(
        "GBTRegressor",
        gbt_best,
        test_df,
    )
    if signal_rows:
        signal_df = spark.createDataFrame(
            signal_rows,
            ["model", "rank", "feature", "signal_type", "signed_value", "absolute_value"],
        )
        write_single_partition_csv(
            signal_df,
            os.path.join(output_root, "model_feature_signals.csv"),
        )

    summary = {
        "task": "regression_tip_amount_credit_card_trips_only",
        "data_source": str(data_note),
        "train_rows": int(train_n),
        "test_rows": int(test_n),
        "scaler_with_mean": bool(scale_center),
        "cross_validation_rmse_focus": True,
        "prediction_postprocess": "negative predictions clipped to 0.0 for metrics and CSV exports",
        "model1": {
            "name": "LinearRegression",
            "test_metrics": lr_metrics,
            "best_params_from_cv": summarize_params(lr_stage),
            "prediction_csv": os.path.join(output_root, "model1_predictions.csv"),
            "persisted_pipeline": os.path.join(models_root, "model1_lr"),
        },
        "model2": {
            "name": "GBTRegressor",
            "test_metrics": gbt_metrics,
            "best_params_from_cv": summarize_params(gbt_stage),
            "prediction_csv": os.path.join(output_root, "model2_predictions.csv"),
            "persisted_pipeline": os.path.join(models_root, "model2_gbt"),
        },
        "feature_signal_csv": os.path.join(output_root, "model_feature_signals.csv"),
    }

    summ_path = os.path.join(output_root, "stage3_training_summary.json")
    with open(summ_path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, default=str)

    print(json.dumps(summary, indent=2, default=str))
    spark.stop()


if __name__ == "__main__":
    main()
