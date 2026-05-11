"""
Stage 3 helper utilities — I/O, data loading, metrics, feature inspection.

Extracted from stage3.py to keep the main ML orchestration module concise.
All public symbols are re-imported by stage3.py so the pipeline API is unchanged.
"""

import csv
import glob
import json
import math
import os
import shutil
import subprocess
import uuid

from pyspark.sql import functions as F

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator

# Stage II hive column aliases (snake/lowercase → TLC-style names engineer_features expects)
STAGE2_TO_TLC_COLUMNS = (
    ("vendorid", "VendorID"),
    ("ratecodeid", "RatecodeID"),
    ("pulocationid", "PULocationID"),
    ("dolocationid", "DOLocationID"),
)

CATEGORICAL_FEATURES = ("VendorID", "RatecodeID", "PULocationID", "DOLocationID")


# URI / path helpers
def _remote_data_uri(path: str) -> bool:
    """Return True when *path* points to a remote (non-local) filesystem."""
    trimmed = path.strip().lower()
    if trimmed.startswith("file:"):
        return False
    if "://" in path:
        return True
    return any(trimmed.startswith(s) for s in ("hdfs:", "s3:", "s3a:", "wasb:", "abfs:", "viewfs:"))


def _normalize_hdfs_uri_for_spark(uri: str) -> str:
    """Normalise ``hdfs:/path`` to ``hdfs:///path`` (Spark needs triple-slash)."""
    if not uri.startswith("hdfs:"):
        return uri
    tail = uri[len("hdfs:"):]
    if tail.startswith("/") and not tail.startswith("//"):
        return "hdfs://" + tail
    return uri


def resolve_filesystem_data_dir(arg: str) -> str:
    """Absolute-ify local path; leave remote URIs untouched."""
    path_str = arg.rstrip("/")
    return path_str if _remote_data_uri(path_str) else os.path.abspath(path_str)


# Hive helpers
def hive_table_qualifier(database, table):
    """Build ``database.table`` from separate args or a dot-qualified table."""
    if not table:
        return None
    db_name = database
    tbl = table
    if db_name:
        if "." in tbl:
            raise ValueError(
                "Use (--hive-database + short table) or qualified --hive-table, not both.",
            )
        return f"{db_name}.{tbl.strip()}"
    if "." in tbl:
        parts = tbl.split(".", 1)
        return f"{parts[0].strip()}.{parts[1].strip()}"
    return tbl.strip()


def load_hive_table(spark, database, table):
    """Load a Hive table via the qualified name."""
    qual = hive_table_qualifier(database, table)
    if not qual:
        raise ValueError("Hive loading needs --hive-table (and optional --hive-database).")
    return spark.table(qual)


# Data discovery and loading
def discover_parquet(data_dir: str):
    """Find TLC-style yellow taxi Parquet files under *data_dir*."""
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
    """Load raw TLC data from Parquet, CSV, or remote URI."""
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


# Column unification / coercion
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
        type_name = typ.lower().split("(")[0].strip()
        if type_name == "timestamp":
            return False
        return type_name in (
            "bigint", "long", "int", "smallint", "short",
            "double", "float", "decimal",
        )

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


# HDFS / local I/O helpers
def _hdfs_scratch_base():
    """Default HDFS scratch directory for CSV/model staging."""
    linux_user = os.environ.get("USER", os.environ.get("LOGNAME", "user"))
    default_scratch = f"hdfs:///user/{linux_user}/project/stage3_scratch"
    return os.environ.get("STAGE3_HDFS_SCRATCH", default_scratch).rstrip("/")


def _use_hdfs_for_writes():
    """True when writes should be staged on HDFS first."""
    if os.environ.get("STAGE3_LOCAL_WRITES_ONLY", "").lower() in ("1", "true", "yes"):
        return False
    return shutil.which("hdfs") is not None


def _hdfs_dfs(args):
    """Run ``hdfs dfs <args>``."""
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
    On Yarn-default-HDFS clusters, stage under hdfs:///user/... then hdfs dfs -get.
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


def write_json_local(payload, outfile: str):
    """Write *payload* as indented JSON to *outfile*."""
    final_path = os.path.abspath(outfile)
    parent_dir = os.path.dirname(final_path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)
    with open(final_path, "w", encoding="utf-8") as file_handle:
        json.dump(payload, file_handle, indent=2, default=str)


# Model params helpers
def summarize_params(model_stage):
    """Extract hyper-parameter map from a fitted Spark ML stage."""
    params_map = {}
    for param_key, param_val in model_stage.extractParamMap().items():
        name = str(param_key.name)
        if hasattr(param_val, "item"):
            param_val = param_val.item()
        elif isinstance(param_val, (list, tuple)) and param_val and hasattr(param_val[0], "item"):
            param_val = type(param_val)(x.item() if hasattr(x, "item") else x for x in param_val)
        params_map[name] = param_val
    return params_map


def save_best_params(output_root, model_key, model_name, pipeline_model):
    """Persist best CV params as JSON and return the param dict."""
    payload = {
        "model": model_name,
        "best_params_from_cv": summarize_params(pipeline_model.stages[-1]),
    }
    write_json_local(payload, os.path.join(output_root, f"{model_key}_best_params.json"))
    return payload["best_params_from_cv"]


def load_saved_best_params(output_root: str, model_key: str):
    """Load previously-saved best-params JSON; return None if missing."""
    path = os.path.join(output_root, f"{model_key}_best_params.json")
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as file_handle:
        payload = json.load(file_handle)
    return payload.get("best_params_from_cv")


# Prediction CSV metrics (offline / resume path)
def _csv_label_stats(path):
    """First pass: count rows and accumulate label sums."""
    row_count, label_sum, label_sq_sum = 0, 0.0, 0.0
    with open(path, "r", encoding="utf-8", newline="") as file_handle:
        for row in csv.DictReader(file_handle):
            label = float(row["label"])
            row_count += 1
            label_sum += label
            label_sq_sum += label * label
    return row_count, label_sum, label_sq_sum


def _csv_error_stats(path):
    """Second pass: accumulate residual and absolute-error statistics."""
    acc = {"err_sum": 0.0, "err_sq_sum": 0.0, "abs_err_sum": 0.0,
           "within_1": 0, "within_2": 0}
    abs_errors = []
    with open(path, "r", encoding="utf-8", newline="") as file_handle:
        for row in csv.DictReader(file_handle):
            err = float(row["prediction"]) - float(row["label"])
            abs_err = abs(err)
            acc["err_sum"] += err
            acc["err_sq_sum"] += err * err
            acc["abs_err_sum"] += abs_err
            acc["within_1"] += int(abs_err <= 1.0)
            acc["within_2"] += int(abs_err <= 2.0)
            abs_errors.append(abs_err)
    abs_errors.sort()
    return acc, abs_errors


def compute_prediction_csv_metrics(prediction_csv: str):
    """Compute regression metrics from a label,prediction CSV file."""
    path = os.path.abspath(prediction_csv)
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"Cannot resume: missing existing prediction CSV {path!r}",
        )

    row_count, label_sum, label_sq_sum = _csv_label_stats(path)
    if row_count == 0:
        raise RuntimeError(f"Cannot compute metrics from empty prediction CSV {path!r}")

    ss_tot = label_sq_sum - (label_sum * label_sum / row_count)
    acc, abs_errors = _csv_error_stats(path)

    def percentile(quantile):
        """Return the *quantile*-th percentile of the sorted absolute errors."""
        return float(abs_errors[int(round((len(abs_errors) - 1) * quantile))])

    mean_error = acc["err_sum"] / row_count
    label_variance = ss_tot / row_count
    residual_variance = (acc["err_sq_sum"] / row_count) - (mean_error * mean_error)
    return {
        "rmse": math.sqrt(acc["err_sq_sum"] / row_count),
        "mae": acc["abs_err_sum"] / row_count,
        "r2": 0.0 if ss_tot == 0.0 else 1.0 - (acc["err_sq_sum"] / ss_tot),
        "explained_variance": (
            0.0 if label_variance == 0.0 else 1.0 - (residual_variance / label_variance)
        ),
        "mean_error": mean_error,
        "median_abs_error": percentile(0.5),
        "p90_abs_error": percentile(0.9),
        "p95_abs_error": percentile(0.95),
        "within_1_dollar": acc["within_1"] / row_count,
        "within_2_dollars": acc["within_2"] / row_count,
    }


# Train/test JSON persistence
def _hdfs_data_base():
    """HDFS base for train/test JSON, aligned with project HDFS layout."""
    linux_user = os.environ.get("USER", os.environ.get("LOGNAME", "user"))
    return f"hdfs:///user/{linux_user}/project/data"


def save_train_test_json(train_df, test_df):
    """Save train and test splits as JSON to HDFS and pull to local data/."""
    base = _hdfs_data_base()
    hdfs_train = f"{base}/train"
    hdfs_test = f"{base}/test"

    train_df.select("features", "label").coalesce(1).write.mode("overwrite").json(hdfs_train)
    test_df.select("features", "label").coalesce(1).write.mode("overwrite").json(hdfs_test)
    print(f"[stage3] Saved train/test JSON → {hdfs_train}, {hdfs_test}")

    if shutil.which("hdfs") is not None:
        os.makedirs("data", exist_ok=True)
        for hdfs_path, local_path in [(hdfs_train, "data/train.json"),
                                      (hdfs_test, "data/test.json")]:
            try:
                if os.path.isfile(local_path):
                    os.remove(local_path)
                _hdfs_dfs(["-getmerge", hdfs_path, local_path])
                print(f"[stage3] Pulled {hdfs_path} → {local_path}")
            except subprocess.CalledProcessError:
                print(f"[stage3] Warning: could not pull {hdfs_path} to {local_path}")


# CV sample builder
def build_cv_sample(train, train_n, max_rows, seed):
    """Down-sample training data for cross-validation grid search."""
    if max_rows <= 0:
        raise ValueError("--cv-sample-size must be positive")
    if train_n <= max_rows:
        return train, train_n

    fraction = min(1.0, (float(max_rows) * 3.0) / float(train_n))
    # Uniform sample; random ordering avoids partition-biased caps
    sample = train.sample(withReplacement=False, fraction=fraction, seed=seed)
    return sample.orderBy(F.rand(seed)).limit(max_rows), None


# CV training
def fit_cv_and_pick_best(
    train, cv_train, pipeline, grid, evaluator, **cv_kwargs,
):
    """Run CrossValidator on cv_train, refit best params on full train."""
    cross_validator = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=grid,
        evaluator=evaluator,
        **cv_kwargs,
    )
    cv_model = cross_validator.fit(cv_train)
    metrics = list(cv_model.avgMetrics)
    if not metrics:
        return cv_model.bestModel

    chooser = max if evaluator.isLargerBetter() else min
    best_index = chooser(range(len(metrics)), key=lambda i: metrics[i])
    best_param_map = grid[best_index]
    return pipeline.copy(best_param_map).fit(train)



# Spark-side regression metrics
def compute_regression_metrics(pred):
    """Compute full regression metric suite from a prediction DataFrame."""
    scored = pred.withColumn("error", F.col("prediction") - F.col("label")).withColumn(
        "abs_error",
        F.abs(F.col("prediction") - F.col("label")),
    )
    evaluators = {
        "rmse": RegressionEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="rmse",
        ),
        "mae": RegressionEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="mae",
        ),
        "r2": RegressionEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="r2",
        ),
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
    """Transform test set and compute metrics (negative predictions clipped to 0)."""
    pred = best_pipeline.transform(test_df).select(
        "label",
        F.greatest(F.col("prediction"), F.lit(0.0)).alias("prediction"),
    )
    return pred, compute_regression_metrics(pred)



# Temporal split
def temporal_train_test_split(frame, split_col="__split_ts", train_ratio=0.70):
    """Split by pickup timestamp quantile into train/test."""
    clean = frame.filter(F.col(split_col).isNotNull())
    cutoffs = clean.approxQuantile(split_col, [train_ratio], 0.001)
    if not cutoffs:
        raise RuntimeError("Temporal split has no valid pickup timestamps.")
    cutoff = cutoffs[0]
    train = clean.filter(F.col(split_col) <= F.lit(cutoff)).drop(split_col)
    test = clean.filter(F.col(split_col) > F.lit(cutoff)).drop(split_col)
    return train, test, cutoff


# Scaler config
def scaler_use_mean(cli_flag: bool) -> bool:
    """Resolve whether StandardScaler should centre features."""
    if cli_flag:
        return True
    return os.environ.get("STAGE3_SCALER_WITH_MEAN", "").lower() in ("1", "true", "yes")



# Feature inspection
def feature_names_from_pipeline(pipeline_model, ref_df):
    """Extract ordered feature names from a fitted pipeline's metadata."""
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
    """Return top feature signals (coefficients or importances) for one model."""
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
