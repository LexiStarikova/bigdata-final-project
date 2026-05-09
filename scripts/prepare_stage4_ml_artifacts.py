"""Prepare Stage IV ML dashboard artifacts from Stage III outputs.

The Stage III model outputs are a mix of CSV and JSON files. Hive/Superset work
best with tabular files, so this script flattens the JSON summaries and creates
bounded prediction samples for interactive charts.
"""
# pylint: disable=missing-function-docstring

import csv
import json
import os
import random
from pathlib import Path


MODELS = (
    ("model1", "LinearRegression"),
    ("model2", "RandomForestRegressor"),
    ("model3", "GBTRegressor"),
)

FEATURE_DESCRIPTIONS = (
    ("raw_trip", "VendorID", "Taxi provider identifier with one-hot encoding."),
    ("raw_trip", "passenger_count", "Passenger count after filtering to 1..6."),
    ("raw_trip", "trip_distance", "Trip distance in miles after outlier filtering."),
    ("raw_trip", "RatecodeID", "Rate code including standard and airport trips."),
    ("raw_trip", "PULocationID", "Pickup taxi zone with one-hot encoding."),
    ("raw_trip", "DOLocationID", "Dropoff taxi zone with one-hot encoding."),
    ("fare", "fare_amount", "Base fare amount before tip."),
    ("fare", "pre_tip_amount", "Fare plus surcharges taxes tolls and fees excluding tip."),
    ("time", "pickup_month_sin", "Cyclical pickup month sine component."),
    ("time", "pickup_month_cos", "Cyclical pickup month cosine component."),
    ("time", "pickup_hour_sin", "Cyclical pickup hour sine component."),
    ("time", "pickup_hour_cos", "Cyclical pickup hour cosine component."),
    ("time", "pickup_dow_sin", "Cyclical pickup day-of-week sine component."),
    ("time", "pickup_dow_cos", "Cyclical pickup day-of-week cosine component."),
    ("time_flag", "is_weekend", "1 when pickup happens on Saturday or Sunday."),
    ("time_flag", "rush_hour", "1 for morning or evening rush-hour pickups."),
    ("time_flag", "night_trip", "1 for late-night trips."),
    ("rate_flag", "airport_rate", "1 for JFK or Newark rate codes."),
)


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def read_json(path: Path):
    with path.open(encoding="utf-8") as file_obj:
        return json.load(file_obj)


def write_csv(path: Path, fieldnames, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def stringify(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    return str(value)


def build_best_params(output_dir: Path, stage4_dir: Path) -> None:
    rows = []
    for model_key, expected_name in MODELS:
        payload = read_json(output_dir / f"{model_key}_best_params.json")
        model_name = payload.get("model", expected_name)
        params = payload.get("best_params_from_cv", {})
        for param_name in sorted(params):
            rows.append(
                {
                    "model": model_name,
                    "param_name": param_name,
                    "param_value": stringify(params[param_name]),
                },
            )
    write_csv(
        stage4_dir / "stage4_best_params.csv",
        ("model", "param_name", "param_value"),
        rows,
    )


def build_training_summary(output_dir: Path, stage4_dir: Path) -> None:
    summary = read_json(output_dir / "stage3_training_summary.json")
    rows = [
        {
            "metric_name": "task",
            "metric_value": stringify(summary.get("task")),
            "description": "Prediction task solved in Stage III.",
        },
        {
            "metric_name": "data_source",
            "metric_value": stringify(summary.get("data_source")),
            "description": "Hive table used as the Stage III training source.",
        },
        {
            "metric_name": "train_rows",
            "metric_value": stringify(summary.get("train_rows")),
            "description": "Rows in chronological training split.",
        },
        {
            "metric_name": "test_rows",
            "metric_value": stringify(summary.get("test_rows")),
            "description": "Rows in chronological held-out test split.",
        },
        {
            "metric_name": "cv_sample_rows",
            "metric_value": stringify(summary.get("cv_sample_rows")),
            "description": "Rows used for cross-validation parameter search.",
        },
        {
            "metric_name": "split_strategy",
            "metric_value": stringify(summary.get("split_strategy")),
            "description": "Method used to separate train and test data.",
        },
        {
            "metric_name": "scaler_with_mean",
            "metric_value": stringify(summary.get("scaler_with_mean")),
            "description": "Whether StandardScaler centered sparse vectors.",
        },
        {
            "metric_name": "prediction_postprocess",
            "metric_value": stringify(summary.get("prediction_postprocess")),
            "description": "Post-processing applied before evaluation exports.",
        },
    ]
    write_csv(
        stage4_dir / "stage4_training_summary.csv",
        ("metric_name", "metric_value", "description"),
        rows,
    )


def build_feature_catalog(stage4_dir: Path) -> None:
    rows = [
        {
            "feature_group": feature_group,
            "feature_name": feature_name,
            "description": description,
        }
        for feature_group, feature_name, description in FEATURE_DESCRIPTIONS
    ]
    write_csv(
        stage4_dir / "stage4_feature_catalog.csv",
        ("feature_group", "feature_name", "description"),
        rows,
    )


def bucket_error(abs_error: float) -> str:
    if abs_error <= 1.0:
        return "A_<=1"
    if abs_error <= 2.0:
        return "B_1_to_2"
    if abs_error <= 5.0:
        return "C_2_to_5"
    return "D_>5"


def sample_prediction_rows(path: Path, sample_size: int, seed: int):
    if sample_size <= 0:
        return []
    rng = random.Random(seed)
    sample = []
    with path.open(encoding="utf-8", newline="") as file_obj:
        for row_number, row in enumerate(csv.DictReader(file_obj), start=1):
            if row_number <= sample_size:
                sample.append((row_number, row))
                continue
            replacement_idx = rng.randrange(row_number)
            if replacement_idx < sample_size:
                sample[replacement_idx] = (row_number, row)
    sample.sort(key=lambda item: item[0])
    return sample


def build_prediction_samples(output_dir: Path, stage4_dir: Path) -> None:
    sample_size = int(os.environ.get("STAGE4_PREDICTION_SAMPLE_SIZE", "5000"))
    seed = int(os.environ.get("STAGE4_RANDOM_SEED", "42"))
    rows = []
    for idx, (model_key, model_name) in enumerate(MODELS):
        prediction_path = output_dir / f"{model_key}_predictions.csv"
        for row_number, row in sample_prediction_rows(prediction_path, sample_size, seed + idx):
            label = float(row["label"])
            prediction = float(row["prediction"])
            error = prediction - label
            abs_error = abs(error)
            rows.append(
                {
                    "model": model_name,
                    "source_row_number": row_number,
                    "label": label,
                    "prediction": prediction,
                    "error": error,
                    "absolute_error": abs_error,
                    "error_bucket": bucket_error(abs_error),
                    "within_1_dollar": int(abs_error <= 1.0),
                    "within_2_dollars": int(abs_error <= 2.0),
                },
            )
    write_csv(
        stage4_dir / "stage4_prediction_samples.csv",
        (
            "model",
            "source_row_number",
            "label",
            "prediction",
            "error",
            "absolute_error",
            "error_bucket",
            "within_1_dollar",
            "within_2_dollars",
        ),
        rows,
    )


def build_sample_prediction(stage4_dir: Path, output_dir: Path) -> None:
    path = output_dir / "sample_prediction_model2.json"
    if not path.is_file():
        return
    payload = read_json(path)
    label = float(payload["label"])
    prediction = float(payload["prediction"])
    abs_error = abs(prediction - label)
    rows = [
        {
            "model": "RandomForestRegressor",
            "sample_source": stringify(payload.get("sample_source")),
            "sample_index": stringify(payload.get("sample_index")),
            "label": label,
            "prediction": prediction,
            "absolute_error": abs_error,
        },
    ]
    write_csv(
        stage4_dir / "stage4_single_prediction.csv",
        ("model", "sample_source", "sample_index", "label", "prediction", "absolute_error"),
        rows,
    )


def validate_required_outputs(output_dir: Path) -> None:
    required = [
        output_dir / "evaluation.csv",
        output_dir / "model_feature_signals.csv",
        output_dir / "stage3_training_summary.json",
    ]
    for model_key, _model_name in MODELS:
        required.append(output_dir / f"{model_key}_predictions.csv")
        required.append(output_dir / f"{model_key}_best_params.json")
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise FileNotFoundError("Missing Stage III outputs: " + ", ".join(missing))


def main() -> None:
    root = repo_root()
    output_dir = root / "output"
    stage4_dir = output_dir / "stage4"
    validate_required_outputs(output_dir)
    build_best_params(output_dir, stage4_dir)
    build_training_summary(output_dir, stage4_dir)
    build_feature_catalog(stage4_dir)
    build_prediction_samples(output_dir, stage4_dir)
    build_sample_prediction(stage4_dir, output_dir)
    print(f"Stage IV ML artifacts written to {stage4_dir}")


if __name__ == "__main__":
    main()
