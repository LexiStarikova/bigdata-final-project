USE ${hivevar:stage4_db};

SET hive.resultset.use.unique.column.names=false;

-- Stage IV external tables over Stage III artifacts copied to HDFS by scripts/stage4.sh.
-- The hivevar stage4_hdfs_root points to a directory such as:
--   /user/team35/project/stage4/ml

DROP TABLE IF EXISTS stage4_evaluation;
CREATE EXTERNAL TABLE stage4_evaluation (
    model STRING,
    rmse DOUBLE,
    mae DOUBLE,
    r2 DOUBLE,
    explainedvariance DOUBLE,
    meanerror DOUBLE,
    medianabserror DOUBLE,
    p90abserror DOUBLE,
    p95abserror DOUBLE,
    within1dollar DOUBLE,
    within2dollars DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:stage4_hdfs_root}/evaluation'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS stage4_model_feature_signals;
CREATE EXTERNAL TABLE stage4_model_feature_signals (
    model STRING,
    rank INT,
    feature STRING,
    signal_type STRING,
    signed_value DOUBLE,
    absolute_value DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:stage4_hdfs_root}/feature_signals'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS stage4_best_params;
CREATE EXTERNAL TABLE stage4_best_params (
    model STRING,
    param_name STRING,
    param_value STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:stage4_hdfs_root}/best_params'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS stage4_training_summary;
CREATE EXTERNAL TABLE stage4_training_summary (
    metric_name STRING,
    metric_value STRING,
    description STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:stage4_hdfs_root}/training_summary'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS stage4_feature_catalog;
CREATE EXTERNAL TABLE stage4_feature_catalog (
    feature_group STRING,
    feature_name STRING,
    description STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:stage4_hdfs_root}/feature_catalog'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS stage4_prediction_samples;
CREATE EXTERNAL TABLE stage4_prediction_samples (
    model STRING,
    source_row_number BIGINT,
    label DOUBLE,
    prediction DOUBLE,
    error DOUBLE,
    absolute_error DOUBLE,
    error_bucket STRING,
    within_1_dollar INT,
    within_2_dollars INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:stage4_hdfs_root}/prediction_samples'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS stage4_single_prediction;
CREATE EXTERNAL TABLE stage4_single_prediction (
    model STRING,
    sample_source STRING,
    sample_index BIGINT,
    label DOUBLE,
    prediction DOUBLE,
    absolute_error DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:stage4_hdfs_root}/single_prediction'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS stage4_model1_predictions;
CREATE EXTERNAL TABLE stage4_model1_predictions (
    label DOUBLE,
    prediction DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:stage4_hdfs_root}/model1_predictions'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS stage4_model2_predictions;
CREATE EXTERNAL TABLE stage4_model2_predictions (
    label DOUBLE,
    prediction DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:stage4_hdfs_root}/model2_predictions'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS stage4_model3_predictions;
CREATE EXTERNAL TABLE stage4_model3_predictions (
    label DOUBLE,
    prediction DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:stage4_hdfs_root}/model3_predictions'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP VIEW IF EXISTS stage4_model_metric_long;
CREATE VIEW stage4_model_metric_long AS
SELECT model, metric_name, metric_value
FROM stage4_evaluation
LATERAL VIEW stack(
    10,
    'RMSE', rmse,
    'MAE', mae,
    'R2', r2,
    'ExplainedVariance', explainedvariance,
    'MeanError', meanerror,
    'MedianAbsError', medianabserror,
    'P90AbsError', p90abserror,
    'P95AbsError', p95abserror,
    'Within1Dollar', within1dollar,
    'Within2Dollars', within2dollars
) metric_stack AS metric_name, metric_value;

DROP VIEW IF EXISTS stage4_prediction_sample_metrics;
CREATE VIEW stage4_prediction_sample_metrics AS
SELECT
    model,
    COUNT(*) AS sampled_rows,
    ROUND(AVG(absolute_error), 4) AS avg_absolute_error,
    ROUND(PERCENTILE_APPROX(absolute_error, 0.5), 4) AS median_absolute_error,
    ROUND(PERCENTILE_APPROX(absolute_error, 0.9), 4) AS p90_absolute_error,
    ROUND(AVG(within_1_dollar), 4) AS within_1_dollar_rate,
    ROUND(AVG(within_2_dollars), 4) AS within_2_dollars_rate
FROM stage4_prediction_samples
GROUP BY model;

DROP VIEW IF EXISTS stage4_error_bucket_summary;
CREATE VIEW stage4_error_bucket_summary AS
SELECT
    model,
    error_bucket,
    COUNT(*) AS sampled_rows
FROM stage4_prediction_samples
GROUP BY model, error_bucket;

-- Lightweight verification output for stage4.sh logs.
SHOW TABLES LIKE 'stage4_*';
SELECT * FROM stage4_evaluation;
SELECT * FROM stage4_prediction_sample_metrics;
