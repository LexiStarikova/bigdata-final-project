USE team35_projectdb;
SET hive.resultset.use.unique.column.names=false;

-- Pickup/dropoff in yellow_taxi_trips_part_buck are BIGINT epoch milliseconds (Avro long).
-- Human time: FROM_UNIXTIME(CAST(ms / 1000 AS BIGINT)).
-- Hour / weekday: substring on that string (avoid HOUR() on TIMESTAMP + bad Avro cast).

-- Q1: Dataset overview 

DROP TABLE IF EXISTS q1_results;
CREATE EXTERNAL TABLE q1_results (
    total_trips       BIGINT,
    earliest_pickup   STRING,
    latest_pickup     STRING,
    total_revenue     DOUBLE,
    avg_fare          DOUBLE,
    avg_distance      DOUBLE,
    avg_tip           DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q1_results';

INSERT INTO q1_results
SELECT
    COUNT(*)                                           AS total_trips,
    CAST(MIN(FROM_UNIXTIME(CAST(tpep_pickup_datetime / 1000 AS BIGINT))) AS STRING) AS earliest_pickup,
    CAST(MAX(FROM_UNIXTIME(CAST(tpep_pickup_datetime / 1000 AS BIGINT))) AS STRING) AS latest_pickup,
    ROUND(SUM(total_amount),    2)                     AS total_revenue,
    ROUND(AVG(fare_amount),     2)                     AS avg_fare,
    ROUND(AVG(trip_distance),   2)                     AS avg_distance,
    ROUND(AVG(tip_amount),      2)                     AS avg_tip
FROM yellow_taxi_trips_part_buck
WHERE year = 2025
  AND tpep_pickup_datetime IS NOT NULL;

SELECT * FROM q1_results;



-- Q2: Data quality 

DROP TABLE IF EXISTS q2_results;
CREATE EXTERNAL TABLE q2_results (
    total_records          BIGINT,
    null_datetime          BIGINT,
    zero_distance          BIGINT,
    zero_or_neg_fare       BIGINT,
    zero_passengers        BIGINT,
    outlier_dist_gt50mi    BIGINT,
    outlier_fare_gt500     BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q2_results';

INSERT INTO q2_results
SELECT
    COUNT(*)                                                   AS total_records,
    COUNT(CASE WHEN tpep_pickup_datetime IS NULL  THEN 1 END)  AS null_datetime,
    COUNT(CASE WHEN trip_distance = 0            THEN 1 END)  AS zero_distance,
    COUNT(CASE WHEN fare_amount <= 0             THEN 1 END)  AS zero_or_neg_fare,
    COUNT(CASE WHEN passenger_count = 0          THEN 1 END)  AS zero_passengers,
    COUNT(CASE WHEN trip_distance > 50           THEN 1 END)  AS outlier_dist_gt50mi,
    COUNT(CASE WHEN fare_amount > 500            THEN 1 END)  AS outlier_fare_gt500
FROM yellow_taxi_trips_part_buck
WHERE year = 2025;

SELECT * FROM q2_results;


-- Q3: Passenger count distribution


DROP TABLE IF EXISTS q3_results;
CREATE EXTERNAL TABLE q3_results (
    passenger_count   INT,
    trip_count        BIGINT,
    avg_fare          DOUBLE,
    avg_tip           DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q3_results';

INSERT INTO q3_results
SELECT
    CAST(passenger_count AS INT)    AS passenger_count,
    COUNT(*)                        AS trip_count,
    ROUND(AVG(fare_amount), 2)      AS avg_fare,
    ROUND(AVG(tip_amount),  2)      AS avg_tip
FROM yellow_taxi_trips_part_buck
WHERE year = 2025
GROUP BY CAST(passenger_count AS INT)
ORDER BY trip_count DESC;

SELECT * FROM q3_results;



-- Q4: Trip distance distribution (bucketed)

DROP TABLE IF EXISTS q4_results;
CREATE EXTERNAL TABLE q4_results (
    distance_bucket   STRING,
    trip_count        BIGINT,
    avg_distance      DOUBLE,
    avg_fare          DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q4_results';

INSERT INTO q4_results
SELECT
    CASE
        WHEN trip_distance = 0      THEN '0_zero_bad_data'
        WHEN trip_distance < 1      THEN 'A_under_1mi'
        WHEN trip_distance < 3      THEN 'B_1to3mi'
        WHEN trip_distance < 7      THEN 'C_3to7mi'
        WHEN trip_distance < 15     THEN 'D_7to15mi'
        WHEN trip_distance < 50     THEN 'E_15to50mi'
        ELSE                             'F_over_50mi_outlier'
    END                             AS distance_bucket,
    COUNT(*)                        AS trip_count,
    ROUND(AVG(trip_distance), 2)    AS avg_distance,
    ROUND(AVG(fare_amount),   2)    AS avg_fare
FROM yellow_taxi_trips_part_buck
WHERE year = 2025
GROUP BY
    CASE
        WHEN trip_distance = 0      THEN '0_zero_bad_data'
        WHEN trip_distance < 1      THEN 'A_under_1mi'
        WHEN trip_distance < 3      THEN 'B_1to3mi'
        WHEN trip_distance < 7      THEN 'C_3to7mi'
        WHEN trip_distance < 15     THEN 'D_7to15mi'
        WHEN trip_distance < 50     THEN 'E_15to50mi'
        ELSE                             'F_over_50mi_outlier'
    END
ORDER BY distance_bucket;

SELECT * FROM q4_results;

-- Q5: Revenue component breakdown — one scan, wide result


DROP TABLE IF EXISTS q5_results;
CREATE EXTERNAL TABLE q5_results (
    total_fare              DOUBLE,
    total_tip               DOUBLE,
    total_mta_tax           DOUBLE,
    total_tolls             DOUBLE,
    total_improvement_sur   DOUBLE,
    total_congestion_sur    DOUBLE,
    total_airport_fee       DOUBLE,
    total_extra             DOUBLE,
    grand_total             DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q5_results';

INSERT INTO q5_results
SELECT
    ROUND(SUM(fare_amount),           2)  AS total_fare,
    ROUND(SUM(tip_amount),            2)  AS total_tip,
    ROUND(SUM(mta_tax),               2)  AS total_mta_tax,
    ROUND(SUM(tolls_amount),          2)  AS total_tolls,
    ROUND(SUM(improvement_surcharge), 2)  AS total_improvement_sur,
    ROUND(SUM(congestion_surcharge),  2)  AS total_congestion_sur,
    ROUND(SUM(airport_fee),           2)  AS total_airport_fee,
    ROUND(SUM(extra),                 2)  AS total_extra,
    ROUND(SUM(total_amount),          2)  AS grand_total
FROM yellow_taxi_trips_part_buck
WHERE year = 2025;

SELECT * FROM q5_results;


-- Q6: Payment type breakdown — how do passengers prefer to pay?

DROP TABLE IF EXISTS q6_results;
CREATE EXTERNAL TABLE q6_results (
    payment_type    INT,
    trip_count      BIGINT,
    total_revenue   DOUBLE,
    avg_fare        DOUBLE,
    avg_tip         DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q6_results';

INSERT INTO q6_results
SELECT
    CAST(payment_type AS INT)       AS payment_type,
    COUNT(*)                        AS trip_count,
    ROUND(SUM(total_amount), 2)     AS total_revenue,
    ROUND(AVG(fare_amount),  2)     AS avg_fare,
    ROUND(AVG(tip_amount),   2)     AS avg_tip
FROM yellow_taxi_trips_part_buck
WHERE year = 2025
GROUP BY CAST(payment_type AS INT)
ORDER BY trip_count DESC;

SELECT * FROM q6_results;



-- Q7: Peak hours — when is demand highest?


DROP TABLE IF EXISTS q7_results;

CREATE EXTERNAL TABLE q7_results (
    hour_of_day    INT,
    trip_count     BIGINT,
    avg_revenue    DOUBLE,
    avg_tip        DOUBLE,
    total_revenue  DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q7_results';

INSERT INTO q7_results
SELECT
    CAST(SUBSTRING(FROM_UNIXTIME(CAST(tpep_pickup_datetime / 1000 AS BIGINT)), 12, 2) AS INT)
        AS hour_of_day,
    COUNT(*)                                                   AS trip_count,
    ROUND(AVG(total_amount), 2)                               AS avg_revenue,
    ROUND(AVG(tip_amount),   2)                                AS avg_tip,
    ROUND(SUM(total_amount), 2)                                AS total_revenue
FROM yellow_taxi_trips_part_buck
WHERE year = 2025
  AND tpep_pickup_datetime IS NOT NULL
GROUP BY CAST(SUBSTRING(FROM_UNIXTIME(CAST(tpep_pickup_datetime / 1000 AS BIGINT)), 12, 2) AS INT)
ORDER BY hour_of_day;

SELECT * FROM q7_results;


-- Q8: Airport vs standard trips — which rate type earns more?
--     ratecodeid: 1=Standard, 2=JFK, 3=Newark,
--                 4=Nassau/Westchester, 5=Negotiated, 6=Group

DROP TABLE IF EXISTS q8_results;
CREATE EXTERNAL TABLE q8_results (
    trip_type      STRING,
    trip_count     BIGINT,
    avg_distance   DOUBLE,
    avg_fare       DOUBLE,
    avg_total      DOUBLE,
    avg_tip        DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q8_results';

INSERT INTO q8_results
SELECT
    CASE CAST(ratecodeid AS INT)
        WHEN 1 THEN '1_Standard'
        WHEN 2 THEN '2_JFK_Airport'
        WHEN 3 THEN '3_Newark_Airport'
        WHEN 4 THEN '4_Nassau_Westchester'
        WHEN 5 THEN '5_Negotiated'
        WHEN 6 THEN '6_Group_ride'
        ELSE        '7_Other'
    END                              AS trip_type,
    COUNT(*)                         AS trip_count,
    ROUND(AVG(trip_distance), 2)     AS avg_distance,
    ROUND(AVG(fare_amount),   2)     AS avg_fare,
    ROUND(AVG(total_amount),  2)     AS avg_total,
    ROUND(AVG(tip_amount),    2)     AS avg_tip
FROM yellow_taxi_trips_part_buck
WHERE year = 2025
GROUP BY CAST(ratecodeid AS INT)
ORDER BY avg_total DESC;

SELECT * FROM q8_results;



-- Q9: Revenue per mile by distance bucket — pricing efficiency


DROP TABLE IF EXISTS q9_results;
CREATE EXTERNAL TABLE q9_results (
    distance_bucket   STRING,
    trip_count        BIGINT,
    avg_distance      DOUBLE,
    avg_fare          DOUBLE,
    revenue_per_mile  DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q9_results';

INSERT INTO q9_results
SELECT
    CASE
        WHEN trip_distance < 1  THEN 'A_under_1mi'
        WHEN trip_distance < 3  THEN 'B_1to3mi'
        WHEN trip_distance < 7  THEN 'C_3to7mi'
        WHEN trip_distance < 15 THEN 'D_7to15mi'
        ELSE                         'E_over_15mi'
    END                                               AS distance_bucket,
    COUNT(*)                                          AS trip_count,
    ROUND(AVG(trip_distance),                    2)   AS avg_distance,
    ROUND(AVG(fare_amount),                      2)   AS avg_fare,
    ROUND(AVG(fare_amount / NULLIF(trip_distance, 0)), 2) AS revenue_per_mile
FROM yellow_taxi_trips_part_buck
WHERE year = 2025
  AND trip_distance > 0
GROUP BY
    CASE
        WHEN trip_distance < 1  THEN 'A_under_1mi'
        WHEN trip_distance < 3  THEN 'B_1to3mi'
        WHEN trip_distance < 7  THEN 'C_3to7mi'
        WHEN trip_distance < 15 THEN 'D_7to15mi'
        ELSE                         'E_over_15mi'
    END
ORDER BY distance_bucket;

SELECT * FROM q9_results;



-- Q10: Day-of-week — avoid date_format(...,'u'): pattern 'u' breaks on many Hive/Java builds.
--      We derive weekday from yyyy-MM-dd (first 10 chars of string TS) vs a fixed Sunday anchor.


DROP TABLE IF EXISTS q10_results;
CREATE EXTERNAL TABLE q10_results (
    day_of_week     STRING,
    trip_count      BIGINT,
    total_revenue   DOUBLE,
    avg_revenue     DOUBLE,
    avg_tip         DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q10_results';

INSERT INTO q10_results
SELECT
    CASE pmod(
            CAST(datediff(
                TO_DATE(SUBSTRING(FROM_UNIXTIME(CAST(tpep_pickup_datetime / 1000 AS BIGINT)), 1, 10)),
                CAST('1999-12-26' AS DATE)) AS INT),
            7)
        WHEN 0 THEN '1_Sunday'
        WHEN 1 THEN '2_Monday'
        WHEN 2 THEN '3_Tuesday'
        WHEN 3 THEN '4_Wednesday'
        WHEN 4 THEN '5_Thursday'
        WHEN 5 THEN '6_Friday'
        WHEN 6 THEN '7_Saturday'
        ELSE        'Unknown'
    END                              AS day_of_week,
    COUNT(*)                         AS trip_count,
    ROUND(SUM(total_amount), 2)      AS total_revenue,
    ROUND(AVG(total_amount), 2)      AS avg_revenue,
    ROUND(AVG(tip_amount),   2)      AS avg_tip
FROM yellow_taxi_trips_part_buck
WHERE year = 2025
  AND tpep_pickup_datetime IS NOT NULL
GROUP BY pmod(
    CAST(datediff(
        TO_DATE(SUBSTRING(FROM_UNIXTIME(CAST(tpep_pickup_datetime / 1000 AS BIGINT)), 1, 10)),
        CAST('1999-12-26' AS DATE)) AS INT),
    7)
ORDER BY day_of_week;

SELECT * FROM q10_results;



-- Q11: Top 10 busiest pickup locations in 2025


DROP TABLE IF EXISTS q11_results;
CREATE EXTERNAL TABLE q11_results (
    pulocationid   INT,
    pickup_count   BIGINT,
    avg_fare       DOUBLE,
    avg_tip        DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q11_results';

INSERT INTO q11_results
SELECT
    pulocationid,
    COUNT(*)                    AS pickup_count,
    ROUND(AVG(fare_amount), 2)  AS avg_fare,
    ROUND(AVG(tip_amount),  2)  AS avg_tip
FROM yellow_taxi_trips_part_buck
WHERE year = 2025
GROUP BY pulocationid
ORDER BY pickup_count DESC
LIMIT 10;

SELECT * FROM q11_results;



-- Q12: Late-night tipping — do night passengers tip more?

DROP TABLE IF EXISTS q12_results;
CREATE EXTERNAL TABLE q12_results (
    time_of_day   STRING,
    trip_count    BIGINT,
    avg_fare      DOUBLE,
    avg_tip       DOUBLE,
    tip_pct       DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'project/hive/warehouse/q12_results';

INSERT INTO q12_results
SELECT
    CASE
        WHEN pickup_hour BETWEEN  6 AND 11 THEN 'A_Morning_06to11'
        WHEN pickup_hour BETWEEN 12 AND 17 THEN 'B_Afternoon_12to17'
        WHEN pickup_hour BETWEEN 18 AND 22 THEN 'C_Evening_18to22'
        ELSE                                 'D_Night_23to05'
    END                                                        AS time_of_day,
    COUNT(*)                                                   AS trip_count,
    ROUND(AVG(fare_amount),                                2)  AS avg_fare,
    ROUND(AVG(tip_amount),                                2)    AS avg_tip,
    ROUND(AVG(tip_amount / NULLIF(fare_amount, 0)) * 100, 2) AS tip_pct
FROM (
    SELECT
        CAST(SUBSTRING(FROM_UNIXTIME(CAST(tpep_pickup_datetime / 1000 AS BIGINT)), 12, 2) AS INT) AS pickup_hour,
        fare_amount,
        tip_amount
    FROM yellow_taxi_trips_part_buck
    WHERE year = 2025
      AND tpep_pickup_datetime IS NOT NULL
      AND fare_amount > 0
      AND CAST(payment_type AS INT) = 1
) h
GROUP BY
    CASE
        WHEN pickup_hour BETWEEN  6 AND 11 THEN 'A_Morning_06to11'
        WHEN pickup_hour BETWEEN 12 AND 17 THEN 'B_Afternoon_12to17'
        WHEN pickup_hour BETWEEN 18 AND 22 THEN 'C_Evening_18to22'
        ELSE                                 'D_Night_23to05'
    END
ORDER BY tip_pct DESC;

SELECT * FROM q12_results;
