-- 1. Clean up and create new database
DROP DATABASE IF EXISTS team35_projectdb CASCADE;
CREATE DATABASE team35_projectdb LOCATION 'project/hive/warehouse';
USE team35_projectdb;

-- 2. Create external table pointing to AVRO files imported by Sqoop
CREATE EXTERNAL TABLE yellow_taxi_trips
STORED AS AVRO
LOCATION 'project/warehouse/yellow_taxi_trips'
TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/yellow_taxi_trips.avsc');


-- Сheck if data loaded correctly
SHOW TABLES;
DESCRIBE yellow_taxi_trips;
SELECT * FROM yellow_taxi_trips LIMIT 5;

-- 3. Enable dynamic partitioning - allows Hive to create partitions automatically
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 4. Drop old partitioned table if it exists
DROP TABLE IF EXISTS yellow_taxi_trips_part_buck;

-- 5. Create partitioned + bucketed external table for better performance
-- Datetime columns MUST stay BIGINT (milliseconds) to match Avro schema (type "long").
-- Declaring TIMESTAMP here while TBLPROPERTIES still point at yellow_taxi_trips.avsc
-- makes the Avro SerDe read/write NULL for those fields — all time-based EDA then looks empty.

CREATE EXTERNAL TABLE yellow_taxi_trips_part_buck (
    trip_id BIGINT,
    vendorid INT,
    tpep_pickup_datetime BIGINT,
    tpep_dropoff_datetime BIGINT,
    passenger_count DOUBLE,
    trip_distance DOUBLE,
    ratecodeid DOUBLE,
    store_and_fwd_flag STRING,
    pulocationid INT,
    dolocationid INT,
    payment_type DOUBLE,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE
)
PARTITIONED BY (month INT)
CLUSTERED BY (vendorid) INTO 4 BUCKETS
STORED AS AVRO
LOCATION 'project/hive/warehouse/yellow_taxi_trips_part_buck'
TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/yellow_taxi_trips.avsc');

-- 6. Load data with automatic partition creation
INSERT INTO yellow_taxi_trips_part_buck PARTITION (month)
SELECT
    trip_id,
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    MONTH(FROM_UNIXTIME(CAST(tpep_pickup_datetime / 1000 AS BIGINT))) AS month
FROM yellow_taxi_trips;


SHOW PARTITIONS yellow_taxi_trips_part_buck;

-- Count total records to ensure all data was transferred
SELECT COUNT(*) FROM yellow_taxi_trips_part_buck;

-- Sample data (human-readable pickup time from epoch ms)
SELECT trip_id,
       vendorid,
       FROM_UNIXTIME(CAST(tpep_pickup_datetime / 1000 AS BIGINT))   AS pickup_ts,
       FROM_UNIXTIME(CAST(tpep_dropoff_datetime / 1000 AS BIGINT))   AS dropoff_ts,
       month
FROM yellow_taxi_trips_part_buck
LIMIT 3;

DROP TABLE yellow_taxi_trips;