-- NYC Yellow Taxi 2025 — relational model (Stage I)
-- Idempotent: safe to run multiple times (see also build_projectdb.py).

START TRANSACTION;

DROP TABLE IF EXISTS yellow_taxi_trips CASCADE;

CREATE TABLE yellow_taxi_trips (
    trip_id                 BIGSERIAL PRIMARY KEY,
    vendorid                INTEGER,
    tpep_pickup_datetime    TIMESTAMP WITHOUT TIME ZONE,
    tpep_dropoff_datetime   TIMESTAMP WITHOUT TIME ZONE,
    passenger_count         DOUBLE PRECISION,
    trip_distance           DOUBLE PRECISION,
    ratecodeid              DOUBLE PRECISION,
    store_and_fwd_flag      VARCHAR(8),
    pulocationid            INTEGER,
    dolocationid            INTEGER,
    payment_type            DOUBLE PRECISION,
    fare_amount             DOUBLE PRECISION,
    extra                   DOUBLE PRECISION,
    mta_tax                 DOUBLE PRECISION,
    tip_amount              DOUBLE PRECISION,
    tolls_amount            DOUBLE PRECISION,
    improvement_surcharge   DOUBLE PRECISION,
    total_amount            DOUBLE PRECISION,
    congestion_surcharge    DOUBLE PRECISION,
    airport_fee             DOUBLE PRECISION
);

COMMIT;
