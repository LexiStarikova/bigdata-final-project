-- Verification queries (executed by scripts/build_projectdb.py)
SELECT COUNT(*) AS yellow_taxi_trips_count FROM yellow_taxi_trips;
SELECT * FROM yellow_taxi_trips ORDER BY trip_id LIMIT 5;
