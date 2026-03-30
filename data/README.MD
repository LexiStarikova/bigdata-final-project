# Dataset: NYC Yellow Taxi Trip Records 2023

## Source
NYC Taxi & Limousine Commission (TLC) Trip Record Data  
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Download
Files are hosted at:
```
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-{MM}.parquet
```
where `{MM}` is the two-digit month (01–12).

To download all 12 months of 2023:
```bash
for m in 01 02 03 04 05 06 07 08 09 10 11 12; do
  curl -O "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-${m}.parquet"
done
```

## Statistics
| Property | Value |
|---|---|
| Files | 12 (one per month, Jan–Dec 2023) |
| Total size | ~607 MB (Parquet) |
| Total records | ~38.3 million |
| Format | Apache Parquet |

## Schema

| Column | Type | Description |
|---|---|---|
| VendorID | int | TPEP provider (1=Creative Mobile, 2=VeriFone) |
| tpep_pickup_datetime | datetime | Meter-engaged timestamp |
| tpep_dropoff_datetime | datetime | Meter-disengaged timestamp |
| passenger_count | float | Number of passengers (driver-entered) |
| trip_distance | float | Trip distance in miles |
| RatecodeID | float | Rate code (1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group) |
| store_and_fwd_flag | string | Whether trip record held in vehicle memory before send (Y/N) |
| PULocationID | int | TLC Taxi Zone pickup location ID |
| DOLocationID | int | TLC Taxi Zone dropoff location ID |
| payment_type | int | Payment method (1=Credit, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided) |
| fare_amount | float | Meter fare |
| extra | float | Miscellaneous extras and surcharges |
| mta_tax | float | MTA tax |
| tip_amount | float | Tip amount (auto-populated for credit card tips) |
| tolls_amount | float | Total tolls paid |
| improvement_surcharge | float | Improvement surcharge |
| total_amount | float | Total charged to passengers |
| congestion_surcharge | float | Congestion surcharge |
| airport_fee | float | Airport pickup fee |

## ML Task
**Binary Classification**: Predict whether a passenger will tip generously  
(tip_ratio = tip_amount / fare_amount > 0.20 → label = 1, else label = 0)

Features used:
- `trip_distance`, `passenger_count`, `fare_amount`, `RatecodeID`, `VendorID`
- `PULocationID`, `DOLocationID`
- Derived: `trip_duration_min`, `pickup_hour`, `pickup_dayofweek`, `pickup_month`, `is_weekend`
