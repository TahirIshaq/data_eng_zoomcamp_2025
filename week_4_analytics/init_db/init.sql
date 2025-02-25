CREATE SCHEMA IF NOT EXISTS raw_data;

CREATE TABLE IF NOT EXISTS raw_data.green_taxi (
    vendor_id               INTEGER,
    lpep_pickup_datetime    TIMESTAMP,
    lpep_dropoff_datetime   TIMESTAMP,
    store_and_fwd_flag      TEXT,
    ratecode_id             INTEGER,
    pu_location_id          INTEGER,
    do_location_id          INTEGER,
    passenger_count         INTEGER,
    trip_distance           DOUBLE PRECISION,
    fare_amount             DOUBLE PRECISION,
    extra                   DOUBLE PRECISION,
    mta_tax                 DOUBLE PRECISION,
    tip_amount              DOUBLE PRECISION,
    tolls_amount            DOUBLE PRECISION,
    ehail_fee               DOUBLE PRECISION,
    improvement_surcharge   DOUBLE PRECISION,
    total_amount            DOUBLE PRECISION,
    payment_type            INTEGER,
    trip_type               INTEGER,
    congestion_surcharge    DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS raw_data.yellow_taxi (
    vendor_id               INTEGER,
    tpep_pickup_datetime    TIMESTAMP,
    tpep_dropoff_datetime   TIMESTAMP,
    passenger_count         INTEGER,
    trip_distance           DOUBLE PRECISION,
    ratecode_id             INTEGER,
    store_and_fwd_flag      TEXT,
    pu_location_id          INTEGER,
    do_location_id          INTEGER,
    payment_type            INTEGER,
    fare_amount             DOUBLE PRECISION,
    extra                   DOUBLE PRECISION,
    mta_tax                 DOUBLE PRECISION,
    tip_amount              DOUBLE PRECISION,
    tolls_amount            DOUBLE PRECISION,
    improvement_surcharge   DOUBLE PRECISION,
    total_amount            DOUBLE PRECISION,
    congestion_surcharge    DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS raw_data.fhv_taxi (
    dispatching_base_num    TEXT,
    pickup_datetime         TIMESTAMP,
    dropOff_datetime        TIMESTAMP,
    pu_location_id          INTEGER,
    do_location_id          INTEGER,
    sr_flag                 INTEGER,
    affiliated_base_number  TEXT
);