CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS staging.stg_yellow_taxi_trips;

CREATE TABLE staging.stg_yellow_taxi_trips (
    vendor_id INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count NUMERIC,
    trip_distance NUMERIC,
    ratecode_id NUMERIC,
    store_and_fwd_flag VARCHAR(10),
    pickup_location_id INTEGER,
    dropoff_location_id INTEGER,
    payment_type_id INTEGER,
    fare_amount NUMERIC,
    extra NUMERIC,
    mta_tax NUMERIC,
    tip_amount NUMERIC,
    tolls_amount NUMERIC,
    improvement_surcharge NUMERIC,
    total_amount NUMERIC,
    congestion_surcharge NUMERIC,
    airport_fee NUMERIC,
    source_file VARCHAR(255),
    load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP VIEW IF EXISTS mart.v_daily_taxi_report;

DROP TABLE IF EXISTS dwh.fact_trips CASCADE;
DROP TABLE IF EXISTS dwh.dim_date CASCADE;
DROP TABLE IF EXISTS dwh.dim_vendor CASCADE;
DROP TABLE IF EXISTS dwh.dim_payment_type CASCADE;
DROP TABLE IF EXISTS dwh.dim_location CASCADE;
DROP TABLE IF EXISTS dwh.data_quality_results CASCADE;

CREATE TABLE dwh.dim_date (
    date_id INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20),
    is_weekend BOOLEAN
);

CREATE TABLE dwh.dim_vendor (
    vendor_id INTEGER PRIMARY KEY,
    vendor_name VARCHAR(100) NOT NULL
);

CREATE TABLE dwh.dim_payment_type (
    payment_type_id INTEGER PRIMARY KEY,
    payment_type_name VARCHAR(100) NOT NULL
);

CREATE TABLE dwh.dim_location (
    location_id INTEGER PRIMARY KEY,
    borough VARCHAR(100),
    zone VARCHAR(255),
    service_zone VARCHAR(100)
);

CREATE TABLE dwh.fact_trips (
    trip_id BIGSERIAL PRIMARY KEY,
    vendor_id INTEGER REFERENCES dwh.dim_vendor(vendor_id),
    pickup_date_id INTEGER REFERENCES dwh.dim_date(date_id),
    dropoff_date_id INTEGER REFERENCES dwh.dim_date(date_id),
    payment_type_id INTEGER REFERENCES dwh.dim_payment_type(payment_type_id),
    pickup_location_id INTEGER REFERENCES dwh.dim_location(location_id),
    dropoff_location_id INTEGER REFERENCES dwh.dim_location(location_id),
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    passenger_count NUMERIC,
    trip_distance NUMERIC,
    fare_amount NUMERIC,
    tip_amount NUMERIC,
    tolls_amount NUMERIC,
    total_amount NUMERIC,
    trip_duration_minutes NUMERIC,
    source_row_hash VARCHAR(64),
    load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dwh.data_quality_results (
    check_id BIGSERIAL PRIMARY KEY,
    check_name VARCHAR(255) NOT NULL,
    check_status VARCHAR(50) NOT NULL,
    checked_rows INTEGER,
    failed_rows INTEGER,
    check_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    comment TEXT
);

CREATE VIEW mart.v_daily_taxi_report AS
SELECT
    dd.full_date AS report_date,
    COUNT(ft.trip_id) AS trips_count,
    SUM(ft.total_amount) AS total_revenue,
    AVG(ft.total_amount) AS avg_trip_amount,
    AVG(ft.trip_distance) AS avg_trip_distance,
    AVG(ft.trip_duration_minutes) AS avg_trip_duration_minutes
FROM dwh.fact_trips ft
JOIN dwh.dim_date dd
    ON ft.pickup_date_id = dd.date_id
GROUP BY dd.full_date
ORDER BY dd.full_date;

CREATE INDEX IF NOT EXISTS idx_fact_trips_pickup_date
ON dwh.fact_trips(pickup_date_id);

CREATE INDEX IF NOT EXISTS idx_fact_trips_payment_type
ON dwh.fact_trips(payment_type_id);

CREATE INDEX IF NOT EXISTS idx_fact_trips_pickup_location
ON dwh.fact_trips(pickup_location_id);

CREATE INDEX IF NOT EXISTS idx_fact_trips_dropoff_location
ON dwh.fact_trips(dropoff_location_id);