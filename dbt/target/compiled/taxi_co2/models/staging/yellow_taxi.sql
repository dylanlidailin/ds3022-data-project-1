

WITH trips AS (
    -- Source: cleaned yellow taxi trips
    SELECT *
    FROM "emissions10yrs"."main"."yellow_taxi_trips_clean"
),

emissions AS (
    -- Source: vehicle emissions lookup table
    SELECT *
    FROM "emissions10yrs"."main"."vehicle_emissions"
    WHERE vehicle_type = 'yellow'  -- Use the correct vehicle_type
)

SELECT
    t.*, -- Include all original columns from the cleaned table

    -- Calculate CO2 emissions in kilograms
    (t.trip_distance * e.co2_grams_per_mile) / 1000.0 AS trip_co2_kgs,

    -- Calculate average MPH, safely handling zero-duration trips
    t.trip_distance / NULLIF((EPOCH(t.tpep_dropoff_datetime) - EPOCH(t.tpep_pickup_datetime)) / 3600.0, 0) AS avg_mph,

    -- Extract time-based features using simpler DuckDB functions
    hour(t.tpep_pickup_datetime) AS hour_of_day,
    dayofweek(t.tpep_pickup_datetime) AS day_of_week,
    weekofyear(t.tpep_pickup_datetime) AS week_of_year,
    month(t.tpep_pickup_datetime) AS month_of_year,
    year(t.tpep_pickup_datetime) AS year
FROM 
    trips t
CROSS JOIN 
    emissions e