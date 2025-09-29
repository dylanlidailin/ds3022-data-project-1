SELECT yellow_taxi.*,
    (
        -- add column to calculate CO2 emissions per trip in kilograms
        yellow_taxi.trip_distance * (
            SELECT co2_grams_per_mile
            FROM vehicle_emissions
            WHERE vehicle_type = 'yellow_taxi'
        )
    ) / 1000 AS trip_co2_kgs,
    -- add column to calculate average speed in miles per hour
    trip_distance / (
        date_diff(
            'second',
            tpep_pickup_datetime,
            tpep_dropoff_datetime
        ) / 3600.0
    ) AS avg_mph,
    -- add column to calculate hour of the day
    CAST(strftime(tpep_pickup_datetime, '%H') AS INTEGER) AS hour_of_day,
    -- add column to calculate the day of the week (1=Monday, 7=Sunday)
    CAST(strftime(tpep_pickup_datetime, '%u') AS INTEGER) AS day_of_week,
    -- add column to extract week number of the year
    CAST(strftime(tpep_pickup_datetime, '%V') AS INTEGER) AS week_of_year,
    -- add column to extract month number of the year
    CAST(strftime(tpep_pickup_datetime, '%m') AS INTEGER) AS month_of_year
    -- add column to extract year number of the year
    CAST
FROM yellow_taxi