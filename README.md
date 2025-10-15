# DS3022 - New York Taxi Emission Analysis

<img src="https://s3.amazonaws.com/uvasds-systems/images/nyc-taxi-graphic.png" style="align:right;float:right;max-width:50%;">

This project demonstrates basic data engineering and data science skills. Using the freely available
NYC Trip Record data you will calculate CO2 output for rides within 2024 and perform some basic statistical
analysis based on transformations you add to these data.

Use the structure of this repository to submit your work. Complete the Python scripts as indicated below,
and add DBT model files within the appropriate subfolder.

Begin by forking this repository into your own account within GitHub. You will push change back to your own fork:

## Data

The data for this assignment are available from the NYC Taxi Commission Trip Record Data page:
**https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page**

I worked with ALL data for 2024 for both YELLOW and GREEN taxis. Each month is available as a Parquet file for each taxi type.

Assemble all trip data (Yellow and Green) into one or two DuckDB tables in a local DuckDB database. Given the full requirements of this project you should determine how to structure your data.

Complete the `load.py` script to create a local, persistent DuckDB database that creates and loads (at most) three tables:

1. A full table of YELLOW taxi trips for all of 2024.
2. A full table of GREEN taxi trips for all of 2024.
3. A lookup table of `vehicle_emissions` based on the included CSV file above.

My `load.py` script include output raw row counts for each of these tables, before cleaning. Recall that once a table exists (perhaps with a  `CREATE TABLE` query defining columns and data types), subsequent DuckDB commands like this will continue to load the same table:

```
-- Insert data from the 8th file
INSERT INTO my_table
SELECT * FROM read_parquet('file8.parquet');

-- Insert data from the 9th file
INSERT INTO my_table
SELECT * FROM read_parquet('file9.parquet');
```

**NOTE:** Given the redundancy of the examples above (nearly identical lines for each Parquet file) inserting multiple data sources into a single table should make use of **programmatic** means of iterating through the various sources, instead of **statically** coding individual INSERT statements, each for a separate data source.

## Clean

Trips should be cleaned and checked for the following conditions (whether or not they exist):

1. Remove any duplicate trips.
2. Remove any trips with `0` passengers.
3. Remove any trips 0 miles in length.
4. Remove any trips longer than 100 miles in length.
5. Remove any trips lasting more than 1 day in length (86400 seconds).

Complete the `clean.py` script to perform these steps and include code that checks/verifies that these conditions no longer exist in your trip table(s) within your DuckDB database. See [this reference](https://github.com/uvasds-systems/data-engineering-essentials/blob/main/synthetic/clean-data-answers.py) for examples.

## Transform

After cleaning you should have 1 or 2 cleaned trip tables representing YELLOW and GREEN trips for all of 2024. Perform the following transformations to the data:

1. Calculate total CO2 output per trip by multiplying the `trip_distance` by the `co2_grams_per_mile` value in the `vehicle_emissions` lookup table, then dividing by 1000 (to calculate Kg). Insert that value as a new column named `trip_co2_kgs`. This calculation should be based upon a real-time lookup from the `vehicle_emissions` table and not hard-coded as a numeric figure.
2. Calculate average miles per hour based on distance divided by the duration of the trip, and insert that value as a new column `avg_mph`.
3. Extract the HOUR of the day from the `pickup_time` and insert it as a new column `hour_of_day`.
4. Extract the DAY OF WEEK from the pickup time and insert it as a new column `day_of_week`.
5. Extract the WEEK NUMBER from the pickup time and insert it as a new column `week_of_year`.
6. Extract the MONTH from the pickup time and insert it as a new column `month_of_year`.


Complete the `transform.py` script to perform these steps using python-based DuckDB commands. For SQL reference see [this page](https://github.com/uvasds-systems/data-engineering-essentials/tree/main/transform) from earlier in the semester.

For an additional 6 points, perform these steps using models in DBT. Save these files to `dbt/models/`.

## Analyze

Complete the `analysis.py` script to report the following calculations using DuckDB/SQL. My Analysis is based on these questions:

1. What was the single largest carbon producing trip of the year for YELLOW and GREEN trips? (One result for each type)
2. Across the entire year, what on average are the most carbon heavy and carbon light hours of the day for YELLOW and for GREEN trips? (1-24)
3. Across the entire year, what on average are the most carbon heavy and carbon light days of the week for YELLOW and for GREEN trips? (Sun-Sat)
4. Across the entire year, what on average are the most carbon heavy and carbon light weeks of the year for YELLOW and for GREEN trips? (1-52)
5. Across the entire year, what on average are the most carbon heavy and carbon light months of the year for YELLOW and for GREEN trips? (Jan-Dec)
6. Use a plotting library of your choice (`matplotlib`, `seaborn`, etc.) to generate a time-series plot or histogram with MONTH
along the X-axis and CO2 totals along the Y-axis. Render two lines/bars/plots of data, one each for YELLOW and GREEN taxi trip CO2 totals.
