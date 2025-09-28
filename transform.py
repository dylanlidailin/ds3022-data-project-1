import duckdb
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def transform_data():
    """
    1. Calculate total CO2 output per trip by multiplying the trip_distance by the co2_grams_per_mile value in the vehicle_emissions lookup table, then dividing by 1000 (to calculate Kg). Insert that value as a new column named trip_co2_kgs. This calculation should be based upon a real-time lookup from the vehicle_emissions table and not hard-coded as a numeric figure.
    2. Calculate average miles per hour based on distance divided by the duration of the trip, and insert that value as a new column avg_mph.
    3. Extract the HOUR of the day from the pickup_time and insert it as a new column hour_of_day.
    4. Extract the DAY OF WEEK from the pickup time and insert it as a new column day_of_week.
    5. Extract the WEEK NUMBER from the pickup time and insert it as a new column week_of_year.
    6. Extract the MONTH from the pickup time and insert it as a new column month_of_year.
    Complete the transform.py script to perform these steps using python-based DuckDB commands. 
    For an additional 6 points, perform these steps using models in DBT. Save these files to dbt/models/.
    """

# Transform green taxi data
def transform_green_data():
    con = None
    try:
        con = duckdb.connect("emissions.duckdb")
        logger.info("Connected to emissions.duckdb database.")
        # Add trip_co2_kgs column
        query = """
            CREATE OR REPLACE TABLE green_taxi_data_clean AS
            SELECT
                t.*,
                (t.trip_distance * e.co2_grams_per_mile) / 1000 AS trip_co2_kgs,

                t.trip_distance / NULLIF(((EPOCH(lpep_dropoff_datetime) - EPOCH(lpep_pickup_datetime)) / 3600.0), 0) AS avg_mph,

                EXTRACT(HOUR FROM lpep_pickup_datetime) AS hour_of_day,
                EXTRACT(DOW FROM lpep_pickup_datetime) AS day_of_week,
                EXTRACT(WEEK FROM lpep_pickup_datetime) AS week_of_year,
                EXTRACT(MONTH FROM lpep_pickup_datetime) AS month_of_year

            FROM green_taxi_data_clean t
            JOIN vehicle_emissions e ON e.vehicle_type = 'green_taxi'
        """
        con.execute(query)
        logger.info("Transformation of green taxi data completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during transformation: {e}")

def transform_yellow_data():
    con = None
    try:
        con = duckdb.connect("emissions.duckdb")
        logger.info("Connected to emissions.duckdb database.")
        # Add trip_co2_kgs column
        query = """
            CREATE OR REPLACE TABLE yellow_taxi_data_clean AS
            SELECT
                t.*,
                (t.trip_distance * e.co2_grams_per_mile) / 1000 AS trip_co2_kgs,

                t.trip_distance / NULLIF(((EPOCH(tpep_dropoff_datetime) - EPOCH(tpep_pickup_datetime)) / 3600.0), 0) AS avg_mph,

                EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour_of_day,
                EXTRACT(DOW FROM tpep_pickup_datetime) AS day_of_week,
                EXTRACT(WEEK FROM tpep_pickup_datetime) AS week_of_year,
                EXTRACT(MONTH FROM tpep_pickup_datetime) AS month_of_year

            FROM yellow_taxi_data_clean t
            JOIN vehicle_emissions e ON e.vehicle_type = 'yellow_taxi'
        """
        con.execute(query)
        logger.info("Transformation of yellow taxi data completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during transformation: {e}")


if __name__ == "__main__":
    transform_green_data()
    transform_yellow_data()