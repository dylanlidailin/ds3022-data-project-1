import duckdb
import logging

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='transform.log',
)
logger = logging.getLogger(__name__)
DB_FILE = "emissions10yrs.duckdb"

def transform_taxi_data(con, taxi_type):
    """
    Transforms cleaned taxi data by adding analytical columns.
    Creates a new, final table for analysis.

    Args:
        con: An active DuckDB connection.
        taxi_type (str): The type of taxi data to transform ('yellow' or 'green').
    """
    print(f"\n--- Transforming {taxi_type.capitalize()} Taxi Data ---")

    # Define table names and date columns dynamically
    cleaned_table = f"{taxi_type}_taxi_trips_clean"
    final_table = f"{taxi_type}_taxi_final"
    date_column_prefix = 'tpep' if taxi_type == 'yellow' else 'lpep'
    pickup_col = f"{date_column_prefix}_pickup_datetime"
    dropoff_col = f"{date_column_prefix}_dropoff_datetime"
    
    try:
        logger.info(f"Transforming data from '{cleaned_table}' into '{final_table}'.")

        # This query creates a new table with all transformations applied.
        # It reads from the _clean table and writes to the _final table.
        transform_query = f"""
            CREATE OR REPLACE TABLE {final_table} AS
            SELECT
                t.*, -- Select all columns from the clean table

                -- 1. Calculate CO2 in kgs using a real-time lookup
                (t.trip_distance * e.co2_grams_per_mile) / 1000 AS trip_co2_kgs,

                -- 2. Calculate average speed, safely handling zero-duration trips
                t.trip_distance / NULLIF((EPOCH({dropoff_col}) - EPOCH({pickup_col})) / 3600.0, 0) AS avg_mph,

                -- 3. Extract HOUR using the simpler hour() function
                hour({pickup_col}) AS hour_of_day,

                -- 4. Extract DAY OF WEEK (1=Sunday, 7=Saturday)
                dayofweek({pickup_col}) AS day_of_week,

                -- 5. Extract WEEK NUMBER
                weekofyear({pickup_col}) AS week_of_year,

                -- 6. Extract MONTH
                month({pickup_col}) AS month_of_year

            FROM 
                {cleaned_table} t
            JOIN 
                vehicle_emissions e ON e.vehicle_type = '{taxi_type}'
        """
        
        con.execute(transform_query)
        
        # Verification
        final_count = con.execute(f"SELECT COUNT(*) FROM {final_table}").fetchone()[0]
        logger.info(f"Successfully created '{final_table}' with {final_count:,} rows.")
        print(f"Successfully created '{final_table}' with {final_count:,} rows.")

    except Exception as e:
        logger.error(f"An error occurred during transformation for {taxi_type} data: {e}")
        print(f"An error occurred during transformation for {taxi_type} data: {e}")

if __name__ == "__main__":
    con = None
    try:
        # Connect to the database once
        con = duckdb.connect(DB_FILE)
        
        # Call the reusable function for each taxi type
        transform_taxi_data(con, 'yellow')
        transform_taxi_data(con, 'green')

        con.execute("DROP TABLE yellow_taxi_trips_clean;")
        print("Dropped table 'yellow_taxi_trips_clean'.")
        logger.info("Dropped table 'yellow_taxi_trips_clean'.")
        
        con.execute("DROP TABLE green_taxi_trips_clean;")
        print("Dropped table 'green_taxi_trips_clean'.")
        logger.info("Dropped table 'green_taxi_trips_clean'.")
        
    except Exception as e:
        print(f"A fatal error occurred in the main process: {e}")
        logger.error(f"A fatal error occurred in the main process: {e}")