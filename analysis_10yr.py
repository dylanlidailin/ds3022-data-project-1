import duckdb
import logging

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='transform.log'
)
logger = logging.getLogger(__name__)
DB_FILE = "emissions10yrs.duckdb"

def transform_taxi_data(con, taxi_type):
    """
    Transforms cleaned taxi data by adding analytical columns.
    Creates a new, final table for analysis.
    """
    print(f"\n--- Transforming {taxi_type.capitalize()} Taxi Data ---")

    cleaned_table = f"{taxi_type}_taxi_trips_clean"
    final_table = f"{taxi_type}_taxi_final"
    date_column_prefix = 'tpep' if taxi_type == 'yellow' else 'lpep'
    pickup_col = f"{date_column_prefix}_pickup_datetime"
    dropoff_col = f"{date_column_prefix}_dropoff_datetime"
    
    try:
        logger.info(f"Transforming data from '{cleaned_table}' into '{final_table}'.")

        transform_query = f"""
            CREATE OR REPLACE TABLE {final_table} AS
            SELECT
                t.*,
                (t.trip_distance * e.co2_grams_per_mile) / 1000 AS trip_co2_kgs,
                t.trip_distance / NULLIF((EPOCH({dropoff_col}) - EPOCH({pickup_col})) / 3600.0, 0) AS avg_mph,
                hour({pickup_col}) AS hour_of_day,
                dayofweek({pickup_col}) AS day_of_week,
                weekofyear({pickup_col}) AS week_of_year,
                month({pickup_col}) AS month_of_year,
                year({pickup_col}) AS year -- <-- THIS LINE WAS ADDED

            FROM 
                {cleaned_table} t
            JOIN 
                vehicle_emissions e ON e.vehicle_type = '{taxi_type}'
        """
        
        con.execute(transform_query)
        
        final_count = con.execute(f"SELECT COUNT(*) FROM {final_table}").fetchone()[0]
        logger.info(f"Successfully created '{final_table}' with {final_count:,} rows.")
        print(f"Successfully created '{final_table}' with {final_count:,} rows.")

    except Exception as e:
        logger.error(f"An error occurred during transformation for {taxi_type} data: {e}")
        print(f"An error occurred during transformation for {taxi_type} data: {e}")

if __name__ == "__main__":
    con = None
    try:
        con = duckdb.connect(DB_FILE)
        transform_taxi_data(con, 'yellow')
        transform_taxi_data(con, 'green')
        

    except Exception as e:
        print(f"A fatal error occurred in the main process: {e}")
        logger.error(f"A fatal error occurred in the main process: {e}")
    finally:
        if con:
            con.close()