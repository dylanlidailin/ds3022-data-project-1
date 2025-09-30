import duckdb
import logging

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log',
)
logger = logging.getLogger(__name__)
DB_FILE = "emissions10yrs.duckdb"

def clean_green_taxi_data():
    """
    Cleans and slims the yellow taxi dataset plus verification
    """
    print("\n--- Cleaning and Verifying Green Taxi Data ---")
    con = None
    try:
        con = duckdb.connect(DB_FILE)
        source_table = "green_taxi_trips"
        cleaned_table = "green_taxi_trips_clean"
        
        logger.info(f"Cleaning and slimming data from '{source_table}' into '{cleaned_table}'.")
        
        con.execute(f"""
            CREATE OR REPLACE TABLE {cleaned_table} AS
            SELECT DISTINCT
                lpep_pickup_datetime,
                lpep_dropoff_datetime,
                passenger_count,
                trip_distance               -- To reduce size, only select necessary columns for cleaning
            FROM 
                {source_table}
            WHERE 
                passenger_count > 0
                AND trip_distance > 0 AND trip_distance <= 100
                AND EPOCH(lpep_dropoff_datetime) - EPOCH(lpep_pickup_datetime) BETWEEN 1 AND 86400
        """)
        logger.info(f"Successfully created '{cleaned_table}'.")

        # --- Verification ---
        min_passengers = con.execute(f"SELECT MIN(passenger_count) FROM {cleaned_table};").fetchone()[0]
        print(f"Minimum passenger count: {min_passengers} (Should be > 0)")

        dist_stats = con.execute(f"SELECT MIN(trip_distance), MAX(trip_distance) FROM {cleaned_table};").fetchone()
        print(f"Minimum trip distance: {dist_stats[0]} (Should be > 0)")
        print(f"Maximum trip distance: {dist_stats[1]} (Should be <= 100)")

        duration_stats = con.execute(f"""
            SELECT 
                MIN(EPOCH(lpep_dropoff_datetime) - EPOCH(lpep_pickup_datetime)),
                MAX(EPOCH(lpep_dropoff_datetime) - EPOCH(lpep_pickup_datetime))
            FROM {cleaned_table};
        """).fetchone()
        print(f"Minimum trip duration (seconds): {duration_stats[0]} (Should be > 0)")
        print(f"Maximum trip duration (seconds): {duration_stats[1]} (Should be <= 86400)")

        total_rows = con.execute(f"SELECT COUNT(*) FROM {cleaned_table};").fetchone()[0]
        print(f"Total rows in cleaned table: {total_rows:,}")

    except Exception as e:
        print(f"An error occurred while cleaning green taxi data: {e}")
        logger.error(f"An error occurred while cleaning green taxi data: {e}")
    finally:
        if con:
            con.close()

def clean_yellow_taxi_data():
    """
    Cleans and slims the yellow taxi dataset plus verficiation
    """
    print("\n--- Cleaning and Verifying Yellow Taxi Data ---")
    con = None
    try:
        con = duckdb.connect(DB_FILE)
        con.execute("PRAGMA max_temp_directory_size = '20GiB'")
        source_table = "yellow_taxi_trips"
        cleaned_table = "yellow_taxi_trips_clean"

        logger.info(f"Cleaning and slimming data from '{source_table}' into '{cleaned_table}'.")
        
        con.execute(f"""
            CREATE OR REPLACE TABLE {cleaned_table} AS
            SELECT DISTINCT
                tpep_pickup_datetime,
                tpep_dropoff_datetime,
                passenger_count,
                trip_distance
            FROM 
                {source_table}
            WHERE 
                passenger_count > 0
                AND trip_distance > 0 AND trip_distance <= 100
                AND EPOCH(tpep_dropoff_datetime) - EPOCH(tpep_pickup_datetime) BETWEEN 1 AND 86400
        """)
        logger.info(f"Successfully created '{cleaned_table}'.")

        # --- Verification ---
        min_passengers = con.execute(f"SELECT MIN(passenger_count) FROM {cleaned_table};").fetchone()[0]
        print(f"Minimum passenger count: {min_passengers} (Should be > 0)")

        dist_stats = con.execute(f"SELECT MIN(trip_distance), MAX(trip_distance) FROM {cleaned_table};").fetchone()
        print(f"Minimum trip distance: {dist_stats[0]} (Should be > 0)")
        print(f"Maximum trip distance: {dist_stats[1]} (Should be <= 100)")

        duration_stats = con.execute(f"""
            SELECT 
                MIN(EPOCH(tpep_dropoff_datetime) - EPOCH(tpep_pickup_datetime)),
                MAX(EPOCH(tpep_dropoff_datetime) - EPOCH(tpep_pickup_datetime))
            FROM {cleaned_table};
        """).fetchone()
        print(f"Minimum trip duration (seconds): {duration_stats[0]} (Should be > 0)")
        print(f"Maximum trip duration (seconds): {duration_stats[1]} (Should be <= 86400)")

        total_rows = con.execute(f"SELECT COUNT(*) FROM {cleaned_table};").fetchone()[0]
        print(f"Total rows in cleaned table: {total_rows:,}")

    except Exception as e:
        print(f"An error occurred while cleaning yellow taxi data: {e}")
        logger.error(f"An error occurred while cleaning yellow taxi data: {e}")

if __name__ == "__main__":
    clean_green_taxi_data()
    clean_yellow_taxi_data()

    con = duckdb.connect(DB_FILE)
    try:
            con.execute("DROP TABLE green_taxi_trips;")
            print("Dropped table 'green_taxi_trips'.")
            logger.info("Dropped table 'green_taxi_trips'.")
            
            con.execute("DROP TABLE yellow_taxi_trips;")
            print("Dropped table 'yellow_taxi_trips'.")
            logger.info("Dropped table 'yellow_taxi_trips'.")

    except Exception as e:
        # This will catch errors from the cleaning functions if they fail
        print(f"\nProcess stopped due to an error. Original tables were NOT dropped.")
        logger.error(f"Process stopped. Original tables were NOT dropped.")