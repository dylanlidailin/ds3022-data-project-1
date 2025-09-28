import duckdb
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)


def clean_green_taxi_data(con):
    """
    Cleans and verifies the green taxi dataset.
    """
    print("\n--- Cleaning and Verifying Green Taxi Data ---")
    
    logger.info("Cleaning data from 'green_taxi_data' into 'green_taxi_data_clean'.")
    con.execute("""
        CREATE OR REPLACE TABLE green_taxi_data_clean AS
        SELECT DISTINCT * FROM green_taxi_data
        WHERE passenger_count > 0
          AND trip_distance > 0
          AND trip_distance <= 100
          AND EPOCH(lpep_dropoff_datetime) - EPOCH(lpep_pickup_datetime) <= 86400
          AND EPOCH(lpep_dropoff_datetime) - EPOCH(lpep_pickup_datetime) > 0
    """)
    logger.info("Successfully created 'green_taxi_data_clean'.")

    # --- Verification ---
    min_passengers = con.execute("SELECT MIN(passenger_count) FROM green_taxi_data_clean;").fetchone()[0]
    print(f"✅ Minimum passenger count: {min_passengers} (Should be > 0)")

    dist_stats = con.execute("SELECT MIN(trip_distance), MAX(trip_distance) FROM green_taxi_data_clean;").fetchone()
    print(f"✅ Minimum trip distance: {dist_stats[0]} (Should be > 0)")
    print(f"✅ Maximum trip distance: {dist_stats[1]} (Should be <= 100)")

    duration_stats = con.execute("""
        SELECT 
            MIN(EPOCH(lpep_dropoff_datetime) - EPOCH(lpep_pickup_datetime)),
            MAX(EPOCH(lpep_dropoff_datetime) - EPOCH(lpep_pickup_datetime))
        FROM green_taxi_data_clean;
    """).fetchone()
    print(f"✅ Minimum trip duration (seconds): {duration_stats[0]} (Should be > 0)")
    print(f"✅ Maximum trip duration (seconds): {duration_stats[1]} (Should be <= 86400)")

    total_rows = con.execute("SELECT COUNT(*) FROM green_taxi_data_clean;").fetchone()[0]
    print(f"✅ Total rows in cleaned table: {total_rows}")


def clean_yellow_taxi_data(con):
    """
    Cleans and verifies the yellow taxi dataset.
    """
    print("\n--- Cleaning and Verifying Yellow Taxi Data ---")
    
    logger.info("Cleaning data from 'yellow_taxi_data' into 'yellow_taxi_data_clean'.")
    con.execute("""
        CREATE OR REPLACE TABLE yellow_taxi_data_clean AS
        SELECT DISTINCT * FROM yellow_taxi_data
        WHERE passenger_count > 0
          AND trip_distance > 0
          AND trip_distance <= 100
          AND EPOCH(tpep_dropoff_datetime) - EPOCH(tpep_pickup_datetime) <= 86400
          AND EPOCH(tpep_dropoff_datetime) - EPOCH(tpep_pickup_datetime) > 0
    """)
    logger.info("Successfully created 'yellow_taxi_data_clean'.")

    # --- Verification ---
    min_passengers = con.execute("SELECT MIN(passenger_count) FROM yellow_taxi_data_clean;").fetchone()[0]
    print(f"Minimum passenger count: {min_passengers} (Should be > 0)")

    dist_stats = con.execute("SELECT MIN(trip_distance), MAX(trip_distance) FROM yellow_taxi_data_clean;").fetchone()
    print(f"Minimum trip distance: {dist_stats[0]} (Should be > 0)")
    print(f"Maximum trip distance: {dist_stats[1]} (Should be <= 100)")

    duration_stats = con.execute("""
        SELECT 
            MIN(EPOCH(tpep_dropoff_datetime) - EPOCH(tpep_pickup_datetime)),
            MAX(EPOCH(tpep_dropoff_datetime) - EPOCH(tpep_pickup_datetime))
        FROM yellow_taxi_data_clean;
    """).fetchone()
    print(f"Minimum trip duration (seconds): {duration_stats[0]} (Should be > 0)")
    print(f"Maximum trip duration (seconds): {duration_stats[1]} (Should be <= 86400)")

    total_rows = con.execute("SELECT COUNT(*) FROM yellow_taxi_data_clean;").fetchone()[0]
    print(f"Total rows in cleaned table: {total_rows}")


def main():
    con = None
    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Successfully connected to DuckDB instance.")
        
        # Call the specific function for each dataset
        clean_green_taxi_data(con)
        clean_yellow_taxi_data(con)
        
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()