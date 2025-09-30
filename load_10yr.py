import duckdb
import os
import logging
import time
import pandas as pd

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

DB_FILE = "emissions10yrs.duckdb"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Loading yellow taxi's emission data
def load_yellow_taxi_data(con):
    """
    The function loads yellow taxi data using DuckDB's direct read_parquet('url') method.
    This version no longer uses a User-Agent header.
    """
    taxi_type = "yellow"
    table_name = "yellow_taxi_trips"
    years = range(2015, 2025)
    
    logger.info(f"--- Starting to load data for {table_name} using direct URL method ---")
    
    try:
        schema_url = f"{BASE_URL}/{taxi_type}_tripdata_2024-01.parquet"
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS 
            SELECT * FROM read_parquet('{schema_url}') LIMIT 0;
        """)
        logger.info(f"Successfully created empty table '{table_name}'.")

    except Exception as e:
        logger.critical(f"Could not create table schema for {table_name}. Aborting. Error: {e}")
        return

    total_inserted_count = 0
    for year in years:
        for month in range(1, 13):
            url = f"{BASE_URL}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            logger.info(f"Processing {url}...")
            
            try:
                result = con.execute(f"""
                    INSERT INTO {table_name}
                    SELECT * FROM read_parquet('{url}')
                """).fetchone()

                inserted_for_month = result[0] if result else 0
                total_inserted_count += inserted_for_month
                logger.info(f"Successfully inserted {inserted_for_month:,} records for {year}-{month:02d}.")
            except Exception as e:
                logger.warning(f"Could not load data for {year}-{month:02d} (yellow). Skipping. Error: {e}")
                
    logger.info(f"--- Finished loading for {table_name}. Total records inserted: {total_inserted_count:,} ---")


def load_green_taxi_data(con):
    """
    Loads green taxi data using DuckDB's fast, direct read_parquet('url') method.
    Includes a 10-second pause after each file request.
    """
    taxi_type = "green"
    table_name = "green_taxi_trips"
    years = range(2015, 2025)
    
    logger.info(f"--- Starting to load data for {table_name} using direct URL method ---")
    
    try:
        schema_url = f"{BASE_URL}/{taxi_type}_tripdata_2024-01.parquet"
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS 
            SELECT * FROM read_parquet('{schema_url}') LIMIT 0;
        """)
        logger.info(f"Successfully created empty table '{table_name}'.")
    except Exception as e:
        logger.critical(f"Could not create table schema for {table_name}. Aborting. Error: {e}")
        return

    total_inserted_count = 0
    for year in years:
        for month in range(1, 13):
            url = f"{BASE_URL}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            logger.info(f"Processing {url}...")
            
            try:
                result = con.execute(f"""
                    INSERT INTO {table_name}
                    SELECT * FROM read_parquet('{url}')
                """).fetchone()

                inserted_for_month = result[0] if result else 0
                total_inserted_count += inserted_for_month
                logger.info(f"Successfully inserted {inserted_for_month:,} records for {year}-{month:02d}.")
            except Exception as e:
                logger.warning(f"Could not load data for {year}-{month:02d} (green). Skipping. Error: {e}")
            
                
    logger.info(f"--- Finished loading for {table_name}. Total records inserted: {total_inserted_count:,} ---")


def create_emissions_lookup(con, csv_path):
    """
    Creates a lookup table for vehicle emissions by loading data from a CSV file.
    
    Args:
        con: An active DuckDB connection.
        csv_path (str): The file path to the vehicle_emissions.csv file.
    """
    logger.info(f"Creating or replacing 'vehicle_emissions' table from {csv_path}...")
    try:
        # Read the CSV file into a pandas DataFrame
        emissions_df = pd.read_csv(csv_path)
        
        # Create a permanent table in DuckDB directly from the DataFrame
        con.execute("""
            CREATE OR REPLACE TABLE vehicle_emissions AS 
            SELECT * FROM emissions_df
        """)

        # Verification
        count = con.execute("SELECT COUNT(*) FROM vehicle_emissions").fetchone()[0]
        logger.info(f"Successfully created 'vehicle_emissions' table with {count} records.")
        print(f"Loaded {count} vehicle emission records.")
        
    except Exception as e:
        logger.error(f"Failed to create vehicle_emissions table from CSV. Error: {e}")
        print(f"Failed to create vehicle_emissions table from CSV. Error: {e}")


def summarize_data(con):
    """
    Summarizes the data for yellow and green taxi trips in the database.
    """
    logger.info("--- Starting Data Summarization ---")
    try:
        # Loop through each taxi type
        for taxi_type in ['yellow', 'green']:
            table_name = f"{taxi_type}_taxi_trips"
            
            # Select the correct timestamp column based on the taxi type
            if taxi_type == 'yellow':
                date_column = 'tpep_pickup_datetime'
            else:
                date_column = 'lpep_pickup_datetime'

            # Build a single, dynamic SQL query
            query = f"""
                SELECT 
                    COUNT(*) as total_trips,
                    MIN({date_column}) as earliest_trip,
                    MAX({date_column}) as latest_trip,
                    AVG(trip_distance) as avg_distance,
                    SUM(trip_distance) as total_distance
                FROM {table_name}
            """
            
            # Execute the query and fetch the results
            stats = con.execute(query).fetchone()

            if stats and stats[0] > 0:
                # --- Log the summary ---
                logger.info(f"--- Summary for {table_name} ---")
                logger.info(f"Total Trips: {stats[0]:,}")
                logger.info(f"Date Range: {stats[1]} to {stats[2]}")
                logger.info(f"Average Trip Distance: {stats[3]:.2f} miles")
                logger.info(f"Total Trip Distance: {stats[4]:,.0f} miles")
                
                # --- Print the summary to the console ---
                print(f"--- Summary for {table_name} ---")
                print(f"Total Trips: {stats[0]:,}")
                print(f"Date Range: {stats[1]} to {stats[2]}")
                print(f"Average Trip Distance: {stats[3]:.2f} miles")
                print(f"Total Trip Distance: {stats[4]:,.0f} miles")
            else:
                logger.warning(f"No data found for {table_name} to summarize.")
                print(f"--- No data found for {table_name} ---")

    except Exception as e:
        logger.error(f"An error occurred during data summarization: {e}")
        print(f"An error occurred during data summarization: {e}")


# --- Main Execution ---
def main():
    """Main function to orchestrate the data loading process."""

    #if os.path.exists(DB_FILE):
        #logger.warning(f"Database file '{DB_FILE}' already exists. Removing it for a fresh start.")
        #os.remove(DB_FILE)

    con = None
    try:

        con = duckdb.connect(database=DB_FILE)
        logger.info(f"Successfully connected to DuckDB at '{DB_FILE}'")

        load_yellow_taxi_data(con)
        
        logger.info("Pausing for 5 seconds before loading green taxi data...")
        #time.sleep(5)

        load_green_taxi_data(con)
        summarize_data(con)
        emissions_csv_path = 'data/vehicle_emissions.csv'
        create_emissions_lookup(con, emissions_csv_path)

    except Exception as e:
        logger.critical(f"A critical error occurred during the main process: {e}")

if __name__ == "__main__":
    main()