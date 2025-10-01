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
EMISSIONS_CSV_PATH = 'data/vehicle_emissions.csv'


def load_taxi_data(con, taxi_type):
    """
    Loads taxi data for a specific type (yellow or green) into the database.
    Includes a pause after each file download to rate limit requests.
    """
    table_name = f"{taxi_type}_taxi_trips"
    years = range(2015, 2025)
    
    logger.info(f"--- Starting to load data for {table_name} ---")
    
    # Create an empty table based on the schema of a recent file
    try:
        schema_url = f"{BASE_URL}/{taxi_type}_tripdata_2024-01.parquet"
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} AS 
            SELECT * FROM read_parquet('{schema_url}') LIMIT 0;
        """)
        logger.info(f"Successfully created empty table '{table_name}'.")
    except Exception as e:
        logger.critical(f"Could not create table schema for {table_name}. Aborting. Error: {e}")
        return

    total_inserted_count = 0
    # Loop through all years and months to load data
    for year in years:
        for month in range(1, 13):
            url = f"{BASE_URL}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            logger.info(f"Processing {url}...")
            
            try:
                # Insert data directly from the URL into the table
                result = con.execute(f"""
                    INSERT INTO {table_name}
                    SELECT * FROM read_parquet('{url}')
                """).fetchone()

                inserted_for_month = result[0] if result else 0
                total_inserted_count += inserted_for_month
                logger.info(f"Successfully inserted {inserted_for_month:,} records for {year}-{month:02d}.")

                # --- ADDED SLEEP ---
                time.sleep(10)

            except Exception as e:
                logger.warning(f"Could not load data for {year}-{month:02d} ({taxi_type}). Skipping. Error: {e}")
                
    logger.info(f"--- Finished loading for {table_name}. Total records inserted: {total_inserted_count:,} ---")


def create_emissions_lookup(con, csv_path):
    """
    Creates a lookup table for vehicle emissions from a local CSV file.
    """
    logger.info(f"Creating or replacing 'vehicle_emissions' table from {csv_path}...")
    try:
        emissions_df = pd.read_csv(csv_path)
        con.execute("CREATE OR REPLACE TABLE vehicle_emissions AS SELECT * FROM emissions_df")
        count = con.execute("SELECT COUNT(*) FROM vehicle_emissions").fetchone()[0]
        logger.info(f"Successfully created 'vehicle_emissions' table with {count} records.")
        print(f"Loaded {count} vehicle emission records.")
    except Exception as e:
        logger.error(f"Failed to create vehicle_emissions table from CSV. Error: {e}")
        print(f"Failed to create vehicle_emissions table from CSV. Error: {e}")


def summarize_data(con):
    """
    Calculates and prints a summary for both yellow and green taxi data.
    """
    logger.info("--- Starting Data Summarization ---")
    for taxi_type in ['yellow', 'green']:
        table_name = f"{taxi_type}_taxi_trips"
        date_column = 'tpep_pickup_datetime' if taxi_type == 'yellow' else 'lpep_pickup_datetime'
        try:
            stats = con.execute(f"""
                SELECT COUNT(*), MIN({date_column}), MAX({date_column}), AVG(trip_distance), SUM(trip_distance)
                FROM {table_name}
            """).fetchone()

            if stats and stats[0] > 0:
                summary = {
                    "Total Trips": f"{stats[0]:,}",
                    "Date Range": f"{stats[1]} to {stats[2]}",
                    "Average Trip Distance": f"{stats[3]:.2f} miles",
                    "Total Trip Distance": f"{stats[4]:,.0f} miles"
                }
                logger.info(f"Summary for {table_name}: {summary}")
                print(f"\n--- Summary for {table_name} ---")
                for key, value in summary.items():
                    print(f"{key}: {value}")
            else:
                logger.warning(f"No data found for {table_name} to summarize.")
                print(f"\n--- No data found for {table_name} ---")
        except Exception as e:
            logger.error(f"Could not summarize {table_name}. Error: {e}")
            print(f"\nCould not summarize {table_name}. Error: {e}")


# --- Main Execution Block ---
def main():
    """Main function to orchestrate the entire data loading pipeline."""
    con = None
    try:
        print("--- Starting Data Loading Pipeline ---")
        con = duckdb.connect(database=DB_FILE)
        logger.info(f"Successfully connected to DuckDB at '{DB_FILE}'")

        # --- STEP 1: Load Yellow Taxi Data ---
        print("\n>>> STEP 1: Loading Yellow Taxi Data...")
        #load_taxi_data(con, 'yellow')

        # --- STEP 2: Load Green Taxi Data ---
        print("\n>>> STEP 2: Loading Green Taxi Data...")
        load_taxi_data(con, 'green')

        # --- STEP 3: Create Emissions Lookup Table ---
        print("\n>>> STEP 3: Creating Emissions Lookup Table...")
        #create_emissions_lookup(con, EMISSIONS_CSV_PATH)
        
        # --- STEP 4: Summarize All Loaded Data ---
        print("\n>>> STEP 4: Summarizing Loaded Data...")
        summarize_data(con)

        print("\n🎉 Data loading pipeline completed successfully!")

    except Exception as e:
        logger.critical(f"A critical error occurred in the main process: {e}")
    finally:
        if con:
            con.close()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    main()