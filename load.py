import duckdb
import os
import logging
import requests
import io

"""
Complete the load.py script to create a local, persistent DuckDB database that creates and loads (at most) three tables:

A full table of YELLOW taxi trips for all of 2024.
A full table of GREEN taxi trips for all of 2024.
A lookup table of vehicle_emissions based on the included CSV file above.
"""

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Download Parquet files for Yellow and Green taxi data for 2024
def download_yellow_taxi_data():
    years = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    db_file = "emissions.duckdb"
    table_name = "yellow_taxi_data"

    # --- Main Logic ---
    # Connect to the database once
    con = duckdb.connect(db_file)
    logger.info(f"Connected to DuckDB database: {db_file}")

    # 1. Create the table with the first month of the first year's data
    first_year = years[0]
    first_month_url = base_url.format(year=first_year, month=1)

    try:
        logger.info(f"Attempting to create table '{table_name}' with data from {first_month_url}")
        con.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM read_parquet('{first_month_url}')
        """)
        logger.info(f"Successfully created table with {first_year}-01 data.")
    except Exception as e:
        # This will catch errors if the table already exists, allowing the script to proceed
        logger.warning(f"Could not create table (it might already exist): {e}")

    # 2. Loop through all years and months to insert the remaining data
    for year in years:
        for month in range(1, 13):
            # Skip the data we already used to create the table
            if year == first_year and month == 1:
                logger.info(f"Skipping {year}-01 data as it was used for table creation.")
                continue

            url = base_url.format(year=year, month=month)
            logger.info(f"Processing data for {year}-{month:02d} from {url}")

            try:
                # Insert data into the existing table
                con.execute(f"""
                    INSERT INTO {table_name}
                    SELECT * FROM read_parquet('{url}')
                """)
                logger.info(f"Successfully inserted data for {year}-{month:02d}.")
            except Exception as e:
                # Log an error if a specific file fails, but continue with the next
                logger.error(f"Failed to insert data from {url}: {e}")

    # Close the database connection
    logger.info(f"Data processing complete. All data saved to '{table_name}' in {db_file}")

# --- Configuration ---
def download_green_taxi_data():
    years = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet"
    db_file = "emissions.duckdb"
    table_name = "green_taxi_data"

    # --- Main Logic ---
    # Connect to the database once
    con = duckdb.connect(db_file)
    logger.info(f"Connected to DuckDB database: {db_file}")

    # 1. Create the table with the first month of the first year's data
    first_year = years[0]
    first_month_url = base_url.format(year=first_year, month=1)

    try:
        logger.info(f"Attempting to create table '{table_name}' with data from {first_month_url}")
        con.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM read_parquet('{first_month_url}')
        """)
        logger.info(f"Successfully created table with {first_year}-01 data.")
    except Exception as e:
        # This will catch errors if the table already exists, allowing the script to proceed
        logger.warning(f"Could not create table (it might already exist): {e}")

    # 2. Loop through all years and months to insert the remaining data
    for year in years:
        for month in range(1, 13):
            # Skip the data we already used to create the table
            if year == first_year and month == 1:
                logger.info(f"Skipping {year}-01 data as it was used for table creation.")
                continue

            url = base_url.format(year=year, month=month)
            logger.info(f"Processing data for {year}-{month:02d} from {url}")

            try:
                # Insert data into the existing table
                con.execute(f"""
                    INSERT INTO {table_name}
                    SELECT * FROM read_parquet('{url}')
                """)
                logger.info(f"Successfully inserted data for {year}-{month:02d}.")
            except Exception as e:
                # Log an error if a specific file fails, but continue with the next
                logger.error(f"Failed to insert data from {url}: {e}")

    # Close the database connection
    logger.info(f"Data processing complete. All data saved to '{table_name}' in {db_file}")

def lookup_vehicle_emissions():
    con = duckdb.connect("emissions.duckdb")
    
    try:
        con.execute("""
            CREATE OR REPLACE TABLE vehicle_emissions (
                vehicle_type VARCHAR,
                co2_grams_per_mile FLOAT
            );
            INSERT INTO vehicle_emissions (vehicle_type, co2_grams_per_mile) VALUES
            ('yellow_taxi', 380),
            ('green_taxi', 350);
        """)
        
        # Verify it worked
        result = con.execute("SELECT COUNT(*) FROM vehicle_emissions").fetchone()
        print(f"Vehicle emissions table created with {result[0]} records")
        
    except Exception as e:
        print(f"Error creating vehicle emissions table: {e}")
    finally:
        con.close()

def load_parquet_files():

    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        con.execute(f"""
            -- SQL goes here
        """)
        logger.info("Dropped table if exists")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    download_yellow_taxi_data()
    download_green_taxi_data()
    #lookup_vehicle_emissions()
    #load_parquet_files()