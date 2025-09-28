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
def download_green_taxi_data(year=2024):
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month:02d}.parquet"
    con = duckdb.connect("emissions.duckdb")
    
    # Create the table by reading the first month's data
    first_month_url = base_url.format(year=year, month=1)
    try:
        logger.info(f"Creating table with January data from {first_month_url}")
        con.execute(f"""
            CREATE TABLE green_taxi_data AS
            SELECT * FROM read_parquet('{first_month_url}')
        """)
        logger.info(f"Created table with {year}-01 green taxi data")
    except Exception as e:
        logger.error(f"Failed to create table with {first_month_url}: {e}")
        return
    
    # Insert data from remaining months
    for month in range(2, 13):
        url = base_url.format(year=year, month=month)
        logger.info(f"Inserting month {month} data from {url}")
        try:
            con.execute(f"""
                INSERT INTO yellow_taxi_data  -- <- Fixed table name
                SELECT * FROM read_parquet('{url}')
            """)
            logger.info(f"Added {year}-{month:02d} green taxi data")
        except Exception as e:
            logger.error(f"Failed to insert data from {url}: {e}")
    
    con.close()
    print("Green taxi data saved to emissions.duckdb")


def download_yellow_taxi_data(year=2024):
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    con = duckdb.connect("emissions.duckdb")
    
    # Create the table by reading the first month's data
    first_month_url = base_url.format(year=year, month=1)
    try:
        logger.info(f"Creating table with January data from {first_month_url}")
        con.execute(f"""
            CREATE TABLE yellow_taxi_data AS
            SELECT * FROM read_parquet('{first_month_url}')
        """)
        logger.info(f"Created table with {year}-01 yellow taxi data")  # <- Fixed log message
    except Exception as e:
        logger.error(f"Failed to create table with {first_month_url}: {e}")
        return
    
    # Insert data from remaining months
    for month in range(2, 13):
        url = base_url.format(year=year, month=month)
        logger.info(f"Inserting month {month} data from {url}")
        try:
            con.execute(f"""
                INSERT INTO yellow_taxi_data  -- <- Fixed table name
                SELECT * FROM read_parquet('{url}')
            """)
            logger.info(f"Added {year}-{month:02d} yellow taxi data")
        except Exception as e:
            logger.error(f"Failed to insert data from {url}: {e}")
    
    con.close()
    print("Yellow taxi data saved to emissions.duckdb")

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
    lookup_vehicle_emissions()
    load_parquet_files()