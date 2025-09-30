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

# Download Parquet files for Yellow and Green taxi data for 2024
def download_yellow_taxi_data():
    years = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    db_file = "emissions.duckdb"
    table_name = "yellow_taxi_data"

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
        logger.warning(f"Could not create table (it might already exist): {e}")

    # 2. Loop through all years and months to insert the remaining data
    for year in years:
        for month in range(1, 13):
            # Skip the data used to create the table
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

    # Loop through all years and months to insert the remaining data
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
    con = duckdb.connect(database='emissions.duckdb', read_only=False)
    download_yellow_taxi_data()
    download_green_taxi_data()
    emissions_csv_path = 'data/vehicle_emissions.csv'
    create_emissions_lookup(con, emissions_csv_path)
    load_parquet_files()