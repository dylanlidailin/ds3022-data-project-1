import duckdb
import logging
import matplotlib.pyplot as plt

# Setting up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log'
)
logger = logging.getLogger(__name__)

def analysis():
    con = None
    try:
        # PROCESS 1: Connect to the database
        con = duckdb.connect("emissions.duckdb", read_only=True)
        # Records a successful database connection.
        logger.info("Successfully connected to DuckDB for analysis.")
        print("Connected to DuckDB for analysis.")
        # argest carbon producing trip
        logger.info("Starting analysis: Largest carbon producing trips.")
        print("\n--- Largest Carbon Producing Trips ---")
        for taxi_type in ['yellow', 'green']:
            # FIX: Querying the _transformed table which contains trip_co2_kgs
            result = con.execute(f"""
                SELECT trip_distance, passenger_count, trip_co2_kgs
                FROM {taxi_type}_taxi_data_clean
                ORDER BY trip_co2_kgs DESC
                LIMIT 1;
            """).fetchone()
            print(f"Largest CO2 trip for {taxi_type.upper()} taxi -> Distance: {result[0]} miles, Passengers: {result[1]}, CO2: {result[2]:.2f} kgs")
        logger.info("Analysis complete: Largest carbon producing trips.")

        # Provide loops for other analyses
        time_periods = {
            "Hour of the Day": "hour_of_day",
            "Day of the Week": "day_of_week",
            "Week of the Year": "week_of_year",
            "Month of the Year": "month_of_year"
        }
        for label, period in time_periods.items():
            logger.info(f"Starting analysis: Averages by {label}.")
            print(f"\n--- Carbon Heavy/Light {label} ---")
            for taxi_type in ['yellow', 'green']:
                results = con.execute(f"""
                    SELECT {period}, AVG(trip_co2_kgs) AS avg_co2
                    FROM {taxi_type}_taxi_data_clean
                    GROUP BY {period}
                    ORDER BY avg_co2 DESC;
                """).fetchall()

                if results:
                    heaviest = results[0]   # First result is the highest average
                    lightest = results[-1]  # Last result is the lowest average
                    print(f"{taxi_type.upper()}:")
                    print(f"  - Most Carbon Heavy -> {label.split(' ')[0]} {heaviest[0]}: {heaviest[1]:.3f} kgs/trip")
                    print(f"  - Most Carbon Light -> {label.split(' ')[0]} {lightest[0]}: {lightest[1]:.3f} kgs/trip")
            logger.info(f"Analysis complete: Averages by {label}.")

        # Time-series plot of MONTH vs CO2 totals
        logger.info("Starting analysis: Monthly CO2 totals for plotting.")
        print("\n--- Generating Time-Series Plot of Monthly CO2 Totals ---")

        # Use one efficient GROUP BY query per taxi type
        yellow_monthly_data = con.execute("""
            SELECT month_of_year, SUM(trip_co2_kgs)
            FROM yellow_taxi_data_clean
            GROUP BY month_of_year ORDER BY month_of_year;
        """).fetchall()

        green_monthly_data = con.execute("""
            SELECT month_of_year, SUM(trip_co2_kgs)
            FROM green_taxi_data_clean
            GROUP BY month_of_year ORDER BY month_of_year;
        """).fetchall()

        # Prepare for plotting
        yellow_totals = {row[0]: row[1] for row in yellow_monthly_data}
        green_totals = {row[0]: row[1] for row in green_monthly_data}
        months = range(1, 13)
        y_vals = [yellow_totals.get(m, 0) for m in months]
        g_vals = [green_totals.get(m, 0) for m in months]

        plt.figure(figsize=(12, 7))
        plt.plot(months, y_vals, marker='o', linestyle='-', label='Yellow Taxi CO2', color='gold')
        plt.plot(months, g_vals, marker='s', linestyle='--', label='Green Taxi CO2', color='green')

        plt.title('Total Monthly CO2 Emissions by Taxi Type', fontsize=16)
        plt.xlabel('Month of the Year', fontsize=12)
        plt.ylabel('Total CO2 (kgs)', fontsize=12)
        plt.xticks(months)
        plt.legend()
        plt.grid(True, which='both', linestyle='--', linewidth=0.5)
        plt.savefig('monthly_co2_totals.png')

        logger.info("Plot saved as 'monthly_co2_totals.png'.")

    except Exception as e:
        logger.error(f"An error occurred during analysis: {e}")

if __name__ == "__main__":
    analysis()