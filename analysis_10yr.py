import duckdb
import logging
import matplotlib.pyplot as plt
import pandas as pd

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log',
    filemode='w'
)
logger = logging.getLogger(__name__)
DB_FILE = "emissions10yrs.duckdb"

def analyze_data():
    """
    Connects to the database and performs the final analysis as required.
    """
    con = None
    try:
        # Connect to the correct database file in read-only mode
        con = duckdb.connect(DB_FILE, read_only=True)
        logger.info(f"Successfully connected to {DB_FILE} for analysis.")
        print(f"Connected to {DB_FILE} for analysis.")
        
        # 1. Largest carbon producing trip
        logger.info("Starting analysis: Largest carbon producing trips.")
        print("\n--- Largest Carbon Producing Trips (Entire Timespan) ---")
        for taxi_type in ['yellow', 'green']:
            table_name = f"{taxi_type}_taxi_final"
            result = con.execute(f"""
                SELECT 
                    { 'tpep_pickup_datetime' if taxi_type == 'yellow' else 'lpep_pickup_datetime' }, 
                    trip_distance, 
                    passenger_count, 
                    trip_co2_kgs
                FROM {table_name}
                ORDER BY trip_co2_kgs DESC
                LIMIT 1;
            """).fetchone()

            if result:
                output = f"Largest CO2 trip for {taxi_type.upper()} taxi -> Time: {result[0]}, Distance: {result[1]} miles, CO2: {result[3]:.2f} kgs"
                print(output)
                logger.info(output)
        logger.info("Analysis complete: Largest carbon producing trips.")

        # 2-5. Averages and Totals by time periods
        time_periods = {
            "Hour of the Day": "hour_of_day",
            "Day of the Week": "day_of_week",
            "Week of the Year": "week_of_year",
            "Month of the Year": "month_of_year"
        }
        for label, period in time_periods.items():
            logger.info(f"Starting analysis: Averages and Totals by {label}.")
            print(f"\n--- Carbon Analysis by {label} ---")
            for taxi_type in ['yellow', 'green']:
                table_name = f"{taxi_type}_taxi_final"
                avg_results = con.execute(f"SELECT {period}, AVG(trip_co2_kgs) AS metric FROM {table_name} GROUP BY {period} ORDER BY metric DESC;").fetchall()
                sum_results = con.execute(f"SELECT {period}, SUM(trip_co2_kgs) AS metric FROM {table_name} GROUP BY {period} ORDER BY metric DESC;").fetchall()

                if avg_results and sum_results:
                    print(f"{taxi_type.upper()}:")
                    avg_heavy = f"  - Most Carbon Heavy (Avg) -> {label.split(' ')[0]} {avg_results[0][0]}: {avg_results[0][1]:.3f} kgs/trip"
                    avg_light = f"  - Most Carbon Light (Avg) -> {label.split(' ')[0]} {avg_results[-1][0]}: {avg_results[-1][1]:.3f} kgs/trip"
                    print(avg_heavy); logger.info(avg_heavy)
                    print(avg_light); logger.info(avg_light)
                    sum_high = f"  - Highest Total CO2 Output -> {label.split(' ')[0]} {sum_results[0][0]}: {sum_results[0][1]:,.0f} kgs"
                    sum_low = f"  - Lowest Total CO2 Output -> {label.split(' ')[0]} {sum_results[-1][0]}: {sum_results[-1][1]:,.0f} kgs"
                    print(sum_high); logger.info(sum_high)
                    print(sum_low); logger.info(sum_low)
            logger.info(f"Analysis complete: Averages and Totals by {label}.")

        # 6. Time-series plot of MONTH vs CO2 totals (Your Original Logic)
        logger.info("Starting analysis: Monthly CO2 totals for plotting.")
        print("\n--- Generating Seasonal Plot of Monthly CO2 Totals ---")

        # Query aggregates all years into 12 monthly totals
        yellow_monthly_data = con.execute("""
            SELECT month_of_year, SUM(trip_co2_kgs)
            FROM yellow_taxi_final
            GROUP BY month_of_year ORDER BY month_of_year;
        """).fetchall()

        green_monthly_data = con.execute("""
            SELECT month_of_year, SUM(trip_co2_kgs)
            FROM green_taxi_final
            GROUP BY month_of_year ORDER BY month_of_year;
        """).fetchall()

        # Process results into a plottable format using your dictionary method
        yellow_totals = {row[0]: row[1] for row in yellow_monthly_data}
        green_totals = {row[0]: row[1] for row in green_monthly_data}
        months = range(1, 13)
        y_vals = [yellow_totals.get(m, 0) for m in months]
        g_vals = [green_totals.get(m, 0) for m in months]

        plt.figure(figsize=(12, 7))
        plt.plot(months, y_vals, marker='o', linestyle='-', label='Yellow Taxi CO2', color='gold')
        plt.plot(months, g_vals, marker='s', linestyle='--', label='Green Taxi CO2', color='green')

        plt.title('Total Monthly CO2 Emissions by Taxi Type (Aggregated 2015-2024)', fontsize=16)
        plt.xlabel('Month of the Year', fontsize=12)
        plt.ylabel('Total CO2 (kgs)', fontsize=12)
        plt.xticks(months)
        plt.legend()
        plt.grid(True, which='both', linestyle='--', linewidth=0.5)
        
        plot_filename = 'monthly_co2_totals_seasonal.png'
        plt.savefig(plot_filename)
        print(f"Plot saved successfully as '{plot_filename}'.")
        logger.info(f"Plot saved as '{plot_filename}'.")

    except Exception as e:
        logger.error(f"An error occurred during analysis: {e}")
        print(f"An error occurred during analysis: {e}")
    finally:
        if con:
            con.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    analyze_data()