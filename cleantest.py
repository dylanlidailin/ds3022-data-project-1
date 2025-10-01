import duckdb

DB_FILE = "emissions10yrs.duckdb"
con = duckdb.connect(DB_FILE)

print("\n--- Contents of 'vehicle_emissions' table ---")
con.sql("SELECT * FROM vehicle_emissions;").show()

con.close()