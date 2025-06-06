import duckdb
import os

# Create output folder if it doesn't exist
os.makedirs("db", exist_ok=True)

# Connect to your DuckDB database
con = duckdb.connect("employee_reports.duckdb", read_only=True)

# Get list of all tables
tables = con.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'main'
""").fetchall()

# Export each table as CSV
for (table_name,) in tables:
    output_path = os.path.join("db", f"{table_name}.csv")
    con.execute(f"COPY {table_name} TO '{output_path}' (HEADER, DELIMITER ',')")
    print(f"✅ Exported {table_name} → {output_path}")

con.close()
print("📁 All tables exported to the 'db' folder.")