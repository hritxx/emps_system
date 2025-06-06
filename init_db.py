import duckdb
import os
import sys

try:

    con = duckdb.connect("employee_reports.duckdb")
except duckdb.IOException as e:
    if "Conflicting lock is held" in str(e):
        print("Database is locked by another process.")
        print("Options:")
        print("1. Close the other process (PID shown in error message)")
        print("2. Run with read-only flag (add --readonly as parameter)")
        
        if len(sys.argv) > 1 and sys.argv[1] == "--readonly":
            print("Opening in read-only mode...")
            con = duckdb.connect("employee_reports.duckdb", read_only=True)
        else:
            sys.exit(1)


csv_tables = {
    "employee_master": "employee_master.csv",
    "employee_exit_report": "employee_exit_report.csv",
    "employee_work_profile": "employee_work_profile.csv",
    "employee_experience_report": "experience_report.csv",
    "daily_attendance": "daily_attendance.csv",
    "timesheets": "timesheet_report.csv"
}

for table_name, file_name in csv_tables.items():
    if os.path.exists(file_name):
        con.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{file_name}')
        """)
        print(f"Loaded {table_name} from {file_name}")
    else:
        print(f"⚠️  {file_name} not found, skipping...")

con.close()
print("✅ DuckDB database initialized: employee_reports.duckdb")


