import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import numpy as np
import os
import json
import requests
from datetime import datetime, date
import dotenv
# Load environment variables from .env file
dotenv.load_dotenv()    

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

st.set_page_config(layout="wide")
st.title("📊 Employee Reports Dashboard")


if 'current_df' not in st.session_state:
    st.session_state.current_df = pd.DataFrame()


db_path = "employee_reports.duckdb"
if not os.path.exists(db_path):
    st.error(f"Database file '{db_path}' not found. Please ensure the database is created first.")
    st.stop()


try:
    con = duckdb.connect(db_path, read_only=True)
    # Test connection
    con.execute("SELECT 1").fetchone()
except Exception as e:
    st.error(f"Failed to connect to database: {e}")
    st.stop()


required_tables = [
    'employee_master', 
    'employee_exit_report', 
    'employee_work_profile', 
    'employee_experience_report',
    'daily_attendance', 
    'timesheets'
]

try:
    existing_tables = con.execute("SHOW TABLES").fetchall()
    existing_table_names = [table[0] for table in existing_tables]
    
    missing_tables = [table for table in required_tables if table not in existing_table_names]
    if missing_tables:
        st.error(f"Missing required tables: {', '.join(missing_tables)}")
        st.info(f"Existing tables: {', '.join(existing_table_names)}")
        st.stop()
        
except Exception as e:
    st.error(f"Error checking database schema: {e}")
    st.stop()



tab1, tab2, tab3 = st.tabs(["📋 Predefined Reports", "🔍 Custom Queries", "🤖 AI Query Assistant"])

with tab1:

    report_options = [
        "Employee Roster",
        "Exit Report", 
        "Work Profile",
        "Experience Summary",
        "Daily Attendance with Timesheet Verification",
        "Project Master Report",
        "Employee Project Summary"
    ]

    choice = st.selectbox("Select a report", report_options)


    try:
        if choice == "Employee Roster":
            df = con.execute("SELECT * FROM employee_master").df()
            if df.empty:
                st.warning("No employee data found.")
            else:
                st.dataframe(df)
                st.session_state.current_df = df

        elif choice == "Exit Report":
            df = con.execute("SELECT * FROM employee_exit_report").df()
            if df.empty:
                st.warning("No exit report data found.")
            else:
                st.dataframe(df)
                st.session_state.current_df = df

        elif choice == "Work Profile":
            df = con.execute("SELECT * FROM employee_work_profile").df()
            if df.empty:
                st.warning("No work profile data found.")
            else:
                st.dataframe(df) 
                st.session_state.current_df = df

        elif choice == "Experience Summary":
            df = con.execute("SELECT * FROM employee_experience_report").df()
            if df.empty:
                st.warning("No experience data found.")
            else:
                st.dataframe(df)
                st.session_state.current_df = df

        elif choice == "Daily Attendance with Timesheet Verification":
            query = """
                SELECT
                    a."Employee Code",
                    a."Date",
                    a."Clock-In Time",
                    a."Clock-Out Time",
                    a."Total Hours" as "Attendance Hours",
                    COALESCE(t."Project Name", 'No Project') as "Project Name",
                    COALESCE(t."Hours Worked", 0) as "Hours Worked"
                FROM daily_attendance a
                LEFT JOIN timesheets t
                ON a."Employee Code" = t."Employee Code" AND a."Date" = t."Date"
                ORDER BY a."Date", a."Employee Code"
            """
            df = con.execute(query).df()
            if df.empty:
                st.warning("No attendance data found.")
            else:
                st.dataframe(df)
                st.session_state.current_df = df
            
        elif choice == "Project Master Report":
            # Check if timesheets table has data
            timesheet_count = con.execute("SELECT COUNT(*) FROM timesheets").fetchone()[0]
            if timesheet_count == 0:
                st.warning("No timesheet data found.")
            else:
                # Comprehensive report about all projects
                query = """
                    SELECT 
                        DISTINCT t."Project ID",
                        t."Project Name",
                        COUNT(DISTINCT t."Employee Code") as "Total Employees",
                        ROUND(SUM(t."Hours Worked"), 2) as "Total Hours",
                        MIN(t."Date") as "Start Date",
                        MAX(t."Date") as "Latest Activity Date",
                        COUNT(DISTINCT t."Date") as "Active Days"
                    FROM timesheets t
                    GROUP BY t."Project ID", t."Project Name"
                    ORDER BY t."Project ID"
                """
                
                # Execute the project summary query
                project_summary_df = con.execute(query).df()
                
                if project_summary_df.empty:
                    st.warning("No project data found.")
                else:
                    # Get project details for each project
                    for i, row in project_summary_df.iterrows():
                        project_id = row["Project ID"]
                        st.subheader(f"Project: {row['Project Name']} ({project_id})")
                        
                        # Display project summary
                        summary_metrics = {
                            "Total Employees": int(row["Total Employees"]),
                            "Total Hours": f"{row['Total Hours']:.1f}",
                            "Start Date": str(row["Start Date"]),
                            "Latest Activity": str(row["Latest Activity Date"]),
                            "Active Days": int(row["Active Days"])
                        }
                        
                        # Show summary metrics in columns
                        cols = st.columns(len(summary_metrics))
                        for col, (metric_name, metric_value) in zip(cols, summary_metrics.items()):
                            col.metric(metric_name, metric_value)
                        
                        # Get employee details for this project - fixed SQL injection vulnerability
                        emp_query = """
                            SELECT 
                                e."Employee Name",
                                e."Department",
                                ROUND(SUM(t."Hours Worked"), 2) as "Total Hours",
                                COUNT(DISTINCT t."Date") as "Days Worked",
                                MIN(t."Date") as "First Day",
                                MAX(t."Date") as "Last Day"
                            FROM timesheets t
                            JOIN employee_master e ON t."Employee Code" = e."Employee Code"
                            WHERE t."Project ID" = ?
                            GROUP BY e."Employee Name", e."Department"
                            ORDER BY "Total Hours" DESC
                        """
                        project_employees_df = con.execute(emp_query, [project_id]).df()
                        
                        # Display employees on this project
                        if not project_employees_df.empty:
                            st.dataframe(project_employees_df)
                        else:
                            st.info("No employee data found for this project.")
                        
                        # Add a separator between projects
                        st.markdown("---")
                
                # Store for analysis tools
                st.session_state.current_df = project_summary_df
            
        elif choice == "Employee Project Summary":
            # Check if we have employee data
            emp_count = con.execute("SELECT COUNT(*) FROM employee_master").fetchone()[0]
            if emp_count == 0:
                st.warning("No employee data found.")
            else:
                # Comprehensive report about employees and their projects
                query = """
                    SELECT
                        e.*,
                        COALESCE((SELECT COUNT(DISTINCT t."Project ID") 
                                 FROM timesheets t 
                                 WHERE t."Employee Code" = e."Employee Code"), 0) as "Projects Count",
                        COALESCE((SELECT ROUND(SUM(t."Hours Worked"), 2) 
                                 FROM timesheets t 
                                 WHERE t."Employee Code" = e."Employee Code"), 0) as "Total Hours Worked"
                    FROM employee_master e
                    ORDER BY e."Employee Code"
                """
                
                # Execute the employee master query with project summary
                emp_master_df = con.execute(query).df()
                
                if emp_master_df.empty:
                    st.warning("No employee data found.")
                else:
                    # Create expandable sections for each employee
                    for i, row in emp_master_df.iterrows():
                        emp_code = row["Employee Code"]
                        emp_name = row["Employee Name"]
                        
                        with st.expander(f"{emp_name} ({emp_code}) - {row['Department']} - {row['Projects Count']} Projects"):
                            # Show employee details
                            st.write(f"**Email:** {row.get('Email', 'N/A')}")
                            st.write(f"**Mobile Number:** {row.get('Mobile Number', 'N/A')}")
                            st.write(f"**Total Hours:** {row['Total Hours Worked']:.1f}")
                            
                            # Get project details for this employee - fixed SQL injection
                            proj_query = """
                                SELECT 
                                    t."Project ID",
                                    t."Project Name",
                                    ROUND(SUM(t."Hours Worked"), 2) as "Total Hours",
                                    COUNT(DISTINCT t."Date") as "Days Worked",
                                    MIN(t."Date") as "First Day",
                                    MAX(t."Date") as "Last Day"
                                FROM timesheets t
                                WHERE t."Employee Code" = ?
                                GROUP BY t."Project ID", t."Project Name"
                                ORDER BY "Total Hours" DESC
                            """
                            emp_projects_df = con.execute(proj_query, [emp_code]).df()
                            
                            # Display projects for this employee
                            if not emp_projects_df.empty:
                                st.dataframe(emp_projects_df)
                            else:
                                st.info("No project assignments found for this employee.")
                
                # Store for analysis tools
                st.session_state.current_df = emp_master_df
            
    except Exception as e:
        st.error(f"Error executing query: {e}")
        st.error(f"Query details: {str(e)}")


with tab2:
    st.header("Custom Query Builder")
    
    try:
        # Get list of employee names with null handling
        employee_query = """
            SELECT DISTINCT "Employee Name" 
            FROM employee_master 
            WHERE "Employee Name" IS NOT NULL AND "Employee Name" != ''
            ORDER BY "Employee Name"
        """
        employee_names_result = con.execute(employee_query).fetchall()
        employee_names = [name[0] for name in employee_names_result]
        
        # Get list of departments with null handling
        dept_query = """
            SELECT DISTINCT Department 
            FROM employee_master 
            WHERE Department IS NOT NULL AND Department != ''
            ORDER BY Department
        """
        departments_result = con.execute(dept_query).fetchall()
        departments = [dept[0] for dept in departments_result]
        
        # Get list of projects with null handling
        proj_query = """
            SELECT DISTINCT "Project Name" 
            FROM timesheets 
            WHERE "Project Name" IS NOT NULL AND "Project Name" != ''
            ORDER BY "Project Name"
        """
        projects_result = con.execute(proj_query).fetchall()
        projects = [proj[0] for proj in projects_result]
        
    except Exception as e:
        st.error(f"Error loading filter options: {e}")
        employee_names, departments, projects = [], [], []
    
    # Create filter columns
    col1, col2, col3 = st.columns(3)
    
    with col1:
        selected_employees = st.multiselect("Select Employees", options=["All"] + employee_names, default=["All"])
    
    with col2:
        selected_departments = st.multiselect("Select Departments", options=["All"] + departments, default=["All"])
    
    with col3:
        selected_projects = st.multiselect("Select Projects", options=["All"] + projects, default=["All"])
    
    # Create date range filter
    col4, col5 = st.columns(2)
    with col4:
        start_date = st.date_input("Start Date", value=None)
    with col5:
        end_date = st.date_input("End Date", value=None)
    
    # Report type selection for custom query
    report_type = st.selectbox(
        "Select Report Type", 
        ["Employee Details", "Project Assignments", "Attendance Records", "Timesheet Summary"]
    )
    
    # Build the query
    if st.button("Generate Report", key="custom_query_report"):
        try:
            df = pd.DataFrame()  # Initialize empty dataframe
            
            if report_type == "Employee Details":
                query = "SELECT * FROM employee_master WHERE 1=1"
                params = []
                
                if "All" not in selected_employees and selected_employees:
                    placeholders = ",".join(["?" for _ in selected_employees])
                    query += f" AND \"Employee Name\" IN ({placeholders})"
                    params.extend(selected_employees)
                
                if "All" not in selected_departments and selected_departments:
                    placeholders = ",".join(["?" for _ in selected_departments])
                    query += f" AND Department IN ({placeholders})"
                    params.extend(selected_departments)
                
                df = con.execute(query, params).df()
                
            elif report_type == "Project Assignments":
                query = """
                    SELECT 
                        t."Date",
                        t."Employee Code",
                        e."Employee Name",
                        e.Department,
                        t."Project ID",
                        t."Project Name",
                        t."Hours Worked"
                    FROM timesheets t
                    JOIN employee_master e ON t."Employee Code" = e."Employee Code"
                    WHERE 1=1
                """
                params = []
                
                if "All" not in selected_employees and selected_employees:
                    placeholders = ",".join(["?" for _ in selected_employees])
                    query += f" AND e.\"Employee Name\" IN ({placeholders})"
                    params.extend(selected_employees)
                
                if "All" not in selected_departments and selected_departments:
                    placeholders = ",".join(["?" for _ in selected_departments])
                    query += f" AND e.Department IN ({placeholders})"
                    params.extend(selected_departments)
                
                if "All" not in selected_projects and selected_projects:
                    placeholders = ",".join(["?" for _ in selected_projects])
                    query += f" AND t.\"Project Name\" IN ({placeholders})"
                    params.extend(selected_projects)
                
                if start_date:
                    query += " AND t.\"Date\" >= ?"
                    params.append(start_date.strftime('%Y-%m-%d'))
            
                if end_date:
                    query += " AND t.\"Date\" <= ?"
                    params.append(end_date.strftime('%Y-%m-%d'))
                
                query += " ORDER BY t.\"Date\", e.\"Employee Name\""
                
                df = con.execute(query, params).df()
                
            elif report_type == "Attendance Records":
                query = """
                    SELECT 
                        a."Date",
                        a."Employee Code", 
                        e."Employee Name",
                        e.Department,
                        a."Clock-In Time",
                        a."Clock-Out Time",
                        a."Total Hours"
                    FROM daily_attendance a
                    JOIN employee_master e ON a."Employee Code" = e."Employee Code"
                    WHERE 1=1
                """
                params = []
                
                if "All" not in selected_employees and selected_employees:
                    placeholders = ",".join(["?" for _ in selected_employees])
                    query += f" AND e.\"Employee Name\" IN ({placeholders})"
                    params.extend(selected_employees)
                
                if "All" not in selected_departments and selected_departments:
                    placeholders = ",".join(["?" for _ in selected_departments])
                    query += f" AND e.Department IN ({placeholders})"
                    params.extend(selected_departments)
                
                if start_date:
                    query += " AND a.\"Date\" >= ?"
                    params.append(start_date.strftime('%Y-%m-%d'))
            
                if end_date:
                    query += " AND a.\"Date\" <= ?"
                    params.append(end_date.strftime('%Y-%m-%d'))
                
                query += " ORDER BY a.\"Date\", e.\"Employee Name\""
                
                df = con.execute(query, params).df()
                
            elif report_type == "Timesheet Summary":
                query = """
                    SELECT 
                        e."Employee Name",
                        e.Department,
                        t."Project Name", 
                        ROUND(SUM(t."Hours Worked"), 2) as "Total Hours"
                    FROM timesheets t
                    JOIN employee_master e ON t."Employee Code" = e."Employee Code"
                    WHERE 1=1
                """
                params = []
                
                if "All" not in selected_employees and selected_employees:
                    placeholders = ",".join(["?" for _ in selected_employees])
                    query += f" AND e.\"Employee Name\" IN ({placeholders})"
                    params.extend(selected_employees)
                
                if "All" not in selected_departments and selected_departments:
                    placeholders = ",".join(["?" for _ in selected_departments])
                    query += f" AND e.Department IN ({placeholders})"
                    params.extend(selected_departments)
                
                if "All" not in selected_projects and selected_projects:
                    placeholders = ",".join(["?" for _ in selected_projects])
                    query += f" AND t.\"Project Name\" IN ({placeholders})"
                    params.extend(selected_projects)
                
                if start_date:
                    query += " AND t.\"Date\" >= ?"
                    params.append(start_date.strftime('%Y-%m-%d'))
            
                if end_date:
                    query += " AND t.\"Date\" <= ?"
                    params.append(end_date.strftime('%Y-%m-%d'))
                
                query += " GROUP BY e.\"Employee Name\", e.Department, t.\"Project Name\""
                query += " ORDER BY e.\"Employee Name\", t.\"Project Name\""
                
                df = con.execute(query, params).df()
            
            # Store the dataframe in session state and display it
            if not df.empty:
                st.session_state.current_df = df
                st.dataframe(df)
                st.success(f"Report generated successfully! Found {len(df)} records.")
            else:
                st.warning("No data found matching the selected criteria.")
            
        except Exception as e:
            st.error(f"Error generating report: {e}")
            df = pd.DataFrame()



with tab3:
    st.header("AI Query Assistant")
    st.info("Describe what you want to find out in plain English, and let AI generate the SQL query for you.")
    
    user_query = st.text_area("What would you like to know?", 
        placeholder="Example: Show me all employees in the Marketing department who worked on Project PRJ003 in June 2025")
    
    if st.button("Generate Report", key="ai_query_report"):
        if not user_query:
            st.warning("Please enter a query description.")
        else:
            with st.spinner("Generating SQL query..."):
                try:
                    # Get database schema information for context
                    table_schema = {}
                    for table in required_tables:
                        schema_query = f"DESCRIBE SELECT * FROM {table} LIMIT 0"
                        table_schema[table] = con.execute(schema_query).df().to_dict(orient='records')
                    
                    # Sample data for each table (first 5 rows)
                    sample_data = {}
                    for table in required_tables:
                        sample_query = f"SELECT * FROM {table} LIMIT 5"
                        sample_data[table] = con.execute(sample_query).df().to_dict(orient='records')
                    
                    # Create context message
                     # Create context message
                    context = {
                        "database_tables": required_tables,
                        "table_schemas": table_schema,
                        "sample_data": sample_data,
                        "query_requirement": user_query
                    }
                    
                    # Custom JSON encoder to handle timestamps and dates
                    class CustomJSONEncoder(json.JSONEncoder):
                        def default(self, obj):
                            if isinstance(obj, (datetime, date)):
                                return obj.isoformat()
                            if pd.isna(obj):
                                return None
                            try:
                                return super().default(obj)
                            except TypeError:
                                return str(obj)
                    
                    # Prepare API request
                    api_key = GEMINI_API_KEY
                    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"
                    
                    prompt = f"""
                    You are a database expert who helps convert natural language queries to SQL queries.

                    Here are the tables in the database:
                    {json.dumps(context['table_schemas'], indent=2, cls=CustomJSONEncoder)}

                    Here's a sample of each table's data:
                    {json.dumps(context['sample_data'], indent=2, cls=CustomJSONEncoder)}

                    The user wants the following information:
                    {user_query}

                    IMPORTANT: Before writing your query, carefully check which columns exist in which tables.
                    - The employee_master table contains "Employee Name" and basic employee information
                    - The timesheets table contains project assignments and hours but NOT employee names
                    - Join tables appropriately to get the information needed

                    Please generate a valid SQL query that can be executed against a DuckDB database.
                    Make sure to use DISTINCT or appropriate GROUP BY clauses to avoid duplicate rows.
                    Return only the SQL query without any explanation or additional text.
                    Your SQL query should be wrapped in triple backticks like this:
                    ```
                    SELECT * FROM table;
                    ```
                    """
                    
                    headers = {
                        'Content-Type': 'application/json'
                    }
                    
                    payload = {
                        "contents": [
                            {
                                "parts": [
                                    {
                                        "text": prompt
                                    }
                                ]
                            }
                        ]
                    }
                    
                    # Make API request
                    response = requests.post(url, headers=headers, json=payload)
                    
                    if response.status_code == 200:
                        response_data = response.json()
                        response_text = response_data['candidates'][0]['content']['parts'][0]['text']
                        
                        # Extract SQL query from response
                        import re
                        sql_match = re.search(r"```(?:sql)?\n([\s\S]*?)\n```", response_text)
                        
                        if sql_match:
                            sql_query = sql_match.group(1).strip()
                            
                            # Display the generated SQL
                            st.subheader("Generated SQL Query:")
                            st.code(sql_query, language="sql")
                            
                            # Execute the query
                            with st.spinner("Executing query..."):
                                try:
                                    result_df = con.execute(sql_query).df()
                                    
                                    # Store results and display
                                    st.session_state.current_df = result_df
                                    st.subheader("Query Results:")
                                    st.dataframe(result_df)
                                    st.success(f"Query executed successfully! Found {len(result_df)} records.")
                                except Exception as e:
                                    st.error(f"Error executing query: {str(e)}")
                        else:
                            st.error("Could not extract SQL query from API response")
                    else:
                        st.error(f"API request failed with status code {response.status_code}")
                        st.error(response.text)
                        
                except Exception as e:
                    st.error(f"Error: {str(e)}")


# Close database connection
try:
    con.close()
except:
    pass