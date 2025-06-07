import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from .config import Config


class DatabaseManager:
    def __init__(self):
        self.config = Config()
        self.connection = None
        self.logger = logging.getLogger(__name__)

        # Table schemas - defines the structure of each table
        self.table_schemas = {
            'employee_exit_report': """
                CREATE TABLE IF NOT EXISTS employee_exit_report (
                    id SERIAL PRIMARY KEY,
                    employee_code VARCHAR(50) UNIQUE NOT NULL,
                    employee_name VARCHAR(255),
                    business_unit VARCHAR(255),
                    designation VARCHAR(255),
                    date_of_joining DATE,
                    exit_date DATE,
                    expected_resignation_date DATE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'employee_master': """
                CREATE TABLE IF NOT EXISTS employee_master (
                    id SERIAL PRIMARY KEY,
                    employee_code VARCHAR(50) UNIQUE NOT NULL,
                    employee_name VARCHAR(255),
                    email VARCHAR(255),
                    additional_email VARCHAR(255),
                    mobile_number VARCHAR(20),
                    secondary_mobile_number VARCHAR(20),
                    gender VARCHAR(10),
                    date_of_joining DATE,
                    date_of_birth DATE,
                    fax VARCHAR(50),
                    marital_status VARCHAR(50),
                    self_service VARCHAR(10),
                    employee_type VARCHAR(100),
                    office_location VARCHAR(255),
                    business_unit VARCHAR(255),
                    designation VARCHAR(255),
                    department VARCHAR(255),
                    grade VARCHAR(50),
                    parent_department VARCHAR(255),
                    primary_manager VARCHAR(255),
                    primary_manager_email VARCHAR(255),
                    bank_name VARCHAR(255),
                    branch_name VARCHAR(255),
                    account_holder_name VARCHAR(255),
                    account_number VARCHAR(100),
                    account_type VARCHAR(50),
                    ifsc_code VARCHAR(20),
                    swift_code VARCHAR(20),
                    pan_number VARCHAR(20),
                    aadhaar_enrollment_number VARCHAR(50),
                    aadhaar_number VARCHAR(20),
                    present_address TEXT,
                    present_state VARCHAR(100),
                    present_city VARCHAR(100),
                    present_pincode VARCHAR(10),
                    present_country VARCHAR(100),
                    permanent_address TEXT,
                    permanent_state VARCHAR(100),
                    permanent_city VARCHAR(100),
                    permanent_pincode VARCHAR(10),
                    permanent_country VARCHAR(100),
                    status VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'employee_work_profile': """
                CREATE TABLE IF NOT EXISTS employee_work_profile (
                    id SERIAL PRIMARY KEY,
                    employee_code VARCHAR(50) UNIQUE NOT NULL,
                    employee_name VARCHAR(255),
                    business_unit VARCHAR(255),
                    parent_designation VARCHAR(255),
                    assigned_department VARCHAR(255),
                    designation VARCHAR(255),
                    office_location_name VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'employee_experience_report': """
                CREATE TABLE IF NOT EXISTS employee_experience_report (
                    id SERIAL PRIMARY KEY,
                    employee_code VARCHAR(50) UNIQUE NOT NULL,
                    employee_name VARCHAR(255),
                    business_unit VARCHAR(255),
                    department VARCHAR(255),
                    designation VARCHAR(255),
                    date_of_joining DATE,
                    current_experience VARCHAR(50),
                    past_experience VARCHAR(50),
                    total_experience VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            'timesheets': """
                CREATE TABLE IF NOT EXISTS timesheets (
                    id SERIAL PRIMARY KEY,
                    date DATE NOT NULL,
                    employee_code VARCHAR(50) NOT NULL,
                    project_id VARCHAR(50) NOT NULL,
                    project_name VARCHAR(255),
                    hours_worked DECIMAL(5,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(date, employee_code, project_id)
                )
            """,
            'daily_attendance': """
                CREATE TABLE IF NOT EXISTS daily_attendance (
                    id SERIAL PRIMARY KEY,
                    date DATE NOT NULL,
                    employee_code VARCHAR(50) NOT NULL,
                    employee_name VARCHAR(255),
                    clock_in_time TIME,
                    clock_out_time TIME,
                    total_hours DECIMAL(5,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(date, employee_code)
                )
            """
        }

    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(
                host=self.config.DB_HOST,
                port=self.config.DB_PORT,
                database=self.config.DB_NAME,
                user=self.config.DB_USER,
                password=self.config.DB_PASSWORD,
                cursor_factory=RealDictCursor
            )
            self.logger.info("Database connection established successfully")
            return True
        except Exception as e:
            self.logger.error(f"Database connection failed: {str(e)}")
            return False

    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed")

    def table_exists(self, table_name):
        """Check if a table exists in the database"""
        try:
            query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            """
            with self.connection.cursor() as cursor:
                cursor.execute(query, (table_name,))
                result = cursor.fetchone()
                return result['exists'] if result else False
        except Exception as e:
            self.logger.error(f"Error checking if table {table_name} exists: {str(e)}")
            return False

    def create_table(self, table_name):
        """Create a table using predefined schema"""
        if table_name not in self.table_schemas:
            self.logger.error(f"No schema defined for table: {table_name}")
            return False

        try:
            schema_sql = self.table_schemas[table_name]
            with self.connection.cursor() as cursor:
                cursor.execute(schema_sql)
                self.connection.commit()

            self.logger.info(f"Successfully created table: {table_name}")
            return True
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Failed to create table {table_name}: {str(e)}")
            return False

    def ensure_table_exists(self, table_name):
        """Ensure table exists, create if it doesn't"""
        if not self.table_exists(table_name):
            self.logger.info(f"Table {table_name} does not exist, creating...")
            return self.create_table(table_name)
        return True

    def create_all_tables(self):
        """Create all predefined tables"""
        success_count = 0
        total_tables = len(self.table_schemas)

        self.logger.info("Creating all predefined tables...")

        for table_name in self.table_schemas.keys():
            if self.ensure_table_exists(table_name):
                success_count += 1

        self.logger.info(f"Table creation completed: {success_count}/{total_tables} successful")
        return success_count == total_tables

    def execute_query(self, query, params=None):
        """Execute a query and return results"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                if query.strip().upper().startswith('SELECT'):
                    return cursor.fetchall()
                else:
                    self.connection.commit()
                    return True
        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Query execution failed: {str(e)}")
            raise e

    def bulk_insert(self, table_name, data_list, on_conflict='NOTHING'):
        """Bulk insert data into specified table"""
        if not data_list:
            return True

        # Ensure table exists before inserting
        if not self.ensure_table_exists(table_name):
            self.logger.error(f"Cannot insert data: table {table_name} does not exist and could not be created")
            return False

        try:
            # Get column names from first record
            columns = list(data_list[0].keys())
            columns_str = ', '.join(columns)
            placeholders = ', '.join(['%s'] * len(columns))

            query = f"""
                INSERT INTO {table_name} ({columns_str})
                VALUES ({placeholders})
                ON CONFLICT DO {on_conflict}
            """

            # Prepare data for bulk insert
            values_list = []
            for record in data_list:
                values = [record.get(col) for col in columns]
                values_list.append(values)

            with self.connection.cursor() as cursor:
                cursor.executemany(query, values_list)
                self.connection.commit()

            self.logger.info(f"Successfully inserted {len(data_list)} records into {table_name}")
            return True

        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Bulk insert failed for table {table_name}: {str(e)}")
            raise e

    def upsert_data(self, table_name, data_list, conflict_columns):
        """Upsert data - update if exists, insert if not"""
        if not data_list:
            return True

        # Ensure table exists before upserting
        if not self.ensure_table_exists(table_name):
            self.logger.error(f"Cannot upsert data: table {table_name} does not exist and could not be created")
            return False

        try:
            columns = list(data_list[0].keys())
            columns_str = ', '.join(columns)
            placeholders = ', '.join(['%s'] * len(columns))

            # Create update clause for non-conflict columns
            update_columns = [col for col in columns if col not in conflict_columns]
            update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])

            conflict_str = ', '.join(conflict_columns)

            query = f"""
                INSERT INTO {table_name} ({columns_str})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_str})
                DO UPDATE SET {update_clause}
            """

            values_list = []
            for record in data_list:
                values = [record.get(col) for col in columns]
                values_list.append(values)

            with self.connection.cursor() as cursor:
                cursor.executemany(query, values_list)
                self.connection.commit()

            self.logger.info(f"Successfully upserted {len(data_list)} records into {table_name}")
            return True

        except Exception as e:
            self.connection.rollback()
            self.logger.error(f"Upsert failed for table {table_name}: {str(e)}")
            raise e