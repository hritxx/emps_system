import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from .config import Config


class DatabaseManager:
    def __init__(self):
        self.config = Config()
        self.connection = None
        self.logger = logging.getLogger(__name__)

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