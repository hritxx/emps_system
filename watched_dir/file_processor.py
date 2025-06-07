import pandas as pd
import os
import shutil
from datetime import datetime
import logging
from .database import DatabaseManager


class FileProcessor:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.logger = logging.getLogger(__name__)

        # File type mappings
        self.file_mappings = {
            'employee_exit_report': {
                'table': 'employee_exit_report',
                'columns': ['employee_code', 'employee_name', 'business_unit', 'designation',
                            'date_of_joining', 'exit_date', 'expected_resignation_date'],
                'conflict_columns': ['employee_code']
            },
            'employee_master': {
                'table': 'employee_master',
                'columns': ['employee_code', 'employee_name', 'email', 'additional_email',
                            'mobile_number', 'secondary_mobile_number', 'gender', 'date_of_joining',
                            'date_of_birth', 'fax', 'marital_status', 'self_service', 'employee_type',
                            'office_location', 'business_unit', 'designation', 'department', 'grade',
                            'parent_department', 'primary_manager', 'primary_manager_email', 'bank_name',
                            'branch_name', 'account_holder_name', 'account_number', 'account_type',
                            'ifsc_code', 'swift_code', 'pan_number', 'aadhaar_enrollment_number',
                            'aadhaar_number', 'present_address', 'present_state', 'present_city',
                            'present_pincode', 'present_country', 'permanent_address', 'permanent_state',
                            'permanent_city', 'permanent_pincode', 'permanent_country', 'status'],
                'conflict_columns': ['employee_code']
            },
            'employee_work_profile': {
                'table': 'employee_work_profile',
                'columns': ['employee_code', 'employee_name', 'business_unit', 'parent_designation',
                            'assigned_department', 'designation', 'office_location_name'],
                'conflict_columns': ['employee_code']
            },
            'experience_report': {
                'table': 'experience_report',
                'columns': ['employee_code', 'employee_name', 'business_unit', 'department',
                            'designation', 'date_of_joining', 'current_experience', 'past_experience',
                            'total_experience'],
                'conflict_columns': ['employee_code']
            },
            'timesheet_report': {
                'table': 'timesheet_report',
                'columns': ['date', 'employee_code', 'project_id', 'project_name', 'hours_worked'],
                'conflict_columns': ['date', 'employee_code', 'project_id']
            },
            'attendance_report_dailycopy': {
                'table': 'attendance_report_dailycopy',
                'columns': ['date', 'employee_code', 'employee_name', 'clock_in_time',
                            'clock_out_time', 'total_hours'],
                'conflict_columns': ['date', 'employee_code']
            }
        }

    def identify_file_type(self, filename):
        """Identify file type based on filename"""
        filename_lower = filename.lower().replace('.csv', '')

        for file_type in self.file_mappings.keys():
            if file_type in filename_lower:
                return file_type

        return None

    def clean_column_names(self, df):
        """Clean column names to match database schema"""
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')
        return df

    def process_dates(self, df, date_columns):
        """Process date columns to proper format"""
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        return df

    def process_file(self, file_path, processed_folder):
        """Process a single CSV file"""
        try:
            filename = os.path.basename(file_path)
            file_type = self.identify_file_type(filename)

            if not file_type:
                self.logger.warning(f"Unknown file type: {filename}")
                return False

            self.logger.info(f"Processing {filename} as {file_type}")

            # Read CSV file
            df = pd.read_csv(file_path)

            if df.empty:
                self.logger.warning(f"Empty file: {filename}")
                return False

            # Clean column names
            df = self.clean_column_names(df)

            # Get file mapping
            mapping = self.file_mappings[file_type]

            # Process date columns
            date_columns = ['date_of_joining', 'date_of_birth', 'exit_date',
                            'expected_resignation_date', 'date']
            df = self.process_dates(df, date_columns)

            # Convert DataFrame to list of dictionaries
            records = df.to_dict('records')

            # Connect to database
            if not self.db_manager.connect():
                return False

            # Upsert data
            success = self.db_manager.upsert_data(
                mapping['table'],
                records,
                mapping['conflict_columns']
            )

            if success:
                # Move file to processed folder
                processed_path = os.path.join(processed_folder, filename)
                shutil.move(file_path, processed_path)
                self.logger.info(f"Successfully processed and moved {filename}")
                return True
            else:
                return False

        except Exception as e:
            self.logger.error(f"Error processing file {filename}: {str(e)}")
            return False
        finally:
            self.db_manager.disconnect()

    def process_folder(self, folder_path, processed_folder):
        """Process all CSV files in a folder"""
        processed_count = 0
        failed_count = 0

        if not os.path.exists(folder_path):
            self.logger.error(f"Folder does not exist: {folder_path}")
            return processed_count, failed_count

        for filename in os.listdir(folder_path):
            if filename.lower().endswith('.csv'):
                file_path = os.path.join(folder_path, filename)

                if self.process_file(file_path, processed_folder):
                    processed_count += 1
                else:
                    failed_count += 1

        return processed_count, failed_count