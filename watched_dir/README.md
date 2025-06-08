# Watched Directory Module

## Overview

The watched directory module provides automated file processing functionality for the Employee Management System. It monitors designated folders for CSV file uploads and automatically processes them to update the database with employee information.

## What It Does

The module continuously monitors two folders:
- `unprocessed/` - For new CSV files that need to be processed
- `underprocessed/` - For CSV files that need reprocessing

When CSV files are dropped into these folders, the system automatically:
1. Detects the file type based on filename
2. Reads and validates the CSV data
3. Updates the corresponding database tables
4. Moves successfully processed files to the `processed/` folder

## Supported File Types

The system processes the following CSV file types and updates corresponding database tables:

- **employee_exit_report.csv** - Employee exit information → `employee_exit_report` table
- **employee_master.csv** - Complete employee master data → `employee_master` table
- **employee_work_profile.csv** - Employee work assignments and profiles → `employee_work_profile` table
- **experience_report.csv** - Employee experience details → `employee_experience_report` table
- **timesheet_report.csv** - Project time tracking data → `timesheets` table
- **attendance_report_dailycopy.csv** - Daily attendance records → `daily_attendance` table


## Usage Instructions

1. **Place CSV files** in either the `unprocessed/` or `underprocessed/` folder
2. **Run the watcher** using the following command:
   ```bash
   python -m watched_dir.main
   ```
3. **Monitor the logs** to see processing status
4. **Check the processed folder** to confirm files were successfully handled

## File Processing

- Files are automatically detected and processed in real-time
- The system uses upsert operations (insert or update) to handle duplicate records
- All processing activities are logged for monitoring and troubleshooting
- Failed processing attempts are logged with error details

## Configuration

The module uses environment variables for configuration:
- Database connection settings
- Folder paths
- Logging preferences

Make sure the `.env` file is properly configured before running the watcher.

## Logging

All processing activities are logged to:
- Console output (real-time monitoring)
- Log file: `logs/app.log`

The logs include information about:
- Files being processed
- Database operations
- Success/failure status
- Error details when issues occur

## Notes

- Only CSV files are processed; other file types are ignored
- The system handles date formatting and data validation automatically
- Files are moved to the processed folder only after successful database updates
- The watcher runs continuously until manually stopped (Ctrl+C)