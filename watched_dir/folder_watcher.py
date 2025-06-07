import os
import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from .file_processor import FileProcessor
from .config import Config


class CSVFileHandler(FileSystemEventHandler):
    def __init__(self, processor, processed_folder):
        self.processor = processor
        self.processed_folder = processed_folder
        self.logger = logging.getLogger(__name__)

    def on_created(self, event):
        if event.is_directory:
            return

        file_path = event.src_path
        filename = os.path.basename(file_path)

        if filename.lower().endswith('.csv'):
            self.logger.info(f"New CSV file detected: {filename}")

            # Wait a moment to ensure file is completely written
            time.sleep(2)

            # Process the file
            self.processor.process_file(file_path, self.processed_folder)

    def on_moved(self, event):
        if event.is_directory:
            return

        dest_path = event.dest_path
        filename = os.path.basename(dest_path)

        if filename.lower().endswith('.csv'):
            self.logger.info(f"CSV file moved to watched folder: {filename}")

            # Wait a moment to ensure file is completely written
            time.sleep(2)

            # Process the file
            self.processor.process_file(dest_path, self.processed_folder)


class FolderWatcher:
    def __init__(self):
        self.config = Config()
        self.processor = FileProcessor()
        self.logger = logging.getLogger(__name__)
        self.observers = []

    def setup_folders(self):
        """Create watched folders if they don't exist"""
        folders = [
            self.config.UNPROCESSED_FOLDER,
            self.config.UNDERPROCESSED_FOLDER,
            self.config.PROCESSED_FOLDER,
            os.path.dirname(self.config.LOG_FILE)
        ]

        for folder in folders:
            if not os.path.exists(folder):
                os.makedirs(folder)
                self.logger.info(f"Created folder: {folder}")

    def process_existing_files(self):
        """Process any existing files in watched folders"""
        self.logger.info("Processing existing files...")

        # Process unprocessed folder
        processed, failed = self.processor.process_folder(
            self.config.UNPROCESSED_FOLDER,
            self.config.PROCESSED_FOLDER
        )
        self.logger.info(f"Unprocessed folder: {processed} processed, {failed} failed")

        # Process underprocessed folder
        processed, failed = self.processor.process_folder(
            self.config.UNDERPROCESSED_FOLDER,
            self.config.PROCESSED_FOLDER
        )
        self.logger.info(f"Underprocessed folder: {processed} processed, {failed} failed")

    def start_watching(self):
        """Start watching folders for new files"""
        self.logger.info("Starting folder watcher...")

        # Watch unprocessed folder
        unprocessed_handler = CSVFileHandler(self.processor, self.config.PROCESSED_FOLDER)
        unprocessed_observer = Observer()
        unprocessed_observer.schedule(
            unprocessed_handler,
            self.config.UNPROCESSED_FOLDER,
            recursive=False
        )
        unprocessed_observer.start()
        self.observers.append(unprocessed_observer)

        # Watch underprocessed folder
        underprocessed_handler = CSVFileHandler(self.processor, self.config.PROCESSED_FOLDER)
        underprocessed_observer = Observer()
        underprocessed_observer.schedule(
            underprocessed_handler,
            self.config.UNDERPROCESSED_FOLDER,
            recursive=False
        )
        underprocessed_observer.start()
        self.observers.append(underprocessed_observer)

        self.logger.info("Folder watchers started successfully")

    def stop_watching(self):
        """Stop all folder watchers"""
        self.logger.info("Stopping folder watchers...")

        for observer in self.observers:
            observer.stop()
            observer.join()

        self.observers.clear()
        self.logger.info("All folder watchers stopped")