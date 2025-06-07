import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Database Configuration
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'epms_db')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')

    # Folder Configuration
    WATCHED_FOLDER_PATH = os.getenv('WATCHED_FOLDER_PATH', './watched_folder')
    UNPROCESSED_FOLDER = os.getenv('UNPROCESSED_FOLDER', './watched_folder/unprocessed')
    UNDERPROCESSED_FOLDER = os.getenv('UNDERPROCESSED_FOLDER', './watched_folder/underprocessed')
    PROCESSED_FOLDER = os.getenv('PROCESSED_FOLDER', './watched_folder/processed')

    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', './logs/app.log')

    @property
    def database_url(self):
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"