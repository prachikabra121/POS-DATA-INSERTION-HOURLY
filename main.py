import os
import io
import gzip
import pyodbc
import logging
import time
from typing import Set, List, Tuple, Optional
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
from threading import Lock
from email_sender import send_mail
import re
import pytz

# Global lock to prevent multiple instances of ETLProcessor from running concurrently
etl_lock = Lock()

def get_connection_string() -> str:
    """Generate the database connection string."""
    return (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.environ['DB_SERVER_NAME']};"
        f"DATABASE={os.environ['DB_NAME']};"
        f"UID={os.environ['DB_USERNAME']};"
        f"PWD={os.environ['DB_PASSWORD']};"
        f"TrustServerCertificate=yes;Encrypt=yes;"
    )

@dataclass
class Config:
    """Configuration settings for the ETL process."""
    azure_connection_string: str
    source_container: str
    backup_container: str
    sql_server: str
    sql_database: str
    sql_username: str
    sql_password: str
    table_name: str
    batch_size: int = 10
    max_workers: int = 4

    @classmethod
    def from_env(cls) -> 'Config':
        """Create configuration from environment variables."""
        return cls(
            azure_connection_string=os.environ["CONNECTION_STR"],
            source_container=os.environ["SOURCE_CONTAINER_NAME"],
            backup_container=os.environ["BACKUP_CONTAINER_NAME"],
            sql_server=os.environ["DB_SERVER_NAME"],
            sql_database=os.environ["DB_NAME"],
            sql_username=os.environ["DB_USERNAME"],
            sql_password=os.environ["DB_PASSWORD"],
            table_name=os.environ["TABLE_NAME"]
        )

class DatabaseConnection:
    """Manages database connections and operations."""

    def __init__(self, config: Config):
        self.config = config
        self.connection_string = get_connection_string()

    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = pyodbc.connect(self.connection_string)
        try:
            yield conn
        finally:
            conn.close()

    def truncate_table(self):
        """Truncate the target table."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {self.config.table_name}")
                row_count = cursor.fetchone()[0]
                if row_count == 0:
                    logging.info(f"Table {self.config.table_name} is already empty.")
                    return

                logging.info(f"Attempting to truncate table: {self.config.table_name}")
                cursor.execute(f"TRUNCATE TABLE {self.config.table_name}")
                conn.commit()
                logging.info(f"Table {self.config.table_name} truncated successfully.")
        except Exception as e:
            logging.error(f"Error truncating table {self.config.table_name}: {e}. Falling back to DELETE.")
            self.delete_all_rows()

    def delete_all_rows(self):
        """Delete all rows from the table as a fallback."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                logging.info(f"Attempting to delete all rows from table: {self.config.table_name}")
                cursor.execute(f"DELETE FROM {self.config.table_name}")
                conn.commit()
                logging.info(f"All rows deleted from table {self.config.table_name}.")
        except Exception as e:
            logging.error(f"Error deleting rows from table {self.config.table_name}: {e}")
            raise

    def execute_stored_procedures(self):
        """Execute the required stored procedures in order."""
        procedures = [
            "SP_Process_SKU_Data_Temp",
            "SP_Process_SKU_Data",
            "SP_Process_Sales_Data",
            "SP_T_DAY_TEN_SALES_FRONT_DATA_HOURLY"
        ]
        with self.get_connection() as conn:
            cursor = conn.cursor()
            for proc in procedures:
                try:
                    logging.info(f"Executing stored procedure: {proc}")
                    cursor.execute(f"EXEC {proc}")
                    conn.commit()
                    logging.info(f"Successfully executed: {proc}")
                except Exception as e:
                    logging.error(f"Error executing stored procedure {proc}: {e}")
                    raise

    def batch_insert(self, batch_data: List[Tuple[str, str]], max_retries: int = 3):
        """Insert a batch of data into the database with retry logic."""
        if not batch_data:
            logging.info("No batch data provided for insertion.")
            return

        retries = 0
        while retries < max_retries:
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    query = f"INSERT INTO {self.config.table_name} (raw_file) VALUES (?)"
                    cursor.fast_executemany = True  # Enable fast execution for bulk inserts
                    cursor.executemany(query, [(record[1],) for record in batch_data])  # Adjust data to match single-column insert
                    conn.commit()
                    logging.info(f"Successfully inserted {len(batch_data)} records into the database.")
                    return
            except pyodbc.Error as e:
                retries += 1
                logging.warning(f"Retry {retries}/{max_retries} for batch insert failed: {e}")
                if retries == max_retries:
                    logging.error("Max retries reached. Batch insert failed.")
                    raise
            except Exception as e:
                logging.error(f"Unexpected error during batch insert: {e}")
                raise


class BlobStorageManager:
    """Manages Azure Blob Storage operations."""

    def __init__(self, config):
        self.config = config
        self.service_client = BlobServiceClient.from_connection_string(config.azure_connection_string)
        self.source_container = self.service_client.get_container_client(config.source_container)
        self.backup_container = self.service_client.get_container_client(config.backup_container)

        # Debug logging for initialization
        logging.info(f"Source container: {config.source_container}")
        logging.info(f"Backup container: {config.backup_container}")

    def get_files_from_last_hour(self, start_time, end_time):
        """Get list of files uploaded in the last hour based on system time."""
        all_files = set()
        prefix = "POS/hourly/data/"
        blobs = self.source_container.list_blobs(name_starts_with=prefix)

        start_time = start_time.replace(tzinfo=timezone.utc)
        end_time = end_time.replace(tzinfo=timezone.utc)

        for blob in blobs:
            if start_time <= blob.last_modified <= end_time:
                all_files.add(blob.name)

        logging.info(f"Found {len(all_files)} files uploaded between {start_time} and {end_time}")
        return all_files

    def process_file(self, file_path):
        """Process a single file from blob storage."""
        try:
            blob_client = self.source_container.get_blob_client(file_path)
            compressed_data = blob_client.download_blob().readall()

            with gzip.GzipFile(fileobj=io.BytesIO(compressed_data)) as gz:
                content = gz.read().decode('utf-8')
                logging.info(f"Successfully processed file: {file_path}")
                return file_path, content
        except Exception as e:
            logging.error(f"Error processing file {file_path}: {e}")
            return None

    def backup_file(self, file_path: str) -> None:
        """Backup and move a file to the backup container."""
        try:
            # Extract the filename and validate file existence
            source_blob = self.source_container.get_blob_client(file_path)
            if not source_blob.exists():
                logging.error(f"Source file {file_path} does not exist. Skipping backup.")
                return

            file_name = file_path.split('/')[-1]
            date_from_file = file_name[8:16]  # Assuming date in YYYYMMDD format in filename

            # Parse date to construct folder structure
            try:
                file_date = datetime.strptime(date_from_file, "%Y%m%d")
                year_folder = file_date.strftime("%Y")
                date_folder = file_date.strftime("%Y%m%d")
                backup_path = f"hourly/{year_folder}/{date_folder}/{file_name}"
            except ValueError:
                logging.warning(f"Invalid or missing date in {file_name}. Storing without folder structure.")
                backup_path = file_name

            # Initialize backup blob client
            backup_blob = self.backup_container.get_blob_client(backup_path)
            if backup_blob.exists():
                logging.info(f"File {file_path} already exists in backup at {backup_path}. Skipping move.")
                return

            # Start copy operation
            logging.info(f"Copying {file_path} to {backup_path} in the backup container...")
            copy_operation = backup_blob.start_copy_from_url(source_blob.url)

            # Wait for copy completion
            while True:
                props = backup_blob.get_blob_properties()
                if props.copy.status == "success":
                    logging.info(f"File {file_path} successfully copied to {backup_path}.")
                    break
                elif props.copy.status == "failed":
                    logging.error(f"Copy operation failed for {file_path}. Aborting backup.")
                    return
                logging.info(f"Copy in progress for {file_path}. Waiting for completion...")
                time.sleep(1)

            # Delete source blob after successful copy
            source_blob.delete_blob()
            logging.info(f"File {file_path} deleted from source container after successful backup.")
        except Exception as e:
            logging.error(f"Error backing up file {file_path}: {e}")


class ETLProcessor:
    """Main ETL process coordinator."""

    def __init__(self, config: Config):
        self.config = config
        self.db = DatabaseConnection(config)
        self.blob_manager = BlobStorageManager(config)
    def process(self):
        """Execute the full ETL process."""
        with etl_lock:
            logging.info("Starting ETL process...")

            start_time = datetime.utcnow()
            total_rows_processed = 0

            try:
                # Step 1: Truncate the raw table
                try:
                    logging.info("Truncating the raw table...")
                    self.db.truncate_table()
                except Exception as e:
                    logging.error(f"Database connection error: {e}")
                    send_mail({"is_file_failed": True, "error_message": f"Database connection failed: {str(e)}"}, processed_hour="Unknown")
                    return

                # Step 2: Fetch files from Blob Storage
                end_time = datetime.utcnow()
                fetch_start_time = end_time - timedelta(hours=1)
                logging.info(f"Fetching files uploaded between {fetch_start_time} and {end_time}...")

                files_from_last_hour = self.blob_manager.get_files_from_last_hour(fetch_start_time, end_time)
                processable_files = [f for f in files_from_last_hour if not f.endswith("_END")]
                non_processable_files = [f for f in files_from_last_hour if f.endswith("_END")]

                logging.info(f"Total files found: {len(files_from_last_hour)}, "
                            f"Processable: {len(processable_files)}, Non-processable: {len(non_processable_files)}")

                # ✅ Extract processed hour from filenames
                fetch_start_time = datetime.utcnow().replace(tzinfo=pytz.utc)

                # Convert to JST
                jst_timezone = pytz.timezone("Asia/Tokyo")
                fetch_start_time_jst = fetch_start_time.astimezone(jst_timezone)

                # Format the hour in JST
                processed_hour = fetch_start_time_jst.strftime("%H:00")
                # processed_hour = fetch_start_time.strftime("%H:00")  # Default if no files found

                if processable_files:
                    first_file = processable_files[0]  # Take first file for reference
                    match = re.search(r"R520\d{4}(\d{8})(\d{2})\d{4}", first_file)  # Extract YYYYMMDD HH MMSS
                    if match:
                        processed_hour = f"{match.group(2)}:00"  # Extracted hour in HH:00 format

                # ✅ If no files are found, send an email and exit
                if not files_from_last_hour:
                    logging.warning("No files found in Blob Storage for the last hour.")
                    json_data = {
                        "is_file_failed": False,
                        "error_message": f"No files were found in the blob storage for the hour: {processed_hour}. ETL process stopped.",
                        "total_time_seconds": 0,
                        "total_rows_processed": 0
                    }
                    send_mail(json_data, processed_hour)
                    return

            except Exception as e:
                logging.error(f"Failed to fetch files from Blob Storage: {e}")
                send_mail({"is_file_failed": True, "error_message": str(e)}, processed_hour)
                return

            try:
                # Step 3: Process files and batch insert into the database (if any)
                if processable_files:
                    results = []
                    with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                        results = list(filter(None, executor.map(self.blob_manager.process_file, processable_files)))

                    if results:
                        batch_size = self.config.batch_size
                        for i in range(0, len(results), batch_size):
                            batch = results[i:i + batch_size]
                            try:
                                logging.info(f"Inserting batch {i // batch_size + 1} with {len(batch)} records...")
                                self.db.batch_insert(batch)
                                logging.info(f"Batch {i // batch_size + 1} inserted successfully.")
                                total_rows_processed += len(batch)  # Track processed rows
                            except Exception as e:
                                logging.error(f"Failed to insert batch {i // batch_size + 1}: {e}")
                                send_mail({"is_file_failed": True, "error_message": str(e)}, processed_hour)
                                return

            except Exception as e:
                logging.error(f"Failed during file processing or batch insert: {e}")
                send_mail({"is_file_failed": True, "error_message": str(e)}, processed_hour)
                return

            try:
                # Step 4: Execute stored procedures (only if processable files exist)
                if processable_files:
                    logging.info("Executing stored procedures...")
                    self.db.execute_stored_procedures()
                    logging.info("Stored procedures executed successfully.")
            except Exception as e:
                logging.error(f"Failed to execute stored procedures: {e}")
                json_data = {
                    "is_file_failed": True,
                    "error_message": str(e),
                    "total_time_seconds": round((datetime.utcnow() - start_time).total_seconds(), 2),
                    "total_rows_processed": total_rows_processed
                }
                send_mail(json_data, processed_hour)
                return

            try:
                # Step 5: Backup files (all files)
                logging.info("Backing up all files to the backup container...")
                all_files_to_backup = processable_files + non_processable_files
                with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                    executor.map(self.blob_manager.backup_file, all_files_to_backup)
                logging.info("All files backed up successfully.")
            except Exception as e:
                logging.error(f"Failed to backup files: {e}")
                send_mail({"is_file_failed": True, "error_message": str(e)}, processed_hour)
                return

            # Compute ETL statistics
            total_time_seconds = round((datetime.utcnow() - start_time).total_seconds(), 2)

            logging.info("ETL process completed successfully.")
            logging.info(f"Total processed files: {len(processable_files)}")

            # ✅ Send success email
            json_data = {
                "is_file_failed": False,
                "error_message": "N/A",
                "total_time_seconds": total_time_seconds,
                "total_rows_processed": total_rows_processed
            }
            send_mail(json_data, processed_hour)

            
def main():
    """Main entry point."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    try:
        config = Config.from_env()  # ✅ Ensure Config object is passed
        processor = ETLProcessor(config)  # ✅ Pass config argument
        processor.process()
    except Exception as e:
        error_message = str(e)
        logging.error(f"ETL process failed: {error_message}")

        # ✅ Extract the processed hour from system time
        fetch_start_time = datetime.utcnow() - timedelta(hours=1)
        processed_hour = fetch_start_time.strftime("%H:00")  # Expected hour

        # ✅ Send Failure Email when ETL Fails
        json_data = {
            "is_file_failed": True,
            "error_message": error_message,
            "total_time_seconds": 0,
            "total_rows_processed": 0
        }
        send_mail(json_data, processed_hour)  # ✅ Pass `processed_hour`
