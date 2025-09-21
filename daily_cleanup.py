import os
import pyodbc
import logging
from datetime import datetime, timedelta, timezone
from email_sender import send_mail

# Define IST timezone offset (+5:30 from UTC)
IST_OFFSET = timedelta(hours=5, minutes=30)
IST = timezone(IST_OFFSET)

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

def delete_old_data():
    """Delete old records from the database based on the retention policy and send success email."""
    connection_string = get_connection_string()
    start_time_utc = datetime.utcnow()  # Store start time in UTC

    try:
        # Calculate the retention date (current date - 2 days)
        today_utc = datetime.utcnow().date()  # Today in UTC
        retention_date = today_utc - timedelta(days=2)

        # Convert execution time to JST (Japan Standard Time)
        JST_OFFSET = timedelta(hours=9)  # JST is UTC+9
        JST = timezone(JST_OFFSET)
        start_time_jst = datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(JST)
        processed_hour = start_time_jst.strftime("%H:%M")  # Example: `12:30` in JST

        logging.info(f"Cleanup retention date: {retention_date}, Processed Hour (JST): {processed_hour}")

        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            query = u"DELETE FROM T_DAY_POS_SKU_DATA WHERE [当稼動日] < ?"
            logging.info(f"Executing query: {query} with retention_date: {retention_date}")
            cursor.execute(query, retention_date)
            conn.commit()

            # Log the number of rows deleted
            rows_deleted = cursor.rowcount
            logging.info(f"✅ Successfully deleted {rows_deleted} records from T_DAY_POS_SKU_DATA older than {retention_date}")

            # ✅ Send Success Email
            total_time_seconds = round((datetime.utcnow() - start_time_utc).total_seconds(), 2)
            json_data = {
                "is_file_failed": False,
                "error_message": "N/A",
                "total_time_seconds": total_time_seconds,
                "total_rows_processed": f"Successfully deleted {rows_deleted} records from T_DAY_POS_SKU_DATA older than {retention_date}. Daily cleanup completed."
            }
            send_mail(json_data, processed_hour)  # ✅ Send success email with JST processed_hour

    except pyodbc.Error as e:
        logging.error(f"❌ Database error during delete_old_data: {e}")
        json_data = {
            "is_file_failed": True,
            "error_message": f"Database error: {str(e)}",
            "total_time_seconds": "",
            "total_rows_processed": 0
        }
        send_mail(json_data, processed_hour)  # ✅ Send failure email with JST processed_hour
        raise

    except Exception as e:
        logging.error(f"❌ Unexpected error during delete_old_data: {e}")
        json_data = {
            "is_file_failed": True,
            "error_message": f"Unexpected error: {str(e)}",
            "total_time_seconds": "",
            "total_rows_processed": 0
        }
        send_mail(json_data, processed_hour)  # ✅ Send failure email with JST processed_hour
        raise
