import azure.functions as func
import logging
import json
import requests
import re
from datetime import datetime
from typing import Dict
import os
from datetime import datetime, timezone, timedelta


# Constants (Replace with your actual values)

LOGIC_APP_URL = os.getenv("LOGIC_APP_URL")
EMAIL_TO = os.getenv("EMAIL_TO")
ENV = "BELC-BI-ESS-BASE-PROD"

SUCCESS_SUBJECT = "🎉 Success! POS Data Inserted Successfully into the Database!"
FAILURE_SUBJECT = "⚠️ Error: Failed to Insert POS Data into the Database."

JST_OFFSET = timedelta(hours=9)
JST = timezone(JST_OFFSET)

def send_mail(json_data: Dict, processed_hour: str):
    """
    Send an email notification based on pipeline success or failure.
    Args:
        json_data (dict): Dictionary containing pipeline execution details.
        processed_hour (str): The hour extracted from the processed file name.
    """
    function_name = "BELC-BI-ESS-BASE-Hourly-POS-Ingestion-Prod"
    is_file_failed = json_data.get("is_file_failed", False)
    error_message = json_data.get("error_message", "None")
    total_time_seconds = float(json_data.get("total_time_seconds", 0))
    total_rows_processed = json_data.get("total_rows_processed", "N/A")


    total_time_minutes = round(total_time_seconds / 60, 2)

    # Convert Current UTC Time to JST
    current_time_jst = datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(JST)
    current_date_jst = current_time_jst.strftime("%Y-%m-%d")
    # current_time_jst_str = current_time_jst.strftime("%H:%M")  # Example: "14:30"

    # Define pipeline status message
    pipeline_status = "✅ Pipeline executed successfully!" if not is_file_failed else f"❌ Pipeline execution failed! Error: {error_message}"

    message = f"""
    <p><b>Environment:</b> {ENV}</p>
    <p><b>Pipeline Status:</b> {pipeline_status}</p>
    <p><b>Function Details:</b></p>
    <ul>
        <li>📁<b>Function Name:</b> {function_name}</li>
        <li>⚠️ <b>Error Message:</b> {error_message}</li>
    </ul>
    <p><b>Execution Summary:</b></p>
    <ul>
        <li>📊 <b>Total Files Processed:</b> {total_rows_processed}</li>
        <li>⏳ <b>Total Execution Time:</b> {total_time_minutes} minutes</li>
        <li>📅 <b>Current Date (JST):</b> {current_date_jst}</li>
        <li>🕒 <b>Processed Hour:</b> {processed_hour}</li>
       
    </ul>
    <p style="color: gray; font-style: italic; font-size: 12px; border-top: 1px solid #ccc; padding-top: 10px;">
        🚫 <b>This is an automated system message. Please do not reply.</b> 
    </p>
    """
    #  <li>🕒 <b>Current Time (JST):</b> {current_time_jst_str}</li>

    subject = FAILURE_SUBJECT if is_file_failed else SUCCESS_SUBJECT

    email_data = {
        "EmailFrom": "sureshjindam@vebuin.com",
        "EmailTo": EMAIL_TO,
        "Subject": subject,
        "Body": message,
    }

    logging.info(f"Sending email with subject: {subject}")

    try:
        response = requests.post(LOGIC_APP_URL, json=email_data)

        if response.status_code == 200:
            logging.info("Email sent successfully via Logic App.")
        else:
            logging.error(f"Failed to send email. Status: {response.status_code}, Response: {response.text}")

    except requests.RequestException as e:
        logging.error(f"Request error occurred: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")