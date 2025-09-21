import logging
import azure.functions as func
from daily_cleanup import delete_old_data
from main import main
from datetime import datetime, time, timedelta

app = func.FunctionApp()

from datetime import datetime, time

@app.timer_trigger(schedule="0 10 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def timer_trigger_hourly(myTimer: func.TimerRequest) -> None:
    # Get the current UTC time
    current_utc_time = datetime.utcnow()

    # Convert UTC time to local time zone (IST is UTC+5:30)
    utc_offset = timedelta(hours=5, minutes=30)
    current_local_time = (current_utc_time + utc_offset).time()

    # Define the allowed time range in local time
    start_time = time(6, 30)  # 6:40 AM IST
    end_time = time(23, 59)  # 12:40 AM IST

    # Check if the current local time is within the allowed range
    if not (start_time <= current_local_time <= end_time):
        logging.info(f"Timer trigger paused. Current time {current_local_time} is outside the allowed range of 6:40 AM to 11:40 PM.")
        return

    if myTimer.past_due:
        logging.info('The timer is past due!')

    main()
    logging.info('Python timer trigger function executed.')

@app.timer_trigger(schedule="30 21 * * *", arg_name="dailyCleanupTimer", run_on_startup=False, use_monitor=False)
def daily_cleanup_trigger(dailyCleanupTimer: func.TimerRequest) -> None:
    if dailyCleanupTimer.past_due:
        logging.info('The daily cleanup timer is past due!')

    try:
        delete_old_data()  # Deletes records older than the last 3 days from the database
        logging.info('Daily cleanup completed successfully.')
    except Exception as e:
        logging.error(f"Error during daily cleanup: {e}")
