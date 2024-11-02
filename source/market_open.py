import datetime
import pytz
import sys
import os
import time
import holidays

def is_weekend():
    """
    Checks if today is a weekend (Saturday or Sunday).

    Returns:
        bool: True if today is Saturday or Sunday, False otherwise.
    """
    local_timezone = pytz.timezone('Europe/Paris')
    current_datetime = datetime.datetime.now(local_timezone)
    return current_datetime.weekday() >= 5  # 5 = Saturday, 6 = Sunday


def is_us_holiday(date, year=None):
    """
    Checks if a given date is a US holiday.

    Args:
        date (datetime.date): Date to check.
        year (int, optional): Year of the holiday calendar. Defaults to current year.

    Returns:
        bool: True if the date is a US holiday, False otherwise.
    """
    if year is None:
        year = date.year
    us_holidays = holidays.US(years=year)
    return date in us_holidays


def calculate_time_difference(desired_time):
    """
    Calculates the time difference between the current UTC time and a desired UTC time.

    Args:
        desired_time (datetime.time): Desired time in UTC.

    Returns:
        datetime.timedelta: Time difference rounded to seconds.
    """
    current_time_utc = datetime.datetime.now(datetime.timezone.utc)
    current_date = datetime.datetime.now().date()
    desired_time_utc = datetime.datetime.combine(current_date, desired_time, tzinfo=datetime.timezone.utc)
    time_difference = desired_time_utc - current_time_utc
    return datetime.timedelta(seconds=int(time_difference.total_seconds()))


def market_is_open(print_event=False):
    """
    Determines if the market is currently open based on time, weekend, and US holiday status.

    Args:
        print_event (bool, optional): If True, prints the market status. Defaults to False.

    Returns:
        bool: True if the market is open, False otherwise.
    """
    local_timezone = pytz.timezone('Europe/Paris')
    current_time = datetime.datetime.now(local_timezone).astimezone(pytz.utc)

    opening_time = datetime.time(9, 30)
    closing_time = datetime.time(17, 30)
    current_day = current_time.strftime("%A")
    current_date = current_time.date()

    # Define opening and closing datetimes in UTC
    datetime_opening = datetime.datetime.combine(current_date, opening_time, tzinfo=datetime.timezone.utc)
    datetime_closing = datetime.datetime.combine(current_date, closing_time, tzinfo=datetime.timezone.utc)

    # Market open conditions
    if datetime_opening <= current_time <= datetime_closing and not is_us_holiday(current_date) and not is_weekend():
        if print_event:
            print(f"The market is OPEN until {datetime_closing.time()} UTC.")
        return True

    # Print closing reasons
    if is_us_holiday(current_date):
        if print_event:
            print("The market is closed. It's a holiday in the US.")
    elif current_day == "Saturday":
        if print_event:
            print("The market is closed. It's Saturday. It will open on Monday at 09:30 UTC.")
    elif current_day == "Sunday":
        if print_event:
            print("The market is closed. It's Sunday. It will open on Monday at 09:30 UTC.")
    else:
        if print_event:
            print("The market is closed.")

    return False


def clear_screen():
    """
    Clears the console screen based on the operating system.
    """
    command = "cls" if os.name == "nt" else "clear"
    os.system(command)


if __name__ == "__main__":
    while True:
        print(market_is_open(print_event=True))
        time.sleep(1)
        clear_screen()
