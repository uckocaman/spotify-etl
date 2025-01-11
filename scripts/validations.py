# Description: This script contains functions to validate the data and timestamps.
import pandas as pd
import logging
import pytz
from dateutil import parser
from datetime import datetime


def check_if_valid_data(df: pd.DataFrame, *keys: str) -> bool:
    """
    Validates the given DataFrame based on the following checks:
    1. DataFrame is not empty.
    2. Primary key(s) are unique.
    3. No null values in the DataFrame.

    Args:
        df (pd.DataFrame): DataFrame to validate.
        keys (str): Column names to be used as primary keys.

    Returns:
        bool: True if all checks pass, False otherwise.
    """
    # Check if dataframe is empty
    if df.empty:
        logging.info("No songs downloaded. Finishing execution")
        return False

    # Primary Key Check
    if keys and not df.set_index(list(keys)).index.is_unique:
        logging.exception("Primary Key check is violated for keys: %s", keys)
        raise ValueError("Primary Key check is violated")

    # Check for nulls
    if df.isnull().values.any():
        logging.exception("Null values found in DataFrame")
        raise ValueError("Null values found")

    return True


def check_if_valid_interval(
    timestamps: list[str], current_datetime: datetime, interval_hour: int
) -> bool:
    """
    Validates if all timestamps are within the specified interval.

    Args:
        timestamps (list[str]): List of timestamp strings to validate.
        current_datetime (datetime): The current datetime for comparison.
        interval_hour (int): The interval in hours.

    Returns:
        bool: True if all timestamps are within the interval, False otherwise.
    """
    # Check that all timestamps are in the interval
    for ts in timestamps:
        # Parse the timestamp and convert to the specified timezone
        ts = parser.parse(ts).astimezone(pytz.timezone("Europe/Istanbul"))
        ts = ts.replace(tzinfo=None)

        # Calculate the difference in hours
        diff = int((current_datetime - ts).seconds // 3600)

        # If the difference is greater than the interval, log and raise an exception
        if diff > interval_hour:
            logging.exception(
                "At least one of the returned songs is not within the interval of %d hours",
                interval_hour,
            )
            raise ValueError("Timestamp not within the valid interval")

    return True
