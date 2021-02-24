import math
import datetime
import pytz

from . import constants as c

def generate_days(start, end, step_in_days):
    """Generates a list of datetime.datetimes between `start` and `end` by `step_in_days`.
    
    Args:
        start (datetime.datetime or str): If string, must be of yyyy-mm-dd format.
        end (datetime.datetime or str): If string, must be of yyyy-mm-dd format.
        step_in_days (int): The desired number of days between each day in the resulting list.
    Returns
        list[datetime.datetime]
    """
    if isinstance(start, str):
        start = datetime.datetime.strptime(start, '%Y-%m-%d')
    if isinstance(end, str):
        end = datetime.datetime.strptime(end, '%Y-%m-%d')

    dates = [(start + datetime.timedelta(days = step_in_days * i)) for i in range((end - start).days + 1)]

    return dates

def unix2datetime(unix_timestamp, from_tz = pytz.timezone('UTC'), to_tz = pytz.timezone('America/New_York')):
    """Converts a unix timestamp at `from_tz` to a datetime.datetime object at `to_tz`.

    Args:
        unix_timestamp (int): A unix timestamp.
        from_tz (pytz.timezone, optional): The timezone of the unix timestamp before conversion.
            Defaults to UTC.
        to_tz (pytz.timezone, optional): The target timezone of the resulting datetime.
            Defaults to America/New_York.
    """
    return datetime.datetime.fromtimestamp(unix_timestamp, tz = from_tz).astimezone(to_tz)


def unix2num(unix_timestamp, tz = pytz.timezone('UTC'), resolution = 'milliseconds'):
    """Converts a unix timestamp to UTC float days.

    Convert a unix timestamp (with specified resolution) to the Gregorian
    date as UTC float days, preserving hours, minutes, seconds, microseconds,
    and nanoseconds.
    
    Args:
        unix_timestamp (float): A unix timestamp.
        tz (pytz.timezone): The timezone of the unix timestamp. Defaults to UTC.
        resolution (str): The resolution of the timestamp. Supported values are:
            - seconds
            - milliseconds
            - microseconds
            - nanoseconds
    
    Returns:
        float: The gregorian date as UTC float days.
    """
    # First convert to second resolution and get nanosecond portion, if any
    nanoseconds = 0
    if resolution == 'seconds':
        pass
    elif resolution == 'milliseconds':
        unix_timestamp *= 1e-3
    elif resolution == 'microseconds':
        unix_timestamp *= 1e-6
    elif resolution == 'nanoseconds':
        nanoseconds = unix_timestamp % 1e6
        unix_timestamp *= 1e-9

    # convert to datetime.datetime instance
    # This gets us to microsecond resolution
    dt = datetime.datetime.utcfromtimestamp(unix_timestamp)

    if tz is not None:
        dt = tz.localize(dt)

    if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
        delta = dt.tzinfo.utcoffset(dt)
        if delta is not None:
            dt -= delta

    base = float(dt.toordinal())
    if hasattr(dt, 'hour'):
        base = math.fsum((
            base,
            dt.hour / c.HOURS_PER_DAY,
            dt.minute / c.MINUTES_PER_DAY,
            dt.second / c.SECONDS_PER_DAY,
            dt.microsecond / c.MUSECONDS_PER_DAY,
            nanoseconds / c.NANOSECONDS_PER_DAY 
            ))

    return base


def duration_to_sec(duration):
    """Converts a duration to number of seconds.

    Note that durations over weeks are not exact.

    Args:
        duration (str): Resolution following ISO 8601 duration
            https://www.cmegroup.com/education/courses/introduction-to-futures/understanding-contract-trading-codes.html
                - Y: Year
                - M: Month
                - W: Week
                - D: Day
                - h: hour
                - m: minute
                - s: second
                - ms: millisecond
                - us: microsecond
                - ns: nanosecond
            
    Returns:
        float: number of seconds in one `duration`.
    """
    duration = str(duration)
    if duration == 'ns':
        return 10.0e-9
    elif duration == 'us':
        return 10.0e-6
    elif duration == 'ms':
        return 10.0e-3
    elif duration == 's':
        return 1.0
    elif duration == 'm':
        return 60.0
    elif duration == 'h':
        return 60.0 * 60.0
    elif duration == 'D':
        return 60.0 * 60.0 * 24.0
    elif duration == 'W':
        return 60.0 * 60.0 * 24.0 * 7
    elif duration == 'M':
        return 60.0 * 60.0 * 24.0 * 7 * 30.25
    elif duration == 'Y':
        return 60.0 * 60.0 * 24.0 * 7 * 365.25