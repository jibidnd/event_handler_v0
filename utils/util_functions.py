import datetime
import math
import pytz
import importlib

#======================================================================================================================
# Useful constants
#======================================================================================================================
HOURS_PER_DAY = 24.0
MINUTES_PER_HOUR = 60.0
SECONDS_PER_MINUTE = 60.0
MUSECONDS_PER_SECOND = 1e6
NANOSECONDS_PER_SECOND = 1e9
MINUTES_PER_DAY = MINUTES_PER_HOUR * HOURS_PER_DAY
SECONDS_PER_DAY = SECONDS_PER_MINUTE * MINUTES_PER_DAY
MUSECONDS_PER_DAY = MUSECONDS_PER_SECOND * SECONDS_PER_DAY
NANOSECONDS_PER_DAY = NANOSECONDS_PER_SECOND * SECONDS_PER_DAY





def generate_days(start, end, step_in_days):
    '''
    start and end can be either datetime.datetime instances or string of yyyy-mm-dd format.
    Retunrs list of dates as datetime.datetime instances
    '''
    if isinstance(start, str):
        start = datetime.datetime.strptime(start, '%Y-%m-%d')
    if isinstance(end, str):
        end = datetime.datetime.strptime(end, '%Y-%m-%d')

    dates = [(start + datetime.timedelta(days = step_in_days * i)) for i in range((end - start).days + 1)]

    return dates

def to_columnar(lst_dicts, data_keys) -> dict:
    '''
    Converts a list of dict to a dict of list.
    Missing values will be filled with None.
    The passed list_dict are rows with columns names as dict keys.
    The output is a dict with keys with column names and values as row values.

    :param list(dict) lst_dicts: a list of rows as dictionaries
    :param list data_keys: a list of column names to extract from the rows
    '''
    return {k: [dict_i.get(k) for dict_i in lst_dicts] for k in data_keys}

def unix2num(unix_timestamp, tz = pytz.timezone('UTC'), resolution = 'milliseconds'):
    """
    Convert a unix timestamp (nanosecond resolution) to the Gregorian
    date as UTC float days, preserving hours, minutes, seconds, microseconds,
    and nanoseconds.
    Return value is a :func:`float`.
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
            dt.hour / HOURS_PER_DAY,
            dt.minute / MINUTES_PER_DAY,
            dt.second / SECONDS_PER_DAY,
            dt.microsecond / MUSECONDS_PER_DAY,
            nanoseconds / NANOSECONDS_PER_DAY 
            ))

    return base

def _str_to_fn(self, fn_as_str):
    """
    If the argument is not a string, return whatever was passed in.
    Parses a string such as package.module.function, imports the module
    and returns the function.
    :param fn_as_str: The string to parse. If not a string, return it.
    """
    if not isinstance(fn_as_str, str):
        return fn_as_str

    path, _, function = fn_as_str.rpartition('.')
    module = importlib.import_module(path)
    return getattr(module, function)
