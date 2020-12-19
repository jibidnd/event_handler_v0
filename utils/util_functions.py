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



# ----------------------------------------------------------------------------------------------------
# Data stuff
# ----------------------------------------------------------------------------------------------------

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

def unix2datetime(unix_timestamp, from_tz = pytz.timezone('UTC'), to_tz = pytz.timezone('America/New_York')):
    '''
    Converts a unix timestamp @ tz in resolution to a datetime.datetime object
    '''
    return datetime.datetime.fromtimestamp(unix_timestamp, tz = from_tz).astimezone(to_tz)


def unix2num(unix_timestamp, tz = pytz.timezone('UTC'), resolution = 'milliseconds'):
    """
    Convert a unix timestamp (with specified resolution) to the Gregorian
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


import socket
# Getting a random free tcp port in python using sockets
def get_free_tcp_address(port = 1234, max_port = 1300, exclude = None):
    exclude = exclude or []
    while port <= max_port:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(('', port))
            host, free_port = sock.getsockname()
            
            address = f'tcp://{host}:{free_port}'
            if (address not in exclude):
                sock.close()
                return address, host, free_port
            else:
                port += 1
                sock.close()
        except OSError as exc:
            sock.close()
            port += 1
        except:
            raise
    raise IOError('No free ports.')


def duration_to_sec(duration):
    '''
        Resolution following ISO 8601 duration
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
        
        Durations over weeks are not exact.
    '''
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
    elif duraiton == 'D':
        return 60.0 * 60.0 * 24.0
    elif duration == 'W':
        return 60.0 * 60.0 * 24.0 * 7
    elif duration == 'M':
        return 60.0 * 60.0 * 24.0 * 7 * 30.25
    elif duration == 'Y':
        return 60.0 * 60.0 * 24.0 * 7 * 365.25


def default_conversion(obj):
    try:
        return float(obj)
    except:
        return str(obj)
