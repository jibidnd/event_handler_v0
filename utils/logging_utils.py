'''
This library is provided to allow standard python logging
to output log data as JSON formatted strings
'''
import logging
import json
import re
from datetime import date, datetime, time
import traceback
import importlib

from . import snowflake_utils


def jsonify_log_record(record) -> str:
    '''
    Formats a log record and serializes to json
    Adapted from https://github.com/madzak/python-json-logger
    '''

    # LogRecord attributes
    # http://docs.python.org/library/logging.html#logrecord-attributes
    RESERVED_ATTRS = (
        'args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename',
        'funcName', 'levelname', 'levelno', 'lineno', 'module',
        'msecs', 'message', 'msg', 'name', 'pathname', 'process',
        'processName', 'relativeCreated', 'stack_info', 'thread', 'threadName')

    message_dict = {}

    # Format the log message
    if isinstance(record.msg, dict):
        # attempt to save it as json
        message_dict['msg'] = json.dumps(record.msg, default = str)    # default to string-ify anything non-serializable
    else:
        # otherwise just a string
        message_dict['msg'] = record.getMessage()
    
    # Load the LogRecord Attributes
    formatter = logging.Formatter()
    message_dict = {record.__dict__.get(attr) for attr in RESERVED_ATTRS}
    # Format exception info
    if message_dict.get('exc_info'):
        message_dict['exc_info'] = formatter.formatException(message_dict.get(exc_info))
    else:
        message_dict['exc_info'] = None
    # Format stack info
    if message_dict.get('stack_info'):
        message_dict['stack_info'] = formatter.formatException(message_dict.get(stack_info))
    else:
        message_dict['stack_info'] = None

    # return the json string
    return json.dumps(message_dict, sort_keys = True)


class SnowflakeLogger(logging.Handler):
    '''
    Writes log message to snowflake.
    '''
    def __init__(self, log_location = None):
        if not isinstance(log_location, str):
            raise Exception('Please indicate table to log to.')
        
        # Connect to snowflake
        self.writer = snowflake_utils.SnowflakeWriter()
        self.cursor = self.writer.cursor()
        
        # Create table to log to if it doesn't already exist
        create_sql = (f"CREATE TABLE IF NOT EXISTS {log_location} ("
                        f"log_message VARIANT"
                        f"CREATED_AT_UTC TIMESTAMP_NTZ(9) DEFAULT CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ(9)))"
                        ")")
        self.cursor.execute(create_sql)
    
    def emit(self, record):      
        insert_sql = (f" INSERT INTO {self.log_location} (log_message) SELECT TRY_PARSE_JSON({jsonify_log_record(record)})")
        self.cursor.execute(insert_sql)