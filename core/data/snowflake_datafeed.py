import datetime
import time
import pytz
import sys
import os

import configparser

import snowflake.connector
from snowflake.connector.converter_null import SnowflakeNoConverterToPython
import zmq
import msgpack

from .. import constants as c

class SnowflakeFeed:

    def __init__(self, query, address, auth):
        
        self.query = query
        self.address = address
        
        # Get auth
        # if a path if provided
        if isinstance(auth, str):
            # read the file
            self.config_path = config_path or os.path.join(os.path.expanduser('~'), 'creds.auth')
            config = configparser.ConfigParser()
            config.read_file(open(config_path))
            # Get information
            user = config.get('snowflake', 'user')
            password = config.get('snowflake', 'password')
            account = config.get('snowflake', 'account')
        elif isinstance(auth, dict):
            # Get information
            user = auth['user']
            password = auth['password']
            account = auth['account']
        
        # connect to snowflake
        # Specify no type conversions to preserve precision
        self.con = snowflake.connector.connect(
            user = user,
            password = password,
            account = account,
            converter_class = SnowflakeNoConverterToPython
        )
        # return results as dictionary
        self.cur = self.con.cursor(snowflake.connector.DictCursor)

        # Connect to a port
        self.context = zmq.Context.instance()
        self.sock = context.socket(zmq.PUB)
        self.sock.bind(address)

    def run(self):
        cur.execute(query)

        # stopping event
        tempts = 0
        stopping_event = {c.EVENT_TS: -1, c.EVENT_TYPE: c.DATA, c.EVENT_SUBTYPE: c.INFO}
    
        while True:
            res = cur.fetchone()

            if res is not None:
                # repackage the event
                res[c.EVENT_TS] = float(res.pop('TIMESTAMP')) / 1000.0  # convert to seconds
                res[c.EVENT_TYPE] = 'DATA'
                res[c.SYMBOL] = res.pop('TICKER')
                # msgpack
                res_packed = msgpack.packb(res, use_bin_type = True, default = self.default_conversion)
                tempts = res[c.EVENT_TS]
                # send the event with the symbol as topic
                try:
                    sock.send_multipart([res[c.SYMBOL].encode(), res_packed], flag = zmq.NOBLOCK)
                except zmq.ZMQError as exc:
                    if exc.errno == zmq.EAGAIN:
                        # Drop messages if queue is full
                        pass
                    else:
                        
                        # sock.send_multipart([b'', msgpack.packb(stopping_event, use_bin_type = True, default = self.default_conversion)])
                        context.destroy()
                        raise
            else:
                # wait a second before we kill this off
                time.sleep(1)
                stopping_event.update(c.EVENT_TS: tempts)
                stopping_event = msgpack.packb(stopping_event, use_bin_type = True, default = default_conversion)
                sock.send_multipart([b'', stopping_event])
                context.destroy()
                break
    
    @staticmethod
    def default_conversion(obj):
        try:
            return float(obj)
        except:
            return str(obj)



