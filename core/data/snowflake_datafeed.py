import time
import configparser

import zmq
import msgpack

import snowflake.connector
from snowflake.connector.converter_null import SnowflakeNoConverterToPython

from ..data import BaseDataFeed
from .. import constants as c

class SnowflakeDataFeed(BaseDataFeed):

    def __init__(self, topic, query, auth, zmq_context = None):

        super().__init__(topic, zmq_context)
        self.query = query
        # Get auth for connecting to snowflake
        # if a path if provided
        if isinstance(auth, str):
            # read the file
            config = configparser.ConfigParser()
            config.read_file(open(auth))
            self.auth = config['snowflake']
        else:
            self.auth = auth
    
    def execute_query(self):
        
        # Otherwise auth should be a dictioanry-like object with the following keys
        # Get information
        user = self.auth['user']
        password = self.auth['password']
        account = self.auth['account']
        
        # connect to snowflake
        # Specify no type conversions to preserve precision
        self.con = snowflake.connector.connect(
            user = user,
            password = password,
            account = account, 
            converter_class = SnowflakeNoConverterToPython
        )
        # request results to be returned as dictionaries
        self.cur = self.con.cursor(snowflake.connector.DictCursor)

        self.cur.execute(self.query)

    def get(self, limit = None):

        # if starting over
        if self.from_beginning:
            self.execute_query()
        
        if limit is None:
            res = self.cur.fetchall()
            res_with_topic = [(self.topic, res_i) for res_i in res]
            return res_with_topic
        else:
            res = self.cur.fetchmany(limit)
            res_with_topic = [(self.topic, res_i) for res_i in res]
            return res_with_topic

    def publish(self):
        # TODO: sync to start

        # if starting over
        if self.from_beginning:
            self.execute_query()

        # stopping event
        # tempts = 0
        # stopping_event = {c.EVENT_TS: -1, c.EVENT_TYPE: c.DATA, c.EVENT_SUBTYPE: c.INFO}

        # send_more = True
        # rows_sent = 0

        while (not self.shutdown_flag) & (not self.is_finished):

            res = self.cur.fetchone()
            # print(res)

            if res is not None:
                # msgpack
                res_packed = msgpack.packb(res, use_bin_type = True, default = self.default_conversion)
                tempts = res[c.EVENT_TS]
                # send the event with a topic
                try:
                    self.sock_out.send_multipart([self.topic.encode(), res_packed], flag = zmq.NOBLOCK)
                    
                except zmq.ZMQError as exc:
                    if exc.errno == zmq.EAGAIN:
                        # Drop messages if queue is full
                        pass
                    else:
                        # sock.send_multipart([b'', msgpack.packb(stopping_event, use_bin_type = True, default = self.default_conversion)])
                        self.sock_out.close()
                        raise
            else:
                # wait a second before we kill this off
                # TODO: what if there are multiple datafeeds?
                time.sleep(1)
                self.is_finished = True
                self.shutdown_flag = True
                # stopping_event.update({c.EVENT_TS: tempts})
                # stopping_event = msgpack.packb(stopping_event, use_bin_type = True, default = self.default_conversion)
                # self.sock_out.send_multipart([b'', stopping_event])
                self.sock_out.close()
                break
        
        self.sock_out.close()

