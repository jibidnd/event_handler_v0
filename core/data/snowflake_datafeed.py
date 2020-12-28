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

        self.from_beginning = False

    def fetch(self, limit = 1):
        # if starting over
        if self.from_beginning:
            self.execute_query()
        
        if limit is None:
            res = self.cur.fetchall()
        else:
            res = self.cur.fetchmany(limit)
        
        # add topic to results
        for res_i in res:
            res_i.update({c.TOPIC: self.topic})

        # set flag if nothing else to get
        if len(res) == 0:
            self.is_finished = True
            return
        
        # return a list only if limit > 1
        if limit > 1:
            return res
        elif limit == 1:
            return res[0]


    def publish(self):

        # if starting over
        if self.from_beginning:
            self.execute_query()

        # Keeping going?
        while (not self.main_shutdown_flag.is_set()) and \
                (not self.shutdown_flag.is_set()) and \
                (not self.is_finished):
            
            # wait for the starting signal
            self.start_sync.wait()

            # get one row of result everytime
            # maybe slower but won't have to worry about size of results
            res = self.cur.fetchone()

            if res is not None:
                # msgpack
                res_packed = msgpack.packb(res, use_bin_type = True, default = self.default_conversion)
                tempts = res[c.EVENT_TS]
                # send the event with a topic
                try:
                    self.sock_out.send_multipart([self.topic.encode(), res_packed], flag = zmq.NOBLOCK)
                except zmq.ZMQError as exc:
                    # Drop messages if queue is full
                    if exc.errno == zmq.EAGAIN:
                        pass
                    else:
                        # unexpected error: shutdown and raise
                        self.shutdown()
                        # sock.send_multipart([b'', msgpack.packb(stopping_event, use_bin_type = True, default = self.default_conversion)])
                        raise
                except zmq.ContextTerminated:
                    # context is being closed by session
                    self.shutdown()
                except:
                    raise
            else:
                # no more results
                self.is_finished = True
                self.shutdown()

                # stopping_event.update({c.EVENT_TS: tempts})
                # stopping_event = msgpack.packb(stopping_event, use_bin_type = True, default = self.default_conversion)
                # self.sock_out.send_multipart([b'', stopping_event])
                break
        
        # shut down gracefully
        self.shutdown()
    
    def shutdown(self):
        self.shutdown_flag.set()
        self.sock_out.close(linger = 10)