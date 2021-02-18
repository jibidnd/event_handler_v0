import configparser

import zmq
import snowflake.connector
from snowflake.connector.converter_null import SnowflakeNoConverterToPython

from . import BaseDataFeed
from .. import utils
from ..utils import constants as c

class SnowflakeDataFeed(BaseDataFeed):
    """Datafeed that gets data from a snowflake database.
    
        Args:
            query (str): The query that returns data as records.
            auth (dict or str): Authentication method to connection to Snowflake.
                If a dict is provided, expected to have the following keys:
                    - user
                    - password
                    - account
                If a string is provided, expected to be a path to a config file with
                    a 'snowflake' section, with the same keys expected in the dict
                    documented above.
    
    """

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
        """Execute the query and get ready to emit data.
        """        
        # Get auth information
        user = self.auth['user']
        password = self.auth['password']
        account = self.auth['account']
        
        # connect to snowflake
        # Specify no type conversions to preserve precision
        self.con = snowflake.connector.connect(
            user = user,
            password = password,
            account = account, 
            converter_class = SnowflakeNoConverterToPython  # is this not working??
        )
        # request results to be returned as dictionaries
        self.cur = self.con.cursor(snowflake.connector.DictCursor)
        # execute the query
        self.cur.execute(self.query)
        # indicate that we have started and do not start from the beginning
        self.from_beginning = False

    def fetch(self, limit = 1):
        """Return the requested number of records.

            `limit` records will be returned. If limit == 1, a dictionary of a single record
                will be returned. If limit > 1, a list of dictionaries will be returned. The keys
                of the dictionaries will be the column names, and the values will be the record values.
            
            If self.from_beginning is True (as it is set when the datafeed is instantiated),
            the query will be executed when this method is called.


        Args:
            limit (int, optional): The number of records to return. Defaults to 1.

        Returns:
            list[dict] or dict: The queried data as record(s).
        """        
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
        """Publishes the queried data to the socket.

            The queried data will be published to the socket, record by record, until
            all records have been published.

            When called, it will wait for the `start_sync` flag to be set, if not already.
            Then, it will fetch one record at a time from the connector cursor, and publish
            the record under the `topic` of the datafeed. The record will be packed as a
            msgpack message.

            If self.from_beginning is True (as it is set when the datafeed is instantiated),
            the query will be executed when this method is called.

            Note that if the ZMQ queue is full, DATA WILL BE (SILENTLY) DROPPED.
        """        
        # if starting over
        if self.from_beginning:
            self.execute_query()
        
        # wait for the starting signal
        self.start_sync.wait()

        # Keep going?
        while (not self.main_shutdown_flag.is_set()) and \
                (not self.shutdown_flag.is_set()) and \
                (not self.is_finished):
            
            # get one row of result everytime
            # maybe slower but won't have to worry about size of results
            res = self.cur.fetchone()

            if res is not None:
                res_packed = utils.packb(res)
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

                break
        
        # shut down gracefully
        self.shutdown()

        return
    
    def shutdown(self):
        """Shuts down gracefully."""
        self.shutdown_flag.set()
        self.sock_out.close(linger = 10)