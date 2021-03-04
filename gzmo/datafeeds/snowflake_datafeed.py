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

    # def __init__(self, topic, query, auth, zmq_context = None):

    #     super().__init__(topic, query, auth, zmq_context)
    
    def execute_query(self):
        """Execute the query and get ready to emit data."""      
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