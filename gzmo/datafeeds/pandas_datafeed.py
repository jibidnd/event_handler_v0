from collections import deque

import pandas as pd

from . import BaseDataFeed
from ..utils import constants as c


class PandasDatafeed(BaseDataFeed):
    """Datafeed from pandas dataframe.
    
    
    """

    def __init__(self, topic, df, auth=None, zmq_context=None):
        super().__init__(topic, query=None, auth=auth, zmq_context=zmq_context)
        
        self.df = df
        self.next_row = 0
    
    # Nothing to format since a pre-formatted dataframe is passed
    def format_query(self):
        pass
    
    # Nothing to format since a pre-formatted dataframe is passed
    def execute_query(self):
        pass
    
    def format_result(self, result):
        result[c.TOPIC] = self.topic
        result[c.EVENT_TS] = pd.Timestamp(result[c.EVENT_TS])
        return result
    
    def fetch(self, limit = 1):
        """Return available records up to `limit` as a list of events (dictionaries).

            If self.from_beginning is True (as it is set when the datafeed is instantiated),
            the query will be executed when this method is called.

        Args:
            limit (int, optional): The max number of records to return. Defaults to 1.

        Returns:
            list of dict: The queried data as record(s).
        """      
        
        d_res = deque()
        for i in range(limit):
            try:
                res = self.df.iloc[self.next_row].to_dict()
                formatted_res = self.format_result(res)
                d_res.append(formatted_res)
                self.next_row+= 1
            except IndexError:
                self.is_finished = True
                break
            except:
                raise
        
        return list(d_res)
                