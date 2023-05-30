from collections import deque

import zmq

from . import BaseDataFeed
from .. import utils
from ..utils import constants as c

class DatafeedSynchronizer(BaseDataFeed):
    """Synchronizes a collection of datafeeds.

        Takes a collection of datafeeds and package them into one synchronized datafeed,
            while preserving the topic.
        Each datafeed in datafeeds must itself emit sorted events.

        DatafeedSynchronizer calls the `fetch` method of each member datafeed, looks for the
        earliest event among the datafeeds, and returns that event. Sorting is done by the passed
        `sync_key`. 

    Args:
        sync_key (str): The key to sort events by
        datafeeds (dict): Iterable of datafeeds to synchronize. Defaults to None.
    """
    def __init__(self, datafeeds, sync_key = 'EVENT_TS', zmq_context = None):

        super().__init__(None, zmq_context)
        self.datafeeds = datafeeds
        # A mapping to keep track of datafeeds
        # self.dict_datafeeds = {i: datafeed for i, datafeed in enumerate(datafeeds)} # number: datafeed
        self.sync_key = sync_key
        # the "current" view of the immediately upcoming event of each datafeed
        self.next_events = {}   # datafeed number: (topic, event_msg)

    def execute_query(self):
        """Executes the queries for each underlying datafeed.
        """        
        for datafeed in self.datafeeds.values():
            datafeed.execute_query()
        self.from_beginning = False

    def fetch(self, limit = 1):
        """Return the requested number of records.

            The way this is done is roughly the following:
                - View the immediately upcoming event from each datafeed
                - Sort the events
                - Return the earliest event
                - Remove the returned event from the queue and `fetch` the next record
                    from the datafeed that emitted that event.
            
            The view of immediately upcoming events is kept in self.next_events.
            
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
        # If need to get results from scratch
        if self.from_beginning:
            self.execute_query()
            self.from_beginning = False
        
        d_res = deque()

        for i in range(limit):
            # Attempt to fill the event queue for any slots that are empty
            for name, datafeed in self.datafeeds.items():
                if (not datafeed.is_finished) and (self.next_events.get(name) is None):
                    if (res := datafeed.fetch(1)):
                        self.next_events[name] = res[0]

            # If there are more events, sort the events and add the first one to results
            if len(self.next_events) > 0:
                # sort key: sync key of the event msg of the dict value ([1] of dict.items())
                next_socket = sorted(self.next_events.items(), key = lambda x: x[1][self.sync_key])[0][0]
                # return first event
                d_res.append(self.next_events.pop(next_socket))
            else:
                # otherwise return whatever we have
                break
        
        return d_res