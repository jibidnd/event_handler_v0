import threading

import zmq
import msgpack

from ..data import *
from .. import constants as c

class DatafeedSynchronizer(BaseDataFeed):
    
    def __init__(self, sync_key = 'EVENT_TS', datafeeds = None, zmq_context = None):
        """
            Takes a collection of datafeeds and package them into one synchronized datafeed,
                while preserving the topic.
            Each datafeed in datafeeds much itself emit sorted events.

        Args:
            sync_key (str): key to sort events by
            datafeeds (iterable, optional): iterable of datafeeds to synchronize. Defaults to None.
        """
        super().__init__(None, zmq_context)
        self.datafeeds = datafeeds or []
        # A mapping to keep track of datafeeds
        self.dict_datafeeds = {i: datafeed for i, datafeed in enumerate(datafeeds)} # number: datafeed
        self.sync_key = sync_key
        self.next_events = {}   # datafeed number: (topic, event_msg)

    def add_datafeed(self, datafeed):
        self.datafeeds.append(datafeed)
        self.dict_datafeeds.update({len(self.dict_datafeeds): datafeed})

    def execute_query(self):
        for datafeed in self.datafeeds:
            datafeed.execute_query()
            self.from_beginning = False

    def fetch(self, limit = 1):

        # If need to get results from scratch
        if self.from_beginning:
            self.execute_query()
            self.from_beginning = False
        
        counter = 0
        results = []

        while counter < limit:
            # Get data from all the datafeeds
            for i, datafeed in self.dict_datafeeds.items():

                # Attempt to fill the event queue for any slots that are empty
                if (not datafeed.is_finished) and (self.next_events.get(i) is None):
                    res = datafeed.fetch(1)
                    # if nonempty results
                    if len(res) > 0:
                        res = res[0]
                        self.next_events[i] = res

            # Sort the events
            if len(self.next_events) > 0:
                # sort key: sync key of the event msg ([1] of (topic, event_msg) tuple) of the dict value ([1] of dict.items())
                next_socket = sorted(self.next_events.items(), key = lambda x: x[1][1][self.sync_key])[0]
                # return first event
                results.append(self.next_events.pop(next_socket))
                counter += 1
            else:
                # otherwise return None
                break

        if len(results) > 1:
            return results
        elif len(results) == 1:
            return results[0]
        else:
            return 

    def publish(self):

        # wait for the starting signal
        self.start_sync.wait()

        while (not self.main_shutdown_flag.is_set()) and \
                (not self.shutdown_flag.is_set()) and \
                (not self.is_finished):

            # get the next event: (topic, event_msg)
            if (event := self.fetch()) is not None:

                try:
                    event_packed = msgpack.packb(event[1], use_bin_type = True, default = self.default_conversion)
                    self.sock_out.send_multipart([event[0].encode(), event_packed], flag = zmq.NOBLOCK)
                except zmq.ZMQError as exc:
                    # Drop messages if queue is full
                    if exc.errno == zmq.EAGAIN:
                        pass
                    else:
                        self.shutdown()
                        raise
            else:
                # no more events; shut down.
                self.is_finished = True
                self.shutdown()
                break
        
        self.shutdown()

    def shutdown(self):
        self.shutdown_flag.set()
        self.sock_out.close(linger = 10)