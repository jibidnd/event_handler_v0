import threading

import zmq
import msgpack

from ..data import *
from .. import constants as c

class DatafeedSynchronizer(BaseDataFeed):
    
    def __init__(self, sync_key, datafeeds = None, zmq_context = None):
        """
            Takes a collection of datafeeds and package them into one synchronized datafeed,
                while preserving the topic.
            Each datafeed in datafeeds much itself emit sorted events.

        Args:
            sync_key (str): key to sort events by
            datafeeds (iterable, optional): iterable of datafeeds to synchronize. Defaults to None.
        """
        super().__init__(None, zmq_context)
        self.sync_key = sync_key
        self.datafeeds = datafeeds or []

    def add_datafeed(datafeed):
        self.datafeeds.append(datafeed)


    def publish(self):

        # Start from beginning
        for datafeed in self.datafeeds:
            datafeed.from_beginning = True

        while (not self.shutdown_flag.is_set()) & (not self.is_finished):

            # A mapping to keep track of datafeeds
            dict_datafeeds = {i: datafeed for i, datafeed in enumerate(self.datafeeds)}
            dict_events = {}

            for i, datafeed in dict_datafeeds.items():
                # Attempt to fill the event queue for any slots that are empty
                if (not datafeed.is_finished) and (dict_events.get(i) is None):
                    res = datafeed.fetch(1)[0]
                    # if nonempty results
                    if len(res) > 0:
                        dict_events[i] = res
            # anything to publish?
            if len(dict_events) > 0:
                events = sorted(dict_events.values(), key = lambda x: x[1][self.sync_key])

                # emit the events
                for event in events:
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
                self.is_finished = True
                self.shutdown()
                break

    def shutdown(self):
        self.shutdown_flag.set()
        self.sock_out,.close(linger = 10)