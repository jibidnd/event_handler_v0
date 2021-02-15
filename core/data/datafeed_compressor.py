import threading

import zmq
import decimal

from ..data import *
from .. import constants as c
from .. import utils
from core import data

class DatafeedCompressor(BaseDataFeed):
    
    def __init__(self, datafeed, compressor, zmq_context = None):
        """
            Takes a datafeed and combine multiple data points into one.

        Args:
            datafeed (iterable): Datafeed to compress.
            compressor (object): An object that takes datapoints and spits out a compressed datapoint when ready.
        """
        super().__init__(None, zmq_context)
        self.datafeed = datafeed
        assert hasattr(compressor, 'process') and callable(getattr(compressor, 'process')), \
            'compressor must have process method.'
        assert hasattr(compressor, 'reset') and callable(getattr(compressor, 'reset')), \
            'compressor must have reset method.'
        self.compressor = compressor
        

    def execute_query(self):
        self.datafeed.execute_query()
        self.from_beginning = False

    def fetch(self, limit = 1):

        # If need to get results from scratch
        if self.from_beginning:
            self.execute_query()
        
        counter = 0
        results = []

        while counter < limit:
            
            if not self.datafeed.is_finished:
                if (datapoint := self.datafeed.fetch(1)) is not None:
                    if (res := self.compressor.process(datapoint)) is not None:
                        results.append(res)
                        counter += 1
                        self.compressor.reset()
        
        if len(results) == 0:
            self.is_finished = True
            return

        if limit > 1:
            return results
        elif limit == 1:
            return results[0]


    def publish(self):
        # wait for the starting signal
        self.start_sync.wait()

        while (not self.main_shutdown_flag.is_set()) and \
                (not self.shutdown_flag.is_set()) and \
                (not self.is_finished):

            # get the next event: (topic, event_msg)
            if (event := self.fetch()) is not None:

                try:
                    event_packed = utils.packb(event)
                    self.sock_out.send_multipart([event[c.TOPIC].encode(), event_packed], flag = zmq.NOBLOCK)
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


class count_compressor:
    def __init__(self, target_count, combiner):
        self.target_count = target_count
        self.combiner = combiner
        self.reset()
    
    def reset(self):
        self.temp_res = {}
        self.count = 0
    
    def process(self, event):
        self.temp_res = self.combiner(self.temp_res, event)
        self.count += 1
        if self.count >= self.target_count:
            return self.temp_res
        return


def combine_bars(orig_bar, new_bar):
    res = {**new_bar}
    for key in [c.OPEN, c.HIGH, c.LOW, c.CLOSE, c.VOLUME]:
        res[key] = (res.get(key) or decimal.Decimal(0.0)) + (orig_bar.get(key) or decimal.Decimal(0.0))
    return res