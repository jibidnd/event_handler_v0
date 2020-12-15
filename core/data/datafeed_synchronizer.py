import threading

import zmq
import msgpack

from ..data import *
from .. import constants as c

class DatafeedSynchronizer(BaseDataFeed):
    
    def __init__(self, topic, sync_key, datafeeds = None, zmq_context = None):
        """
            Takes a collection of datafeeds and package them into one synchronized datafeed,
                while preserving the topic.
            Each datafeed in datafeeds much itself emit sorted events.

        Args:
            sync_key (str): key to sort events by
            datafeeds (iterable, optional): iterable of datafeeds to synchronize. Defaults to None.
        """
        super().__init__(topic, zmq_context)
        self.sync_key = sync_key
        self.datafeeds = datafeeds or []

    def add_datafeed(datafeed):
        self.datafeeds.append(datafeed)

    def publish(self):
        
        # # Find ports that are available
        # # TODO: take care of race conditions where someone else grabs that port in between us finding it and binding to it
        # self.datafeed_address_in, host_in, port_in = get_free_tcp_address()
        
        # # publisher facing socket
        # backend = self.zmq_context.socket(zmq.SUB)
        # backend.bind(self.datafeed_address_in)
        # # no filtering here
        # backend.setsockopt(zmq.SUBSCRIBE, b'')

        # # client facing socket
        # frontend = self.sock_out

        # for datafeed in self.datafeeds:
        #     # Let datafeeds know where to publish to
        #     datafeed.publish_to(self.datafeed_address_in)


        while True:
            # # collection of threads
            # datafeed_threads = []
            # collection of events
            events = []

            for datafeed in self.datafeeds:
                if not datafeed.is_finished:
                    res = datafeed.get(100)
                    datafeed.from_beginning = False
                    
                    if len(res) > 0:
                        events.extend(res)
            if len(events) > 0:
                events = sorted(events, key = lambda x: x[1][self.sync_key])

                # emit the events
                for event in events:
                    try:
                        event_packed = msgpack.packb(event[1], use_bin_type = True, default = self.default_conversion)
                        self.sock_out.send_multipart([event[0].encode(), event_packed], flag = zmq.NOBLOCK)
                    
                    except zmq.ZMQError as exc:
                        if exc.errno == zmq.EAGAIN:
                            # Drop messages if queue is full
                            pass
                        else:
                            self.sock_out.close()
                            raise
            else:
                self.is_finished = True
                self.shutdown_flag = True
                self.sock_out.close()
                break
                
            #     if res
            #     events.extend(datafeed.get(100))

            # # request up to 100 of rows to be sent from each datafeed
            # for datafeed in self.datafeeds:
            #     if datafeed.shutdown_flag:
            #         pass
            #     else:
            #         datafeed_thread = threading.Thread(target = datafeed.run, args = (100,))
            #         datafeed_thread.daemon = True
            #         datafeed_threads.append(datafeed_thread)
            
            # # if all the datafeeds are finished, we're done
            # if len(datafeed_threads) == 0:
            #     break
            # else:
            #     for datafeed_thread in datafeed_threads:
            #         datafeed_thread.start()
            
            # # receive the events
            # while True:
            #     try:
            #         topic, event = backend.recv_multipart(zmq.NOBLOCK)
            #         events.append((topic, event))
            #     except zmq.ZMQError as exc:
            #         if exc.errno == zmq.EAGAIN:
            #             # We have received all the events in this batch
            #             pass
            #         else:
            #             raise
            
            # # sort the events
            # events = sorted(events, key = lambda x: x[1][self.sync_key])

            # # emit the events
            # for event in events:
            #     try:
            #         event_packed = msgpack.packb(event[1], use_bin_type = True, default = self.default_conversion)
            #         print(event[0], event[1])
            #         fronend.send_multipart([event[0].encode(), event_packed], flag = zmq.NOBLOCK)
                
            #     except zmq.ZMQError as exc:
            #         if exc.errno == zmq.EAGAIN:
            #             # Drop messages if queue is full
            #             pass
            #         else:
            #             # sock.send_multipart([b'', msgpack.packb(stopping_event, use_bin_type = True, default = self.default_conversion)])
            #             frontend.close()
            #             backend.close()
            #             raise
        



