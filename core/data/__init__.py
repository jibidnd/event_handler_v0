import abc
import zmq
import msgpack
import threading

from .. import constants as c
from ...utils.util_functions import get_free_tcp_address
from ... import utils
from utils import util_functions as utils

class BaseDataFeed:

    def __init__(self, topic, zmq_context = None):
        """Base datafeed class.

            ALL BARS MUST BE RIGHT-EDGE-ALIGNED (timestamp for bar signifies end of bar period).

            A datafeed provides data in one of two ways: `publish` and `fetch`.

            `publish` publishes data to the datafeed's address, which can be specified after initialization.
            `fetch` returns a record when called.

            An optional `start_sync` can be used to sync starting times between multiple datafeeds.
                To use it, simply assign `datafeed.start_sync` to a common threading.Event() instance,
                start all the datafeeds, and `set` the threading.Event() instance.

            `from_beginning` allows for pauses between calls without losing progress. If set to True when `fetch` or `publish`
                is called, the datafeed will re-execute the query, if approapriate.
            
            `main_shutdown_flag` is intended to be set by the main thread, if used in a threading context.
            `shutdown_flag` can be set to indicate intention to stop the datafeed. The choice of threading.Event instead of
                boolean is somewhat arbitrary.

        Args:
            topic (str): the topic attached to the datafeed.
            zmq_context (zmq.Context.instance(), optional): ZMQ context instance for publishing. Defaults to None.
        """
        self.topic = topic
        self.zmq_context = zmq_context or zmq.Context.instance()
        self.from_beginning = True
        self.start_sync = threading.Event(); self.start_sync.set()  # default to not block for a sync
        self.is_finished = False
        # main_shutdown_flag is intended to be set by the main thread, if used in a threading context
        self.main_shutdown_flag = threading.Event()
        self.shutdown_flag = threading.Event()

    def publish_to(self, address):
        """tell the datafeed where to publish to (if publishing is desired).

        Args:
            address (str): socket to publish data to
        """
        self.address = address
        
        # Connect to a port
        self.sock_out = self.zmq_context.socket(zmq.PUB)
        # Note here we connect to this addres (instead of bind) because we have
        #    a multiple publisher (datafeeds) - one subscriber (session) pattern
        self.sock_out.connect(address)

    def execute_query(self):
        pass

    def fetch(self, limit = 1):
        pass
        
    def publish(self):
        pass
