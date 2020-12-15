import abc
import zmq
import msgpack

from .. import constants as c
from ...utils.util_functions import get_free_tcp_address

class BaseDataFeed:

    def __init__(self, topic, zmq_context = None):

        self.topic = topic
        self.zmq_context = zmq_context or zmq.Context.instance()
        self.from_beginning = True
        self.is_finished = False
        self.shutdown_flag = False

    def publish_to(self, address):
        self.address = address
        
        # Connect to a port
        self.sock_out = self.zmq_context.socket(zmq.PUB)
        # Note here we connect to this addres (instead of bind) because we have
        #    a multiple publisher (datafeeds) - one subscriber (session) pattern
        self.sock_out.connect(address)

    @abc.abstractmethod
    def publish(self):
        pass
    
    @abc.abstractmethod
    def fetch(self, limit = None):
        pass

    @staticmethod
    def default_conversion(obj):
        try:
            return float(obj)
        except:
            return str(obj)
