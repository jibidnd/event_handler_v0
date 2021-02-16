import abc
import uuid
import threading
from decimal import Decimal

import zmq

from .. import event_handler
from .. import utils
from ..utils import constants as c


class BaseBroker(event_handler.EventHandler):
    def __init__(self, name, fill_method, zmq_context = None, data_address = None, order_address = None, logging_addresses = None):
        
        # initialization params
        self.name = name
        self.zmq_context = zmq_context
        self.broker_id = str(uuid.uuid1())  # Note that this shows the network address.
        self.open_orders = {}               # better to refer to orders by id so we can refer to the same order even if attributes change
        self.closed_orders = []
        self.fill_method = fill_method

        self.data_address = data_address
        self.order_address = order_address
        self.logging_addresses = logging_addresses

        # Connection things
        self.data_socket = None         # datasource to know what prices we can fill at
        self.order_socket = None        # Broker of brokers
        self.logging_socket = None

        if self.data_address is not None:
            self.connect_data_socket(data_address)
        if self.order_address is not None:
            self.connect_order_socket(order_address)
        if self.logging_addresses is not None:
            for logging_address in logging_addresses:
                self.connect_logging_socket(logging_address)
        
        # These can be overriden for threads, but are otherwise just placeholders
        self.main_shutdown_flag = threading.Event()
        self.shutdown_flag = threading.Event()
        
        # clock is only used in "socket modes"
        self.clock = Decimal(0.00)
    
    # ----------------------------------------------------------------------------------------
    # Connections
    # ----------------------------------------------------------------------------------------
    def connect_data_socket(self, data_address):

        # if new address, overwrite the current record
        self.data_address = data_address
        
        # establish a context if none provided
        if self.zmq_context is None:
            self.zmq_context = zmq.Context.Instance()
        
        # Create a data socket if none exists yet
        if not self.data_socket:
            socket = self.zmq_context.socket(zmq.SUB)
            self.data_socket = socket
        
        # subscribe to everything because we don't know what we'll need to match orders against
        self.data_socket.setsockopt(zmq.SUBSCRIBE, b'')
        self.data_socket.connect(data_address)
        
        return
    
    def connect_order_socket(self, order_address):

        # if new address, overwrite the current record
        self.order_address = order_address

        # establish a context if none provided
        if self.zmq_context is None:
            self.zmq_context = zmq.Context.Instance()

        # Create a order socket if none exists yet
        if not self.order_socket:
            socket = self.zmq_context.socket(zmq.DEALER)
            # note that this broker is identified by its name
            socket.setsockopt(zmq.IDENTITY, self.name.encode())
            self.order_socket = socket
        self.order_socket.connect(order_address)

        return
    
    def connect_logging_socket(self, logging_address):
        # if new address, add it to the list
        if logging_address not in self.logging_addresses:
            self.logging_addresses.append(logging_address)
        # Create a logging socket if none exists yet
        if not self.logging_socket:
            socket = self.zmq_context.socket(zmq.DEALER)
            socket.setsockopt(zmq.IDENTITY, self.broker_id.encode())
            self.logging_socket = socket
        self.logging_socket.conenct(logging_address)

    # ----------------------------------------------------------------------------------------
    # Event handling
    # ----------------------------------------------------------------------------------------
    @abc.abstractmethod
    def _handle_data(self, data):
        pass

    def handle_data(self, data):
        pass
    
    @abc.abstractmethod
    def _handle_order(self, order):
        # print('received order for {} at {}'.format(utils.unix2datetime(order['EVENT_TS']), utils.unix2datetime(self.clock)))

        order_response = self.take_order(order)
        if (order_response is not None) and (self.order_socket is not None):
            # emit the response if there is any
            order_response_packed = utils.packb(order_response)
            self.order_socket.send(order_response_packed, flags = zmq.NOBLOCK)
        return self.handle_order(order)

    def handle_order(self, order):
        pass

    @abc.abstractmethod
    def take_order(self, order):
        pass

    @abc.abstractmethod
    def try_fill_with_data(self, data):
        pass

    def run(self):

        # the event "queue"
        next_events = {}

        while (not self.main_shutdown_flag.is_set()) and (not self.shutdown_flag.is_set()):
            
            # get from data socket if slot is empty
            if next_events.get(c.DATA_SOCKET) is None:
                try:
                    # This is a SUB
                    topic, event_packed = self.data_socket.recv_multipart(zmq.NOBLOCK)
                    next_events[c.DATA_SOCKET] = utils.unpackb(event_packed)
                except zmq.ZMQError as exc:
                    # nothing to get
                    if exc.errno == zmq.EAGAIN:
                        pass
                    else:
                        raise
            
            # get from order socket if slot is empty
            if next_events.get(c.ORDER_SOCKET) is None:
                try:
                    # This is a dealer
                    order_packed = self.order_socket.recv(zmq.NOBLOCK)
                    order_unpacked = utils.unpackb(order_packed)
                    next_events[c.ORDER_SOCKET] = order_unpacked
                except zmq.ZMQError as exc:
                    # nothing to get
                    if exc.errno == zmq.EAGAIN:
                        pass
                    else:
                        raise

            # Sort the events
            if len(next_events) > 0:
                # Handle the socket with the next soonest event (by EVENT_TS)
                next_socket = sorted(next_events.items(), key = lambda x: x[1][c.EVENT_TS])[0][0]
                next_event = next_events.pop(next_socket)        # remove the event from next_events
                # tick the clock if it has a larger timestamp than the current clock (not a late-arriving event)
                if (tempts := next_event[c.EVENT_TS]) > self.clock:
                    self.clock = tempts

                self._handle_event(next_event)

    def shutdown(self):
        self.shutdown_flag.set()
        for socket in [self.data_socket, self.order_socket, self.logging_socket]:
            socket.close(linger = 10)