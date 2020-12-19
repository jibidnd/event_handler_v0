import collections
import uuid
from decimal import Decimal
import datetime
import pytz
import time
import threading

import zmq
import msgpack

from .. import constants as c
from .. import event_handler
from ..event_handler import event, lines
from .position import Position, CashPosition
from ... import utils

'''
    Needs:
        - data
        - fill strategy: quantity + price
        - 
'''

class Broker(event_handler.EventHandler):
    def __init__(self, name, zmq_context = None, data_address = None, order_address = None, logging_addresses = None):
        
        # initialization params
        self.name = name
        self.zmq_context = zmq_context or zmq.Context.instance()
        self.broker_id = str(uuid.uuid1()) # Note that this shows the network address.
        # self.datas = {}
        self.open_orders = {}

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
        
        self.main_shutdown_flag = threading.Event()
        self.shutdown_flag = threading.Event()
        
        self.clock = 0.00
    
    # ----------------------------------------------------------------------------------------
    def connect_data_socket(self, data_address):
        # if new address, overwrite the current record
        self.data_address = data_address
        # Create a data socket if none exists yet
        if not self.data_socket:
            socket = self.zmq_context.socket(zmq.SUB)
            self.data_socket = socket
        # subscribe to everything because we need to track the data ahead of time
        self.data_socket.setsockopt(zmq.SUBSCRIBE, b'')
        self.data_socket.connect(data_address)
        
        return
    
    def connect_order_socket(self, order_address):
        # if new address, overwrite the current record
        self.order_address = order_address
        # Create a order socket if none exists yet
        if not self.order_socket:
            socket = self.zmq_context.socket(zmq.DEALER)
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
            socket.setsockopt(zmq.IDENTITY, self.strategy_id.encode())
            self.logging_socket = socket
        self.logging_socket.conenct(logging_address)

    # ----------------------------------------------------------------------------------------
    def _handle_data(self, data):
        # '''Update lines in self.datas with data event'''
        # try:
        #     symbol = data.get(c.SYMBOL)
        # except KeyError:
        #     # TODO:
        #     raise
        #     # log error: data event not well formed
        #     return
        # # If data is of type tick, bar, or quote, keep track of it
        # if (data.get(c.EVENT_TYPE) == c.DATA) & (data.get(c.EVENT_SUBTYPE) in [c.TICK, c.BAR, c.QUOTE]):
        #     # Add a line if none exists yet
        #     if self.datas.get(symbol) is None:
        #         self.datas[symbol] = lines()
        #     # update the line
        #     self.datas[symbol].update_with_data(data)

        # first update clock
        self.clock = data[c.EVENT_TS]
        
        # keeping track of closed orders
        closed = []
        # try to fill each order
        for order_id, open_order in self.open_orders.items():
            # if there any fills (full or partial)
            if immediate_fill(open_order, data):
                # emit the resulting order
                # Note that this is the current status of the order, not the incremental fills
                order_packed = msgpack.packb(open_order, use_bin_type = True, default = utils.default_conversion)
                self.order_socket.send(order_packed, flags = zmq.NOBLOCK)
                # can forget this order if the order is fully filled
                if open_order[c.QUANTITY_OPEN] == 0:
                    closed.append(order_id)
                    open_order[c.EVENT_SUBTYPE] = c.FILLED
        
        for order_id in closed:
            self.open_orders.pop(order_id)
            
        return self.handle_data(data)

    def handle_data(self, data):
        pass

    def _handle_order(self, order):
        if order[c.EVENT_SUBTYPE] == c.REQUESTED:
            if order[c.ORDER_TYPE] in [c.MARKET, c.LIMIT]:
                # Add to open orders
                order_id = order[c.ORDER_ID]
                # add quantity open field if none set
                if order.get(c.QUANTITY_OPEN) is None:
                    order[c.QUANTITY_OPEN] = order[c.QUANTITY]
                if order.get(c.QUANTITY_FILLED) is None:
                    order[c.QUANTITY_FILLED] = 0
                # Add to the collection
                self.open_orders[order_id] = order
            elif order[c.ORDER_TYPE] == c.CANCELLATION:
                # remove from open orders and acknowledge cancellation
                try:
                    cancelled_order = self.open_orders.pop(order[c.ORDER_ID])
                    cancelled_order.update({c.EVENT_TS: self.clock, c.EVENT_SUBTYPE: c.CANCELLED})
                except KeyError:
                    cancelled_order = order.update({c.EVENT_TS: self.clock, c.EVENT_SUBTYPE: c.INVALID})
                cancelled_order_packed = msgpack.packb(cancelled_order, use_bin_type = True, default = utils.default_conversion)
                self.order_socket.send(cancelled_order_packed, flags = zmq.NOBLOCK)

        self.handle_order(order)
        return

    def handle_order(self, order):
        pass

    def run(self):

        # the event "queue"
        next_events = {}

        while (not self.main_shutdown_flag.is_set()) and (not self.shutdown_flag.is_set()):
            
            # get from data socket if slot is empty
            if next_events.get(c.DATA_SOCKET) is None:
                try:
                    # This is a SUB
                    topic, event = self.data_socket.recv_multipart(zmq.NOBLOCK)
                    next_events[c.DATA_SOCKET] = msgpack.unpackb(event)
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
                    order = self.order_socket.recv(zmq.NOBLOCK)
                    order = msgpack.unpackb(order)
                    next_events[c.ORDER_SOCKET] = order
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





def immediate_fill(order, data, fill_strategy = c.CLOSE, commission = 0.0):
    """ Immediately fill the strategy.
        Note that in the event of a bar, the time the order is filled at the close of the bar.

    Args:
        order (dict): The order. See event_handler.event.order_event for details.
        data (dict): Data event. see event_handler.event.data_event for details.
        fill_strategy (str or callable, optional): str or callable. For market orders.
            If a str is provided, attempts to get `fill_strategy` from the data event
                to determine fill price in the case of BARs and QUOTEs.
            If a callable is provided, must take arguments `order` and `data` and
                return the fill price.
            Defaults to c.CLOSE.
    """    

    assert order.get(c.QUANTITY_OPEN) > 0, 'Nothing to fill.'
    # calculate fill price
    p = None
    if order[c.ORDER_TYPE] == c.MARKET:
        if data[c.EVENT_SUBTYPE] == c.BAR:
            p = data[fill_strategy]
        elif data[c.EVENT_SUBTYPE] == c.TICK:
            p = data[c.PRICE]
        elif data[c.EVENT_SUBTYPE] == c.QUOTE:
            p = data[fill_strategy]
    elif order[c.ORDER_TYPE] == c.LIMIT:
        if data[c.EVENT_SUBTYPE] == c.BAR:
            if data[c.EVENT_SUBTYPE] == c.BAR:
                # If long, fill if limit price <= high
                # if short, fill if limit price >= low
                if ((order[c.QUANTITY] > 0) and (order[c.PRICE] <= data[c.HIGH])) or \
                    ((order[c.QUANTITY] < 0) and (order[c.PRICE >= data[c.LOW]])):
                    p = order[c.PRICE]
        elif data[c.EVENT_SUBTYPE] == c.TICK:
                if ((order[c.QUANTITY] > 0) and (order[c.PRICE] <= data[c.PRICE])) or \
                    ((order[c.QUANTITY] < 0) and (order[c.PRICE >= data[c.PRICE]])):
                    p = data[c.PRICE]
        elif data[c.EVENT_SUBTYPE] == c.QUOTE:
                if ((order[c.QUANTITY] > 0) and (order[c.PRICE] >= data[c.ASK])) or \
                    ((order[c.QUANTITY] < 0) and (order[c.PRICE <= data[c.BID]])):
                    p = data[c.PRICE]
        else:
            raise NotImplementedError
    
    if p is not None:
        # note that dictionaries are mutable
        # fully fill the order
        q = order[c.QUANTITY_OPEN]
        order[c.QUANTITY_OPEN] -= q
        order[c.QUANTITY_FILLED] += q
        # update the fill price
        order[c.CREDIT] += (-q) * p if q < 0 else 0
        order[c.DEBIT] += q * p if q > 0 else 0
        order[c.NET] = order[c.CREDIT] - order[c.DEBIT]
        order[c.AVERAGE_PRICE] = abs(order[c.NET]) / order[c.QUANTITY_FILLED]
        if data[c.EVENT_TYPE] == c.BAR:
            order[c.EVENT_TS] = data[c.EVENT_TS] + data[c.MULTIPLIER] + utils.duration_to_sec(data[c.RESOLUTION])
        else:
            order[c.EVENT_TS] = data[c.EVENT_TS]
        return True
    else:
        return False


# fill function return true/false?
# for every data event received:
#     for every open order:
#         try to fill
#         if any is filled, send (possibly partially) filled order

# write functions for the following?

# fill params
# - prob fill
# - latency | fill
# - q | fill
# - p | fill
