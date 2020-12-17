import collections
import uuid
from decimal import Decimal
import datetime
import pytz
import time

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
    def __init__(self, zmq_context = None, data_address = None, order_address = None, logging_addresses = None, strategy_address = None,
                    data_subscriptions = None, params = None, rms = None, local_tz = 'America/New_York'):
        
        # initialization params
        self.name = name
        self.zmq_context = zmq_context or zmq.Context.instance()
        self.broker_id = str(uuid.uuid1()) # Note that this shows the network address.
        self.datas = {}
        self.pending_orders = {}

        self.logging_address = logging_address
        self.data_address = data_address

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
        
        # Keeping track of time: internally, time will be tracked as local time
        # For communication, (float) timestamps on the second resolution @ UTC will be used.
        self.clock = utils.unix2datetime(0.0)
    
    
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
            socket.setsockopt(zmq.IDENTITY, self.broker_id.encode())
            self.order_socket = socket
        self.order_socket.connect(order_address)

        return

    def _handle_data(self, data):
        '''Update lines in self.datas with data event'''
        try:
            symbol = data.get(c.SYMBOL)
        except KeyError:
            # TODO:
            raise
            # log error: data event not well formed
            return
        # If data is of type tick, bar, or quote, keep track of it
        if (data.get(c.EVENT_TYPE) == c.DATA) & (data.get(c.EVENT_SUBTYPE) in [c.TICK, c.BAR, c.QUOTE]):
            # Add a line if none exists yet
            if self.datas.get(symbol) is None:
                self.datas[symbol] = lines()
            # update the line
            self.datas[symbol].update_with_data(data)

        return self.handle_data(data)

    def _handle_order(self, order):
        self.handle_order(order)
        return


def immediate_fill(order, data, fill_strategy = c.CLOSE, commission = 0.0):
    """ Immediately fill the strategy

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
            order[c.PRICE] = p
            order[c.CREDIT] += (-q) * p if q < 0 else 0
            order[c.DEBIT] += q * p if q > 0 else 0
            order[c.NET] = order[c.CREDIT] - order[c.DEBIT]
            order[c.AVERAGE_PRICE] = order[c.NET] / order[c.QUANTITY_FILLED]

            return