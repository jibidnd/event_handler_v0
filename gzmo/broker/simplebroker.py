
# import collections
# import copy

# import uuid
# from decimal import Decimal
# import datetime
# import pytz
# import time
# import threading

# import zmq

# from .. import constants as c
# from .. import event_handler
# from ..event_handler import event, lines
# from .. import utils

import copy

import zmq

from . import BaseBroker
from .. import utils
from ..utils import constants as c

'''
    Needs:
        - data
        - fill strategy: quantity + price
        - 
'''

class SimpleBroker(BaseBroker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
    
    # ----------------------------------------------------------------------------------------
    # Event handling
    # ----------------------------------------------------------------------------------------
    def _handle_data(self, data):
        # The broker does not keep track of data history

        # first update clock
        # data is expected to be right aligned (EVENT_TS marks the end of the event)
        self.clock = data[c.EVENT_TS]

        fills = self.try_fill_with_data(data)
        
        if (fills is not None) and (self.order_socket is not None):
            
            # emit the order if there are any fills
            for order in fills:
                order_packed = utils.packb(order)
                self.order_socket.send(order_packed, flags = zmq.NOBLOCK)

        return self.handle_data(data)

    def handle_data(self, data):
        pass

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

    def take_order(self, order):
        _order = copy.deepcopy(order)
        if _order[c.EVENT_SUBTYPE] == c.REQUESTED:
            if _order[c.ORDER_TYPE] in [c.MARKET, c.LIMIT]:
                # Add to open orders
                order_id = _order[c.ORDER_ID]
                # change order status to subitted
                _order[c.EVENT_SUBTYPE] = c.SUBMITTED
                # add quantity open field if none set
                if _order.get(c.QUANTITY_OPEN) is None:
                    _order[c.QUANTITY_OPEN] = _order[c.QUANTITY]
                if _order.get(c.QUANTITY_FILLED) is None:
                    _order[c.QUANTITY_FILLED] = 0
                # Add to the collection
                self.open_orders[order_id] = _order

                # acknowledge acceptance of order (with submitted status)
                return copy.deepcopy(_order)

            elif _order[c.ORDER_TYPE] == c.CANCELLATION:
                # remove from open orders and acknowledge cancellation
                try:
                    cancelled_order = self.open_orders.pop(_order[c.ORDER_ID])
                    cancelled_order = _order.update({c.EVENT_TS: self.clock, c.EVENT_SUBTYPE: c.CANCELLED})
                except KeyError:
                    cancelled_order = _order.update({c.EVENT_TS: self.clock, c.EVENT_SUBTYPE: c.INVALID})
                finally:
                    return copy.deepcopy(cancelled_order)


    def try_fill_with_data(self, data):
        """ Tries to fill all currently open orders against data.
            returns a list of fills if any, otherwise returns None.

        Args:
            data (tuple(bytes, dict)): (topic, data event).

        Returns:
            list or None: list of fills if any, otherwise None.
        """
        # keeping track of orders closed/filled with this data event
        # recall that we cannot change the dictionary size mid-iteration
        closed = []
        fills = []

        # nothing to do if it is not price data
        if (symbol_data := data.get(c.SYMBOL)) is None:
            return

        else:
            # try to fill each order
            for order_id, open_order in self.open_orders.items():
                if (symbol_order := open_order[c.SYMBOL]) == symbol_data:
                    # print('broker filling with ' + str(data))
                    # If there is a fill (partial or complete)
                    if self.fill_method(open_order, data):
                        # can forget this order if the order is fully filled
                        if open_order[c.QUANTITY_OPEN] == 0:
                            closed.append(order_id)
                            open_order[c.EVENT_SUBTYPE] = c.FILLED
                        else:
                            open_order[c.EVENT_SUBTYPE] = c.PARTIALLY_FILLED
                        
                        fills.append(open_order.copy())
            
            # closed orders from self.open_orders to self.closed_orders
            for order_id in closed:
                self.closed_orders.append(self.open_orders.pop(order_id))

            return fills

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
    # time.sleep(0.1)
    assert order.get(c.QUANTITY_OPEN) != 0, 'Nothing to fill.'
    # calculate fill price
    p = None
    if order[c.ORDER_TYPE] == c.MARKET:
        if data[c.EVENT_SUBTYPE] == c.BAR:
            p = data[fill_strategy]
        elif data[c.EVENT_SUBTYPE] == c.TICK:
            p = data[fill_strategy]
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
                if ((order[c.QUANTITY] > 0) and (order[c.PRICE] <= data[fill_strategy])) or \
                    ((order[c.QUANTITY] < 0) and (order[c.PRICE >= data[fill_strategy]])):
                    p = data[fill_strategy]
        elif data[c.EVENT_SUBTYPE] == c.QUOTE:
                if ((order[c.QUANTITY] > 0) and (order[c.PRICE] >= data[c.ASK])) or \
                    ((order[c.QUANTITY] < 0) and (order[c.PRICE <= data[c.BID]])):
                    p = data[fill_strategy]
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
        order[c.AVERAGE_PRICE] = abs(order[c.CREDIT] - order[c.DEBIT]) / order[c.QUANTITY_FILLED]
        order[c.EVENT_TS] = data[c.EVENT_TS] #+ data[c.MULTIPLIER] * data[c.RESOLUTION] / 2
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
