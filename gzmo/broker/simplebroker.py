"""A simple broker simulator that is capable of market and limit orders.

The simple broker must be provided datafeeds for any symbols against which orders need to be filled.
The simple broker is supplied with a fill_method that specifies how fills will be done, and whether
any slippage or commission is present.


TODO:
    fill function return true/false?
    for every data event received:
        for every open order:
            try to fill
            if any is filled, send (possibly partially) filled order

    write functions for the following?

    fill params
    - prob fill
    - latency | fill
    - q | fill
    - p | fill

"""
import copy

import zmq

from . import BaseBroker
from .. import utils
from ..utils import constants as c

class SimpleBroker(BaseBroker):
    """A simple broker to simulate taking and filling market and limit orders.
    """    
    def __init__(self, name, fill_method, zmq_context = None):
        """Inits a simple broker.

        Args:
            fill_method (callable[[dict], [dict]]): A callable with signature (order, data) that returns any
                (partially or fully) filled order.
        """  
        super().__init__(name, zmq_context)
        self.fill_method = fill_method
    
    # ----------------------------------------------------------------------------------------
    # Event handling
    # ----------------------------------------------------------------------------------------
    def _handle_data(self, data):
        """Tries to fill existing open orders against incoming data.

            Note that data is expected to be right-aligned (EVENT_TS marks the end of the event).
        
        Args:
            data (dict): Data event.
        """        
        # first update clock
        self.clock = data[c.EVENT_TS]
        # try to fill any existing open orders
        fills = self.try_fill_with_data(data)
        # emit the order if there are any fills
        if (fills is not None) and (self.order_socket is not None):            
            for order in fills:
                order_packed = utils.packb(order)
                self.order_socket.send(order_packed, flags = zmq.NOBLOCK)

        return self.handle_data(data)

    def _handle_order(self, order):
        """Handles an incoming order.

        Args:
            order (dict): ORDER event.
        """
        order_response = self.take_order(order)
        if (order_response is not None) and (self.order_socket is not None):
            # emit the response if there is any
            order_response_packed = utils.packb(order_response)
            self.order_socket.send(order_response_packed, flags = zmq.NOBLOCK)
        return self.handle_order(order)

    def take_order(self, order):
        """Processes an incoming order.

        Args:
            order (dict): Incoming ORDER event.

        Returns:
            order: Response in the form of an ORDER event.
        """        
        # make a copy because we don't want to break things elsewhere.
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
                # copy because we don't want others to break our internal record of the order
                return copy.deepcopy(_order)

            elif _order[c.ORDER_TYPE] == c.CANCELLATION:
                # remove from open orders and acknowledge cancellation
                try:
                    cancelled_order = self.open_orders.pop(_order[c.ORDER_ID])
                    cancelled_order = _order.update({c.EVENT_TS: self.clock, c.EVENT_SUBTYPE: c.CANCELLED})
                except KeyError:
                    # let the requester know if there is nothing to cancel
                    cancelled_order = _order.update({c.EVENT_TS: self.clock, c.EVENT_SUBTYPE: c.INVALID})
                finally:
                    # copy because we don't want others to break our internal record of the order
                    return copy.deepcopy(cancelled_order)


    def try_fill_with_data(self, data):
        """ Tries to fill all currently open orders against data.
            Returns a list of fills (or empty list if no fills).

        Args:
            data (dict): DATA event to be filled against.

        Returns:
            list (list): list of any fills.
        """
        # keeping track of orders closed/filled with this data event
        # since we cannot change the dictionary size mid-iteration
        closed = []
        fills = []

        # nothing to do if it is not price data
        if (symbol_data := data.get(c.SYMBOL)) is None:
            return

        else:
            # try to fill each order
            for order_id, open_order in self.open_orders.items():
                if (symbol_order := open_order[c.SYMBOL]) == symbol_data:
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