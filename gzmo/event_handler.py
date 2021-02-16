# We want the message to be self-describing and relatively quick. Cross-platform is a nice-to-have.
# Messages will be very small but sent in a continuous stream.
# This pretty much takes out protobuf, pickle, and parquet; and leaves us with JSON and msessagepack.
# Now it mostly comes down to speed vs human-readability (some benchmarks here: https://medium.com/@shmulikamar/python-serialization-benchmarks-8e5bb700530b)

# One other nice thing about messagepack is that it seems to support more types, even objects:https://github.com/msgpack/msgpack/blob/master/spec.md#types-extension-type
# However, it is not a python standard lib
# Overall, I think that messagepack's speed advantage outweighs the human-readability part (we don't have to read the message on the wire, anyways)

import abc
import collections
import datetime
import uuid
import bisect
import itertools

# for converting lines to dataframes
import pandas as pd

from .. import constants as c


class EventHandler(abc.ABC):
    '''
    Event handler is the base class for the two main classes: Strategy and Position.

    A Strategy is really an event handler with added logic and communication infrastructure,
    whereas a Position is an event handler that updates attributes given order and data events.
    '''

    def __init__(self):
        pass

    def _handle_event(self, event):
        '''process the next event'''
        try:
            event_type = event[c.EVENT_TYPE]
        except KeyError:
            raise Exception('Event type not specified')
        
        self.handle_event(event)

        if event_type == c.DATA:
            return self._handle_data(event)
        elif event_type == c.ORDER:
            return self._handle_order(event)
        elif event_type == c.COMMUNICATION:
            return self._handle_communication(event)
        else:
            raise Exception('Event type {} not supported.'.format(event_type))


    
    @abc.abstractmethod
    def _handle_data(self, data):
        '''Handle a data event'''
        return
    
    @abc.abstractmethod
    def _handle_order(self, order):
        '''Handle an order event.'''
        return

    # @abc.abstractmethod
    def _handle_communication(self, communication):
        return
    
    def _preprocess_event(self, event):
        return event
    
    def preprocess_event(self, event):
        return event

    def handle_event(self, event):
        return

class EventHandlerTemplate(EventHandler):
    '''
    Template for how to set up a custom event handler
    '''

    def __init__(self):
        pass
    
    def _handle_data(self, data):
        # class methods
        # do something that all instances should do
        # instance methods
        # the instance will override handle_data
        self.handle_data(data)
        return

    def _handle_order(self, order):
        # class methods
        # do something that all instances should do
        # instance methods
        # the instance will override handle_order
        self.handle_order(order)
        return

    def _handle_communication(self, communication):
        # class methods
        # do something that all instances should do
        # instance methods
        # the instance will override handle_communication
        self.handle_communication(communication)
        return

    # @abc.abstractmethod
    def handle_data(self, data):
        return

    # @abc.abstractmethod
    def handle_order(self, order):
        return

    # @abc.abstractmethod
    def handle_communication(self, communication):
        return

# Define events
# All events will be in the form of dictionary, since they will all just be data packets, and dicts are very fast
# Templates
class event:

    @staticmethod
    def order_event(dict_order_details = {}):
        '''
        Create an order with default arguments.
        See constants.py for event_subtypes for orders.
        '''
        order = {
            c.EVENT_TYPE: c.ORDER,
            c.EVENT_SUBTYPE: c.REQUESTED,
            c.EVENT_TS: datetime.datetime.now().timestamp(),
            c.SYMBOL: None,
            c.ASSET_CLASS: c.EQUITY,
            c.ORDER_TYPE: c.MARKET,
            c.PRICE: None,
            c.QUANTITY: None,
            c.STRATEGY_ID: None,
            c.TRADE_ID: None,
            c.ORDER_ID: None,
            c.EVENT_ID: str(uuid.uuid1()),
            # order fill information
            c.QUANTITY_OPEN: None,
            c.QUANTITY_FILLED: None,
            # credit, debit, and net are changes to order[strategy_id]
            c.CREDIT: 0,
            c.DEBIT: 0,
            c.BROKER: None,
            c.COMMISSION: 0
        }

        order.update(**dict_order_details)

        return order
    
    # @staticmethod
    # def cashflow_order(amount, **dict_order_details):
    #     """Create an order event for deposit/withdrawal of cash.

    #     Args:
    #         amount (numeric): amount of cash to add (negative for withdrawals).
    #     """
    #     o = order_event({
    #         c.SYMBOL: c.CASH,
    #         c.CREDIT: max(amount, 0),
    #         c.DEBIT: min(amount, 0),
    #         c.NET: amount
    #         }).update(**dict_order_details)
    #     return o

    @staticmethod
    def data_event(dict_data_details = {}):
        '''
        Create a data event with default arguments.
        See constants.py for event_subtypes for data.
        '''
        data = {
            c.EVENT_TYPE: c.DATA,
            c.EVENT_SUBTYPE: None,
            c.EVENT_TS: datetime.datetime.now(),
            c.SYMBOL: None
        }
        data.update(**dict_data_details)

        return data
    
    @staticmethod
    def communication_event(dict_communication_details = {}):
        '''
        Create a data event with default arguments.
        See constants.py for event_subtypes for data.
        '''
        communication = {
            c.EVENT_TYPE: c.COMMUNICATION,
            c.EVENT_SUBTYPE: c.REQUEST,
            c.EVENT_TS: datetime.datetime.now(),
            c.CONTEXT: None
        }
        communication.update(**dict_communication_details)

        return communication