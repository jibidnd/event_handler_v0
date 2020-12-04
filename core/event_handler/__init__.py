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
        
        if event_type == c.DATA:
            self._handle_data(event)
        elif event_type == c.ORDER:
            self._handle_order(event)
        elif event_type == c.COMMAND:
            self._handle_command(event)
        else:
            raise Exception('Event type {} not supported.'.format(event_type))
        
        return self.handle_event(event)
    
    @abc.abstractmethod
    def _handle_data(self, data):
        '''Handle a data event'''
        return
    
    @abc.abstractmethod
    def _handle_order(self, order):
        '''Handle an order event.'''
        return

    @abc.abstractmethod
    def _handle_command(self, command_event):
        return
    
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

    def _handle_command(self, command):
        # class methods
        # do something that all instances should do
        # instance methods
        # the instance will override handle_command
        self.handle_command(command)
        return

    # @abc.abstractmethod
    def handle_data(self, data):
        return

    # @abc.abstractmethod
    def handle_order(self, order):
        return

    # @abc.abstractmethod
    def handle_command(self, command):
        return

# class Trade:
#     '''A trade consists of a group of transactions that should be viewed together'''
#     def __init__(self):
#         # TODO: get name of strategy from which it's called
#         # Define the following
#         # self.strategy_id
#         # self.trade_id
#         # self.trade_name
#         # self.status
#         # self.risk
#         # self.realized
#         # self.unrealized
#         # self.commission
#         # self.positions
#         pass
    
#     def update(self, event):
#         # Handle events: Order, Fill, Flow, Command
#         pass
    
#     def close(self, params):
#         # liquidate the position
#         pass



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
            c.ORDER_TYPE: c.MARKET,
            c.PRICE: None,
            c.QUANTITY: None,
            c.CREDIT: None,
            c.DEBIT: None,
            c.NET: None,
            c.BROKER: None,
            c.STRATEGY_ID: None,
            c.TRADE_ID: None,
            c.ORDER_ID: None,
            c.EVENT_ID: str(uuid.uuid1()),
            c.COMMISSION: 0
        }

        order.update(**dict_order_details)

        return order
    
    @staticmethod
    def cashflow_order(amount, **dict_order_details):
        """Create an order event for deposit/withdrawal of cash.

        Args:
            amount (numeric): amount of cash to add (negative for withdrawals).
        """
        o = order_event({
            c.SYMBOL: c.CASH,
            c.CREDIT: max(amount, 0),
            c.DEBIT: min(amount, 0),
            c.NET: amount
            }).update(**dict_order_details)
        return o

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
