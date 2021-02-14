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

# class lines0(dict):
#     """
#     A lines object is a collection of time series (deques) belonging to one asset (symbol).
    
#     The lines object will keep track of fields from data events, as specified in the __init__
#         method. `include_only` and `exclude` cannot be modified after initialization to keep
#         all deques in sync.
#     If `include_only` is None (default), the first data event will determine what
#         fields are tracked.

#     Args:
#         data_type (str): The type of data (e.g. TICK, BAR, QUOTE, SIGNAL, STRATEGY). For reference only.
#         symbol (str): An identifier for the underlying data. For reference only.
#         include_only (None or set, optional): Only track these fields in a data event. 
#             If None, track all fields of a given data event (if data has new fields, track those too).
#             Cannot be modified after initialization. Defaults to None.
#         exclude (None or set, optional): Do not track these fields in a data event.
#             If include_only and exclude specify the same field, exclude takes precedence. 
#             Cannot be modified after initialization. Defaults to None.
        
        
#     """
#     def __init__(self, symbol, data_type = None, include_only = None, exclude = set([c.EVENT_TYPE, c.SYMBOL, c.TOPIC]), maxlen = None, mark_line = 'CLOSE'):       
        
#         self.__initialized = False
#         self.symbol = symbol
#         self.data_type = data_type
#         self.include_only = include_only
#         self.exclude = exclude
#         self.mark_line = mark_line

#         # Keep a tab on what's tracked
#         if include_only is not None:
#             self._tracked = include_only - exclude
#             for line in self._tracked:
#                 self[line] = collections.deque(maxlen = maxlen)
#         else:
#             self._tracked = None
        
#         self.__initialized = True

#     def update_with_data(self, data):
#         # find out what should be tracked if we don't know yet  
#         if self._tracked is None:
#             if self.include_only is None:
#                 self._tracked = set(data.keys()) - self.exclude
#             else:
#                 self._tracked = self.include_only - self.exclude
            
#             # Create a deque for each tracked field  
#             self.update({line: collections.deque() for line in self._tracked})

#         for line in self._tracked:
#             self[line].append(data.get(line))

#     @property
#     def mark(self):
#         return self.get(self.mark_line)
    
#     def __len__(self):
#         if len(self.keys()) == 0:
#             return 0
#         else:
#             return len(next(iter(self.values())))

#     @property
#     def include_only(self):
#         return self._include_only
    
#     @include_only.setter
#     def include_only(self, include_only):
#         if (self.__initialized) and (len(self.keys()) > 0):
#             raise Exception('Cannot modify attribute after initialization.')
#         else:
#             self._include_only = include_only

#     @property
#     def exclude(self):
#         return self._exclude

#     @exclude.setter
#     def exclude(self, exclude):
#         if self.__initialized:
#             raise Exception('Cannot modify attribute after initialization.')
#         else:
#             self._exclude = exclude

class lines(dict):
    """
    A lines object is a collection of time series (deques) belonging to one asset (symbol).
    
    The lines object will keep track of fields from data events, as specified in the __init__
        method. `include_only` and `exclude` cannot be modified after initialization to keep
        all deques in sync.
    If `include_only` is None (default), the first data event will determine what
        fields are tracked.

    Args:
        symbol (str): An identifier for the underlying data. For reference only.
        data_type (str): The type of data (e.g. TICK, BAR, QUOTE, SIGNAL, STRATEGY).
        resolution (str): The resolution of data (e.g. D, h, m, s).
        index_line (str): The line to use as index. Default 'EVENT_TS'.
        mark_line (str): The line to use as mark. Default 'CLOSE'.
        tracked (None or set, optional): Track these fields in a data event. Defaults to OHLCV.
        maxlen (int, optional): The number of datapoints to keep. If None, keep all data. Default None.
        
    """
    def __init__(self, symbol, data_type = c.BAR, resolution = 'm', index_line = c.EVENT_TS, mark_line = c.CLOSE, tracked = None, maxlen = None):       
        
        self.symbol = symbol
        self.data_type = data_type
        self.resolution = resolution
        self.index_line = index_line
        self.mark_line = mark_line
        self.tracked = tracked or [c.OPEN, c.HIGH, c.LOW, c.CLOSE, c.VOLUME]

        self.index = collections.deque(maxlen = maxlen)
        for line in self.tracked:
            self[line] = collections.deque(maxlen = maxlen)

    def __getattr__(self, key):

        if key in self.keys():
            return self[key]
        elif (matching_keys := [k for k in self.keys() if k.lower() == key.lower()]) != []:
            # 3 times slower, try to avoid this
            _key = matching_keys[0]
            return self[_key]
        else:
            raise KeyError(key)

    @property
    def mark(self):
        return self[self.mark_line]
    
    def __len__(self):
        if len(self.keys()) == 0:
            return 0
        else:
            return len(self.index)

    def update_with_data(self, data):

        self.index.append(data.get(self.index_line))
        for line in self.tracked:
            self[line].append(data.get(line))
    
    def as_pandas_df(self, columns = None):
        columns = columns or self.keys()
        df = pd.DataFrame(data = {k: v for k, v in self.items() if k in columns}, index = self.index)
        df.index.name = self.index_line
        return df

    def as_pandas_series(self, column = None):
        column = column or self.mark_line
        s = pd.Series(self[column], index = self.index)
        s.index.name = self.index_line
        return s