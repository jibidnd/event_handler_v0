"""Event handler base class."""


# We want the message to be self-describing and relatively quick. Cross-platform is a nice-to-have.
# Messages will be very small but sent in a continuous stream.
# This pretty much takes out protobuf, pickle, and parquet; and leaves us with JSON and msessagepack.
# Now it mostly comes down to speed vs human-readability (some benchmarks here: https://medium.com/@shmulikamar/python-serialization-benchmarks-8e5bb700530b)

# One other nice thing about messagepack is that it seems to support more types, even objects:https://github.com/msgpack/msgpack/blob/master/spec.md#types-extension-type
# However, it is not a python standard lib
# Overall, I think that messagepack's speed advantage outweighs the human-readability part (we don't have to read the message on the wire, anyways)

import datetime
import uuid

from .utils import constants as c


class EventHandler:
    """Base class for classes that handle events.
    
    Event handler is the base class for the main classes: Strategy, Position, and Broker.
    The main function of an event handler is, as the name suggests, handle events. This seeks
    to emulate a real life scenario, where "events" arrive at the code and decisions have to
    be made.
    In that sense, a Strategy is really an event handler with added logic (to buy/sell) that
    reacts to data and order events, a Position is an event handler that updates attributes given
    order events, and a Broker is an event handler that responds to data and order events.

    The base class defines methods that are intended to be overridden.
    Methods with an underscore prepended ("internal methods") are methods that are to be overridden
    by module classes that implements the base class.
    Methods without the underscore prepended ("external methods") are method that are to be overridden
    by the user to add additional custom behaviour.
    External methods should be called by internal methods, after all action in the internal methods are done.

    In general, there are three stages to handling an event:
        - Before receiving the next event:
            _prenext, prenext
        - Preprocess the event:
            _preprocess_event, preprocess_event
        - Process the event
            _process_event (calls the individual _process_xxx and process_xxx methods), process_event
    """

    def __init__(self):
        pass

    def _handle_event(self, event):
        "Handles an event."
        self._prenext()
        event = self._preprocess_event(event)
        self._process_event(event)
        self.handle_event(event)
        return
    
    def handle_event(self, event):
        return

    def _prenext(self):
        """Things to do prior to receiving the next event.

        Place holder for internal use. For user-defined events, see `prenext`.
        """
        return self.prenext()
    
    def prenext(self):
        """To be overriden for actions to do before receiving next event."""
        return

    def _preprocess_event(self, event):
        return self.preprocess_event(event)
    
    def preprocess_event(self, event):
        return event

    def _process_event(self, event):
        """process the next event"""
        try:
            event_type = event[c.EVENT_TYPE]
        except KeyError:
            raise Exception('Event type not specified')

        if event_type == c.DATA:
            self._process_data(event)
        elif event_type == c.ORDER:
            self._process_order(event)
        elif event_type == c.COMMUNICATION:
            self._process_communication(event)
        else:
            raise Exception('Event type {} not supported.'.format(event_type))
        
        self.process_event(event)
        return 
    
    def process_event(self, event):
        return

    def _process_data(self, data):
        """Processes a data event."""
        self.process_data(data)
        return
    
    def process_data(self, data):
        """Additional action when processing data. To be overriden.

        Called after `_process_data`.
        """
        pass
    
    def _process_order(self, order):
        """Processes an order event."""
        self.process_order(order)
        return
    
    def process_order(self, order):
        """Additional action when processing orders. To be overriden.

        Called after `_process_order`.
        """
        pass

    def _process_communication(self, communication):
        """Handles a communication event."""
        self.process_communication(communication)
        return
    
    def process_communication(self, communication):
        """Additional action when processing communication. To be overriden.

        Called after `_process_communication`.
        """
        pass

    def _before_stop(self):
        """Things to execute prior to exiting.

        Gives user a chance to wrap things up and exit clean. To be overridden.
        Called before `stop` is called.
        """
        pass
    
    def before_stop(self):
        """Additional action when exiting.

        Called after `_before_stop`.
        """
        pass

    def _stop(self):
        """Exits clean.

        Called after `before_stop` is called.
        """
        return

# Define events
# All events will be in the form of dictionary, since they will all just be data packets, and dicts are very fast
# Templates
class event:

    @staticmethod
    def order_event(dict_order_details = {}):
        """Create an order with default arguments.
        
        See constants.py for event_subtypes for orders.
        """
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

    @staticmethod
    def data_event(dict_data_details = {}):
        """
        Create a data event with default arguments.
        See constants.py for event_subtypes for data.
        """
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
        """
        Create a data event with default arguments.
        See constants.py for event_subtypes for data.
        """
        communication = {
            c.EVENT_TYPE: c.COMMUNICATION,
            c.EVENT_SUBTYPE: c.REQUEST,
            c.EVENT_TS: datetime.datetime.now(),
            c.CONTEXT: None
        }
        communication.update(**dict_communication_details)

        return communication