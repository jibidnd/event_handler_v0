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
import abc

import pandas as pd
import zmq

from .utils import constants as c
from . import utils


class EventHandler(abc.ABC):
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
        - Before processing the next event:
            _prenext, prenext
        - Preprocess the event:
            _preprocess_event, preprocess_event
        - Process the event
            _process_event (calls the individual _process_xxx and process_xxx methods), process_event
    """

    def __init__(self):

        self.data_socket = None
        self.order_socket = None
        self.communication_socket = None
        self.logging_socket = None
        self.next_events = {}
    
    # -------------------------------------------------------------------

    def run(self, session_shutdown_flag = None):
        self._before_start()
        self.before_start()
        self._start(session_shutdown_flag = session_shutdown_flag)
        self._before_stop()
        self.before_stop()
        self._stop()
        self.stop()
        
    # -------------------------------------------------------------------

    def _before_start(self):
        """Class internal method for actions prior to starting.

        Called before `before_start`.
        """
        pass
    
    def before_start(self):
        """To be overriden for user-defined actions prior to starting.

        e.g. passing cash, warm up data, etc.
        Called after `_before_start`.
        """
        pass
    
    def _start(self):
        """Start the main event loop."""
        pass
    
    def _get_next_events(self):
        """Fills self.next_events to get next event to be processed."""
        if (self.next_events.get(c.COMMUNICATION_SOCKET) is None) and (self.communication_socket is not None):
            try:
                event = self.communication_socket.recv(zmq.NOBLOCK)
                self.next_events[c.COMMUNICATION_SOCKET] = utils.unpackb(event)
            except zmq.ZMQError as exc:
                if exc.errno == zmq.EAGAIN:
                    # Nothing to grab
                    pass
                else:
                    raise
        
        if (self.next_events.get(c.DATA_SOCKET) is None) and (self.data_socket is not None):
            try:
                topic, event = self.data_socket.recv_multipart(zmq.NOBLOCK)
                self.next_events[c.DATA_SOCKET] = utils.unpackb(event)
            except zmq.ZMQError as exc:
                if exc.errno == zmq.EAGAIN:
                    # Nothing to grab
                    pass
                else:
                    raise

        if (self.next_events.get(c.ORDER_SOCKET) is None) and (self.order_socket is not None):
            try:
                event = self.order_socket.recv(zmq.NOBLOCK)
                self.next_events[c.ORDER_SOCKET] = utils.unpackb(event)
            except zmq.ZMQError as exc:
                if exc.errno == zmq.EAGAIN:
                    # Nothing to grab
                    pass
                else:
                    raise

    def next(self):
        """Processes the next event.

            Returns True if something was processed,
            otherwise returns False.
        """

        had_activity = False

        self._get_next_events()

        # Now we can sort the upcoming events and process the next event
        if len(self.next_events) > 0:
            # Handle the socket with the next soonest event (by EVENT_TS)
            # take the first item (socket name) of the first item ((socket name, event)) of the sorted queue
            next_socket = sorted(self.next_events.items(), key = lambda x: x[1][c.EVENT_TS])[0][0]
            next_event = self.next_events.pop(next_socket)        # remove the event from next_events
            self._handle_event(next_event)
            had_activity = True
        
        return had_activity
    
    def _before_stop(self):
        """Class internal method for actions prior to exiting.

        Called before `before_stop`.
        """

    def before_stop(self):
        """To be overriden for user-defined actions prior to exiting.

        e.g. cancel open orders, etc
        Called after `_before_stop`.
        """

    def _stop(self):
        """Class internal method for actions when exiting.

        Called before `stop`.
        """

    def stop(self):
        """To be overriden for user-defined actions after exiting.

        e.g. return results, etc
        Called after `_stop`.
        """
    # -------------------------------------------------------------------

    def _handle_event(self, event):
        "Handles an event."
        self._prenext()
        event = self._preprocess_event(event)
        self._process_event(event)
        self.handle_event(event)
        return

    def handle_event(self, event):
        return
    
    # -------------------------------------------------------------------

    def _prenext(self):
        """Class internal method for actions prior to receiving the next event.

        Called before `prenext`.
        """
        self.prenext()
        return

    def prenext(self):
        """To be overriden for user-defined actions prior to receiving the next event.

        Called after `prenext`.
        """
        return

    def _preprocess_event(self, event):
        """Class internal method for preprocessing an event.

        Called before `prenext`.
        """
        return self.preprocess_event(event)

    def preprocess_event(self, event):
        """To be overriden for user-defined actions for preprocessing an event.

        Called after `_preprocess_event`.
        """
        return event

    def _process_event(self, event):
        """Class internal method for delegating events to be processed.

        Calls one of `_process_data`, `_process_order`, or `_process_communication`
            depending on the `event_type` of the event, then calls `process_event`.
        """
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
        """To be overriden for user-defined actions for processing an event.

        Called after `_preprocess_event`.
        """

    def _process_data(self, data):
        """Processes events with event_type DATA."""
        self.process_data(data)
        return

    def process_data(self, data):
        """To be overriden for user-defined actions for processing a DATA event.

        Called after `_process_data`.
        """
        pass

    def _process_order(self, order):
        """Processes events with event_type ORDER."""
        self.process_order(order)
        return

    def process_order(self, order):
        """To be overriden for user-defined actions for processing an ORDER event.

        Called after `_process_order`.
        """
        pass

    def _process_communication(self, communication):
        """Processes events with event_type COMMUNICATION."""
        self.process_communication(communication)
        return

    def process_communication(self, communication):
        """To be overriden for user-defined actions for processing a COMMUNICATION event.

        Called after `_process_communication`.
        """
        pass

class Event(dict):
    """Subclasses dict to provide dottable access of properties.

    This class provides the general structure of 1 single event.
    It serves as the template for how events should be consumed -- fields to
    expect, structure, etc.

    Since dicts are much faster than dataclasses in creation, and we'll be
    creating lots of datapoints, we'll subclass a dict instead of using
    dataclasses.
    """

    def __init__(
            self,
            event_type,
            event_subtype,
            event_ts):

        super().__init__()
        self[c.EVENT_TYPE] = event_type
        self[c.EVENT_SUBTYPE] = event_subtype
        self[c.EVENT_TS] = event_ts

    def __getattr__(self, key):
        """Makes the dot notation available.

        Args:
            key (str): The key of the dictionary to access. A case-sensitive
                match is first attempted, before a case-insensitive attempt.
                A case-insensitive attempt is ~3 times slower.

        Raises:
            KeyError: If no matches are found.

        Returns:
            deque: The requested element of the dict; a deque object.
        """
        if key in self.keys():
            return self[key]
        elif (matching_keys := [k for k in self.keys() if k.lower() == key.lower()]) != []:
            # 3 times slower, try to avoid this
            _key = matching_keys[0]
            return self[_key]
        else:
            raise KeyError(key)
