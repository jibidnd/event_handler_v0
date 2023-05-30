"""Event handler base class."""


# We want the message to be self-describing and relatively quick. Cross-platform is a nice-to-have.
# Messages will be very small but sent in a continuous stream.
# This pretty much takes out protobuf, pickle, and parquet; and leaves us with JSON and msessagepack.
# Now it mostly comes down to speed vs human-readability (some benchmarks here: https://medium.com/@shmulikamar/python-serialization-benchmarks-8e5bb700530b)

# One other nice thing about messagepack is that it seems to support more types, even objects:https://github.com/msgpack/msgpack/blob/master/spec.md#types-extension-type
# However, it is not a python standard lib
# Overall, I think that messagepack's speed advantage outweighs the human-readability part (we don't have to read the message on the wire, anyways)

import abc
import time

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

    This base class is a template for classes to handle data, orders, and communication via
    the corresponding sockets. Subclasses simply have to spcify the sockets to have events from
    those sockets processed.

    The base class defines methods that are intended to be overridden.
    Methods with an underscore prepended ("internal methods") are methods that can be overridden
    by module classes that implements the base class.
    Methods without the underscore prepended ("external methods") are method that are to be overridden
    by the user to add additional custom behaviour.
    External methods should be called by internal methods, after all action in the internal methods are done.

    In general, there are three stages to handling an event:
        - Before processing the next event:
            _pre_event, pre_event
        - Get the next event
            _get_next_events
        - Process the event
            _process_event
                process_event
                _process_data, process_data
                _process_order, process_order
                _process_communication, process_communication
    """

    def __init__(self):

        self.data_socket = None
        self.order_socket = None
        self.communication_socket = None
        self.logging_socket = None
        self.next_events = {}
    
      
    # -------------------------------------------------------------------

    def run(self):
        # setup
        self._setup()
        self.setup()
        # start looping
        self._start()
        # exit
        self._stop()
        self.stop()

    def _setup(self):
        pass
    
    def _start(self, shutdown_flag):
        """Main event loop to handle events from sockets.

        The main event loop continuously checks for events from the communication, data,
            and order sockets and handles them.

        At any time, the event handler will have visibility of the next event from each socket:
        Data, order, communication; as stored in next_events.
        If any of these slots are empty, the event handler will make 1 attempt to receive data
        from the socket.

        Then, the events in next_events will be sorted by event_ts, and the very next
        event will be handled.

        The logic then resets and loops forever.

        The logic is roughly as follow:
            - Populate next communication event if not already populated
            - Populate next data event if not already populated
            - Populate next order event if not already populated
            - Sort the next communication, data, and order events by EVENT_TS
            - Process the earliest of the 3 events
            - Loop
        """

        while not shutdown_flag.is_set():
            if not self.next():
                # so we don't take up too much resources when backtesting by
                # going in an empty infinitely loop
                time.sleep(0.01)
        return
    
    def _stop(self):
        """Class internal method for actions when exiting.

        Sets the shutdown floag and closes any open sockets.
        Called before `stop`.
        """


        # # close sockets
        # for socket in [self.communication_socket, self.data_socket, self.order_socket, self.logging_socket]:
        #     if ((socket is not None) and (~socket.closed)):
        #         socket.close(linger = 10)
        self.stop()
        return

    # -------------------------------------------------------------------

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

        self._pre_event()
        self.pre_event()

        had_activity = False

        self._get_next_events()

        # Now we can sort the upcoming events and process the next event
        if len(self.next_events) > 0:
            # Handle the socket with the next soonest event (by EVENT_TS)
            # take the first item (socket name) of the first item ((socket name, event)) of the sorted queue
            next_socket = sorted(self.next_events.items(), key = lambda x: x[1][c.EVENT_TS])[0][0]
            next_event = self.next_events.pop(next_socket)        # remove the event from next_events
            self._process_event(next_event)
            had_activity = True
        
        return had_activity


    # -------------------------------------------------------------------
    # Internal Interface
    # -------------------------------------------------------------------
    def _pre_event(self):
        """Class internal method for action prior to getting/handling the next event.
        """
        return


    def _process_event(self, event):
        """Class internal method for delegating events to be processed.

        Calls one of `_process_data`, `_process_order`, or `_process_communication`
            depending on the `event_type` of the event, then calls `process_event`.
        """
        
        # preprocess event?
        # event_ts = event[c.EVENT_TS]
        # if isinstance(event_ts, (int, float, decimal.Decimal)):
        #     event_ts = utils.unix2datetime(event_ts, to_tz = self.local_tz)
        # elif isinstance(event_ts, datetime.datetime):
        #     event_ts = event_ts.astimezone(self.local_tz)
        # event[c.EVENT_TS] = event_ts

        # localize the event ts
        # event_ts should already be a pd.Timestamp object from unpacking
        # if no timezone information, assume already in local time?? TODO what's going on here?
        # event[c.EVENT_TS] = event[c.EVENT_TS].tz_convert(self.local_tz)

        # tick the clock if it has a larger timestamp than the current clock (not a late-arriving event)
        # if (event_ts := event[c.EVENT_TS]) > self.clock:
        #     self.clock = event_ts

        self.process_event(event)

        try:
            event_type = event[c.EVENT_TYPE]
        except KeyError:
            raise Exception('Event type not specified')

        if event_type == c.DATA:
            self._process_data(event)
            self.process_data(event)
        elif event_type == c.ORDER:
            self._process_order(event)
            self.prcess_order(event)
        elif event_type == c.COMMUNICATION:
            self._process_communication(event)
            self.process_communication(event)
        else:
            raise Exception('Event type {} not supported.'.format(event_type))
        
        return

    def _process_data(self, data):
        """Processes events with event_type DATA."""
        return

    def _process_order(self, order):
        """Processes events with event_type ORDER."""
        return

    def _process_communication(self, communication):
        """Processes events with event_type COMMUNICATION."""
        return

    # -------------------------------------------------------------------
    # User Interface
    # -------------------------------------------------------------------

    def setup(self):
        """To be overriden for user-defined actions prior to receiving the next event.

        Called after `_setup`.
        """
        return

    def pre_event(self):
        """To be overriden for user-defined actions prior to receiving the next event.

        Called after `_pre_event`.
        """
        return

    def process_event(self):
        """To be overriden for user-defined actions when processing an event

        Called in `_process_event` before `_process_data`.
        """
        return

    def process_data(self, data):
        """To be overriden for user-defined actions for processing a DATA event.

        Called after `_process_data`.
        """
        pass

    def process_order(self, order):
        """To be overriden for user-defined actions for processing an ORDER event.

        Called after `_process_order`.
        """
        pass

    def process_communication(self, communication):
        """To be overriden for user-defined actions for processing a COMMUNICATION event.

        Called after `_process_communication`.
        """
        pass
    
    def stop(self):
        """To be overriden for user-defined actions when stopping.

        Called after `_stop`.
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
        try:
            return super().__getattr__(key)
        except:
            raise
            # if (matching_keys := [k for k in self.keys() if k.lower() == key.lower()]) != []:
            #     # 3 times slower, try to avoid this
            #     _key = matching_keys[0]
            #     return self[_key]
            # else:
            #     raise KeyError(key)
