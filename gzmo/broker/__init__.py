"""Base broker class."""

import abc
import uuid
import threading
from decimal import Decimal
import configparser
import time

import pandas as pd
import zmq

from .. import event_handler
from .. import utils
from ..utils import constants as c


class BaseBroker(event_handler.EventHandler):
    """A broker is an interface that takes orders and/or data, and return responses to orders.

    The responses can either be sent via a socket (which the receiving end will connect to),
    or be returned through a broker method.

    Attributes:
        name (str): A name for the broker.
        broker_id (str): A unique ID for the broker generated by the uuid module.
        zmq_context (zmq.Context): A zmq context on which sockets will be established.
        open_orders (dict): Dictionary of currently open orders (waiting to be filled).
        closed_orders (list): A list of filled/closed orders.
        data_address (str): If using sockets, the zmq address to receive (price) data from,
            so the broker (simulator) can know whether/how to fill an incoming order.
        order_address (str): If using sockets, the address to receive orders from.
        logging_address(str): If using sockets, the address to send logs to.
        data_socket (zmq socket): If using sockets, the socket to receive (price) data from.
        order_socket (zmq socket): If using sockets, the socket to receive orders from.
        logging_socket (zmq socket): If using sockets, the socket to send logs to.
        shutdown_flag (threading.Event): If threading is used, this event can be set to signal the broker to shut down.
        clock (decimal.Decimal): UTC timestamp to keep track of time.
    
    Methods:
        format_order_in: formats order to internal formats.
        format_order_out: formats order to external (broker specific) formats.
        place_order: places the order to the broker and sends any response to order_socket (called by _process_order).
    """

    def __init__(self, name, auth = None, zmq_context = None):
        """Instantiates a broker object.

        Args:
            name (str): A human readable name for the broker.
            zmq_context (zmq.Context, optional): ZMQ context on which sockets will be established. Defaults to None.
        """        
        # initialization params
        self.name = name
        self.broker_id = str(uuid.uuid1())  # Note that this shows the network address.
        self.zmq_context = zmq_context

        # Get auth file if a path if provided
        if isinstance(auth, str):
            # read the file
            config = configparser.ConfigParser()
            config.read_file(open(auth))
            self.auth = config
        else:
            self.auth = auth

        # Connection things
        self.data_address = None
        self.order_address = None
        self.logging_addresses = None
        self.data_socket = None
        self.order_socket = None
        self.logging_socket = None
        
        self._shutdown_flag = threading.Event()

        # clock to keep track of time
        # self.clock = Decimal(0.00)
    
    # ----------------------------------------------------------------------------------------
    # Connections
    # ----------------------------------------------------------------------------------------
    def connect_data_socket(self, data_address):
        """Connects a socket to the specified `data_address`.

        Args:
            data_address (str): A ZMQ address string in the form of 'protocol://interface:port’.
        """
        # if new address, overwrite the current record
        self.data_address = data_address
        
        # establish a context if none provided
        if self.zmq_context is None:
            self.zmq_context = zmq.Context.Instance()
        
        # Create a data socket if none exists yet
        if not self.data_socket:
            socket = self.zmq_context.socket(zmq.SUB)
            self.data_socket = socket
        
        # subscribe to everything because we don't know what we'll need to match orders against
        self.data_socket.setsockopt(zmq.SUBSCRIBE, b'')
        self.data_socket.connect(data_address)
        
        return
    
    def connect_order_socket(self, order_address):
        """Connects a socket to the specified `order_address`.

        Args:
            order_address (str): A ZMQ address string in the form of 'protocol://interface:port’.
        """
        # if new address, overwrite the current record
        self.order_address = order_address

        # establish a context if none provided
        if self.zmq_context is None:
            self.zmq_context = zmq.Context.Instance()

        # Create a order socket if none exists yet
        if not self.order_socket:
            socket = self.zmq_context.socket(zmq.DEALER)
            # note that this broker is identified by its name
            socket.setsockopt(zmq.IDENTITY, self.name.encode())
            self.order_socket = socket
        self.order_socket.connect(order_address)

        return
    
    def connect_logging_socket(self, logging_address):
        """Connects a socket to the specified `logging_address`.

        Args:
            logging_address (str): A ZMQ address string in the form of 'protocol://interface:port’.
        """
        # if new address, add it to the list
        if logging_address not in self.logging_addresses:
            self.logging_addresses.append(logging_address)
        
        # Create a logging socket if none exists yet
        if not self.logging_socket:
            socket = self.zmq_context.socket(zmq.DEALER)
            socket.setsockopt(zmq.IDENTITY, self.broker_id.encode())
            self.logging_socket = socket
        self.logging_socket.conenct(logging_address)

        return

    # ----------------------------------------------------------------------------------------
    # Event handling
    # ----------------------------------------------------------------------------------------
 
    # @abc.abstractmethod
    def format_order_out(self, order):
        return order
    
    # @abc.abstractmethod
    def format_order_in(self, order):
        return order

    def _start(self, session_shutdown_flag = None, pause = 1):
        """Processes events from sockets.

            Sequentially process events arriving at the data and order sockets.
            All *received* data are synced and processed chronologically, as indicated
            by the EVENT_TS field of the event.
        """

        if session_shutdown_flag is None:
            session_shutdown_flag = self._shutdown_flag
            session_shutdown_flag.clear()

        # the event "queue"
        next_events = {}

        while (not session_shutdown_flag.is_set()) and (not self._shutdown_flag.is_set()):
            
            # get from data socket if slot is empty
            if (next_events.get(c.DATA_SOCKET) is None) and (self.data_socket is not None):
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
            if (next_events.get(c.ORDER_SOCKET) is None) and (self.order_socket is not None):
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
            else:
                time.sleep(pause)

    def _process_order(self, order):
        """Class method to process an incoming order events.

        Args:
            order (dict): Event with EVENT_TYPE = 'ORDER'. Should carry order parameters
                that the broker needs to fill the order.
        """
        order_internal = self.format_order_in(order)
        order_external = self.format_order_out(order)

        # if this is an order request from a strategy
        if order_internal[c.EVENT_SUBTYPE] == c.REQUESTED:
            if (response := self._place_order(order_external)) is not None:
                self._handle_event(response)
        # otherwise it's a response from the broker. Send it to the strategy
        else:
            if (self.order_socket is not None):
                self.order_socket.send(utils.packb(order_internal))
        
        self.process_order(order)
        return

    def _stop(self):
        """Set the shutdown flag and close the sockets.
        """        
        self._shutdown_flag.set()
        for socket in [self.data_socket, self.order_socket, self.logging_socket]:
            socket.close(linger = 10)



class OrderEvent:
    """Subclasses Event to have data-specific events.
    
    Each datafeed should return DataEvent objects to keep a consistent
    format for consumption.
    """
    
    class base_order_event(event_handler.Event):

        def __init__(
            self,
            event_subtype,
            order_type,
            order_class,
            symbol,
            event_ts,
            # order creation
            quantity = None,
            notional = None,
            time_in_force = c.GTC,
            limit_price = None,
            stop_price = None,
            trail_price = None,
            trail_percent = None,
            extended_hours = False,
            take_profit = None,
            stop_loss = None,
            # order fills
            quantity_filled = None,
            average_fill_price = None,
            credit = 0,
            debit = 0,
            commission = 0,
            # order info
            strategy_id = None,
            broker = None,
            order_id = uuid.uuid1(),
            broker_order_id = None,
            event_ts = datetime.datetime.now(),
            created_at = datetime.datetime.now(),
            updated_at = datetime.datetime.now(),
            submitted_at = None,
            memo = None,
            asset_class = None
            # ):

            super().__init__(
                event_type = c.ORDER,
                event_subtype = event_subtype,
                event_ts = event_ts)

            for kwarg, name in [
                (order_type, c.ORDER_TYPE),
                (order_class, c.ORDER_CLASS),
                (symbol, c.SYMBOL),
                (quantity, c.QUANTITY),
                (notional, c.NOTIONAL),
                (time_in_force, c.TIME_IN_FORCE),
                (limit_price, c.LIMIT_PRICE),
                (stop_price, c.STOP_PRICE),
                (trail_price, c.TRAIL_PRICE),
                (trail_percent, c.TRAIL_PERCENT),
                (extended_hours, c.EXTENDED_HOURS),
                (take_profit, c.TAKE_PROFIT),
                (stop_loss, c.STOP_LOSS),
                # order fills
                (quantity_filled, c.QUANTITY_FILLED),
                (average_fill_price, c.AVERAGE_FILL_PRICE),
                (credit, c.CREDIT),
                (debit, c.DEBIT),
                (commission, c.COMMISSION),
                # order info
                (strategy_id, c.STRATEGY_ID),
                (broker, c.BROKER),
                (order_id, c.ORDER_ID),
                (broker_order_id, c.BROKER_ORDER_ID),
                (event_ts, c.EVENT_TS),
                (created_at, c.CREATED_AT),
                (updated_at, c.UPDATED_AT),
                (submitted_at, c.SUBMITTED_AT),
                (memo, c.MEMO),
                (asset_class, c.ASSET_CLASS)]:
                
                self[name] = kwarg

            return
    
    class order_fill(base_order_event):
        def __init__(
            self,
            order,
            event_ts,
            quantity_filled,
            credit,
            debit,
            commission = 0
        ):
            super().__init__(
                event_subtype = c.FILLED,
                order_type = order[c.ORDER_TYPE],
                order_class = order[c.ORDER_CLASS],
                symbol = order[c.SYMBOL],
                event_ts = event_ts
            )
            
            self.update(order)

            for name, val in [
                    (c.QUANTITY_FILLED, quantity_filled),
                    (c.CREDIT, credit),
                    (c.DEBIT, debit),
                    (c.COMMISSION, commission)]:
                self[name] = val


    class cash_order(order_fill):
        def __init__(
            self,
            symbol,
            notional,
            order_id = None,
            event_ts = None,
            memo = None,
            asset_class = c.STRATEGY,
            **kwargs
        ):
            notional = Decimal(notional)
            order = {
                c.ORDER_TYPE: c.CASHFLOW,
                c.ORDER_CLASS: c.SIMPLE,
                c.SYMBOL: symbol,
                c.NOTIONAL: notional,
                **kwargs
            }
            for name, val in [
                    (c.ORDER_ID, order_id),
                    (c.MEMO, memo),
                    (c.ASSET_CLASS, asset_class)]:
                if val is not None:
                    order[name] = val
            super().__init__(
                order = order,
                event_ts = event_ts or pd.Timestamp.now(),
                quantity_filled = notional,
                credit = 0 if notional >= 0 else -notional,
                debit = notional if notional >= 0 else 0
            )


            return

    class limit_order(base_order_event):
        def __init__(
            self,
            symbol,
            limit_price,
            quantity = None,
            notional = None,
            event_subtype = c.REQUESTED,
            time_in_force = c.GTC,
            broker = None,
            order_id = None,
            broker_order_id = None,
            event_ts = None,
            created_at = None,
            updated_at = None,
            memo = None,
            asset_class = c.EQUITY,
            **kwargs
        ):

            super().__init__(
                event_subtype = event_subtype,
                order_type = c.LIMIT,
                order_class = c.SIMPLE,
                symbol = symbol,
                event_ts = event_ts
            )

            self[c.LIMIT_PRICE] = limit_price
            # how much to order?
            if quantity is not None:
                self[c.QUANTITY] = quantity
            elif notional is not None:
                self[c.NOTIONAL] = notional
            else:
                raise Exception('Must provide one of quantity or notional')
            # get time
            now = pd.Timestamp.now()
            for time, val in [
                    (c.EVENT_TS, event_ts),
                    (c.CREATED_AT, created_at),
                    (c.UPDATED_AT, updated_at)]:
                self[time] = val or now
            # other args
            for name, val in [
                    (c.TIME_IN_FORCE, time_in_force),
                    (c.BROKER, broker),
                    (c.ORDER_ID, order_id),
                    (c.BROKER_ORDER_ID, broker_order_id),
                    (c.MEMO, memo),
                    (c.ASSET_CLASS, asset_class)]:
                if val is not None:
                    self[name] = val
            return

    class market_order(base_order_event):
        def __init__(
            self,
            symbol,
            quantity = None,
            notional = None,
            event_subtype = c.REQUESTED,
            time_in_force = c.GTC,
            broker = None,
            order_id = None,
            broker_order_id = None,
            event_ts = None,
            created_at = None,
            updated_at = None,
            memo = None,
            asset_class = c.EQUITY
        ):

            super().__init__(
                event_subtype = event_subtype,
                order_type = c.LIMIT,
                order_class = c.SIMPLE,
                symbol = symbol,
                event_ts = event_ts
            )

            self[c.ORDER_ID] = order_id or str(uuid.uuid1())

            # how much to order?
            if quantity is not None:
                self[c.QUANTITY] = quantity
            elif notional is not None:
                self[c.NOTIONAL] = notional
            else:
                raise Exception('Must provide one of quantity or notional')

            # get time
            now = pd.Timestamp.now()
            for time, val in [
                    (c.EVENT_TS, event_ts),
                    (c.CREATED_AT, created_at),
                    (c.UPDATED_AT, updated_at)]:
                self[time] = val or now

            # other args
            for name, val in [
                    (c.TIME_IN_FORCE, time_in_force),
                    (c.BROKER, broker),
                    (c.ORDER_ID, order_id),
                    (c.BROKER_ORDER_ID, broker_order_id),
                    (c.MEMO, memo),
                    (c.ASSET_CLASS, asset_class)]:
                if val is not None:
                    self[name] = val
            return