'''Strategies contain the business logic for making actions given events.'''


import abc
import collections
import uuid
from decimal import Decimal
import datetime
import pytz

import zmq
import msgpack

from .. import constants as c
from .. import event_handler
from ..event_handler import event
from .position import Position, CashPosition
from ... import utils
# from .. import event

class Strategy(event_handler.EventHandler):
    def __init__(self, name = None, zmq_context = None, parent_address = None, children_addresses = None,
                    data_address = None, order_address = None, logging_addresses = None, strategy_address = None,
                    params = {}, rms = None, local_tz = 'America/New_York'):
        # initialization params
        self.name = name
        self.zmq_context = zmq_context or zmq.Context.instance()
        self.parent_address = parent_address
        self.children_addresses = children_addresses
        self.data_address = data_address
        self.order_address = order_address
        self.logging_addresses = logging_addresses
        self.strategy_address = strategy_address
        self.params = params
        self.rms = rms or RMS()

        # data and record keeping
        self.parent_id = None
        self.strategy_id = str(uuid.uuid1()) # Note that this shows the network address.
        self.children_ids = {}        # name: strategy_id
        # If given, sends request to children in self.prenext
        # Otherwise children will only update with marks when prompted by self.to_child
        self.children_update_freq = {}  # strategy_id: datetime.timedelta
        self.datas = {}
        self.cash = CashPosition()
        self.open_trades = {}           # trade_id: [position_ids]
        self.closed_trades = {}         # trade_id: [position_ids]
        self.open_positions = {}        # position_id: position object
        self.closed_positions = {}      # position_id: position object
        # TODO
        self.pending_orders = {}        # orders not yet filled: order_id: order_event
        # The event "queue"
        self.next_events = {}

        # Keeping track of time: internally, time will be tracked as local time
        # For communication, (float) timestamps on the second resolution @ UTC will be used.
        self.clock = None


        # Connection things
        self.parent = None
        self.children = None
        self.data_socket = None
        self.order_socket = None
        self.logging_socket = None

        if self.parent_address is not None:
            self.connect_parent(parent_address)
        if self.children_addresses is not None:
            for children_address in children_addresses:
                self.connect_children(children_address)
        if self.data_address is not None:
            self.connect_data_socket(data_address)
        if self.order_address is not None:
            self.connect_order_socket(order_address)
        if self.logging_addresses is not None:
            for logging_address in logging_addresses:
                self.connect_logging_socket(logging_address)

    # ----------------------------------------------------------------------------------------------------
    # Communication stuff
    # ----------------------------------------------------------------------------------------------------
    def connect_parent(self, parent_address):
        # if new address, overwrite the current record
        self.parent_address = parent_address
        # Create a parent socket if none exists yet
        if not self.parent:
            socket = self.zmq_context.socket(zmq.DEALER)
            socket.setsockopt(zmq.IDENTITY, self.strategy_id.encode())
            self.parent = socket
        self.parent.connect(self.parent_address)

    def connect_children(self, children_address):
        # if new address, add it to the list
        if children_address not in self.children_addresses:
            self.children_addresses.append(children_address)
        # Create a children socket if none exists yet
        if not self.children:
            socket = self.zmq_context.socket(zmq.ROUTER)
            self.children = socket
        # Bind to the new address (on top of the current ones, if any)
        self.children.bind(children_address)

    def connect_data_socket(self, data_address):
        # if new address, overwrite the current record
        self.data_address = data_address
        # Create a data socket if none exists yet
        if not self.data_socket:
            socket = self.zmq_context.socket(zmq.SUB)
            # subscribe to data with prefix the strategy's id
            socket.setsockopt(zmq.SUBSCRIBE, self.strategy_id.encode())
            self.data_socket = socket
        self.data_socket.connect(data_address)
        
    def connect_order_socket(self, order_address):
        # if new address, overwrite the current record
        self.order_address = order_address
        # Create a order socket if none exists yet
        if not self.order_socket:
            socket = self.zmq_context.socket(zmq.DEALER)
            socket.setsockopt(zmq.IDENTITY, self.strategy_id.encode())
            self.order_socket = socket
        self.order_socket.connect(order_address)

    def connect_logging_socket(self, logging_address):
        # if new address, add it to the list
        if logging_address not in self.logging_addresses:
            self.logging_addresses.append(logging_address)
        # Create a logging socket if none exists yet
        if not self.logging_socket:
            socket = self.zmq_context.socket(zmq.DEALER)
            socket.setsockopt(zmq.IDENTITY, self.strategy_id.encode())
            self.logging_socket = socket
        self.logging_socket.conenct(logging_address)
    
    def subscribe_to_data(self, topic):
        assert self.data_socket is not None, \
            'No data socket to subscribe data from.'
        self.data_socket.setsockopt(zmq.SUBSCRIBE, topic.encode())

    def add_child(self, child_name, child_strategy_id = None, child_update_freq = datetime.timedelta(seconds = 1), child_address = None):
        """Add a child.

            If child_address is provided, connect to that address.
            Otherwise assume that child is reacheable via one of children_addresses.

        Args:
            child_name (str): name of the child. Must be unique in the strategy.
            child_strategy (str): The strategy_id to identify the (existing) child by. If none, one will be generated by uuid.uuid1()
            child_update_freq (datetime.timedelta): How often the child's mark should be updated.
                                                    To update constantly (each _prenext), set to <0.
            child_address (str, optional): Address of the child if not already in self.children_addresses. Defaults to None.
        """
        self.children_ids[child_name] = child_strategy_id or str(uuid.uuid1())
        self.children_update_freq[child_strategy_id] = child_update_freq
        if child_address is not None:
            self.connect_children(child_address)
        return


    # ----------------------------------------------------------------------------------------------------
    # Session stuff
    # ----------------------------------------------------------------------------------------------------
    def start(self):
        '''Initialization'''
        pass
    
    def run(self):
        '''Main event loop. The session should call this.
        
            At any time, the strategy will have visibility of the next event from each socket:
                Data, order, parent, children; as stored in self.next_events.
                If any of these slots are empty, the strategy will make 1 attempt to receive data
                from the corresponding sockets.
            
                Then, the events in self.next_events will be sorted by event_ts, and the very next
                event will be handled.

                The logic then resets and loops forever.
        '''   
        # TODO: what if we lag behind in processing? how do we catch up?
        global keep_running
        keep_running = True
        while keep_running:

            # Attempt to fill the event queue for any slots that are empty
            for name, socket in zip([c.DATA_SOCKET, c.ORDER_SOCKET, c.PARENT_SOCKET, c.CHILDREN_SOCKET],
                                    [self.data_socket, self.order_socket, self.parent, self.children]):
                if socket is None: continue
                if self.next_events.get(name) is None:
                    try:
                        # Attempt to receive from each socket, but do not block
                        if socket.socket_type in [zmq.SUB, zmq.ROUTER]:
                            topic, event = socket.recv_multipart(zmq.NOBLOCK)
                        elif socket.socket_type in [zmq.DEALER]:
                            event = socket.recv(zmq.NOBLOCK)
                        self.next_events[name] = msgpack.unpackb(event)
                    except zmq.ZMQError as exc:
                        if exc.errno == zmq.EAGAIN:
                            pass
                        else:
                            raise
                
            # Now we can sort the upcoming events and handle the next event
            if len(self.next_events) > 0:
                # Run code that needs to be executed bofore handling any event            
                self._prenext()
                # Handle the socket with the next soonest event (by EVENT_TS)
                next_socket = sorted(self.next_events.items(), key = lambda x: x[1][c.EVENT_TS])[0][0]
                next_event = self.next_events.pop(next_socket)        # remove the event from self.next_events
                # tick the clock if it's a valid timestamp
                print(next_event[c.EVENT_TS])
                if (tempts := next_event[c.EVENT_TS]) > 0:
                    self.clock = utils.unix2datetime(tempts)
                self._handle_event(next_event)

    def stop(self):
        '''Prior to exiting. Gives user a chance to wrap things up and exit clean'''
        global keep_running
        keep_running = False
        print('stopping')
        return

    # ----------------------------------------------------------------------------------------------------
    # Business logic go here
    # The private methods perform default actions (e.g. passing an order, etc)
    #   and calls the abstractmethods, which contain the business logic unique to the strategy.
    # ----------------------------------------------------------------------------------------------------

    def _prenext(self):
        '''Prior to processing the next event'''
        # update children marks if it is time
        for child_name, freq in self.children_update_freq.items():
            data = self.datas[child_name]
            # has it been a while since we had the last mark?
            if (len(data) == 0) or (self.clock - data[c.EVENT_TS[-1]]):
                self.to_child(child, c.NET_ASSET_VALUE)
        return self.prenext()

    def _handle_data(self, data):
        '''Update lines in self.datas with data event'''
        try:
            symbol = data.get(c.SYMBOL)
        except KeyError:
            # TODO:
            raise
            # log error: data event not well formed
            return
        # If a line exists, update it
        if self.datas.get(symbol) is not None:
            self.datas[symbol].update_with_data(data)

        return self.handle_data(data)

    def _handle_order(self, order):
        '''
        Handle an order event.

        Two types of order events can happen here:
        1) A "pass through"
            - The order is from one of the children, and subtype == REQUESTED.
                the order is assessed by the Strategy's RMS.
                If approved, the order is sent upchain (to the Strategy's parent),
                or if there are no parents, sent to the broker.
                If the Strategy's RMS rejects the order, an ORDER event with event_subtype REJECTED
                is published to the child that requested that order.
        2) Result of a prior order sent
            - If the order is of subtype in [DENIED, SUBMITTED, FAILED,
                                                RECEIVED, FILLED, EXPIRED,
                                                CANCELLED, REJECTED]
                (which implies that the origin of the order is from this strategy),
                the order is sent to the corresponding position to update the position.
        '''
        event_subtype = order.get(c.EVENT_SUBTYPE)
        from_children = (order.get(c.STRATEGY_ID) in self.children_ids.values())
        from_self = (str(order.get(c.STRATEGY_ID)) == str(self.strategy_id))

        if from_children  and event_subtype == c.REQUESTED:
        # receiving a REQUESTED order from one of the children
            # call RMS
            approved = self.rms.request_order_approval(order)
            if approved:
                self.place_order(order, trade = None)
            else:
                self.deny_order(order)
        elif from_self:
        # otherwise it is an order resolution. Update positions
            # update the positions that the order came from
            self.open_positions[order.get(c.POSITION_ID)]._handle_event(order)
            # update cash
            self.cash._handle_event(order)
        
        return self.handle_order(order)
        
    # TODO: handle liquidation rquest
    def _handle_command(self, command):
        answer = getattr(self, command[c.REQUEST])
        if hasattr(answer, '__call__'):
            if command.get(c.KWARGS) is not None:
                answer = answer(**command[c.KWARGS])
            else:
                answer = answer()
        # return the answer if there is one
        if answer is None:
            pass
        else:
            wrapped_answer = event.data_event({c.EVENT_SUBTYPE: c.SIGNAL, c.SYMBOL: self.strategy_id})
            wrapped_answer.update({c.REQUEST: answer})

            return self.to_parent(wrapped_answer)
        
        return

    def prenext(self):
        pass

    @abc.abstractmethod
    def handle_data(self, event):
        pass

    @abc.abstractmethod
    def handle_order(self, order):
        pass

    @abc.abstractmethod
    def handle_command(self, command_event):
        pass


    # ----------------------------------------------------------------------------------------------------
    # Info things
    # ----------------------------------------------------------------------------------------------------
    def get_mark(self, identifier):
        """Get the mark of an asset

        Args:
            asset (str): the name / identifier of the asset

        Returns:
            decimal: The last known value of the asset
        """
        # Get most recent datapoint
        if (data := self.datas.get(identifier)):
            return data.mark[-1]
        else:
            return None

    def value_open(self, identifier = None):
        """
        This is the sum of values of current open positions, EXCLUDING CASH.
        Value is calculated using self.get_mark.

        Args:
            identifier (str, optional): Could be a symbol, trade_id, or position_id.
                If None, returns the value of all open positions in the strategy. Defaults to None.

        Returns:
            value: the open value of the requested item.
        """
        if identifier is None:
            # sum value of all open positions
            v = sum([position.value_open for position_id, position in self.open_positions.items()])
        else:
            # sum value of all open positions having any of (trade_id, position_id, symbol) matching identifier
            v = sum([position.value_open for position_id, position in self.open_positions.items()
                        if identifier in (position_id, position.trade_id, position.symbol)])
        return v
    
    def value_pending(self, identifier = None):
        """
        This is the sum of pending values of current open positions.
        Value is calculated using self.get_mark.

        Args:
            identifier (str, optional): Could be a symbol, trade_id, or position_id.
                If None, returns the value of all open positions in the strategy. Defaults to None.

        Returns:
            value: the open value of the requested item.
        """
        if identifier is None:
            # sum value of all open positions
            v = sum([position.value_pending for position_id, position in self.open_positions.items()])
        else:
            # sum value of all open positions having any of (trade_id, position_id, symbol) matching identifier
            v = sum([position.value_pending for position_id, position in self.open_positions.items()
                        if identifier in (position_id, position.trade_id, position.symbol)])
        return v

    def total_pnl(self, identifier = None):
        '''Realized + unrealized PNL'''
        if identifier is None:
            credit = sum([position.credit for position_id, position in self.open_positions.items()]) \
                        + sum([position.credit for position_id, position in self.closed_positions.items()])
            debit = sum([position.debit for position_id, position in self.open_positions.itmes()]) \
                        + sum([position.debit for position_id, position in self.closed_positions.itmes()]) 
        return credit + self.value_open - debit

    @property
    def net_asset_value(self):
        """
        This is the sum of cash and values of current open positions.
        """
        nav = sum([position.value_open for position_id, position in self.open_positions.items()])
        nav += self.cash.value_open
        return nav
    
    @property
    def price(self):
        """NAV / total cash deposited"""
        return self.net_asset_value / self.cash.net_flow

    def to_child(self, child, message):
        self.children.send_multipart([child, message])
    
    def to_parent(self, message):
        self.parent.send(message)

    # ----------------------------------------------------------------------------------------------------
    # Action stuff
    # ----------------------------------------------------------------------------------------------------
    def create_position(self, symbol, asset_class = c.EQUITY, trade_id = None, position_id = None):
        position = Position(strategy = self, symbol = symbol, asset_class = asset_class, trade_id = trade_id, position_id = position_id)
        self.open_positions[position.position_id] = position
        return position

    def create_order(self, symbol, quantity, order_type = c.MARKET, position = None, order_details = {}):
        '''Note that order does not impact strategy until it is actually placed'''
        if position is None:
            # If there is a position with the symbol already, use the latest one of the positions with the same symbol
            current_positions_in_symbol = [pos for pos in self.open_positions.values() if pos.symbol == symbol]
            if len(current_positions_in_symbol) > 0:
                position = current_positions_in_symbol[-1]
            else:
                # else create a new position
                position = self.create_position(symbol = symbol)
        elif isinstance(position, dict):
            # If position details are supplied in a dict, create a new position with those details
            position = self.create_position(strategy = self, symbol = symbol, **position)
        elif isinstance(position, Position):
            pass
        else:
            raise Exception('position must be one of {None, dict, gzmo.core.event_handler.position.Position}')
        
        default_order_details = {
            c.STRATEGY_ID: self.strategy_id,
            c.TRADE_ID: position.trade_id,
            c.POSITION_ID: position.position_id,
            c.ORDER_ID: str(uuid.uuid1()),
            c.SYMBOL: position.symbol,
            c.QUANTITY: quantity,
            c.EVENT_TS: self.clock.timestamp(),
            c.PRICE: self.get_mark(symbol) or 0.0
            }
        
        return event.order_event({**default_order_details, **order_details})

    def place_order(self, order):
        '''
        Place an order to the parent,
        or in the case there is no parent,
        place the order to the broker.
        Assumes that any RMS action has already been performed.

        The order must contain position_id to update the corresponding positions.
        '''

        pass_through = (str(order[c.STRATEGY_ID]) != str(self.strategy_id))
        assert (position_id := order.get(c.POSITION_ID)) is not None, 'Order must belong to a position.'

        # Update the position with the order
        if not pass_through:
            self.open_positions[position_id]._handle_order(order)

        # place the order
        if self.parent is None:
            self.order_socket.send(msgpack.packb(order, use_bin_type = True, default = default_msgpack_packer))
        else:
            self.to_parent(order)
    
    def deny_order(self, order):
        '''
        Deny the request to place an order.
        Send the denial to the child.
        '''
        order = order.update(event_subtype = c.DENIED)
        self.children.send_multipart(order[c.STRATEGY_ID], order)

    def liquidate(self, id_or_symbol = None, order_details = {c.ORDER_TYPE: c.MARKET}):
        """Liquidate a trade, position, or positions on the specified symbol.

        Args:
            id_or_symbol (str): trade_id, position_id, or symbol to liquidate.
                If None, liquidate all positions. Default None.

        Raises:
            Exception: No trade/position/child strategy found to liquidate

        Returns:
            True: order submitted
        """
        # If the trade/position is already closed, let the user know
        if id_or_symbol in self.closed_trades.keys():
            raise Exception('Trade already closed.')
        elif (id_or_symbol in self.closed_positions):
            raise Exception('Posititon already closed.')
        # If this is a child strategy, send them the note
        elif id_or_symbol in self.children_ids.keys():
            self.to_child(children_ids[id_or_symbol], c.LIQUIDATE)
        elif id_or_symbol in self.children_ids.values():
            self.to_child(id_or_symbol, c.LIQUIDATE)
        elif (id_or_symbol in [position.symbol for position in self.open_positions]):
            for position in self.open_positions.values():
                if position.symbol == id_or_symbol:
                    closing_order = {c.QUANTITY: -position.quantity_open}
                    self.place_order(self.create_order(order_details = closing_order, position = position))
            for order in self.pending_orders.values():
                if order.get(c.SYMBOL) == id_or_symbol:
                    self.place_order(order.update({c.ORDER_TYPE: c.CANCELLATION}))
        else:
            raise Exception('No trade/position/child strategy found to liquidate.')



# ----------------------------------------------------------------------------------------------------
# Misc Classes and fucntions
# ----------------------------------------------------------------------------------------------------

def default_msgpack_packer(obj):
    try:
        return float(obj)
    except:
        return str(obj)

class RMS(abc.ABC):
    '''A risk management system that monitors risk and approves/denies order requests'''

    def request_order_approval(self, position = None, order = None):
        '''approve or deny the request to place an order. Default True'''
        return True

class lines(dict):
    """
    A lines object is a collection of time series (deques) belonging to one asset (symbol).
    
    The lines object will keep track of fields from data events, as specified in the __init__
        method. `include_only` and `exclude` cannot be modified after initialization to keep
        all deques in sync.
    If `include_only` is None (default), the first data event will determine what
        fields are tracked.

    Args:
        data_type (str): The type of data (TICK, BAR, QUOTE, SIGNAL). For reference only.
        symbol (str): An identifier for the underlying data. For reference only.
        include_only (None or set, optional): Only track these fields in a data event. 
            If None, track all fields of a given data event (if data has new fields, track those too).
            Cannot be modified after initialization. Defaults to None.
        exclude (None or set, optional): Do not track these fields in a data event.
            If include_only and exclude specify the same field, exclude takes precedence. 
            Cannot be modified after initialization. Defaults to None.
        
        
    """
    def __init__(self, data_type, symbol, include_only = None, exclude = set(), maxlen = None, mark_line = 'CLOSE'):       
        
        self.__initialized = False
        self.data_type = data_type
        self.symbol = symbol
        self.include_only = include_only
        self.exclude = exclude
        self.mark_line = mark_line

        # Keep a tab on what's tracked
        if include_only is not None:
            self._tracked = include_only - exclude
            for line in self._tracked:
                self[line] = collections.deque(maxlen = maxlen)
        else:
            self._tracked = None
        
        self.__initialized = True

    def update_with_data(self, data):
        # find out what should be tracked if we don't know yet  
        if self._tracked is None:
            if self.include_only is None:
                self._tracked = set(data.keys()) - self.exclude
            else:
                self._tracked = self.include_only - self.exclude
            
            # Create a deque for each tracked field  
            self.update({line: collections.deque() for line in self._tracked})

        for line in self._tracked:
            self[line].append(data.get(line))

    @property
    def mark(self):
        return self.get(self.mark_line)
    
    def __len__(self):
        if len(self.keys()) == 0:
            return 0
        else:
            return len(next(iter(self.values())))

    @property
    def include_only(self):
        return self._include_only
    
    @include_only.setter
    def include_only(self, include_only):
        if (self.__initialized) and (len(self.keys()) > 0):
            raise Exception('Cannot modify attribute after initialization.')
        else:
            self._include_only = include_only

    @property
    def exclude(self):
        return self._exclude

    @exclude.setter
    def exclude(self, exclude):
        if self.__initialized:
            raise Exception('Cannot modify attribute after initialization.')
        else:
            self._exclude = exclude