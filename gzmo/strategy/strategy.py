"""A Strategy contains the business logic for making actions given events."""

import abc
import collections
import uuid
import decimal
from collections import deque, defaultdict
import datetime
import pytz
import time
import threading
import copy
import warnings
import logging

import pandas as pd

import json

import zmq
from zmq.log.handlers import PUBHandler

from .position import Position, CashPosition
from .lines import Lines
from .. import event_handler
from ..event_handler import Event
from ..broker import OrderEvent
from .. import utils
from ..utils import constants as c

class Strategy(event_handler.EventHandler):
    """A Strategy contains the business logic for making actions given events.

    The Strategy object is the center piece of any trading algorithm.
    It serves the following functions:
        - Keeps track of positions
        - Keeps track of cash
        - Defines action when given a piece of data
        - Defines buy/sell logic
        - Connects with brokers
        - Connects with datafeeds
        - Connects with a "command center"

    (Not all attributes listed)
    Attributes:
        name
        zmq_context
        communication_address
        data_address
        order_address
        logging_addresses
        local_tz
        params
        parent
        strategy_id
        children
        data_subscriptions
        shares

    Methods:
        start
        prenext
        process_data
        process_communication
        process_order
    """
    def __init__(self, name = None, zmq_context = None, communication_address = None,
                    data_address = None, order_address = None, logging_address = None,
                    data_subscriptions = None, params = None, rms = None, local_tz = 'America/New_York'):
        super().__init__()
        
        # initialization params
        self.name = name
        self.zmq_context = zmq_context or zmq.Context.instance()
        self.communication_address = communication_address
        self.data_address = data_address
        self.order_address = order_address
        self.logging_address = logging_address
        self.local_tz = pytz.timezone(local_tz)
        self.params = params or dict()
        self.rms = rms or RMS()

        # data and record keeping
        self.parent = None
        self.strategy_id = str(uuid.uuid1()) # Note that this shows the network address.
        self.children = {}                  # name: strategy_id
        # If given, sends request to children in self.prenext
        # Otherwise children will only update with marks when prompted by self.to_child
        # self.children_update_freq = {}      # strategy_id: datetime.timedelta
        self.datas = {}
        self.data_subscriptions = (data_subscriptions or set()) | {self.strategy_id}   # list of topics subscribed to
        # self.open_trades = {}               # trade_id: [position_ids]
        # self.closed_trades = {}             # trade_id: [position_ids]
        self.positions = {}                 # position_id: position object
        self.shares = None                  # if none specified, shares will be set to $invested when add_cash is called for the first time.
        self.cash = CashPosition(owner = self.name); self.positions[self.cash.position_id] = self.cash

        # Keeping track of time: internally, time will be tracked as local time
        # For communication, (float) timestamps on the second resolution @ UTC will be used.
        self.clock = pd.Timestamp(0, tz = self.local_tz)
        self._shutdown_flag = threading.Event()

        # Connection things
        if self.communication_address is not None:
            self.connect_communication_socket(communication_address)
        if self.data_address is not None:
            self.connect_data_socket(data_address)
        if self.order_address is not None:
            self.connect_order_socket(order_address)
        if self.logging_address is not None:
            self.connect_logging_socket(logging_address)
        
        # logging
        self.logger = logging.getLogger(f'strategy[{self.name}]')
        self.logger.setLevel(logging.DEBUG)
    def test(self):
        self.logger.info('abc')
    def __str__(self):
        """Summary of the Strategy as string representation.

        Returns:
            str: Summary of the Strategy.
        """
        keys = ['name', 'params', 'parent', 'strategy_id', 'children', 'shares']
        d = {}
        for key in keys:
            d.update({key: self.__dict__[key]})

        d.update({'nav': self.get_nav()})
        try:
            d.update({'mark': self.get_mark()})
        except:
            pass
        d.update({'cash': self.cash.quantity_open})

        return repr(d)

    # ----------------------------------------------------------------------------------------------------
    # Communication stuff
    # ----------------------------------------------------------------------------------------------------
    def connect_communication_socket(self, communication_address):
        """Connects a socket to the specified `communication_address`.

        Args:
            communication_address (str): A ZMQ address string in the form of 'protocol://interface:port’.
        """
        # if new address, overwrite the current record
        self.communication_address = communication_address
        # Create a parent socket if none exists yet
        if not self.communication_socket:
            socket = self.zmq_context.socket(zmq.DEALER)
            socket.setsockopt(zmq.IDENTITY, self.strategy_id.encode())
            self.communication_socket = socket
        self.communication_socket.connect(communication_address)

    def connect_data_socket(self, data_address):
        """Connects a ZMQ SUB socket to the specified `data_address`.

        Also automatically subscribe to any topics in self.data_subscription.

        Args:
            data_address (str): A ZMQ address string in the form of 'protocol://interface:port’.
        """
        # if new address, overwrite the current record
        self.data_address = data_address
        # Create a data socket if none exists yet
        if not self.data_socket:
            socket = self.zmq_context.socket(zmq.SUB)
            self.data_socket = socket
            # subscribe to data with prefix the strategy's id
        for topic in self.data_subscriptions:
            self.subscribe_to_data(topic, track = (topic != self.strategy_id))
        self.data_socket.connect(data_address)

    def connect_order_socket(self, order_address):
        """Connects a ZMQ DEALER socket to the specified `order_address`.

        Args:
            order_address (str): A ZMQ address string in the form of 'protocol://interface:port’.
        """
        # if new address, overwrite the current record
        self.order_address = order_address
        # Create a order socket if none exists yet
        if not self.order_socket:
            socket = self.zmq_context.socket(zmq.DEALER)
            socket.setsockopt(zmq.IDENTITY, self.strategy_id.encode())
            self.order_socket = socket
        self.order_socket.connect(order_address)

    def connect_logging_socket(self, logging_address):
        """Connects a ZMQ PUB socket to the specified `logging_address`.

        Args:
            logging_address (str): A ZMQ address string in the form of 'protocol://interface:port’.
        """
        self.logging_address = logging_address
        # Create a logging socket if none exists yet
        if not self.logging_socket:
            socket = self.zmq_context.socket(zmq.PUB)
            self.logging_socket = socket
        self.logging_socket.connect(logging_address)
        self.logger.addHandler(utils.ZMQHandler(self.logging_socket))

    def subscribe_to_data(self, topic, track = True, line_args = None, lazy = False):
        """Subscribe to a data topic published to the data socket.

        Args:
            topic (str): The topic to the data will be published under.
                Pass an empty string `''` to subscribe to all topics. If an empty string
                is passed, `track` is ignored and data will not be tracked (unless
                separately subscribed to).
            track (bool, optional): If True, create lines to track the
                data. If False, the Strategy will still receive the data,
                handle it as usual, but will not store the data. Default True.
            line_args (dict, optional): If `track` is True, the argumnts to set
                up the datalines. See .strategy.lines for details. Default None.
            lazy (bool, optional): If True, immediately `setsockopt` on the datasocket.
                Set to False when subscription for data occurs before binding to a socket,
                e.g. in the case of a session.
        """
        if line_args is None:
            line_args = dict()

        # Keep a record
        # store topics as strings
        topic = str(topic)
        if topic not in self.data_subscriptions:
            self.data_subscriptions |= set([topic])

        # establish lines if tracked
        if track:
            # ignore wildcard subscriptions
            if topic == '':
                return
            else:
                # what to call this datafeed?
                if line_args.get('symbol') is None:
                    line_args['symbol'] = topic
                    # Create new lines if none exist yet
                if line_args['symbol'] not in self.datas.keys():
                    self.datas[line_args['symbol']] = Lines(**line_args)
        return

    def add_parent(self, parent_strategy = None):
        """Adds a parent.

        The child must know the parent's strategy_id. Either pass an object that supports
        calling .get('STRATEGY_ID') or directly pass the parent's strategy_id as a string.

        To create a parent-child relationship, both the child and the parent must add each other.

        Example:
            >>> # To create a parent-child relationship, do:
            >>> child.add_parent(parent)
            >>> parent.add_child(child)

        Args:
            parent_strategy (supports.get or str): If string, the parent's strategy_id. Otherwise
                parent_strategy.get('STRATEGY_ID') will be called to get the parent's strategy_id.
        """
        if isinstance(parent_strategy, str):
            self.parent = parent_strategy
        else:
            try:
                parent_strategy_id = parent_strategy.get(c.STRATEGY_ID)
            except:
                raise
            assert (parent_strategy_id) is not None, f'Cannot get strategy_id from {parent_strategy}'
            self.parent = parent_strategy_id

        return

    def add_child(self, symbol, child_strategy):
        """Adds a child.

        All of the child's information (the child's positions, cash balance, etc) are also tracked by the parent.
        To do so, the parent must know the child's positions at the time the child is added.
        Thus, the child must either be passed directly as a Strategy object (to support .get_openpositionsummary())
        or be a Strategy connected through the communication socket.

        Args:
            symbol (str): The symbol under which to track the child. Since the child strategy is essentially
                tracked like a ticker, the symbol must be unique in the (parent) strategy across the set of all symbols
                among all assets.
            child_strategy (Strategy, dict, or None): Used to get the child's strategy_id and current open positions.
                If a Strategy object is provided, `add_parent` and `get_openpositionsummary` will be called on the child
                    Strategy.
                If a dict is provided, must have keys `STRATEGY_ID' and 'openpositionsummary'. `add_parent` method is remotely
                    called via the communication channel to finish establishing the relationship but is not verified.
                If a string is provided, records it as the child's strategy_id, and `add_parent` and `get_openpositionsummary`
                    will be called via the communication channel in a separate thread. The parent will wait up to 5 seconds for
                    the open position summary information to arrive before throwing an Exception.
        """
        if isinstance(child_strategy, self.__class__.__bases__):
            # Establish the relationship
            child_strategy.add_parent(self.strategy_id) # all orders are now routed through the parent
            # Create the child positions
            child_strategy_id = child_strategy.strategy_id
            # Add a position for the child strategy
            self.create_position(symbol, asset_class = c.STRATEGY)
            self.children[symbol] = child_strategy_id
            openpositionsummary = child_strategy.get_openpositionsummary(ext = True)
            self.load_openpositions(openpositionsummary)

        elif isinstance(child_strategy, dict):
            # Get info
            child_strategy_id = child_strategy[c.STRATEGY_ID]
            openpositionsummary = child_strategy['openpositionsummary']
            # Add a position for the child strategy
            self.create_position(symbol, asset_class = c.STRATEGY)
            self.children[symbol] = child_strategy_id
            self.load_openpositions(openpositionsummary)
            # child must `add_parent` to finish establishing the relationship
            self.to_child(symbol, {c.TOPIC: 'add_parent', c.MESSAGE: 'add_parent', c.KWARGS: self.strategy_id})
            warnings.warn('Verify that the child as called `add_parent` to confirm relationship is established.')

        elif isinstance(child_strategy, str):
            self.children[symbol] = child_strategy
            # remotely add the child
            self.to_child(symbol, {c.TOPIC: 'add_parent', c.MESSAGE: 'add_parent', c.KWARGS: self.strategy_id})
            self.to_child(symbol, {c.TOPIC: 'openpositionsummary', c.MESSAGE: 'get_openpositionsummary'})
            # Check for a cash position to verify that the child's positions have been loaded.
            child_positions_loaded = False
            for i in range(5):
                child_cash_positions = list(filter(lambda x: (x.owner == symbol) and (x.asset_class == c.CASH), self.positions.values()))
                if len(child_cash_positions) > 0:
                    child_positions_loaded = True
                else:
                    time.sleep(1)
            if not child_positions_loaded:
                raise Exception('Unable to load child positions')
        return

    def add_cash(self, amount):
        """Adds cash.

        Cash is added by creating a "filled" order event (and hence the transaction will not have any 'REQUESTED' counterparts).
        The shares are diluted so that the mark of the strategy is not changed by adding cash.

        Args:
            amount (numeric): The net desired change in cash to self. Pass a negative amount to indicate withdrawal of cash.
                The amount is automatically converted to decimal.
        """
        amount = decimal.Decimal(amount)
        nav_0 = self.get_nav()

        cash_order = OrderEvent.cash_order(
            symbol = self.name,
            notional = amount,
            event_ts = self.clock
        )
        # Need to flip credit and debit since OrderEvent.cash_order is initialized
        # to be external facing
        cash_order[c.CREDIT], cash_order[c.DEBIT] = cash_order[c.DEBIT], cash_order[c.CREDIT]
        _cash_order, _ = self.prepare_order(cash_order)

        self.cash._handle_event(_cash_order)
        

        # dilute shares so that the mark of the position is not changed by adding cash
        if self.shares is None:
            self.shares = self.cash.investment
        else:
            self.shares = self.shares * (self.get_nav() + amount) / nav_0

        return

    # ----------------------------------------------------------------------------------------------------
    # Business Logic
    # ----------------------------------------------------------------------------------------------------


    def _process_data(self, data):
        """Updates lines in self.datas with data event."""
        try:
            symbol = data[c.SYMBOL]
        except KeyError:
            # TODO:
            raise
            # log error: data event not well formed
        # If a line exists, update it
        if self.datas.get(symbol) is not None:
            self.datas[symbol].update_with_data(data)
        
        return super()._process_data(data)

    def _process_order(self, order):
        """Processes an order event.

        Two types of order events can happen here:
        1) A "pass through"
            - The length of the strategy chain is non-zero
            - The order is from one of the children, and subtype == REQUESTED.
                the order is assessed by the Strategy's RMS.
                If approved, the order is sent upchain (to the Strategy's parent),
                or if there are no parents, sent to the broker.
                If the Strategy's RMS rejects the order, an ORDER event with event_subtype REJECTED
                is published to the child that requested that order.
            -
        2) Result of a prior order sent
            - The strategy chain of the order has reached length 0
            - The cash of the strategy is updated accordingly
            - The corrsponding position is updated

        This will be called 3*(#levels - 1) for each order, once for each of request, submitted, and filled.
        We should probably make this faster.
        """
        event_subtype = order.get(c.EVENT_SUBTYPE)

        # order on the way going "up"
        if event_subtype == c.REQUESTED:
            # receiving a REQUESTED order; implies that it is from one of the children
            # call RMS
            approved = self.rms.request_order_approval(order)
            if approved:
                self.place_order(order)
                return
            else:
                self.deny_order(order)
                return
        # order on the way going "down"
        else:
            # update positions
            self.positions_handle_order(order)
            # pass it down if this strategy is not the final destination
            order = self.format_strategy_chain(order, c.DOWN)
            if len(order[c.STRATEGY_CHAIN]) != 0:
                original_sender = order[c.STRATEGY_CHAIN].pop()
                self.to_child(original_sender, order)
        
        return super()._process_order(order)

    def _process_communication(self, communication):
        """Processes a communication event.

        If the communication is an attribute, respond to the sender with the value of the attribute.
        If the communication is a method of the strategy, call the method and respond to the sender
            with any returned values from the method.
        If the communication is a response from a prior communication, actions must be specified
            (e.g. openpositionsummary).

        Args:
            communication (event): The communication event.
        """
        if communication[c.EVENT_SUBTYPE] == c.REQUEST:

            answer = getattr(self, communication[c.MESSAGE])
            if hasattr(answer, '__call__'):
                if communication.get(c.KWARGS) is not None:
                    answer = answer(**communication[c.KWARGS])
                else:
                    answer = answer()
            # return the answer if there is one
            if answer is None:
                pass
            else:
                wrapped_answer = Event.communication_event({c.EVENT_SUBTYPE: c.INFO, c.SYMBOL: self.strategy_id})
                wrapped_answer.update({c.MESSAGE: {communication[c.MESSAGE]: answer}})
                return self.to_parent(wrapped_answer)

        elif communication[c.EVENT_SUBTYPE] == c.INFO:
            # handling info
            info = communication[c.MESSAGE]
            # current open positions
            if communication[c.TOPIC] == 'openpositionsummary':
                self.load_openpositions(info)
        else:
            pass
        
        return super()._process_communication(communication)
    
    
    """
    ----------------------------------------------------------------------------------------------------
    Info things
    ----------------------------------------------------------------------------------------------------
    """
    def get(self, identifier, obj_type = None):
        """ `get`s an attribute from the strategy by the identifier.

            The identifier could be a property/method name, a symbol, or an attribute or method name
            of the Strategy.
            If obj_type is not specified, the `get` method finds matches in the following order:
                - Data
                - Position
                - Property or method

        Args:
            identifier (str): The name of the object or an identifier of the object. Can be one of the following:
                - symbol of a dataline
                - position_id of a position
                - symbol of a position
                - name of a property/method
            obj_type (str, optional): One of ['DATA', 'POSITION', 'PROPERTY', 'METHOD'] or None.
                Specifices the type of the object the user is looking for. If obj_type is None,
                the `get` method finds matches in the following order:
                    - Data
                    - Position
                    - Property or method

        """
        obj_type = (obj_type or '').upper()

        # if looking for datalines
        if (obj_type == c.DATA) or (obj_type == ''):
            if (obj := self.datas.get(identifier)) is not None:
                return obj
        # if looking for a position
        if (obj_type == c.POSITION) or (obj_type == ''):
            if (position := self.positions.get(identifier)) is not None:
                return position
            else:
                # try to get positions with matching symbols
                positions = list(filter(lambda x: (x.symbol == identifier) or (x.owner == identifier), self.positions.values()))
                if len(positions) > 0:
                    return positions
        # Otherwise see if there is a property/method with name identifier.
        if (obj_type in ['PROPERTY', 'METHOD']) or (obj_type == ''):
            try:
                answer = getattr(self, identifier)
            except AttributeError:
                try:
                    # Try not to use this as this is 20x slower
                    key = [x for x in self.__dir__() if x.upper() == identifier.upper()][0]
                    answer = getattr(self, key)
                except IndexError:
                    raise Exception(f'No attribute by the name of {identifier}')
            return answer

        return

    def get_nav(self, identifier = None):
        """Returns the net asset value of an asset.

        Args:
            identifier (str, optional): Identifier of the asset. Can be a symbol
                of a non-strategy asset, or the symbol of a child strategy. If None,
                returns the NAV of the current Strategy. Defaults to None.

        Returns:
            The net asset value of the asset.
        """
        nav = 0
        # Get values of all non-strategy open positions of the strategy.
        # This will include all assets owned by any child strategies.
        if identifier is None:
            for position in filter(lambda x: ~x.is_closed, self.positions.values()):
                if position.asset_class != c.STRATEGY:
                    if position.asset_class == c.CASH:
                        mark = decimal.Decimal('1.0')
                    else:
                        mark = self.get_mark(position.symbol)
                    nav +=  mark * position.quantity_open

        else:
            # otherwise the identifier is one of two things:
            # either a child strategy, or a non-strategy asset.
            # If it is a strategy, sum up the values of all assets under that strategy,
            # if it is a non-strategy asset, sum up the values of all positions under the specified symbol.
            for position in filter(lambda x: ~x.is_closed, self.positions.values()):
                if (position.owner == identifier) or ((position.symbol == identifier) and (position.asset_class != c.STRATEGY)): # ??
                    if position.asset_class == c.CASH:
                        mark = decimal.Decimal('1.0')
                    else:
                        mark = self.get_mark(position.symbol)
                    nav +=  mark * position.quantity_open
        return nav

    def get_ncf(self, identifier = None):
        """Returns the net cash flow caused  by a position/strategy.

        Args:
            identifier (str, optional): Identifier of the position. If None,
                returns the net cash flow caused by the current strategy (w.r.t.
                to the account holding the strategy).
        """
        identifier = identifier or self.name

        ncf = 0
        for position in filter(lambda x: ~x.is_closed, self.positions.values()):
            if (position.owner == identifier) or (position.symbol == identifier):
                ncf += (position.credit - position.debit)
        return ncf

    def get_mark(self, identifier = None):
        """Returns the mark of an asset.

        Args:
            asset (str): the name / identifier of the asset

        Returns:
            decimal.Decimal: The last known value of the asset
        """
        if identifier in self.children.keys():
            identifier = self.children[identifier]
        # If this is something whose value we already track
        if (data := self.datas.get(identifier)) is not None:
            return data.mark[-1]
        elif identifier == c.CASH:
            return decimal.Decimal(1.00)
        else:
            # Get NAV
            nav = self.get_nav(identifier)
            # Get number of shares
            if identifier is None:
                assert self.shares is not None, \
                    'Number of shares not defined. Is there any cash in the strategy?'
                shares = self.shares
            else:
                position = None
                for position_i in filter(lambda x: ~x.is_closed, self.positions.values()):
                    if position_i.symbol == identifier:
                        position = position_i
                        break
                assert position is not None, f'No positions with symbol {identifier}.'
                shares = position.quantity_open

            mark = nav / shares

            return mark


    def get_totalpnl(self, identifier = None):
        """Returns realized + unrealized PNL.

        Returns credit - debit + open value.
        Realized and unrealized PNL are not available separately
        because they depend on accounting methods.

        Args:
            asset (str): the name / identifier of the asset. If None,
                returns the total pnl of the current strategy. Defaults to None.

        Returns:
            decimal.Decimal: Total cash flow + net asset value.
        """
        if identifier == c.CASH:
            return 0.0
        else:
            return self.get_ncf(identifier) + self.get_nav(identifier)

    def get_openpositionsummary(self, ext = False):
        """Summarize current positions on the owner/symbol level"""
        d = {}
        for position in filter(lambda x: ~x.is_closed, self.positions.values()):
            # first get position summary
            summary = position.get_summary()
            # convert names to strategy_ids if needed
            if ext:
                summary['owner'] = self.externalize_item(summary['owner'])
                summary['symbol'] = self.externalize_item(summary['symbol'])
            owner = summary['owner']
            symbol = summary['symbol']
            # start a new item if this is a new owner
            if owner not in d.keys():
                d[owner] = {}
                d[owner][symbol] = summary
            # start a new item if this is a new symbol for the owner
            elif symbol not in d[owner].keys():
                d[owner][symbol] = summary
            # else update quantities
            else:
                for quantity in ['risk', 'quantity_open', 'quantity_pending',
                                    'commission', 'credit', 'debit']:
                    try:
                        d[owner][symbol][quantity] += summary[quantity]
                    except:
                        pass

        return d

    def load_openpositions(self, openpositionsummary: dict):
        """Create open positions from a dicionary of open position summary"""
        for owner_summary in openpositionsummary.values():
            for owner_symbol_summary in owner_summary.values():
                owner_symbol_summary['owner'] = self.internalize_item(owner_symbol_summary['owner'])
                owner_symbol_summary['symbol'] = self.internalize_item(owner_symbol_summary['symbol'])
                # Create a dummy position
                if owner_symbol_summary['asset_class'] == c.CASH:
                    position = CashPosition(owner = owner_symbol_summary['owner'])
                else:
                    position = Position(
                        symbol = owner_symbol_summary['symbol'],
                        owner = owner_symbol_summary['owner'],
                        asset_class = owner_symbol_summary['asset_class'])
                # update the dummy position with details
                position.__dict__.update(owner_symbol_summary)
                self.positions[position.position_id] = position


    #TODO
    # def value_open(self, identifier = None):
    #     """
    #     This is the sum of values of current open positions, EXCLUDING CASH.
    #     Value is calculated using self.get_mark.

    #     Args:
    #         identifier (str, optional): Could be a symbol, trade_id, or position_id.
    #             If None, returns the value of all open positions in the strategy. Defaults to None.

    #     Returns:
    #         value: the open value of the requested item.
    #     """
    #     if identifier is None:
    #         # sum value of all open positions
    #         v = sum([position.value_open for position_id, position in self.open_positions.items()])
    #     else:
    #         # sum value of all open positions having any of (trade_id, position_id, symbol) matching identifier
    #         v = sum([position.value_open for position_id, position in self.open_positions.items()
    #                     if identifier in (position_id, position.trade_id, position.symbol)])
    #     return v

    # def value_pending(self, identifier = None):
    #     """
    #     This is the sum of pending values of current open positions.
    #     Value is calculated using self.get_mark.

    #     Args:
    #         identifier (str, optional): Could be a symbol, trade_id, or position_id.
    #             If None, returns the value of all open positions in the strategy. Defaults to None.

    #     Returns:
    #         value: the open value of the requested item.
    #     """
    #     if identifier is None:
    #         # sum value of all open positions
    #         v = sum([position.value_pending for position_id, position in self.open_positions.items()])
    #     else:
    #         # sum value of all open positions having any of (trade_id, position_id, symbol) matching identifier
    #         v = sum([position.value_pending for position_id, position in self.open_positions.items()
    #                     if identifier in (position_id, position.trade_id, position.symbol)])
    #     return v



    def to_child(self, child, message):
        """Sends a message to a child.

        Args:
            child (str or bytes): The identifier of the child. Can be one of the following:
                - Child symbol as string
                - Child strategy_id as string
                - Child strategy_id as bytes
                To broadcast the message to all children, specify child = ''.
            message (str or dict): If a string is provided, the message will be sent in the
                'MESSAGE' field of the communication event with the default parameters.
                If a dict is provided, the message will be padded with the default communication
                event parameters.
        """
        # convenient method to broadcast message to all children
        if child == '':
            for child_id in self.children.values():
                self.to_child(child_id, message)
            return

        # Who to send to?
        # if child is specified by name (symbol), get the strategy_id, as it is what the child is subscribed to
        if child in self.children.keys():
            child_id = self.children[child]
        # else assume it is the strategy_id of the child
        elif isinstance(child, str):
            child_id= child
        else:
            assert isinstance(child, bytes), 'Child must be specified by str of bytes.'
            child_id = child

        # What to send?
        default_params = {c.EVENT_TYPE: c.COMMUNICATION, c.EVENT_SUBTYPE: c.REQUEST, c.EVENT_TS: self.clock}
        communication_params = {c.SENDER_ID: self.strategy_id, c.RECEIVER_ID: child_id}
        if isinstance(message, dict):
            # add default message info
            message = {**default_params, **message, **communication_params}
        elif isinstance(message, str):
            message = {**default_params, c.MESSAGE: message, **communication_params}
        self.communication_socket.send(utils.packb(message))
        return

    def to_parent(self, message):
        """Sends a message to the parent.

        Args:
            message (str or dict): If a string is provided, the message will be sent in the
                'MESSAGE' field of the communication event with the default parameters.
                If a dict is provided, the message will be padded with the default communication
                event parameters.
        """
        # default message details
        default_params = {c.EVENT_TYPE: c.COMMUNICATION, c.EVENT_SUBTYPE: c.INFO, c.EVENT_TS: self.clock}
        communication_params = {c.SENDER_ID: self.strategy_id, c.RECEIVER_ID: self.parent}
        if isinstance(message, dict):
            # add default message info
            message = {**default_params, **message, **communication_params}
        elif isinstance(message, str):
            message = {**default_params, c.MESSAGE: message, **communication_params}
        self.communication_socket.send(utils.packb(message))

        return

    def format_strategy_chain(self, event, direction):
        """Creates/adds to the trace of communication.
            If event is on the way up, add self to the end of the chain.
            If event is on the way down, remove self from the end of the chain.

        Args:
            event (dict-like): The event to be processed.
            direction (UP / DOWN): "UP" means towards the parent; "DOWN" means towards the children.
        """
        if c.STRATEGY_CHAIN not in event.keys():
            event[c.STRATEGY_CHAIN] = []
        if len(event[c.STRATEGY_CHAIN]) > 0:
            # make it so that the last item is not self
            last = event[c.STRATEGY_CHAIN].pop()
            if last != self.strategy_id:
                event[c.STRATEGY_CHAIN].append(last)
        if direction == c.UP:
            event[c.STRATEGY_CHAIN].append(self.strategy_id)
        
        return event

    # ----------------------------------------------------------------------------------------------------
    # Action stuff
    # ----------------------------------------------------------------------------------------------------
    def create_position(self, symbol, owner = None, asset_class = None, trade_id = None, position_id = None):
        """Creates a position and add it to self.positions.

        Args:
            symbol (str): The symbol of the asset for the position.
            owner (str, optional): The owner of the position. If None, defaults to self. Defaults to None.
            asset_class (str, optional): The asset class the asset belongs to. If None, defaults to equity. Defaults to None.
            trade_id (str, optional): A convenient way to keep track of trades. Defaults to None.
            position_id (str, optional): The position ID. If None, one is automatically generated by UUID. Defaults to None.

        Returns:
            strategy.position: The newly created position.
        """
        owner = owner or self.name
        if symbol in self.children.values():
            default_asset_class = c.STRATEGY
        else:
            default_asset_class = c.EQUITY
        asset_class = asset_class or default_asset_class

        position = Position(symbol = symbol, owner = owner, asset_class = asset_class, trade_id = trade_id, position_id = position_id)
        self.positions[position.position_id] = position
        return position

    def prepare_order(self, order, position = None):
        """Prepare an order with default arguments and create a position if none exists.

        Args:
            symbol (str): The symbol to create an order for.
            quantity (numeric): The quantity of the order.
            position (position.Position, optional): The position to associate this order with.
                If None, 1) the first position with matching symbol and owner will be used.
                If (1) is None, a new position will be created. Defaults to None.
            order_details (dict, optional): Other order details. Defaults to None.

        Returns:
            event.order_event: A properly formatted order associated with a position.
        """
        """Note that order does not impact strategy until it is actually placed"""

        order[c.OWNER] = order.get(c.OWNER) or self.name
        
        # internalize_order and externalize_order change the symbol and owners of the order
        _order = self.internalize_order(order)
        _owner = _order.get(c.OWNER)
        _symbol = _order[c.SYMBOL]
        
        # assign the order to a position
        if position is None:
            # If there is a position with the symbol already, use the latest one of the positions with the same symbol
            current_positions_in_symbol = list(filter(lambda pos: (~pos.is_closed) and (pos.symbol == _symbol) and (pos.owner == _owner), self.positions.values()))
            if len(current_positions_in_symbol) > 0:
                position = current_positions_in_symbol[-1]
            else:
                # else create a new position
                position = self.create_position(symbol = _symbol, owner = _owner)
        elif isinstance(position, dict):
            # If position details are supplied in a dict, create a new position with those details
            position = self.create_position(symbol = _symbol, owner = _owner, **position)
        elif isinstance(position, Position):
            pass
        else:
            raise Exception('position must be one of {None, dict, strategy.position.Position}')

        # add information to order if not already there
        # for orders from children, the position_id and trade_id will not match
        # the ones the ones that are used in the parent position,
        # since the ones that are used in the parent position are aggregated on
        # the owner/symbol level
        info = {
            c.OWNER: _owner,
            c.TRADE_ID: order.get(c.TRADE_ID) or position.trade_id,
            c.POSITION_ID: order.get(c.POSITION_ID) or position.position_id,
            c.ORDER_ID: order.get(c.ORDER_ID) or str(uuid.uuid1()),
            c.EVENT_TS: self.clock
        }

        order = {**_order, **info}
        _order = self.internalize_order(order)
        order = self.externalize_order(order)
        return _order, order

    def internalize_order(self, order):
        """ Returns a copy of the order for internal processing.
            Any child strategy_ids will be converted to the symbol under which
            the child is tracked.

        Args:
            order (event.order_event): order to internalize.
        """

        _order = copy.deepcopy(order)
        # change child strategy_ids to internal child name
        # symbol
        if (symbol := _order.get(c.SYMBOL)) in self.children.values():
            _order[c.SYMBOL] = [k for k, v in self.children.items() if v == symbol][0]
        # same for owner
        if (owner := _order.get(c.OWNER)) in self.children.values():
            _order[c.OWNER] = [k for k, v in self.children.items() if v == owner][0]
        
        # and put the strategy's name instead of the strategy_id
        if symbol == self.strategy_id:
            _order[c.SYMBOL] = self.name
        if owner == self.strategy_id:
            _order[c.OWNER] = self.name

        return _order

    def externalize_order(self, order):
        """ Returns a copy of the order for external processing.
            Any child symbols will be converted to the child's actual strategy_id.

        Args:
            order (event.order_event): order to externalize.
        """

        order = copy.deepcopy(order)
        # change internal child name to child strategy_ids
        # symbol
        if (symbol := order.get(c.SYMBOL)) in self.children.keys():
            order[c.SYMBOL] = self.children[symbol]
        # same for owner
        if (owner := order.get(c.OWNER)) in self.children.keys():
            order[c.OWNER] = self.children[owner]

        # and put the strategy's name instead of the strategy_id
        if symbol == self.name:
            order[c.SYMBOL] = self.strategy_id
        if owner == self.name:
            order[c.OWNER] = self.strategy_id
        return order
    
    def internalize_item(self, item):
        if item == self.strategy_id:
            return self.name
        elif item in self.children.values():
            return list(filter(lambda x: x[1] == item, self.children.items()))[0][0]
        else:
            return item
    
    def externalize_item(self, item):
        if item == self.name:
            return self.strategy_id
        elif item in self.children.keys():
            return self.children[item]
        else:
            return item

    def positions_handle_order(self, order):
        """Have the strategy's positions handle an order.

        Args:
            order (event.order_event): order to process.
        """
        _order = self.internalize_order(order)
        _owner = _order[c.OWNER]
        _symbol = _order[c.SYMBOL]
        _cash_for_self = (_symbol == self.name)
        _cash_for_child = (_symbol in self.children.keys())

        # If cash for self, update the cash position and we're done
        if _cash_for_self:
            # NAV before adding cash
            nav_0 = self.get_nav()
            # handle the cash flow
            self.cash._handle_event(_order)
            # negate the net amount to sender to get net amount to self
            amount = -(order[c.CREDIT] - order[c.DEBIT])
            # dilute shares so that the mark of the position is not changed by adding cash
            if self.shares is None:
                self.shares = self.cash.investment
            else:
                self.shares = self.shares * (self.get_nav() + amount) / nav_0
            return

        # Otherwise this is an order for other strategies/assets
        # if this strategy owns this order
        if _order[c.OWNER] == self.strategy_id:
            position = self.positions[_order[c.POSITION_ID]]
            position._handle_event(_order)
        # otherwise find the position that owns this order
        else:
            filter(lambda pos: ~pos.is_closed, self.positions.values())
            matching_positions = list(filter(lambda pos: (~pos.is_closed) and (pos.owner == _owner) and (pos.symbol == _symbol), self.positions.values()))
            if len(matching_positions) == 0:
                raise Exception(f'No position for {_symbol} by {_owner} found')
            else:
                position = matching_positions[0]
        # have the corresponding position handle the order
        position._handle_event(_order)

        # Also need to update the cash positions
        matching_cash_positions = list(filter(lambda pos: (~pos.is_closed) and (pos.owner == _owner) and (pos.asset_class == c.CASH), self.positions.values()))
        if len(matching_cash_positions) == 0:
            raise Exception(f'No cash position of {_owner} found')
        else:
            matching_cash_position = matching_cash_positions[0]
        # handle the order
        matching_cash_position._handle_event(_order)

        # If this is cash for the child, also update the child's cash position
        if _cash_for_child:
            # a cash flow event to a child
            # get receiver cash position
            receiver_cash_positions = list(filter(lambda pos: (~pos.is_closed) and (pos.owner == _symbol) and (pos.asset_class == c.CASH), self.positions.values()))
            if len(receiver_cash_positions) == 0:
                raise Exception(f'No cash position of {_symbol} found')
            else:
                receiver_cash_position = receiver_cash_positions[0]
            # handle the order
            receiver_cash_position._handle_event(_order)

        return True

    def place_order(self, order = None, symbol = None, quantity = None):
        """Places an order to a child, a parent, or a broker.
            Either order has to be specified as a dictionary, or symbol and quantity must be provided.

        Args:
            order (dict, optional): The order to place. Defaults to None.
            symbol (str, optional): The symbol of the order. Defaults to None.
            quantity (str, optional): The quantity to order. Defaults to None.
        """
        # CREATE THE ORDER
        assert not ((order is None) and ((symbol is None) or (quantity is None))), \
            'Either order has to be specified as a dictionary, or symbol and quantity must be provided.'

        # pad order with default args: This is to create a properly formatted order, and create a position if none exists yet.
        # Note that create_order automatically converts any child symbols to child strategy_ids
        if order is None:
            if symbol in self.children.keys():
                order = OrderEvent.cash_order(symbol, quantity)
            else:
                order = OrderEvent.market_order(symbol, quantity)
        _order, order = self.prepare_order(order, position = None)
        self.positions_handle_order(_order)
        # PLACE THE ORDER
        if order[c.SYMBOL] in self.children.values():
            order = self.format_strategy_chain(order, c.DOWN)
            self.to_child(order[c.SYMBOL], order)
        else:
            order = self.format_strategy_chain(order, c.UP)
            # send it "up"
            if self.parent is None:
                self.order_socket.send(utils.packb(order))
            else:
                self.to_parent(order)
        return

    def deny_order(self, order):
        """Denies the request to place an order.

        Sends the denied order to the child.
        """
        order = order.update(event_subtype = c.DENIED)
        order = self.format_strategy_chain(Event, c.DOWN)
        self.to_child(order[c.STRATEGY_CHAIN].pop(), order)

    # def liquidate(self, id_or_symbol = None, order_details = {c.ORDER_TYPE: c.MARKET}):
    #     """Liquidate a trade, position, or positions on the specified symbol.

    #     Args:
    #         id_or_symbol (str): trade_id, position_id, or symbol to liquidate.
    #             If None, liquidate all positions. Default None.

    #     Raises:
    #         Exception: No trade/position/child strategy found to liquidate

    #     Returns:
    #         True: order submitted
    #     """
    #     # If the trade/position is already closed, let the user know
    #     if id_or_symbol in self.closed_trades.keys():
    #         raise Exception('Trade already closed.')
    #     elif (id_or_symbol in self.closed_positions):
    #         raise Exception('Posititon already closed.')
    #     # If this is a child strategy, send them the note
    #     elif id_or_symbol in self.children.keys():
    #         self.to_child(id_or_symbol, c.LIQUIDATE)
    #     elif (id_or_symbol in [position.symbol for position in self.open_positions.values()]):
    #         for position in self.open_positions.values():
    #             if position.symbol == id_or_symbol:
    #                 closing_order = {c.QUANTITY: -position.quantity_open}
    #                 self.place_order(self.create_order(order_details = closing_order, position = position))
    #         for order in self.pending_orders.values():
    #             if order.get(c.SYMBOL) == id_or_symbol:
    #                 self.place_order(order.update({c.ORDER_TYPE: c.CANCELLATION}))
    #     else:
    #         raise Exception('No trade/position/child strategy found to liquidate.')

    def get_equity_curves(self, data = None, agg = None, use_float = False):
        """Returns the equity curve of assets under the strategy.

        The equity curves can be requested at one of three levels:
            - individual position (unique on position_id)
            - symbols (exposures under one asset are aggregated)
            - owner (exposures under one owner are aggregated)

        Args:
            data (pd.DataFrame, optional): If provided, used as the marks of the assets.
                The data should be provided as a dataframe, with timestamp (consistent with
                the strategy's internal format) as the index, and the symbol of assets as column
                headers. If None, self.datas will be used. Defaults to None.
            agg (str, optional): The level to aggregate to. If provided, must be one of ['OWNER', 'SYMBOL'].
                If None, each position's equity curve will be returned as separate columns. Defaults to None.
            use_float (bool, optional): Whether to convert the returned dataframe values to float. Defaults to False.

        Returns:
            pd.DataFrame: The equity curves of positions/symbols/strategies as columns of a dataframe, indexed by timestamp.
        """

        # If data is not provided, use self.datas
        if data is None:
        #     data = pd.concat([
        #         pd.DataFrame(data = {lines.symbol: lines.mark}, index = lines.index)
        #         # lines.as_pandas_df(columns = lines.mark_line).rename(columns = {lines.mark_line: lines.symbol})
        #         for lines in self.datas.values()
        #     ], axis = 1).sort_index()
        #     data = data.fillna(method = 'ffill').fillna(decimal.Decimal('0.0'))
        #     data[c.CASH] = decimal.Decimal('1.0')
            data = {lines.symbol: pd.Series(data = lines.mark, index = lines.index) for lines in self.datas.values()}
        else:
            # data should be a dataframe
            data = data.applymap(decimal.Decimal)

        # get position quantities
        eq_curves = {}
        for pos in self.positions.values():
            # skip strategies---we will aggregate the underlying posiitons directly
            if pos.asset_class != c.STRATEGY:
                quantities = pd.Series(data = pos.quantity_history[1], index = pos.quantity_history[0])
                quantities = quantities.groupby(level = 0).last()
                # Get data with corresponding symbol
                if pos.asset_class == c.CASH:
                    marks = decimal.Decimal('1.00')
                else:
                    try:
                        marks = data[pos.symbol]
                    except KeyError:
                        raise Exception(f'No data for {pos.symbol} provided.')
                    # Need to align with data's index and ffill the gaps
                    idx = set(quantities.index) | set(marks.index)
                    quantities = quantities.reindex(idx).sort_index()
                    quantities = quantities.fillna(method = 'ffill').fillna(decimal.Decimal('0.0'))
                    quantities = quantities.reindex(marks.index)
                eq_curve = quantities * marks

                name = f'{pos.owner}||||{pos.symbol}||||{pos.position_id}'
                # get last item if there are multiple items for a single timestamp
                eq_curves[name] = eq_curve.groupby(level = 0).last()
        
        df_eq_curves = pd.DataFrame(eq_curves)
        if agg is None:
            if use_float:
                df_eq_curves = df_eq_curves.astype(float)
            return df_eq_curves
        else:
            if agg == c.OWNER:
                n = 0
            elif agg == c.SYMBOL:
                n = 1
            else:
                raise NotImplementedError(f'Aggregation type {agg} not implemented.')

            # aggregate by owner/symbol
            agg_eq_curves = collections.defaultdict(pd.DataFrame)
            for name, eq_curve in eq_curves.items():
                agg_key = name.split('||||')[n]
                agg_eq_curves[agg_key] = pd.concat([agg_eq_curves[agg_key], eq_curve], axis = 1).fillna(method = 'ffill')

            # construct dataframe and return
            df_eq_curves = pd.DataFrame({k: v.sum(axis = 1) for k, v in agg_eq_curves.items()})
            if use_float:
                df_eq_curves = df_eq_curves.astype(float)
            return df_eq_curves

# ----------------------------------------------------------------------------------------------------
# Misc Classes and fucntions
# ----------------------------------------------------------------------------------------------------

class RMS(abc.ABC):
    """A risk management system that monitors risk and approves/denies order requests"""

    def request_order_approval(self, position = None, order = None):
        """approve or deny the request to place an order. Default True"""
        return True