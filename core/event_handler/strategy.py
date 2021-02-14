'''Strategies contain the business logic for making actions given events.'''


import abc
import collections
import uuid
import decimal
from collections import deque
import datetime
import pytz
import time
import threading
import copy

import json

import zmq

from .position import Position, CashPosition
from .. import constants as c
from .. import event_handler
from ..event_handler import event, lines
from .. import utils

class Strategy(event_handler.EventHandler):
    def __init__(self, name = None, zmq_context = None, communication_address = None,
                    data_address = None, order_address = None, logging_addresses = None,
                    data_subscriptions = None, params = None, rms = None, local_tz = 'America/New_York'):
        # initialization params
        self.name = name
        self.zmq_context = zmq_context or zmq.Context.instance()
        self.communication_address = communication_address
        self.data_address = data_address
        self.order_address = order_address
        self.logging_addresses = logging_addresses
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
        self.open_trades = {}               # trade_id: [position_ids]
        self.closed_trades = {}             # trade_id: [position_ids]
        self.open_positions = {}            # position_id: position object
        self.closed_positions = {}          # position_id: position object  # TODO: closed positions need to be moved to closed_positions from open_positions
        self.shares = None                  # if none specified, shares will be set to $invested when add_cash is called for the first time.
        self.cash = CashPosition(owner = self.strategy_id); self.open_positions[self.cash.position_id] = self.cash
        # TODO
        self.pending_orders = {}            # orders not yet filled: order_id: order_event

        # Keeping track of time: internally, time will be tracked as local time
        # For communication, (float) timestamps on the second resolution @ UTC will be used.
        self.clock = utils.unix2datetime(0.0)
        self.shutdown_flag = threading.Event()


        # Connection things
        self.communication_socket = None
        self.data_socket = None
        self.order_socket = None
        self.logging_socket = None

        if self.communication_address is not None:
            self.connect_communication_socket(communication_address)
        if self.data_address is not None:
            self.connect_data_socket(data_address)
        if self.order_address is not None:
            self.connect_order_socket(order_address)
        if self.logging_addresses is not None:
            for logging_address in logging_addresses:
                self.connect_logging_socket(logging_address)
        
    def __str__(self):

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
        # if new address, overwrite the current record
        self.communication_address = communication_address
        # Create a parent socket if none exists yet
        if not self.communication_socket:
            socket = self.zmq_context.socket(zmq.DEALER)
            socket.setsockopt(zmq.IDENTITY, self.strategy_id.encode())
            self.communication_socket = socket
        self.communication_socket.connect(communication_address)

    def connect_data_socket(self, data_address):
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
    
    def subscribe_to_data(self, topic, track = True, line_args = None, lazy = False):
        '''
            Topic subscriptions from the data socket.
            If `lazy`, store topic and establish lines (if specified), but do not
                subscribe to data on data socket. This is for when a subscription for
                data occurs before binding to a socket (e.g. we don't know what socket
                to bind to yet, like in the case of a session)
        '''
        if line_args is None:
            line_args = dict()

        # Keep a record
        # store topics as strings
        topic = str(topic)
        if topic not in self.data_subscriptions:
            self.data_subscriptions |= set([topic])
        
        # establish lines if desired
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
                    self.datas[line_args['symbol']] = lines(**line_args)
        
        # subscribe to data now if not lazy
        if not lazy:
            assert self.data_socket is not None, \
                'No data socket to subscribe data from.'
            self.data_socket.setsockopt(zmq.SUBSCRIBE, topic.encode())
        return

    def add_parent(self, parent_strategy = None, parent_strategy_id = None):
        """Add a child.
        
        Args:
            parent_strategy (Strategy or supports .get): if provided, will be used to get parent_strategy_id.
            parent_strategy_id (str): The strategy_id to identify the parent by. If None, must be provided by child_strategy.
        """
        # if child_strategy is provided, attempt to get these information
        if parent_strategy is not None:
            parent_strategy_id = parent_strategy.get(c.STRATEGY_ID)
        
        assert (parent_strategy_id) is not None, 'Need strategy_id of parent being added.'
        
        self.parent = parent_strategy_id

        # TODO: to have parent load open positions, do:
        # - child.add_parent
        # - parent.add_child
        # - child.to_parent(command({REQUEST: load_openpositions, MESSAGE: child.get_openpositionsummary}))
        # - (parent should handle communication)

        return

    def add_child(self, symbol, child_strategy = None, child_strategy_id = None, child_update_freq = datetime.timedelta(seconds = 1)):
        """Add a child.
        
        Args:
            symbol (str): name of the child. Must be unique in the strategy across the set of all symbols (i.e. including tickers).
            child_strategy (Strategy): if provided, will be used to get child_strategy_id and current open positions.
            child_strategy_id (str): The strategy_id to identify the (existing) child by. If None, must be provided by child_strategy.
            child_update_freq (datetime.timedelta): How often the child's mark should be updated.
                                                    To update constantly (each _prenext), set to <0.
        """
        # if child_strategy is provided, attempt to get these information
        if child_strategy is not None:
            child_strategy_id = child_strategy.get(c.STRATEGY_ID)

        assert (child_strategy_id) is not None, 'Need strategy_id of child being added.'
        
        # Add the child
        self.children[symbol] = child_strategy_id

        # Add a position for the child strategy
        self.create_position(symbol, asset_class = c.STRATEGY)

        # open position summary should be provided as either
        # - an element in dictionary if child_strategy is a dictionary
        # - get_openpositionsummary if child_strategy is a strategy
        # - via the communication channel (i.e. for a remote child add by strategy_id only, the communication channel must already be running)
        openpositionsummary = None
        try:
            openpositionsummary = child_strategy.get_openpositionsummary()
        except AttributeError:
            try:
                openpositionsummary = child_strategy.get('openpositionsummary')
            except:
                if self.communication_socket is not None:
                    self.to_child(symbol, {c.TOPIC: 'openpositionsummary', c.MESSAGE: 'get_openpositionsummary'})
                else:
                    raise Exception(f'Unable to get open position summary for child {symbol}')

        self.load_openpositions(openpositionsummary)

        return

    def add_cash(self, amount):
        '''Adds cash to the cash position by creating a "filled" order event.
            `amount` is net change of cash to self.
        '''
        nav_0 = self.get_nav()

        cash = {
            c.STRATEGY_ID: self.strategy_id,
            c.EVENT_TYPE: c.ORDER,
            c.EVENT_SUBTYPE: c.CASHFLOW,
            c.EVENT_TS: self.clock.timestamp(),
            c.SYMBOL: self.strategy_id,
            c.ASSET_CLASS: c.STRATEGY,
            c.CREDIT: amount if amount > 0 else 0,
            c.DEBIT: amount if amount < 0 else 0
        }
        self.cash._handle_event(cash)

        # dilute shares so that the mark of the position is not changed by adding cash
        if self.shares is None:
            self.shares = self.cash.investment
        else:
            self.shares = self.shares * (self.get_nav() + amount) / nav_0

        return
    # ----------------------------------------------------------------------------------------------------
    # Session stuff
    # ----------------------------------------------------------------------------------------------------
    
    def before_start(self):
        '''Things to be run before start'''
        pass

    def start(self):
        '''Initialization. Things to be run before starting to handle events.'''
        self.before_start()
        return
    
    def run(self, shutdown_flag = None):
        '''Main event loop. The session should call this.
        
            At any time, the strategy will have visibility of the next event from each socket:
                Data, order, communication; as stored in next_events.
                If any of these slots are empty, the strategy will make 1 attempt to receive data
                from the corresponding sockets.
            
                Then, the events in next_events will be sorted by event_ts, and the very next
                event will be handled.

                The logic then resets and loops forever.
        '''   
        # TODO: what if we lag behind in processing? how do we catch up?
        
        if shutdown_flag is None:
            shutdown_flag = self.shutdown_flag
            shutdown_flag.clear()

        # start the strategy
        self.start()

        # the event 'queue'
        next_events = {}

        while not shutdown_flag.is_set():

            # Attempt to fill the event queue for any slots that are empty
            for name, socket in zip([c.COMMUNICATION_SOCKET, c.DATA_SOCKET, c.ORDER_SOCKET],
                                    [self.communication_socket, self.data_socket, self.order_socket]):
                if socket is None: continue
                if next_events.get(name) is None:
                    try:
                        # Attempt to receive from each socket, but do not block
                        if socket.socket_type in [zmq.SUB, zmq.ROUTER]:
                            topic, event = socket.recv_multipart(zmq.NOBLOCK)
                        elif socket.socket_type in [zmq.DEALER]:
                            event = socket.recv(zmq.NOBLOCK)
                        else:
                            raise Exception(f'Unanticipated socket type {socket.socket_type}')
                        next_events[name] = self._preprocess_event(utils.unpackb(event))
                    except zmq.ZMQError as exc:
                        if exc.errno == zmq.EAGAIN:
                            # Nothing to grab
                            pass
                        else:
                            raise

                
            # Now we can sort the upcoming events and handle the next event
            if len(next_events) > 0:
                # Handle the socket with the next soonest event (by EVENT_TS)
                # take the first item (socket name) of the first item ((socket name, event)) of the sorted queue
                next_socket = sorted(next_events.items(), key = lambda x: x[1][c.EVENT_TS])[0][0]
                next_event = next_events.pop(next_socket)        # remove the event from next_events
                self._handle_event(next_event)

    def before_stop(self):
        '''Prior to exiting. Gives user a chance to wrap things up and exit clean. To be overridden.'''
        pass

    def stop(self):
        
        self.before_stop()

        self.shutdown_flag.set()

        time.sleep(1)

        # close sockets
        for socket in [self.communication_socket, self.data_socket, self.order_socket, self.logging_socket]:
            if (isinstance(socket, zmq.sugar.socket.Socket)) and ((socket is not None) and (~socket.closed)):
                socket.close(linger = 10)

        return

    # ----------------------------------------------------------------------------------------------------
    # Business logic go here
    # The private methods perform default actions (e.g. passing an order, etc)
    #   and calls the abstractmethods, which contain the business logic unique to the strategy.
    # ----------------------------------------------------------------------------------------------------

    def _prenext(self):
        '''Prior to processing the next event'''
        return self.prenext()

    def _preprocess_event(self, event):
        # preprocess received events prior to handling them

        # localize the event ts
        event_ts = event[c.EVENT_TS]
        if isinstance(event_ts, (int, float, decimal.Decimal)):
            event_ts = utils.unix2datetime(event_ts, to_tz = self.local_tz)
        elif isinstance(event_ts, datetime.datetime):
            event_ts = event_ts.astimezone(self.local_tz)
        event[c.EVENT_TS] = event_ts
        
        return self.preprocess_event(event)

    def _handle_event(self, event):

        # Run code that needs to be executed bofore handling any event
        self._prenext()

        # tick the clock if it has a larger timestamp than the current clock (not a late-arriving event)
        if (event_ts := event[c.EVENT_TS]) > self.clock.timestamp():
            self.clock = utils.unix2datetime(event_ts)
        return super()._handle_event(event)

    def _handle_data(self, data):
        '''Update lines in self.datas with data event'''
        try:
            symbol = data[c.SYMBOL]
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
        '''
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
        return self.handle_order(order)
        
    def _handle_communication(self, communication):
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
                wrapped_answer = event.communication_event({c.EVENT_SUBTYPE: c.INFO, c.SYMBOL: self.strategy_id})
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

        return

    def prenext(self):
        pass

    @abc.abstractmethod
    def handle_data(self, event):
        pass

    # @abc.abstractmethod
    def handle_order(self, order):
        pass

    # @abc.abstractmethod
    def handle_command(self, command_event):
        pass


    # ----------------------------------------------------------------------------------------------------
    # Info things
    # ----------------------------------------------------------------------------------------------------
    def get(self, identifier):
        """ `get`s an object from the strategy by the identifier.
            The identifier could be a property/method name, a symbol with a corresponding lines object,
            a position, or a trade.
            If an id is provided, the method will look in the following order: 
                self.open_trades, self.open_positions, self.closed_trades, self.closed_positions

        Args:
            identifier (str): The name of the object or an identifier of the object.
        """

        # Try getting it from datas
        # First try exact match
        if identifier in self.datas.keys():
            return self.datas[identifier]
        # Otherwise see if there are lines with names specified with resolution
        elif (keys := [key for key in self.datas.keys() if identifier in key]) !=  []:
            # return last added
            return self.datas[keys[-1]]
        # Next, try to get from trades and positions
        elif (trade_or_position := self.open_trades.get(identifier) \
                                    or self.open_positions.get(identifier) \
                                    or self.closed_trades.get(identifier) \
                                    or self.closed_positions.get(identifier)) is not None:
            return trade_or_position
        # Otherwise see if there is a property/method with name identifier.
        else:
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
    
    def get_nav(self, identifier = None):
        # Get net asset value of a strategy / asset
        nav = 0
        if identifier is None:
            for position in self.open_positions.values():
                if position.asset_class != c.STRATEGY:
                    if position.quantity_open != 0:
                        nav += self.get_mark(position.symbol) * position.quantity_open
            
        else:
            for position in self.open_positions.values():
                if (position.owner == identifier) or ((position.symbol == identifier) and (position.asset_class != c.STRATEGY)): # ??
                    if position.quantity_open != 0:
                        nav += self.get_mark(position.symbol) * position.quantity_open
        return nav
    
    def get_ncf(self, identifier = None):
        '''
            Get the net cash flow caused by a position/strategy.
            If identifier is None (default), gets the net cash flow caused
            by the strategy to the account holding the strategy.
        '''
        identifier = identifier or self.strategy_id
        
        ncf = 0
        for position in self.open_positions.values():
            if (position.owner == identifier) or (position.symbol == identifier):
                ncf += (position.credit - position.debit)
                # if position.symbol == c.CASH:
                #     ncf -= position.investment
        return ncf

    def get_mark(self, identifier = None):
        """Get the mark of an asset

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
                for position_i in self.open_positions.values():
                    if position_i.symbol == identifier:
                        position = position_i
                        break
                assert position is not None, f'No positions with symbol {identifier}.'
                shares = position.quantity_open
            
            mark = nav / shares

            return mark


    def get_totalpnl(self, identifier = None):

        if identifier == c.CASH:
            return 0.0
        else:
            return self.get_ncf(identifier) + self.get_nav(identifier)
    
    def get_openpositionsummary(self):
        '''summarize current positions on the owner/symbol level'''
        d = {}
        for position in self.open_positions.values():
            if position.owner not in d.keys():
                d[position.owner] = {}
                d[position.owner][position.symbol] = position.get_summary()
            elif position.symbol not in d[position.owner].keys():
                d[position.owner][position.symbol] = position.get_summary()
            else:
                d[position.owner][position.symbol]['risk'] += position.risk
                d[position.owner][position.symbol]['quantity_open'] += position.quantity_open
                d[position.owner][position.symbol]['quantity_pending'] += position.quantity_pending
                d[position.owner][position.symbol]['commission'] += position.commission
                d[position.owner][position.symbol]['credit'] += position.credit
                d[position.owner][position.symbol]['debit'] += position.debit
        return d
    
    def load_openpositions(self, openpositionsummary: dict):
        '''Create open positions from a dicionary of open position summary'''
        for owner_summary in openpositionsummary.values():
            for owner_symbol_summary in owner_summary.values():
                # convert owner to internal symbol if direct child
                if owner_symbol_summary['owner'] in self.children.values():
                    owner_symbol_summary['owner'] = [symbol for symbol, strategy_id in self.children.items() if strategy_id == owner_symbol_summary['owner']][0]
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
                self.open_positions[position.position_id] = position



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

    # def total_pnl(self, identifier = None):
    #     # TODO: identifier can be a trade or a symbol
    #     '''Realized + unrealized PNL'''
    #     if identifier is None:
    #         credit = sum([position.credit for position_id, position in self.open_positions.items()]) \
    #                     + sum([position.credit for position_id, position in self.closed_positions.items()])
    #         debit = sum([position.debit for position_id, position in self.open_positions.items()]) \
    #                     + sum([position.debit for position_id, position in self.closed_positions.items()]) 
    #     else:
    #         if identifier in self.open_positions.keys():
    #             credit = self.open_positions[identifier].credit
    #             debit = self.open_positions[identifier].debit
    #         elif identifier in self.closed_positions.keys():
    #             credit = self.closed_positions[identifier].credit
    #             debit = self.closed_positions[identifier].debit
    #         else:
    #             raise Exception('position not found')
    #     return credit + self.value_open - debit

    # @property
    # def net_asset_value(self):
    #     """
    #     This is the sum of cash and values of current open positions.
    #     """
    #     # pending orders do not affect cash balance
    #     nav = sum([position.value_open for position_id, position in self.open_positions.items()])
    #     nav += self.cash.balance
    #     return nav
    
    # @property
    # def mark(self):
    #     """NAV / total cash deposited"""
    #     if self.cash.net_flow != 0:
    #         return self.net_asset_value / self.cash.net_flow
    #     else:
    #         return self.net_asset_value

    def to_child(self, child, message):
        '''
            To send message to all children, simply specify child = ''
        '''

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
            direction (UP / DOWN): [description]
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
        
        owner = owner or self.strategy_id
        asset_class = asset_class or c.EQUITY

        position = Position(symbol = symbol, owner = owner, asset_class = asset_class, trade_id = trade_id, position_id = position_id)
        self.open_positions[position.position_id] = position
        return position

    def create_order(self, symbol, quantity, position = None, order_details = None):
        """Creates an order with the provided symbol and quantity.
            Other parameters are filled with defaults unless otherwise specified in order_details.

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
        '''Note that order does not impact strategy until it is actually placed'''

        order_details = order_details or {}
        
        _owner = order_details.get(c.STRATEGY_ID) or self.strategy_id
        if _owner in self.children.values():
            _owner = [k for k, v in self.children.items() if v == _owner][0]
        
        if position is None:
            # If there is a position with the symbol already, use the latest one of the positions with the same symbol
            current_positions_in_symbol = [pos for pos in self.open_positions.values() if pos.symbol == symbol and pos.owner == _owner]
            if len(current_positions_in_symbol) > 0:
                position = current_positions_in_symbol[-1]
            else:
                # else create a new position
                position = self.create_position(symbol = symbol, owner = _owner)
        elif isinstance(position, dict):
            # If position details are supplied in a dict, create a new position with those details
            position = self.create_position(strategy = self, symbol = symbol, **position)
        elif isinstance(position, Position):
            pass
        else:
            raise Exception('position must be one of {None, dict, gzmo.core.event_handler.position.Position}')
        

        if symbol in self.datas:
            default_price = self.get_mark(symbol)
        else:
            default_price = None
        
        default_order = {
            c.STRATEGY_ID: self.strategy_id,
            c.TRADE_ID: position.trade_id,
            c.POSITION_ID: position.position_id,
            c.ASSET_CLASS: c.EQUITY,
            c.ORDER_ID: str(uuid.uuid1()),
            c.SYMBOL: position.symbol,
            c.ORDER_TYPE: c.MARKET,
            c.QUANTITY: quantity,
            c.EVENT_TS: self.clock.timestamp(),
            c.PRICE: default_price,
            c.QUANTITY_OPEN: quantity
            }
        
        # if the target is a child, this is a cashflow. Change a few default arguments
        if symbol in self.children.keys() :
            symbol = self.children[symbol]
            default_order[c.SYMBOL] = symbol
            order_details.pop(c.SYMBOL)
        if symbol in self.children.values():
            default_order[c.ASSET_CLASS] = c.STRATEGY
            default_order[c.EVENT_SUBTYPE] = c.CASHFLOW
            default_order[c.CREDIT] = -quantity if quantity < 0 else 0
            default_order[c.DEBIT] = quantity if quantity > 0 else 0
            default_order[c.QUANTITY_FILLED] = default_order.pop(c.QUANTITY_OPEN)

        order = {**default_order, **order_details}
        
        return event.order_event(order)

    def internalize_order(self, order):
        """ Returns a copy of the order for internal processing.
            Any child strategy_ids will be converted to the symbol under which
            the child is tracked.

        Args:
            order (event.order_event): order to internalize.
        """
        
        _order = copy.deepcopy(order)
        if (symbol := _order.get(c.SYMBOL)) in self.children.values():
            _order[c.SYMBOL] = [k for k, v in self.children.items() if v == symbol][0]

        # same for owner
        if (owner := _order.get(c.STRATEGY_ID)) in self.children.values():
            _order[c.STRATEGY_ID] = [k for k, v in self.children.items() if v == owner][0]
        
        return _order

    def externalize_order(self, order):
        """ Returns a copy of the order for external processing.
            Any child symbols will be converted to the child's actual strategy_id.

        Args:
            order (event.order_event): order to externalize.
        """
        
        order = copy.deepcopy(order)
        if (symbol := order.get(c.SYMBOL)) in self.children.keys():
            order[c.SYMBOL] = self.children[symbol]

        # same for owner
        if (owner := order.get(c.STRATEGY_ID)) in self.children.keys():
            order[c.STRATEGY_ID] = self.children[owner]
        
        return order
    
    def positions_handle_order(self, order):
        """Have the strategy's positions handle an internalized order.

        Args:
            order (event.order_event): order to process.
        """
        _order = self.internalize_order(order)
        _owner = _order[c.STRATEGY_ID]
        _symbol = _order[c.SYMBOL]
        _cash_for_self = (_symbol == self.strategy_id)
        _cash_for_child = (_symbol in self.children.keys())

        if _cash_for_self:
            # NAV before adding cash
            nav_0 = self.get_nav()
            # handle the cash flow
            self.cash._handle_event(order)
            # negate the net amount to sender to get net amount to self
            amount = -(order[c.CREDIT] - order[c.DEBIT])
            # dilute shares so that the mark of the position is not changed by adding cash
            if self.shares is None:
                self.shares = self.cash.investment
            else:
                self.shares = self.shares * (self.get_nav() + amount) / nav_0
            return

        # a "regular" order. Update the corresponding symbol's position and cash position
        # find a corresponding position; otherwise create one
        if _order[c.STRATEGY_ID] == self.strategy_id:
            position = self.open_positions[_order[c.POSITION_ID]]
        else:
            matching_positions = [position for position in self.open_positions.values() if (position.owner == _owner) and (position.symbol == _symbol)]
            if len(matching_positions) == 0:
                raise Exception(f'No position for {_symbol} by {_owner} found')
            else:
                position = matching_positions[0]
        # have the corresponding position handle the order
        position._handle_event(_order)

        # also find a corresponding cash position
        matching_cash_positions = [position for position in self.open_positions.values() if (position.owner == _owner) and (position.asset_class == c.CASH)]
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
            receiver_cash_positions = [position for position in self.open_positions.values() if (position.owner == _symbol) and (position.asset_class == c.CASH)]
            if len(receiver_cash_positions) == 0:
                raise Exception(f'No cash position of {_symbol} found')
            else:
                receiver_cash_position = receiver_cash_positions[0]
            # handle the order
            receiver_cash_position._handle_event(_order)
        
        return True



    def place_order(self, order = None, symbol = None, quantity = None):
        """Places an order to a child, a parent, or the broker.
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
        order = order or {}
        symbol = symbol or order.get(c.SYMBOL)
        quantity = quantity or order.get(c.QUANTITY)
        order = self.create_order(symbol = symbol, quantity = quantity, order_details = order)
        order = self.externalize_order(order)
        self.positions_handle_order(order)
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
        '''
        Deny the request to place an order.
        Send the denial to the child.
        '''
        order = order.update(event_subtype = c.DENIED)
        order = self.format_strategy_chain(event, c.DOWN)
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



# ----------------------------------------------------------------------------------------------------
# Misc Classes and fucntions
# ----------------------------------------------------------------------------------------------------

class RMS(abc.ABC):
    '''A risk management system that monitors risk and approves/denies order requests'''

    def request_order_approval(self, position = None, order = None):
        '''approve or deny the request to place an order. Default True'''
        return True