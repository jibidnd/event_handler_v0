'''Positions are exposures in assets (could also be a strategy)'''


import abc
import collections
import uuid

from .. import constants as c
from .. import event_handler


class Position(event_handler.EventHandler):
    '''
    A Position describes the exposure in an asset, within the scope that the Position is in.
    
    A Position can be in any asset class, including a `Strategy`.

    A Position is mostly intended to be like a dataclass, but with some capabilities to be udpated based on events.

    Filled orders are reflected in the `_open` attributes,
        and pending orders are reflected in the `_pending` attributes.

    Long positions are represented as positive quantities,
        while short positions as negative quantities.

    Orders that are sent for approval (REQUESTED)/ to the broker (SUBMITTED) are treated as submitted,
        and are reverted if a FAILED/DENIED/EXPIRED/REJECTED order event is received.
    
    If no trade_id is specified, defaults to most recent open trade. If there are no open trades, create a new one.
    '''

    def __init__(
            self,
            owner,
            symbol,
            asset_class = c.EQUITY,
            trade_id = None,
            position_id = None
    ):
        self.owner = owner
        self.symbol = symbol
        self.asset_class = asset_class
        self.trade_id = trade_id or ([str(uuid.uuid1())] + list(self.strategy.open_trades.keys()))[-1]
        self.position_id = position_id or str(uuid.uuid1())
        self.status = None
        self.risk = None
        self.quantity_open = 0
        self.quantity_pending = 0
        self.value_pending = 0
        self.commission = 0
        self.credit = 0 # net increases to cash (filled orders)
        self.debit = 0  # net decreases to cash (filled orders)
        self.transactions = collections.deque(maxlen = None)

    def _handle_data(self, data):
        return self.handle_data(data)

    def _handle_order(self, order):
        '''Handle order events'''

        # confirm that the order belongs to this position
        assert order.get(c.POSITION_ID) == self.position_id,\
            f'Order {order.get(c.ORDER_ID)} belongs to position {order.get(c.POSITION_ID)}, not {self.position_id}'

        # log the transaction
        self.transactions.append(order)        

        # Process order events initiated on the strategy's side
        event_subtype = order[c.EVENT_SUBTYPE]
        if event_subtype == c.REQUESTED:
            # Order has been sent for approval
            # Treat orders as submitted
            self.quantity_pending += order[c.QUANTITY]
            self.value_pending += order[c.QUANTITY] * order[c.PRICE]    # approx fill price
        elif event_subtype == c.SUBMITTED:
            pass
        elif event_subtype == c.DENIED:
            # Order was not submitted, revert pending quantities
            self.quantity_pending -= order[c.QUANTITY]
            self.value_pending -= order[c.QUANTITY] * order[c.PRICE]    # approx fill price
        # Process order events initiated on the broker's side
        elif event_subtype == c.RECEIVED:
            # order are assumed to be received when submitted
            # A FAILED order event would indicate otherwise
            pass
        elif event_subtype == c.FILLED:
            # Update open quantity
            self.quantity_open += order[c.QUANTITY]
            self.credit += order[c.CREDIT]
            self.debit += order[c.DEBIT]
            self.commission += order[c.COMMISSION]
            self.value_pending -= order[c.QUANTITY] * order[c.PRICE]    # approx fill price
            # update pending quantity
            self.quantity_pending -= order[c.QUANTITY]
        elif event_subtype in [c.FAILED, c.EXPIRED, c.CANCELLED, c.REJECTED]:
            self.quantity_pending -= order[c.QUANTITY]
            self.value_pending -= order[c.QUANTITY] * order[c.PRICE]    # approx fill price
        
        return self.handle_order(order)
    
    def _handle_command(self, command):
        return self.handle_command(command)

    @property
    def total_pnl(self):
        '''Realized + unrealized PNL'''
        return self.credit + self.value_open - self.debit

    # for if we want to add custom action (e.g. notify on fill)
    def handle_data(self, data):
        pass
    def handle_order(self, order):
        pass
    def handle_command(self, command):
        pass


class CashPosition(Position):
    """A position in cash. Handles order a little differently than other positions
    """
    def __init__(self):
        super().__init__()
        self.balance = 0        # updated on fills
        self.net_flow = 0       # the net deposit/withdrawals to this cash position
        self.value_pending = 0
        self.transactions = collections.deque(maxlen = None)

    def _handle_order(self, order):
        '''Handle order events'''

        # confirm that the order belongs to this position
        assert order.get(c.POSITION_ID) == self.position_id,\
            f'Order {order.get(c.ORDER_ID)} belongs to position {order.get(c.POSITION_ID)}, not {self.position_id}'

        # confirm that the order is a cashflow
        assert order[c.SYMBOL] == c.CASH, \
                f'Order {order.get(c.ORDER_ID)} is not a cashflow but is passed to a cash position {self.position_id}'

        # log the transaction
        self.transactions.append(order)

        if order[c.EVENT_SUBTYPE] in [c.FILLED, c.PARTIALLY_FILLED]:
            self.balance += order[c.NET]
            self.net_flow += order[c.NET]
        return