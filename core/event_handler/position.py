'''Positions are exposures in assets (could also be a strategy)'''


import abc
import collections

import constants as c
import event_handler



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
    '''

    def __init__(
            self,
            strategy,
            scope = None,
            strategy_id = None,
            trade_id = None,
            asset_type = None,
            asset_id = None,
            symbol = None
    ):
        self.strategy = strategy
        self.scope = scope
        self.strategy_id = strategy_id
        self.trade_id = trade_id
        self.asset_type = None
        self.asset_id = asset_id
        self.symbol = None
        self.status = None
        self.risk = None
        self.quantity_open = 0
        self.quantity_pending = 0
        self.commission = 0
        self.credit = 0 # net increases to cash (filled orders)
        self.debit = 0  # net decreases to cash (filled orders)
        self.transactions = collections.deque(maxlen = None)

    def handle_data(self, data):
        pass

    def handle_order(self, order):
        '''Handle order events that are generated from this strategy'''
        # log the transaction
        self.transactions.append(order)

        event_subtype = order[c.EVENT_SUBTYPE]

        # Process order events initiated on the strategy's side
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

    # read only properties are computed when requested
    @property
    def value_open(self):
        return self.quantity_open * self.strategy.get_mark(self.asset_id)
    
    @property
    def value_pending(self):
        return self.quantity_pending * self.strategy.get_mark(self.asset_id)

    @property
    def total_pnl(self):
        '''Realized + unrealized PNL'''
        return self.credit + self.value_open - self.debit


class CashPosition(event_handler.EventHandler):
    """A position in cash. Handles order a little differently than other positions
    """
    def __init__(self):
        self.balance = 0
        self.net_flow = 0    # the net deposit/withdrawals to this cash position
        self.value_pending = 0
        self.transactions = collections.deque(maxlen = None)
    
    def handle_data(self, data):
        pass

    def handle_order(self, order):
        '''Handle order events that are generated from this strategy'''
        # log the transaction
        self.transactions.append(order)

        is_cashflow = order[c.SYMBOL] == c.CASH    # Deposits/withdrawals are in the form of filled cash orders
        event_subtype = order[c.EVENT_SUBTYPE]
        
        if not is_cashflow:
        # if a regular order, update cash balance as appropriate
            # Process order events initiated on the strategy's side
            if event_subtype == c.REQUESTED:
                # Order has been sent for approval
                # Treat orders as submitted
                self.value_pending -= order[c.QUANTITY] * order[c.PRICE]    # approx fill price
            elif event_subtype == c.SUBMITTED:
                pass
            elif event_subtype == c.DENIED:
                # Order was not submitted, revert pending quantities
                self.value_pending += order[c.QUANTITY] * order[c.PRICE]    # approx fill price
            # Process order events initiated on the broker's side
            elif event_subtype == c.RECEIVED:
                # order are assumed to be received when submitted
                # A FAILED order event would indicate otherwise
                pass
            elif event_subtype == c.FILLED:
                # Update open quantity
                self.balance -= order[c.COMMISSION]
                self.balance += order[c.NET]
            elif event_subtype in [c.FAILED, c.EXPIRED, c.CANCELLED, c.REJECTED]:
                self.value_pending -= order[c.QUANTITY] * order[c.PRICE]    # approx fill price
        else:
        # else a cash flow event. Update balance and net_flow accordingly
            self.balance += order[c.NET]
            self.net_flow += order[c.NET]