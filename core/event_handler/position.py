'''Positions are exposures in assets (could also be a strategy)'''


import abc
import collections
import uuid
import decimal

from .. import constants as c
from .. import event_handler

import traceback


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
            symbol,
            owner,
            asset_class,
            trade_id = None,
            position_id = None
    ):
        self.owner = owner
        self.symbol = symbol
        self.asset_class = asset_class
        self.trade_id = trade_id or str(uuid.uuid1())
        self.position_id = position_id or str(uuid.uuid1())
        self.status = None
        self.risk = None
        self.quantity_open = decimal.Decimal('0.0')
        self.quantity_pending = decimal.Decimal('0.0')
        self.commission = decimal.Decimal('0.0')
        self.credit = decimal.Decimal('0.0') # net increases to cash (filled orders)
        self.debit = decimal.Decimal('0.0')  # net decreases to cash (filled orders)
        self.transactions = collections.deque(maxlen = None)
        self.quantity_history = (collections.deque(), collections.deque())
    
    def __str__(self):

        keys = ['symbol', 'position_id', 'quantity_open', 'quantity_pending']
        d = {}
        for key in keys:
            d.update({key: self.__dict__[key]})

        return repr(d)

    def _handle_data(self, data):
        return self.handle_data(data)

    def _handle_order(self, order):
        '''Handle order events'''

        # confirm that the order belongs to this position
        # assert order.get(c.STRATEGY_ID) == self.owner,\
        #     f'Order {order.get(c.ORDER_ID)} belongs to owner {order.get(c.STRATEGY_ID)}, not {self.owner}'
        assert order.get(c.SYMBOL) == self.symbol,\
            f'Order {order.get(c.ORDER_ID)} has symbol {order.get(c.SYMBOL)}, not {self.symbol}'
        
        for k in [c.QUANTITY, c.QUANTITY_FILLED, c.QUANTITY_OPEN, c.CREDIT, c.DEBIT, c.COMMISSION]:
            if k in order.keys():
                try:
                    order[k] = decimal.Decimal(order[k])
                except TypeError as e:
                    pass

        # log the transaction
        self.transactions.append(order)
        
        # Process order events initiated on the strategy's side
        event_subtype = order[c.EVENT_SUBTYPE]

        if event_subtype == c.REQUESTED:
            # Order has been sent for approval
            # Treat orders as submitted
            self.quantity_pending += order[c.QUANTITY]
        elif event_subtype == c.SUBMITTED:
            pass
        elif event_subtype == c.DENIED:
            # Order was not submitted, revert pending quantities
            self.quantity_pending -= order[c.QUANTITY]
        # Process order events initiated on the broker's side
        elif event_subtype == c.RECEIVED:
            # order are assumed to be received when submitted
            # A FAILED order event would indicate otherwise
            pass
        elif event_subtype in [c.FILLED, c.PARTIALLY_FILLED]:
            # Update open quantity
            self.quantity_open += order[c.QUANTITY_FILLED]
            self.credit += order[c.CREDIT]
            self.debit += order[c.DEBIT]
            self.commission += order[c.COMMISSION]
            # update pending quantity
            self.quantity_pending -= order[c.QUANTITY]
            # update quantity history
            self.quantity_history[0].append(order[c.EVENT_TS])
            self.quantity_history[1].append(self.quantity_open)
        elif event_subtype in [c.FAILED, c.EXPIRED, c.CANCELLED, c.REJECTED]:
            self.quantity_pending -= order[c.QUANTITY]
        # elif event_subtype in [c.CASHFLOW]:
        #     # Update open quantity
        #     self.quantity_open += order[c.QUANTITY_FILLED]
        #     self.credit += order[c.CREDIT]
        #     self.debit += order[c.DEBIT]
        
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
    
    def get_summary(self):
        d = {
            'owner': self.owner,
            'symbol': self.symbol,
            'asset_class': self.asset_class,
            'status': self.status,
            'risk': self.risk,
            'quantity_open': self.quantity_open,
            'quantity_pending': self.quantity_pending,
            'commission': self.commission,
            'credit': self.credit,
            'debit': self.debit
        }
        return d


class CashPosition(Position):
    """A position in cash. Handles order a little differently than other positions
    """
    def __init__(self, owner, low_bal = 100):
        super().__init__(symbol = c.CASH, owner = owner, asset_class = c.CASH)
        self.investment = 0
        self.low_bal = low_bal


    def _handle_order(self, order):
        '''Handle order events
            Note that credit and debits are handled a little differently for cash flow events.
            Cash injections to the strategy cause a DEBIT while withdrawals cause a CREDIT.
            This is as if credit and debit are with resepct to the account holding the strategy,
            while `net` is with respect to the strategy's own internal account.
        '''
        
        for k in [c.QUANTITY, c.QUANTITY_FILLED, c.QUANTITY_OPEN, c.CREDIT, c.DEBIT, c.COMMISSION]:
            if k in order.keys():
                try:
                    order[k] = decimal.Decimal(order[k])
                except TypeError as e:
                    pass

        # log the transaction
        self.transactions.append(order)
        event_subtype = order[c.EVENT_SUBTYPE]
        iscashflow = event_subtype == c.CASHFLOW
        isfromhere = (order[c.STRATEGY_ID] == self.owner)
        isforhere = (order[c.SYMBOL] == self.owner)

        # depending on order type this may or may not be exact
        pending_amount = -(order.get(c.QUANTITY) or decimal.Decimal(0.0)) * (order.get(c.PRICE) or decimal.Decimal(0.0))
        commission = order.get(c.COMMISSION) or decimal.Decimal(0.0)
        credit = order[c.CREDIT]
        debit = order[c.DEBIT]
        net = credit - debit
        

        # some special handling if it is a cashflow
        if iscashflow:
            if (not isfromhere) and isforhere:
                net = -net
                # flip signs
                credit = net if net > 0 else 0
                debit = -net if net < 0 else 0

            pending_amount = 0
            self.investment += (credit - debit)
            self.quantity_open += (credit - debit)
            self.credit += credit
            self.debit += debit
            
            return self.handle_order(order)
        
        # otherwise handle the order's impact to the cash account
        else:

            # handle the order based on subtype
            if event_subtype == c.REQUESTED:
                # make sure there is enough money
                available = self.quantity_open + self.quantity_pending
                if available + pending_amount < self.low_bal:
                    raise Exception(f'Attempting an order for  {pending_amount} but only {available} available.')
                # Order is being sent for approval
                # Treat orders as submitted
                self.quantity_pending += pending_amount    # approx fill price
            elif event_subtype == c.SUBMITTED:
                pass
            elif event_subtype == c.DENIED:
                # Order was not submitted, revert pending quantities
                self.quantity_pending -= pending_amount    # prior approx fill price
            # Process order events initiated on the broker's side
            elif event_subtype == c.RECEIVED:
                # order are assumed to be received when submitted
                # A FAILED order event would indicate otherwise
                pass
            elif event_subtype in [c.FILLED, c.PARTIALLY_FILLED]:
                self.quantity_pending -= pending_amount    # prior approx fill price
                self.quantity_open += (credit - debit)
                self.credit += credit
                self.debit += debit
                self.commission += commission
                # update quantity history
                self.quantity_history[0].append(order[c.EVENT_TS])
                self.quantity_history[1].append(self.quantity_open)
                
            elif event_subtype in [c.FAILED, c.EXPIRED, c.CANCELLED, c.REJECTED]:
                self.quantity_pending -= pending_amount    # prior approx fill price

            return self.handle_order(order)
        
    @property
    def total_pnl(self):
        return 0.0