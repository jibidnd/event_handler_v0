"""A Position describes the owner's exposure in an asset."""

import collections
import uuid
import decimal

from ..utils import constants as c
from .. import event_handler


class Position(event_handler.EventHandler):
    """A Position describes the owner's exposure in an asset.

    The position keeps track of quantities, credits, and debits. The position
    updates these figures by handling order events.

    Long positions are represented as positive quantities,
        while short positions as negative quantities.

    Orders that are sent for approval (REQUESTED)/ to the broker (SUBMITTED) are treated as submitted,
    and are reverted if a FAILED/DENIED/EXPIRED/REJECTED order event is received.

    Attributes:
        owner: The owner of this position.
        symbol: The symbol of the underlying asset.
        asset_class: The asset class of the underlying asset.
        trade_id: The trade that this position belongs to.
        position_id: The unique identifier for this position.
        is_closed: Boolean.
        risk: TODO.
        quantity_open: Open quantity in the asset in this position.
        # quantity_pending: Pending quantity in the asset in this position. TODO: estimate quantity for notional orders
        commission: Total commission paid on this position. Updated on fills.
        credit: Cumulative credit on this position. Updated on fills.
        debit: Cumulative debit on this position. Updated on fills.
        transactions: Record of all orders processed.
        quantity_history: Record of open quantities in the position's lifetime. Updated on fills.
    """

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
        self.is_closed = False
        self.risk = None
        self.quantity_open = decimal.Decimal('0.0')
        # self.quantity_pending = decimal.Decimal('0.0')
        self.commission = decimal.Decimal('0.0')
        self.credit = decimal.Decimal('0.0') # net increases to cash (filled orders)
        self.debit = decimal.Decimal('0.0')  # net decreases to cash (filled orders)
        self.open_orders = {}
        self.transactions = collections.deque(maxlen = None)
        self.quantity_history = (collections.deque(), collections.deque())
    
    def __str__(self):
        """Summary of the position as string representation.

        Returns:
            str: Summary of the position.
        """
        keys = ['owner', 'symbol', 'position_id', 'quantity_open']
        d = {}
        for key in keys:
            d.update({key: self.__dict__[key]})

        return repr(d)

    def _process_order(self, order):
        """Updates the position's attributes with the order.
        
        Orders that are sent for approval (REQUESTED)/ to the broker (SUBMITTED) are treated as submitted,
        and are reverted if a FAILED/DENIED/EXPIRED/REJECTED order event is received.
        
        Args:
            order (dict): Order event to be handled.
        """
        # for debugging
        assert order.get(c.SYMBOL) == self.symbol,\
            f'Order {order.get(c.ORDER_ID)} has symbol {order.get(c.SYMBOL)}, not {self.symbol}'
        
        # convert to decimal
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
            self.open_orders[order[c.ORDER_ID]] = order
        elif event_subtype == c.SUBMITTED:
            pass
        elif event_subtype in [c.DENIED, c.FAILED, c.EXPIRED, c.CANCELLED, c.REJECTED, c.INVALID]:
            self.open_orders.pop(order[c.ORDER_ID], None)
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
            if (open_order := self.open_orders.get(order[c.ORDER_ID])) is not None:
                order_filled = False
                if open_order.get(c.QUANTITY) is not None:
                    open_order[c.QUANTITY] -= order[c.QUANTITY]
                    order_filled = (open_order[c.QUANTITY] == 0.0)
                elif open_order.get(c.NOTIONAL) is not None:
                    open_order[c.NOTIONAL] += (order[c.CREDIT] - order[c.DEBIT])
                    order_filled = (open[c.NOTIONAL] == 0.0)
                else:
                    raise Exception('Why are both quantity and notional None???')
            # update quantity history
            self.quantity_history[0].append(order[c.EVENT_TS])
            self.quantity_history[1].append(self.quantity_open)
        else:
            raise Exception(f'Unexpected order event subtype: {event_subtype}')
        
        return self.process_order(order)

    @property
    def total_pnl(self):
        """Returns realized + unrealized PNL.
        
        Returns credit - debit + open value.
        Realized and unrealized PNL are not available separately
        because they depend on accounting methods.
        """
        return self.credit + self.value_open - self.debit
    
    def get_summary(self):
        """Returns a summary of the position as a dictionary.

        Returns:
            dict: Summary of the position.
        """        
        d = {
            'owner': self.owner,
            'symbol': self.symbol,
            'asset_class': self.asset_class,
            'is_closed': self.is_closed,
            'risk': self.risk,
            'quantity_open': self.quantity_open,
            'commission': self.commission,
            'credit': self.credit,
            'debit': self.debit
        }
        return d


class CashPosition(Position):
    """A position in cash. Handles order a little differently than other positions.
    
    In the event of a "regular" order (where the target is an asset like equity or other strategies),
    credits and debits the account as indicated in the order.

    In the event of a "cash" order via the strategy's `add_cash` method, credits and debits the
    account as indicated in the order. The position also keeps track of the total investment (total cash
    deposited).

    In the event of an incoming "cash" order (i.e. a cash injection from another strategy),
    signs of credit and debits are reversed since the quantities were originally specified with
    respect to the strategy that created the order.
    
    Attributes:
        investment: The total investment (net cash deposited) into the strategy.
        low_bal: Throws an Exception if any order would result in the cash account dipping
            below `low_bal`.
    """
    def __init__(self, owner, low_bal = 100):
        super().__init__(symbol = owner, owner = owner, asset_class = c.CASH)
        self.investment = 0
        self.low_bal = low_bal
        self.quantity_pending = decimal.Decimal('0.0')


    def _process_order(self, order):
        """Handled order events.

        See class docstring of handling of cash flows.
        """
        
        for k in [c.QUANTITY, c.QUANTITY_FILLED, c.QUANTITY_OPEN, c.CREDIT, c.DEBIT, c.COMMISSION]:
            if k in order.keys():
                try:
                    order[k] = decimal.Decimal(order[k])
                except TypeError as e:
                    pass

        # log the transaction
        self.transactions.append(order)
        event_subtype = order[c.EVENT_SUBTYPE]
        iscashflow = order[c.ORDER_TYPE] == c.CASHFLOW
        isfromhere = (order[c.OWNER] == self.owner)
        isforhere = (order[c.SYMBOL] == self.owner)

        # depending on order type this may or may not be exact
        if (notional := order.get(c.NOTIONAL)) is not None:
            pending_amount = -notional
        else: 
            pending_amount = -(order.get(c.QUANTITY) or decimal.Decimal(0.0)) * (order.get(c.PRICE) or decimal.Decimal(0.0))
        commission = order.get(c.COMMISSION) or decimal.Decimal(0.0)
        credit = order[c.CREDIT]
        debit = order[c.DEBIT]
        

        # some special handling if it is a cashflow
        if iscashflow:
            # increment the investment if the cash is for here
            if isforhere:
                self.investment += (credit - debit)
                # An order from `add_cash` would be for here but also from here
                if (not isfromhere):
                    credit, debit = debit, credit

            pending_amount = 0
            self.quantity_open += (credit - debit)
            self.credit += credit
            self.debit += debit

            # update quantity history
            self.quantity_history[0].append(order[c.EVENT_TS])
            self.quantity_history[1].append(self.quantity_open)
            
            return self.process_order(order)
        
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
                
            elif event_subtype in [c.FAILED, c.EXPIRED, c.CANCELLED, c.REJECTED, c.INVALID]:
                self.quantity_pending -= pending_amount    # prior approx fill price

            return self.process_order(order)
        
    @property
    def total_pnl(self):
        return 0.0
    
    def __str__(self):
        """Summary of the position as string representation.

        Returns:
            str: Summary of the position.
        """
        keys = ['owner', 'symbol', 'position_id', 'quantity_open', 'quantity_pending']
        d = {}
        for key in keys:
            d.update({key: self.__dict__[key]})

        return repr(d)