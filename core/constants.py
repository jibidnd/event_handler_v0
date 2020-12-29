# CONSTANTS

# scope
STRATEGY = 'STRATEGY'
TRADE = 'TRADE'
POSITION = 'POSITION'

# sockets
DATA_SOCKET = 'DATA_SOCKET'
ORDER_SOCKET = 'ORDER_SOCKET'
PARENT_SOCKET = 'PARENT_SOCKET'
CHILDREN_SOCKET = 'CHILDREN_SOCKET'

# statuses 
OPEN = 'OPEN'
CLOSED = 'CLOSED'
PENDING = 'PENDING'

# event_subtype for orders
# from strategy
REQUESTED = 'REQUESTED'         # an order request is created for approval
DENIED = 'DENIED'               # an order request is denied
SUBMITTED = 'SUBMITTED'         # order is successfully submitted to the broker
FAILED = 'FAILED'    # Order is submitted to the broker but no RECEIVED message was received

# from broker
RECEIVED = 'RECEIVED'           # order is received by the broker
FILLED = 'FILLED'               # order is filled (complete)
PARTIALLY_FILLED = \
    'PARTIALLY_FILLED'          # order is filled (partial)
EXPIRED = 'EXPIRED'             # order is expired
CANCELLED = 'CANCELLED'         # order has been cancelled
REJECTED = 'REJECTED'           # order is rejected
INVALID = 'INVALID'

# Order types
MARKET = 'MARKET'
LIMIT = 'LIMIT'
CANCELLATION = 'CANCELLATION'

# event_type
DATA = 'DATA'
ORDER = 'ORDER'
COMMAND = 'COMMAND'

# event_subtype for data
TICK = 'TICK'
BAR = 'BAR'
QUOTE = 'QUOTE'
SIGNAL = 'SIGNAL'
INFO = 'INFO'
# STRATEGY = 'STRATEGY'

# Bar attributes
MULTIPLIER = 'MULTIPLIER'
RESOLUTION = 'RESOLUTION'
LEFT = 'LEFT'
CENTER = 'CENTER'
RIGHT = 'RIGHT'

# event_subtype for commands
INFO = 'INFO'
ACTION = 'ACTION'

# command to be handled
REQUEST = 'REQUEST'
KWARGS = 'KWARGS'

# datalines
# OPEN = 'OPEN'
HIGH = 'HIGH'
LOW = 'LOW'
CLOSE = 'CLOSE'
PRICE = 'PRICE'
MARK = 'MARK'
TOPIC = 'TOPIC'

# Asset classes
# STRATEGY = 'STRATEGY'
ASSET_CLASS = 'ASSET_CLASS'
CASH = 'CASH'
EQUITY = 'EQUITY'
# STRATEGY = 'STRATEGY'

'''
general naming conventions
# <symbol>[optaional: _<int multiplier>][optional: _<str resolution following ISO 8601 duration>]
    https://www.cmegroup.com/education/courses/introduction-to-futures/understanding-contract-trading-codes.html
        - Y: Year
        - M: Month
        - W: Week
        - D: Day
        - h: hour
        - m: minute
        - s: second
        - ms: millisecond
        - us: microsecond
        - ns: nanosecond
    e.g.
        - AAPL
        - AAPL_1_ms
        - AAPL_m

options naming conventions:
    https://en.wikipedia.org/wiki/Option_naming_convention
    Symbol (max. 6 characters)
    Yr (YY)
    Mo (MM)
    Day (DD)
    Call or Put (C/P)
    Strike Price (#####.###) listed with five digits before the decimal and three digits following the decimal

futures naming coventions:
    https://www.cmegroup.com/education/courses/introduction-to-futures/understanding-contract-trading-codes.html
    <product code><month code><year code>
        January – F
        February - G
        March -H
        April -J
        May - K
        June - M
        July - N
        August - Q
        September -U
        October - V
        November -X
        December -Z
'''


# Strategy / Position attributes
NET_ASSET_VALUE = 'NET_ASSET_VALUE'
QUANTITY_OPEN = 'QUANTITY_OPEN'
QUANTITY_PENDING = 'QUANTITY_PENDING'
VALUE_OPEN = 'VALUE_OPEN'
VALUE_PENDING = 'VALUE_PENDING'

# General event keys:
EVENT_TYPE = 'EVENT_TYPE'
EVENT_SUBTYPE = 'EVENT_SUBTYPE'
EVENT_TS = 'EVENT_TS'
STRATEGY_ID = 'STRATEGY_ID'
TRADE_ID = 'TRADE_ID'
POSITION_ID = 'POSITION_ID'
ORDER_ID = 'ORDER_ID'
EVENT_ID = 'EVENT_ID'
SENDER_ID = 'SENDER_ID'
STRATEGY_ADDRESS = 'STRATEGY_ADDRESS'

# Order keys
# from request
SYMBOL = 'SYMBOL'
ORDER_TYPE = 'ORDER_TYPE'
PRICE = 'PRICE'
QUANTITY = 'QUANTITY'
BROKER = 'BROKER'
# incremental
CREDIT = 'CREDIT'
DEBIT = 'DEBIT'
NET = 'NET'
COMMISSION = 'COMMISSION'
QUANTITY_FILLED = 'QUANTITY_FILLED'
AVERAGE_PRICE = 'AVERAGE_PRICE'
# cumulative
CREDIT_TOTAL = 'CREDIT_TOTAL'
DEBIT_TOTAL = 'DEBIT_TOTAL'
NET_TOTAL = 'NET_TOTAL'
COMMISSION_TOTAL = 'COMMISSION_TOTAL'
QUANTITY_OPEN = 'QUANTITY_OPEN'
QUANTITY_FILLED_TOTAL = 'QUANTITY_FILLED_TOTAL'
AVERAGE_PRICE_TOTAL = 'AVERAGE_PRICE_TOTAL'

# session socket modes
ALL = 'ALL'
STRATEGIES_FULL = 'STRATEGIES_FULL'
STRATEGIES_ORDERSONLY = 'STRATEGIES_ORDERSONLY'
STRATEGIES_INTERNALONLY = 'STRATEGIES_INTERNALONLY'