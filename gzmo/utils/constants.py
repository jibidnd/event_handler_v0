"""Constants (strings) as variables.
Like enums but better.
"""

# scope
TRADE = 'TRADE'
POSITION = 'POSITION'

# sockets
DATA_SOCKET = 'DATA_SOCKET'
ORDER_SOCKET = 'ORDER_SOCKET'
COMMUNICATION_SOCKET = 'COMMUNICATION_SOCKET'

# event_subtype for orders
# order statuses
REQUESTED = 'REQUESTED'         # an order request is created for approval
DENIED = 'DENIED'               # an order request is denied
FAILED = 'FAILED'               # Order is attempted but is unsuccessful
SUBMITTED = 'SUBMITTED'         # order is successfully submitted to the broker
RECEIVED = 'RECEIVED'           # order is received by the broker
FILLED = 'FILLED'               # order is filled (complete)
PARTIALLY_FILLED = \
    'PARTIALLY_FILLED'          # order is filled (partial)
EXPIRED = 'EXPIRED'             # order is expired
CANCELLED = 'CANCELLED'         # order has been cancelled/replaced
REJECTED = 'REJECTED'           # order is rejected
INVALID = 'INVALID'             # order is invalid
PENDING_CANCEL = \
    'PENDING_CANCEL'            # order is pending cancel/replace
CASHFLOW = 'CASHFLOW'


# Order types
MARKET = 'MARKET'
LIMIT = 'LIMIT'
CANCELLATION = 'CANCELLATION'
TRAILING_STOP = 'TRAILING_STOP'


# Order classes
SIMPLE = 'SIMPLE'
OCO = 'OCO'
BRACKET = 'BRACKET'
OTO = 'OTO'



# event_types
DATA = 'DATA'
ORDER = 'ORDER'
COMMUNICATION = 'COMMUNICATION'

# event_subtype for data
TICK = 'TICK'
BAR = 'BAR'
QUOTE = 'QUOTE'
SIGNAL = 'SIGNAL'
INFO = 'INFO'

# Bar attributes
MULTIPLIER = 'MULTIPLIER'
RESOLUTION = 'RESOLUTION'
LEFT = 'LEFT'                   # Alignment
CENTER = 'CENTER'               # Alignment
RIGHT = 'RIGHT'                 # Alignment

# event_subtype for COMMUNICATION
INFO = 'INFO'
ACTION = 'ACTION'
REQUEST = 'REQUEST'

# COMMUNICATIONS to be handled
MESSAGE = 'MESSAGE'
KWARGS = 'KWARGS'
# TOPIC = 'TOPIC'

# Event directions
UP = 'UP'                       # towards the parents
DOWN = 'DOWN'                   # towards the children

# datalines
OPEN = 'OPEN'
HIGH = 'HIGH'
LOW = 'LOW'
CLOSE = 'CLOSE'
TOPIC = 'TOPIC'
VOLUME = 'VOLUME'

# Asset classes
ASSET_CLASS = 'ASSET_CLASS'
CASH = 'CASH'
EQUITY = 'EQUITY'
STRATEGY = 'STRATEGY'

"""
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
"""


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
RECEIVER_ID = 'RECEIVER_ID'
STRATEGY_CHAIN = 'STRATEGY_CHAIN'
STRATEGY_ADDRESS = 'STRATEGY_ADDRESS'

# Order keys
# from request
SYMBOL = 'SYMBOL'
ORDER_TYPE = 'ORDER_TYPE'
ORDER_CLASS = 'ORDER_CLASS'
EXTENDED_HOURS = 'EXTENDED_HOURS'
PRICE = 'PRICE'
LIMIT_PRICE = 'LIMIT_PRICE'
STOP_PRICE = 'STOP_PRICE'
TRAIL_PRICE = 'TRAIL_PRICE'
TRAIL_PERCENT = 'TRAIL_PERCENT'
TAKE_PROFIT = 'TAKE_PROFIT'
STOP_LOSS = 'STOP_LOSS'
QUANTITY = 'QUANTITY'
NOTIONAL = 'NOTIONAL'
BROKER = 'BROKER'
BROKER_ORDER_ID = 'BROKER_ORDER_ID'
MEMO = 'MEMO'
CREATED_AT = 'CREATED_AT'
UPDATED_AT = 'UPDATED_AT'
SUBMITTED_AT = 'SUBMITTED_AT'
# incremental
CREDIT = 'CREDIT'
DEBIT = 'DEBIT'
COMMISSION = 'COMMISSION'
QUANTITY_FILLED = 'QUANTITY_FILLED'
AVERAGE_FILL_PRICE = 'AVERAGE_FILL_PRICE'
OWNER = 'OWNER'
TIME_IN_FORCE = 'TIME_IN_FORCE'
# cumulative
# CREDIT_TOTAL = 'CREDIT_TOTAL'
# DEBIT_TOTAL = 'DEBIT_TOTAL'
# NET_TOTAL = 'NET_TOTAL'
# COMMISSION_TOTAL = 'COMMISSION_TOTAL'
# QUANTITY_OPEN = 'QUANTITY_OPEN'
# QUANTITY_FILLED_TOTAL = 'QUANTITY_FILLED_TOTAL'
# AVERAGE_PRICE_TOTAL = 'AVERAGE_PRICE_TOTAL'

# Order time in force values
GTC = 'GTC'     # Good till cancel
DAY = 'DAY'     # Day
MOO = 'MOO'     # Market on open
LOO = 'LOO'     # Limit on open
MOC = 'MOC'     # Market on close
LOC = 'LOC'     # Limit on close
FOK = 'FOK'     # Fill or kill
IOC = 'IOC'     # Immediate or cancel


# data attributes
# CONNECTION_TYPE = 'CONNECTION_TYPE'
# REST = 'REST'
# STREAM = 'STREAM'

DATA_TYPE = 'DATA_TYPE'
BAR = 'BAR'
QUOTE = 'QUOTE'
TICK = 'TICK'

DATA_ATTRIBUTES = 'DATA_ATTRIBUTES'
MULTIPLIER = 'MULTIPLIER'
RESOLUTION = 'RESOLUTION'
ALIGNMENT = 'ALIGNMENT'
START = 'START'
END = 'END'

# quotes
ASK_EXCHANGE = 'ASK_EXCHANGE'
ASK_PRICE = 'ASK_PRICE'
ASK_SIZE = 'ASK_SIZE'
BID_EXCHANGE = 'BID_EXCHANGE'
BID_PRICE = 'BID_PRICE'
BID_SIZE = 'BID_SIZE'
CONDITIONS = 'CONDITIONS'

# ticks
EXCHANGE = 'EXCHANGE'
PRICE = 'PRICE'
SIZE = 'SIZE'
CONDITIONS = 'CONDITIONS'
TRADE_ID = 'TRADE_ID'
TAPE = 'TAPE'

#======================================================================================================================
# Useful constants
#======================================================================================================================
HOURS_PER_DAY = 24.0
MINUTES_PER_HOUR = 60.0
SECONDS_PER_MINUTE = 60.0
MUSECONDS_PER_SECOND = 1e6
NANOSECONDS_PER_SECOND = 1e9
MINUTES_PER_DAY = MINUTES_PER_HOUR * HOURS_PER_DAY
SECONDS_PER_DAY = SECONDS_PER_MINUTE * MINUTES_PER_DAY
MUSECONDS_PER_DAY = MUSECONDS_PER_SECOND * SECONDS_PER_DAY
NANOSECONDS_PER_DAY = NANOSECONDS_PER_SECOND * SECONDS_PER_DAY
