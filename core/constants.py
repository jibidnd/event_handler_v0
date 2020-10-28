# CONSTANTS

# scope
STRATEGY = 'STRATEGY'
TRADE = 'TRADE'
POSITION = 'POSITION'

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
FILLED = 'FILLED'               # order is filled (complete or partial)
EXPIRED = 'EXPIRED'             # order is expired
CANCELLED = 'CANCELLED'         # order has been cancelled
REJECTED = 'REJECTED'           # order is rejected

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

# event_subtype for commands
INFO_REQUEST = 'INFO_REQUEST'

# command to be handled
REQUEST = 'REQUEST'
# ARGS = 'ARGS'
KWARGS = 'KWARGS'

# datalines
# OPEN = 'OPEN'
HIGH = 'HIGH'
LOW = 'LOW'
CLOSE = 'CLOSE'
PRICE = 'PRICE'

# Asset classes
# STRATEGY = 'STRATEGY'
CASH = 'CASH'
EQUITY = 'EQUITY'

# Strategy / Position attributes
NET_ASSET_VALUE = 'NET_ASSET_VALUE'
QUANTITY_OPEN = 'QUANTITY_OPEN'
QUANTITY_PENDING = 'QUANTITY_PENDING'
VALUE_OPEN = 'VALUE_OPEN'
VALUE_PENDING = 'VALUE_PENDING'

# General event keys:
EVENT_TYPE: 'EVENT_TYPE'
EVENT_SUBTYPE: 'EVENT_SUBTYPE'
EVENT_TS: 'EVENT_TS'
STRATEGY_ID: 'STRATEGY_ID'
TRADE_ID: 'TRADE_ID'
POSITION_ID: 'POSITION_ID'
ORDER_ID: 'ORDER_ID'
EVENT_ID: 'EVENT_ID'
# Order keys
SYMBOL: 'SYMBOL'
ORDER_TYPE: 'ORDER_TYPE'
PRICE: 'PRICE'
QUANTITY: 'QUANTITY'
CREDIT: 'CREDIT'
DEBIT: 'DEBIT'
NET: 'NET'
BROKER: 'BROKER'
COMMISSION: 'COMMISSION'