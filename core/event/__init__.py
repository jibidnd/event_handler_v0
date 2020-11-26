'''
The event traffic controller is responsible for dispatching events, synchronizing them if necessary.

Instead of a central ETC, one can also have each strategy be its own ETC, so that it orders its own data, connect to broker(s), etc.
However having a central ETC to dispatch events is easier to manage, and strategies only have to connect to this central ETC.
'''


# Datahandler --> publishes data
# Orderhandler --> routes order to broker
# Fillhandler --> publishes fill event
# Commandhandler --> launch/pause strategies, change params, allocate capital


# We want the message to be self-describing and relatively quick. Cross-platform is a nice-to-have.
# Messages will be very small but sent in a continuous stream.
# This pretty much takes out protobuf, pickle, and parquet; and leaves us with JSON and msessagepack.
# Now it mostly comes down to speed vs human-readability (some benchmarks here: https://medium.com/@shmulikamar/python-serialization-benchmarks-8e5bb700530b)

# One other nice thing about messagepack is that it seems to support more types, even objects:https://github.com/msgpack/msgpack/blob/master/spec.md#types-extension-type
# However, it is not a python standard lib
# Overall, I think that messagepack's speed advantage outweighs the human-readability part (we don't have to read the message on the wire, anyways)

# Define events
# All events will be in the form of dictionary, since they will all just be data packets, and dicts are very fast
# Templates

import datetime
from .. import constants as c

def order_event(dict_order_details = {}):
    '''
    Create an order with default arguments.
    See constants.py for event_subtypes for orders.
    '''
    order = {
        c.EVENT_TYPE: c.ORDER,
        c.EVENT_SUBTYPE: c.REQUESTED,
        c.EVENT_TS: datetime.datetime.now(),
        c.SYMBOL: None,
        c.ORDER_TYPE: c.MARKET,
        c.PRICE: None,
        c.QUANTITY: None,
        c.CREDIT: None,
        c.DEBIT: None,
        c.NET: None,
        c.BROKER: None,
        c.STRATEGY_ID: None,
        c.TRADE_ID: None,
        c.ORDER_ID: None,
        c.EVENT_ID: None,
        c.COMMISSION: None
    }

    order.update(**dict_order_details)

    return order

def cashflow_order(amount, **dict_order_details):
    """Create an order event for deposit/withdrawal of cash.

    Args:
        amount (numeric): amount of cash to add (negative for withdrawals).
    """
    o = order_event({
        c.SYMBOL: c.CASH,
        c.CREDIT: max(amount, 0),
        c.DEBIT: min(amount, 0),
        c.NET: amount
        }).update(**dict_order_details)
    return o


def data_event(dict_data_details = {}):
    '''
    Create a data event with default arguments.
    See constants.py for event_subtypes for data.
    '''
    data = {
        c.EVENT_TYPE: c.DATA,
        c.EVENT_SUBTYPE: None,
        c.EVENT_TS: datetime.datetime.now(),
        c.SYMBOL: None
    }
    data.update(**dict_data_details)

    return data


# Command = {
    #  'EVENT_TYPE: 'COMMAND',
#     **action_details
# }