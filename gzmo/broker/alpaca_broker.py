"""Alpaca broker class."""

import requests
import urllib.parse

import zmq

from . import BaseBroker
from .. import utils
from ..utils import constants as c

class AlpacaBroker(BaseBroker):
    """Broker instance to connect to Alpaca.
    
    https://alpaca.markets/docs/api-documentation/api-v2/orders/
    """
    
    def __init__(self, name, auth = None, zmq_context = None, live = False):
        """Inits the broker.

        Args:
            fill_method (callable[[dict], [dict]]): A callable with signature (order, data) that returns any
                (partially or fully) filled order.
        """  
        super().__init__(name, auth, zmq_context)
        if live:
            self.auth = self.auth['alpaca_live']
            self.base_url = 'https://api.alpaca.markets/v2/'
        else:
            self.auth = self.auth['alpaca_paper']
            self.base_url = 'https://paper-api.alpaca.markets/v2/'

        self.endpoint = 'orders'
        self.url = self.base_url + self.endpoint

        # headers (auth)
        self.request_headers = {
            'APCA-API-KEY-ID': self.auth['APCA-API-KEY-ID'],
            'APCA-API-SECRET-KEY': self.auth['APCA-API-SECRET-KEY']
        }
    

        # possible keys: https://alpaca.markets/docs/api-documentation/api-v2/orders/
        # ['id', 'client_order_id',
        #     'created_at', 'updated_at', 'submitted_at', 'filled_at', 'expired_at',
        #     'canceled_at', 'failed_at', 'replaced_at', 'replaced_by', 'replaces',
        #     'asset_id', 'symbol', 'asset_class', 'qty', 'filled_qty', 'type', 'side',
        #     'time_in_force', 'limit_price', 'stop_price', 'filled_avg_price', 'status',
        #     'extended_hours', 'legs', 'trail_price', 'trail_percent', 'hwm']
        # (internal key, external key)
        self.key_mapping = [
            (c.EVENT_SUBTYPE, 'status'),
            (c.BROKER_ORDER_ID, 'id'),
            (c.ORDER_ID, 'client_order_id'),
            (c.SYMBOL, 'symbol'),
            (c.QUANTITY, 'qty'),
            (c.QUANTITY_FILLED, 'filled_qty'),
            (c.ORDER_TYPE, 'type'),
            (c.TIME_IN_FORCE, 'time_in_force'),
            (c.LIMIT_PRICE, 'limit_price'),
            (c.STOP_PRICE, 'stop_price'),
            (c.AVERAGE_FILL_PRICE, 'filled_avg_price'),
            (c.TRAIL_PRICE, 'trail_price'),
            (c.TRAIL_PERCENT, 'trail_percent'),
        ]

        self.key_mapping_in = {external: internal for internal, external in self.key_mapping}
        self.key_mapping_out = {internal: external for internal, external in self.key_mapping}

    def format_order_in(self, order):
        # Convert times to isoformat
        _order = utils.to_isoformat(order)
        # first convert keys
        _order = {(self.key_mapping_in.get(key) or key): value for key, value in _order.items()}
        
        # TODO: opg, cls time in force
        
        # Add a few fields
        _order[c.EVENT_TYPE] = c.ORDER
        _order[c.EVENT_TS] = _order.get(c.EVENT_TS) or _order.get('updated_at')


        if (status := _order[c.EVENT_SUBTYPE]) in \
                ['partially_filled', 'filled', 'cancelled', 'expired', 'pending_cancel', 'rejected']:
            _order[c.EVENT_SUBTYPE] = status.upper()
        elif status in \
                ['new', 'accepted', 'done_for_day', 'pending_new', 'accepted_for_bidding', 'calculated', 'stopped']:
            # stopped: "The order has been stopped, and a trade is guaranteed for the order,
            #   usually at a stated price or better, but has not yet occurred."
            _order[c.EVENT_SUBTYPE] = c.RECEIVED
        elif status in ['replaced']:
            _order[c.EVENT_SUBTYPE] = c.CANCELLED
        elif status in ['pending_replace']:
            _order[c.EVENT_SUBTYPE] = c.PENDING_CANCEL


        # format times

        return _order
    
    def format_order_out(self, _order):
        order = {(self.key_mapping_out.get(key) or key): value for key, value in _order.items()}
        if order['qty'] > 0:
            order['side'] = 'buy'
        else:
            order['side'] = 'sell'
        
        return order
    
    def _place_order(self, order):
        """Takes an externalized order and sends it to the broker."""
        response = requests.post(self.url, json = order, headers = self.request_headers)

        if response.status_code == 403:
            # Forbidden: Buying power or shares is not sufficient
            response = {
                **self.format_order_in(order),
                c.EVENT_SUBTYPE: c.REJECTED,
                c.MEMO: str(response.json()) if order.get(c.MEMO) is None else ';'.join([order[c.MEMO], str(response.json())])
            }
        elif response.status_code == 422:
            # Unprocessable: Input parameters are not recognized.
            response = {
                **self.format_order_in(order),
                c.EVENT_SUBTYPE: c.INVALID,
                c.MEMO: str(response.json()) if order.get(c.MEMO) is None else ';'.join([order[c.MEMO], str(response.json())])
            }
        else:
            response = response.json()

        if response is not None:
            self._handle_event(response)
        
        self.place_order(self, order)
        return
    
    def place_order(self, order):
        pass