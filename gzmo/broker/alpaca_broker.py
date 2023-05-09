"""Alpaca broker class."""

import requests
import urllib.parse
import json
import websocket
import threading

import zmq
import pandas as pd
import decimal

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
            self.ws_url = 'wss://api.alpaca.markets/stream'
        else:
            self.auth = self.auth['alpaca_paper']
            self.base_url = 'https://paper-api.alpaca.markets/v2/'
            self.ws_url = 'wss://paper-api.alpaca.markets/stream'

        self.endpoint = 'orders'
        self.url = self.base_url + self.endpoint

        # headers (auth)
        self.request_headers = {
            'APCA-API-KEY-ID': self.auth['APCA-API-KEY-ID'],
            'APCA-API-SECRET-KEY': self.auth['APCA-API-SECRET-KEY']
        }

        # Start a thread to listen to order updates
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open = lambda ws: self.on_open(ws),
            on_message = lambda ws, message: self.on_message(ws, message),
            on_error = lambda ws, error: print(error)
        )
        self.streaming_thread = threading.Thread(name = f'broker_{self.name}_ws', target = self.ws.run_forever, daemon = True)
        self.streaming_thread.start()
    

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
            (c.ASSET_CLASS, 'asset_class'),
            (c.CREATED_AT, 'created_at'),
            (c.EXTENDED_HOURS, 'extended_hours'),
            (c.NOTIONAL, 'notional'),
            (c.ORDER_TYPE, 'order_type'),
            (c.UPDATED_AT, 'updated_at'),
            (c.SYMBOL, 'symbol'),
            (c.QUANTITY, 'qty'),
            (c.QUANTITY_FILLED, 'filled_qty'),
            (c.ORDER_TYPE, 'type'),
            (c.ORDER_CLASS, 'order_class'),
            (c.TIME_IN_FORCE, 'time_in_force'),
            (c.LIMIT_PRICE, 'limit_price'),
            (c.STOP_PRICE, 'stop_price'),
            (c.AVERAGE_FILL_PRICE, 'filled_avg_price'),
            (c.TRAIL_PRICE, 'trail_price'),
            (c.TRAIL_PERCENT, 'trail_percent'),
        ]

        self.order_params = [
            'symbol',
            'qty',
            'notional',
            'side',
            'type',
            'time_in_force',
            'limit_price',
            'stop_price',
            'trail_price',
            'trail_percent',
            'extended_hours',
            'client_order_id',
            'order_class',
            'take_profit',
            'stop_loss'
        ]

        self.key_mapping_in = {external: internal for internal, external in self.key_mapping}
        self.key_mapping_out = {internal: external for internal, external in self.key_mapping}

    def format_order_in(self, order):
        # Convert times to timestamps
        _order = utils.to_timestamp(order)
        # first convert keys
        _order = {(self.key_mapping_in.get(key) or key): value for key, value in _order.items()}
        
        # TODO: opg, cls time in force
        
        # Add a few fields
        _order[c.EVENT_TYPE] = c.ORDER
        _order[c.EVENT_TS] = pd.Timestamp(_order.get(c.EVENT_TS) or _order.get('UPDATED_AT'))

        # add event subtype
        if (status := _order.get(c.EVENT_SUBTYPE)) is not None:
            status = status.lower()
            if status in \
                ['partially_filled', 'filled', 'cancelled', 'expired', 'pending_cancel', 'rejected']:
                _order[c.EVENT_SUBTYPE] = status.upper()
            elif status in \
                    ['new', 'accepted', 'done_for_day', 'pending_new', 'accepted_for_bidding', 'calculated', 'stopped', 'received']:
                # stopped: "The order has been stopped, and a trade is guaranteed for the order,
                #   usually at a stated price or better, but has not yet occurred."
                _order[c.EVENT_SUBTYPE] = c.RECEIVED
            elif status in ['replaced']:
                _order[c.EVENT_SUBTYPE] = c.CANCELLED
            elif status in ['pending_replace']:
                _order[c.EVENT_SUBTYPE] = c.PENDING_CANCEL
            else:
                _order[c.EVENT_SUBTYPE] = status.upper()
        
        # Credit and debit
        if _order.get('side') is not None:
            quantity_filled = decimal.Decimal(_order.get(c.QUANTITY_FILLED) or '0')
            average_fill_price = decimal.Decimal(_order.get(c.AVERAGE_FILL_PRICE) or '0')
            amount = quantity_filled * average_fill_price
            if _order['side'] == 'buy':
                _order[c.CREDIT] = 0
                _order[c.DEBIT] = amount
            else:
                _order[c.CREDIT] = amount
                _order[c.DEBIT] = 0

        # for key in [c.ORDER_TYPE, c.ORDER_CLASS, c.TIME_IN_FORCE]:
        #     _order[key] = _order[key].upper()


        return _order
    
    def format_order_out(self, _order):
        order = {(self.key_mapping_out.get(key) or key): value for key, value in _order.items()}
        order = {k: v for k, v in order.items() if k in self.order_params}

        if float(order['qty']) > 0:
            order['side'] = 'buy'
        else:
            order['side'] = 'sell'
            order['qty'] = -order['qty']
        
        for key in ('type', 'order_class', 'time_in_force'):
            if order.get(key) is not None:
                order[key] = order[key].lower()
        
        return order
    
    def _place_order(self, order):
        """Takes an externalized order and sends it to the broker."""
        response = requests.post(self.url, data = json.dumps(order, default = str), headers = self.request_headers)

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
            response = self.format_order_in(response.json())
        
        self.place_order(order)
        return response
    
    def place_order(self, order):
        pass
    
    def _stop(self):
        self.ws.close()
        self.stop()

    def on_open(self, ws):
        # authenticate
        ws.send(json.dumps({
            'action': 'authenticate',
            'data': {
                'key_id': self.auth['APCA-API-KEY-ID'],
                'secret_key': self.auth['APCA-API-SECRET-KEY']
            }
        }))
        # listen to trade updates
        ws.send(json.dumps({
            'action': 'listen',
            'data': {
                'streams': ['trade_updates']
            }
        }))
        return

    def on_message(self, ws, message):
        message = json.loads(message)
        order = message['data']['order']
        _order = self.format_order_in(order)
        self._handle_event(_order)
        return