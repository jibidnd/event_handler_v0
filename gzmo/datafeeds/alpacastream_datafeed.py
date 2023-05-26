import websocket
import json
from collections import defaultdict, deque
import time
import threading
import itertools

import pandas as pd # for parsing RFC-3330 timestamps and preserve nanoseocnds

import zmq

from . import BaseDataFeed
from .. import utils
from ..utils import constants as c

class AlpacaStreamDataFeed(BaseDataFeed):
    """Datafeed that gets data from Alpaca websocket.

    Note that this is a streaming datafeed. The `fetch` method is implemented,
    but only returns up to `cache` records.
    """

    def __init__(self,  *args, cache = 100,**kwargs):
        """Inits the stream datafeed object

        Data from the websocket is streamed and cached in a deque.
        Items from the deque is removed when `fetch` is called,
        When the deque gets full, oldest items are removed to make space.

        Args:
            cache (int, optional): Number of records to cache for `fetch` method. Defaults to 10.
        """        
        super().__init__(*args, **kwargs)

        self.data_type = self.query[c.DATA_TYPE]

        # auth
        self.auth = self.auth['alpaca_paper']

        # set up the request
        # format the query
        self.format_query()
        
        # Base URL
        self.source = 'iex'

        self.results = deque(maxlen = cache)


        return

    def format_query(self):
        """Translates the query from standard format to Alpaca specific format."""

        self.subscribe_msg = defaultdict(list)
        self.subscribe_msg['action'] = 'subscribe'

        if isinstance(self.query[c.DATA_TYPE], list):
            assert len(self.query[c.DATA_TYPE]) == self.query[c.SYMBOL], \
                'Length of data types != length of symbol lists'
            zipped_list = zip(self.query[c.DATA_TYPE], self.query[c.SYMBOL])
        elif self.query[c.DATA_TYPE] in [c.BAR, c.QUOTE, c.TICK]:
            zipped_list = zip([self.query[c.DATA_TYPE]], [self.query[c.SYMBOL]])
        else:
            raise NotImplementedError('DATATYPE must be a list of datatypes or one of BAR, QUOTE, or TICK.')
        
        for data_type, symbols in zipped_list:
            if data_type == c.BAR:
                self.subscribe_msg['bars'].append(symbols)
            elif data_type == c.QUOTE:
                self.subscribe_msg['quotes'].append(symbols)
            elif data_type == c.TICK:
                self.subscribe_msg['trades'].append(symbols)
        
        return

    def execute_query(self):
        """Makes the connection and get ready to emit data.
        
        This function starts the streaming and cache some results in `self.results`.
        When the `self.fetch` is called, the cached results will be returned.
        """

        auth_msg = {
            'action': 'auth',
            'key': self.auth['APCA-API-KEY-ID'],
            'secret': self.auth['APCA-API-SECRET-KEY']
        }
        
        socket = f'wss://stream.data.alpaca.markets/v2/{self.source}'

        self.ws = websocket.WebSocketApp(
            socket,
            on_open = lambda ws: self.on_open(ws, auth_msg),
            on_message = lambda ws, messages: self.on_message(ws, messages)
        )
        
        self.from_beginning = False
        thread = threading.Thread(name = f'{self.topic}_ws', target = self.ws.run_forever, daemon = True)
        thread.start()
        return
    
    def on_open(self, ws, auth_msg):
        ws.send(json.dumps(auth_msg))
        ws.send(json.dumps(self.subscribe_msg))
        return


    def on_message(self, ws, messages):
        messages = json.loads(messages)
        for message in messages:
            # format the message
            message = self.format_result(message)
            # append the message to results for fetching later
            if message is not None:
                self.results.append(message)

    
    def _stop(self):
        """Overrides parent method to close websocket."""
        super()._stop()
        self.ws.close()
    
    # @staticmethod
    def format_result(self, result):
        _result = {**result}
        # this is a data event
        _result[c.EVENT_TYPE] = c.DATA
        _result[c.TOPIC] = self.topic
        # bar/quote/tick
        if (message_type := _result.pop('T')) == 'b':
            # bars
            _result[c.EVENT_SUBTYPE] = c.BAR
            _result[c.SYMBOL] = _result.pop('S')
            _result[c.OPEN] = _result.pop('o')
            _result[c.HIGH] = _result.pop('h')
            _result[c.LOW] = _result.pop('l')
            _result[c.CLOSE] = _result.pop('c')
            _result[c.VOLUME] = _result.pop('v')
            _result[c.EVENT_TS] = pd.Timestamp(_result.pop('t'))
        elif message_type == 'q':
            # quotes
            _result[c.EVENT_SUBTYPE] = c.QUOTE
            _result[c.SYMBOL] = _result.pop('S')
            _result[c.ASK_EXCHANGE] = _result.pop('ax')
            _result[c.ASK_PRICE] = _result.pop('ap')
            _result[c.ASK_SIZE] = _result.pop('as')
            _result[c.BID_EXCHANGE] = _result.pop('bx')
            _result[c.BID_PRICE] = _result.pop('bp')
            _result[c.BID_SIZE] = _result.pop('bs')
            _result[c.EVENT_TS] = pd.Timestamp(_result.pop('t'))
            _result[c.CONDITIONS] = _result.pop('c')
            _result[c.TAPE] = _result.pop('z')
        elif message_type == 't':
            # ticks
            _result[c.EVENT_SUBTYPE] = c.TICK
            _result[c.SYMBOL] = _result.pop('S')
            _result[c.TRADE_ID] = _result.pop('i')
            _result[c.EXCHANGE] = _result.pop('x')
            _result[c.PRICE] = _result.pop('p')
            _result[c.SIZE] = _result.pop('s')
            _result[c.EVENT_TS] = pd.Timestamp(_result.pop('t'))
            _result[c.CONDITIONS] = _result.pop('c')
            _result[c.TAPE] = _result.pop('z')
        else:
            # TODO: log unexpected message
            print("non data item",  _result)
            return None
        return _result

    def fetch(self, limit = 1):
        """Return available records up to `limit` as a list of events (dictionaries).
            
            If self.from_beginning is True (as it is set when the datafeed is instantiated),
            the query will be executed when this method is called.

        Args:
            limit (int, optional): The max number of records to return. Defaults to 1.

        Returns:
            list of dict: The queried data as record(s).
        """        
        # if starting over
        if self.from_beginning:
            self.execute_query()

        # return up to `limit` records
        d_res = deque()
        for i in range(limit):
            try:
                d_res.append(self.results.popleft())
            except IndexError:
                break
            except:
                raise
        
        return list(d_res)
        
        