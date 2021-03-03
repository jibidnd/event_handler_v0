import websocket
import json
from collections import defaultdict, deque
import time

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

    def __init__(self,  *args, cache = 10,**kwargs):
        """Inits the stream datafeed object

        Args:
            cache (int, optional): Number of records to cache for `fetch` method. Defaults to 10.
        """        
        super().__init__(*args, **kwargs)

        self.data_type = self.query[c.DATA_TYPE]

        # auth
        self.auth = self.auth['alpaca_live']

        # set up the request
        # format the query
        self.format_query()
        
        # Base URL
        self.source = 'iex'

        self.results = deque(maxlen = cache)

        # override parent method to skip fetching
        self.publish = self.execute_query


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

    def execute_query(self, live = False):
        """Makes the connection and get ready to emit data."""

        auth_msg = {
            'action': 'auth',
            'key': self.auth['APCA-API-KEY-ID'],
            'secret': self.auth['APCA-API-SECRET-KEY']
        }
        
        socket = f'wss://stream.data.alpaca.markets/v2/{self.source}'

        self.ws = websocket.WebSocketApp(
            socket,
            on_open = lambda ws: self.on_open(ws, auth_msg),
            on_message = print#self.on_message
        )
        
        self.from_beginning = False
        return
    
    def on_open(self, ws, auth_msg):
        ws.send(json.dumps(auth_msg))
        ws.send(json.dumps(self.subscribe_msg))
        return


    def on_message(self, ws, message):
        
        # format the message
        message = self.format_results(message)

        # append the message to results for fetching later
        self.results.append(message)
        
        # publish the message if there is a socket
        if self.sock_out is not None:
            try:
                # send the event with a topic
                res_packed = utils.packb(message)
                self.sock_out.send_multipart([self.topic.encode(), res_packed], flag = zmq.NOBLOCK)
            except zmq.ZMQError as exc:
                # Drop messages if queue is full
                if exc.errno == zmq.EAGAIN:
                    pass
                else:
                    # unexpected error: shutdown and raise
                    self.shutdown()
                    raise
            except zmq.ContextTerminated:
                # context is being closed by session
                self.shutdown()
            except:
                raise
    
    @staticmethod
    def format_result(result):
        
        _result = {**result}
        # this is a data event
        _result[c.EVENT_TYPE] = c.DATA
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
            _result[c.EVENT_TS] = pd.Timestamp(_result.pop('t')).isoformat()
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
            _result[c.EVENT_TS] = pd.Timestamp(_result.pop('t')).isoformat()
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
            _result[c.EVENT_TS] = pd.Timestamp(_result.pop('t')).isoformat()
            _result[c.CONDITIONS] = _result.pop('c')
            _result[c.TAPE] = _result.pop('z')
        else:
            pass
        
        return _result

    def fetch(self, limit = 1):
        """Return the requested number of records.

            Note that since this is a live datafeed, the method will block
            until `limit` records are returned.

            If limit == 1, a dictionary of a single record
                will be returned. If limit > 1, a list of dictionaries will be returned. The keys
                of the dictionaries will be the column names, and the values will be the record values.
            
            If self.from_beginning is True (as it is set when the datafeed is instantiated),
            the query will be executed when this method is called.

        Args:
            limit (int, optional): The number of records to return. Defaults to 1.

        Returns:
            list[dict] or dict: The queried data as record(s).
        """        
        # if starting over
        if self.from_beginning:
            self.execute_query()

        res = []

        if limit is None:
            res.extend(list(self.results))
            return res
        else:
            while len(res) < limit:
                if len(self.results) == 0:
                    time.sleep(0.1)
                    continue
                else:
                    if len(self.results) > (n_needed := (limit - len(res))):
                        res.extend(list(self.results)[:n_needed])
                        self.results = self.results[n_needed:]
                    elif 0 < len(self.results) <= n_needed:
                        # add what we have
                        res.extend(list(self.results))

            # return a list only if limit > 1
            if limit > 1:
                return res
            elif limit == 1:
                return res[0]