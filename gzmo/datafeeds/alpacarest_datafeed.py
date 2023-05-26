import requests
import urllib.parse
import decimal
from collections import deque

import pandas as pd

from . import BaseDataFeed
from .. import utils
from ..utils import constants as c

class AlpacaRestDataFeed(BaseDataFeed):
    """Datafeed that gets data from Alpaca.
    
        Args:
            auth (dict or str): Authentication method to connection to Snowflake.
                If a dict is provided, expected to have the following keys:
                    - user
                    - password
                    - account
                If a string is provided, expected to be a path to a config file with
                    a 'snowflake' section, with the same keys expected in the dict
                    documented above.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # if (connection_type := self.query[c.CONNECTION_TYPE]) in [c.REST, c.STREAM]:
        #     self.connection_type = connection_type
        # else:
        #     raise NotImplementedError(f'Expected one of (REST, STREAM) for connection_type, got {connection_type}.')

        self.data_type = self.query[c.DATA_TYPE]

        # auth
        self.auth = self.auth['alpaca_live']

        # set up the request
        # format the query
        self._query = self.format_query(self.query)
        
        # Base URL
        base_url = 'https://data.alpaca.markets/v2/'
        
        # endpoint
        if (data_type := self.query[c.DATA_TYPE]) == c.BAR:
            endpoint = f'stocks/{self.query[c.SYMBOL]}/bars'
        elif data_type == c.QUOTE:
            endpoint = f'stocks/{self.query[c.SYMBOL]}/quotes'
        elif data_type == c.TICK:
            endpoint = f'stocks/{self.query[c.SYMBOL]}/trades'
        else:
            raise NotImplementedError(f'DATATYPE "{data_type}" not supported')
        
        # url
        self.url = base_url + endpoint

        # params
        self.request_params = {}
        if self.query[c.DATA_TYPE] == c.BAR:
            param_keys = ['start', 'end', 'limit', 'page_token', 'timeframe']
        else:
            param_keys = ['start', 'end', 'limit', 'page_token']
        
        for key in param_keys:
            if (param := self._query.get(key)) is not None:
                self.request_params[key] = param

        # headers (auth)
        self.request_headers = {
            'APCA-API-KEY-ID': self.auth['APCA-API-KEY-ID'],
            'APCA-API-SECRET-KEY': self.auth['APCA-API-SECRET-KEY']
        }

        self.results = deque()
        
        return

    @staticmethod
    def format_query(query):
        """Translate the query from standard format to Alpaca specific format."""
        _query = {**query}

        _query['start'] = _query.pop(c.START)
        _query['end'] = _query.pop(c.END)
        _query['symbol'] = _query.pop(c.SYMBOL)

        if (_query.get(c.MULTIPLIER) or 1) != 1:
            raise NotImplementedError('Multipliers other than 1 are not supported.')
        
        if (resolution := _query.get(c.RESOLUTION)) is not None:
            if resolution == 'm':
                _query['timeframe'] = '1Min'
            elif resolution == 'h':
                _query['timeframe'] = '1Hour'
            elif resolution == 'D':
                _query['timeframe'] = '1Day'

        return _query

    def execute_query(self):
        """Make the connection and get ready to emit data."""    
        
        # Need to NOT encode the colon
        request_params = urllib.parse.urlencode(self.request_params, safe = ':')

        # Make the request
        resp = requests.get(self.url, params = request_params, headers = self.request_headers).json()

        # get the results
        # allow mixed data types
        if (bars := resp.get('bars')) is not None:
            bars = [self.format_result(bar, c.BAR, {c.SYMBOL: resp['symbol']}) for bar in bars]
            self.results.extend(bars)
        if (quotes := resp.get('quotes')) is not None:
            quotes = [self.format_result(quote, c.QUOTE, {c.SYMBOL: resp['symbol']}) for quote in quotes]
            self.results.extend(quotes)
        if (ticks := resp.get('trades')) is not None:
            ticks = [self.format_result(tick, c.TICK, {c.SYMBOL: resp['symbol']}) for tick in ticks]
            self.results.extend(ticks)


        # Store next page token if there are more results
        self.request_params.pop('page_token', None)        # pop the token if exists
        if (token := resp.get('next_page_token')) is not None:
            self.request_params['page_token'] = token

        self.from_beginning = False
        return resp
    
    # @staticmethod
    def format_result(self, result, event_subtype, additional_info = None):
        
        _result = {**result}
        if additional_info is None:
            additional_info = {}
        
        # this is a data event
        _result[c.EVENT_TYPE] = c.DATA
        _result[c.TOPIC] = self.topic
        _result[c.SYMBOL] = self.query[c.SYMBOL]

        if event_subtype == c.BAR:
            # bars
            _result[c.EVENT_SUBTYPE] = c.BAR
            _result[c.OPEN] = decimal.Decimal(_result.pop('o'))
            _result[c.HIGH] = decimal.Decimal(_result.pop('h'))
            _result[c.LOW] = decimal.Decimal(_result.pop('l'))
            _result[c.CLOSE] = decimal.Decimal(_result.pop('c'))
            _result[c.VOLUME] = decimal.Decimal(_result.pop('v'))
            _result[c.RESOLUTION] = self.query[c.RESOLUTION]
            _result[c.MULTIPLIER] = self.query[c.MULTIPLIER]

            if self.query[c.ALIGNMENT] == c.LEFT:
                _result[c.EVENT_TS] = pd.Timestamp(_result.pop('t'))    # packer will isoformat this
                _result[c.ALIGNMENT] = c.LEFT
            elif self.query[c.ALIGNMENT] == c.RIGHT:
                offset = pd.Timedelta(value = self.query[c.MULTIPLIER], unit = self.query[c.RESOLUTION])
                _result[c.EVENT_TS] = (pd.Timestamp(_result.pop('t')) + offset).isoformat()
                _result[c.ALIGNMENT] = c.RIGHT
            elif self.query[c.ALIGNMENT] == c.CENTER:
                offset = pd.Timedelta(value = self.query[c.MULTIPLIER], unit = self.query[c.RESOLUTION]) / 2
                _result[c.EVENT_TS] = (pd.Timestamp(_result.pop('t')) + offset).isoformat()
                _result[c.ALIGNMENT] = c.CENTER
            else:
                raise NotImplementedError(f"Expected one of 'LEFT', 'RIGHT', or 'CENTER' for bar aligntment. Got '{self.query[c.ALIGNMENT]}' instead")
        elif event_subtype == c.QUOTE:
            # quotes
            _result[c.EVENT_SUBTYPE] = c.QUOTE
            _result[c.ASK_EXCHANGE] = _result.pop('ax')
            _result[c.ASK_PRICE] = _result.pop('ap')
            _result[c.ASK_SIZE] = _result.pop('as')
            _result[c.BID_EXCHANGE] = _result.pop('bx')
            _result[c.BID_PRICE] = _result.pop('bp')
            _result[c.BID_SIZE] = _result.pop('bs')
            _result[c.EVENT_TS] = pd.Timestamp(_result.pop('t'))    # packer will isoformat this
            _result[c.CONDITIONS] = _result.pop('c')
            _result[c.TAPE] = _result.pop('z')
        elif event_subtype == c.TICK:
            # ticks
            _result[c.EVENT_SUBTYPE] = c.TICK
            _result[c.TRADE_ID] = _result.pop('i')
            _result[c.EXCHANGE] = _result.pop('x')
            _result[c.PRICE] = _result.pop('p')
            _result[c.SIZE] = _result.pop('s')
            _result[c.EVENT_TS] = pd.Timestamp(_result.pop('t'))    # packer will isoformat this
            _result[c.CONDITIONS] = _result.pop('c')
            _result[c.TAPE] = _result.pop('z')
        else:
            pass

        
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
                # get next page of results if there are more
                if self.request_params.get('page_token') is not None:
                    self.execute_query()
                    d_res.append(self.results.popleft())
                else:
                    # since the query has been executed at least once,
                    # if there is no page_token, it means we're done.
                    self.is_finished = True
                    break
            except:
                raise
        
        return list(d_res)