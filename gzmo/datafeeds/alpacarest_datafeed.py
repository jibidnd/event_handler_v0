import requests
import urllib.parse

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

        self.results = []
        
        return

    @staticmethod
    def format_query(query):
        """Translate the query from standard format to Alpaca specific format."""
        _query = {**query}

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

    def execute_query(self, live = False):
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
        print(resp)
        return resp
    
    @staticmethod
    def format_result(result, event_subtype, additional_info = None):
        
        _result = {**result}
        if additional_info is None:
            additional_info = {}
        
        # this is a data event
        _result[c.EVENT_TYPE] = c.DATA
        # bar/quote/tick

        if event_subtype == c.BAR:
            # bars
            _result[c.EVENT_SUBTYPE] = c.BAR
            _result[c.OPEN] = _result.pop('o')
            _result[c.HIGH] = _result.pop('h')
            _result[c.LOW] = _result.pop('l')
            _result[c.CLOSE] = _result.pop('c')
            _result[c.VOLUME] = _result.pop('v')
            _result[c.EVENT_TS] = pd.Timestamp(_result.pop('t')).isoformat()
        elif event_subtype == c.QUOTE:
            # quotes
            _result[c.EVENT_SUBTYPE] = c.QUOTE
            _result[c.ASK_EXCHANGE] = _result.pop('ax')
            _result[c.ASK_PRICE] = _result.pop('ap')
            _result[c.ASK_SIZE] = _result.pop('as')
            _result[c.BID_EXCHANGE] = _result.pop('bx')
            _result[c.BID_PRICE] = _result.pop('bp')
            _result[c.BID_SIZE] = _result.pop('bs')
            _result[c.EVENT_TS] = pd.Timestamp(_result.pop('t')).isoformat()
            _result[c.CONDITIONS] = _result.pop('c')
            _result[c.TAPE] = _result.pop('z')
        elif event_subtype == c.TICK:
            # ticks
            _result[c.EVENT_SUBTYPE] = c.TICK
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

            `limit` records will be returned. If limit == 1, a dictionary of a single record
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

        while (limit is None) or (len(res) < limit):
            # Fill res with API responses, `get`-ing more if needed
            if len(self.results) > (n_needed := (limit - len(res))):
                res.extend(self.results[:n_needed])
                self.results = self.results[n_needed:]
            elif len(self.results) <= n_needed:
                # add what we have
                res.extend(self.results)
                self.results = []
                # get next page of results if there are more
                if self.request_params.get('page_token') is not None:
                    self.execute_query()
                else:
                    # since the query has been executed at least once,
                    # if there is no page_token, it means we're done.
                    self.is_finished = True
                    break

        # return a list only if limit > 1
        if limit > 1:
            return res
        elif limit == 1:
            if len(res) > 0:
                return res[0]
            else:
                return
    
    def format_record(self, result):
        # TODO
        pass