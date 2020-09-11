import sys
import os
import datetime
import time
import configparser
import logging
import io
import re
import itertools
import math
import psutil
import traceback
import copy
import multiprocessing as mp
from queue import Empty
import multiprocessing_logging
import json

import pyarrow as pa
from pyarrow import parquet as pq
import requests
import aiohttp
import asyncio
import boto3

from ..utils import utils

#============================================================================================
# Global Vars
#============================================================================================
# Read in config
# config = configparser.ConfigParser()
# config.read_file(open('./creds.auth'))
# apiKey = config.get('PolygonIO', 'api_key')

# host url
url = 'https://api.polygon.io'

apiKey = None
def set_APIKey(key):
    global apiKey
    apiKey = key

#============================================================================================
# async Functions
#============================================================================================

async def historic_v2(endpoint, session, request_params, loop = False, attempts = 3) -> list:
    """
    Async function to request all data for one `endpoint` with the given `request_params`.
    
    :param str endpoint: The endpoint to request data from
    :param aiohttp.ClientSession session: an aiohttp.ClientSession to make the request on,
        benefitting from connection pooling.
    :param dict request_params: Parameters for the request as a dictionary
    :param bool loop: If True, attempts to fetch multiple pages of results from `endpoint`
        using the "timestamp" field in the results for offset. Default False.
    :params int attempts: Number of attempts for retry if receiving a non-404 error. Default 3.
    """

    # dummy initial timestamp of the endpoint
    next_t = 0
    #list to store the results (1 dict per record in results)
    lst_results = []
    # Number of attempts thus far
    attempt = 0

    # perform the request up to `attempts` attempts
    while attempt < attempts:
        request_params.update({'timestamp': next_t})
        try:
            # Make the call with async and get the JSON contents
            async with await session.get(endpoint, params = request_params) as resp:
                # Get the URL used
                resp_url = resp.url.human_repr()
                print(resp_url)
                resp_json = await resp.json(content_type = None)    # disable content type check
        except json.decoder.JSONDecodeError:
            # TODO: log error
            break
        except Exception as exc:
            # TODO: log exception
            # Wait 1 second before retrying
            # TODO: log retrying attempt
            attempt += 1
            await asyncio.sleep(0.1)
            break
        # Handle the response
        else:
            # regular case: get `results` field from response
            if resp.status == 200:
                results = resp_json['results']
                # TODO: log number of records returned

                # If there are no more results, we've reached the end
                if resp_json.get('results_count') == 0 or resp_json.get('resultsCount') == 0:
                    break
                # Oherwise append to `lst_results` and set `next_t` as last timestamp + 1 nanosecond
                else:
                    lst_results.extend(results)
                    next_t = results[-1]['t'] + 1
                # Quit if looping is not needed
                if not loop:
                    break
            
            # 404 error
            elif resp.status == 404:
                # no data, not a trading day
                # TODO: log no results returned
                break
            # other errors: increment attempt by 1 and try again
            else:
                # TODO: log retrying attempt
                attempt += 1
                await asyncio.sleep(0.1)
    return lst_results

async def historic_v2_batch(lst_endpoints, request_params, sort_key = 't', loop = False, **kwargs) -> list:
    """
    Perform an asynchronous batch request and format data.
    Calls `historic_v2` for each endpoint in `lst_endpoints`.

    :param list lst_endpoints: list of endpoints to query.
    :param dict request_params: Parameters for the request as a dictionary
    :param str sort_key: Since results are returned asynchronously, records are
        sorted by 't' (the SIP timestamp in most cases) by default. Pass sort_keys
        explicitly to change this behaviour.
    :param bool loop: Whether multiple pages of results are expected per endpoint. See historic_v2
    """
    # Open one session per call of this function. May not be the most optimal but there's less stuff to get passed around.
    # Plus we can have fewer async functions this way.
    # change the limit on simultaneously open connections. Default 100
    timeout = aiohttp.ClientTimeout(total = 60)
    async with aiohttp.ClientSession(timeout = timeout) as session:
        # TODO: logging
        tasks = [
                    historic_v2(endpoint, session = session, request_params = request_params, loop = loop)
                    for endpoint in lst_endpoints
                ]
        # this will return a list of lists: each child list consists of all results from 1 endpoint in *lst_endpoints*
        shallow_lst_results = await asyncio.gather(*tasks)

    # flatten the list
    lst_results = list(itertools.chain.from_iterable(shallow_lst_results))
    # Sort the list by sort_key:
    if sort_key:
        lst_results.sort(key = lambda x: x[sort_key])
    return lst_results

def load_historic_v2_batch(q_to_request, q_to_write) -> None:
    '''
    Load data using historic_v2_batch for a job in `q_jobs`,
        and places the results as a pyarrow table in q_results.
    `endpoint_name` is used to specify the following:
        - columns to return in pyarrow table. Uses  `get_polygon_names`
        - pyarrow table schema. Uses `get_polygon_schema`

    :param multiprocessing.JoinableQueue q_jobs: The queue to get requests from. Each item should be a dict containing
        the following information to be passed to `historic_v2_batch`
        - endpoint_name: string
        - lst_endpoints: string
        - request_params: dict
        - sort_key: string
        - loop: boolean
    :param multiprocessing.JoinableQueue q: The queue to load results to.
    '''
    while True:
        try:
            # Get job from queue
            job = q_to_request.get(block = False)
        except Empty:
            return
        else:
            # TODO: First check memory usage??
            # Run historic_v2_batch and gather results
            rows = asyncio.run(historic_v2_batch(**job))
            # Convert to pyarrow table
            pa_tbl = to_pa_tbl(lst_dicts = rows, endpoint_name = job['endpoint_name'], additional_data = {'ticker': job['ticker']})
            # Add data to queue if it's not empty
            if pa_tbl.num_rows > 0:
                q_to_write.put({'body': pa_tbl, **job})
            q_to_request.task_done()

        # return
##

def load_historic_v2_batches(lst_tickers, request_params, writer, writer_params, data_params = {}):

    """
    Takes a list of tickers, request the data from polygon using multiprocessing, and sends the writer a queue containing the results as pyarrow tables.
    
    :param list lst_tickers: The list of tickers for which data is wanted.
    :param dict request_params: Parameters for request.
        Must contain the api key with key 'apiKey'
        Other acceptable params are:
            - data_type: Data type to request. Default 'bar'.
                Acceptable values are:
                - bar
                - tick
                - quote
            - start: start date of data requested, if any. Default None; datetime.datetime
            - end: end date of data requested, if any. Default None; datetime.datetime
            - multiplier: multiplier to aggregate bars by (compression). Default 1; int
            - timespan: bar type. Default 'minute'; string. Acceptable values are:
                - 'minute'
                - 'hour'
                - 'day'
                - 'week'
                - 'month'
                - 'quarter'
                - 'year'
            - unadjusted: 'true' or 'false'. Default 'true'; string
            - sort: 'asc' or 'desc'. Default 'asc'; string
        Note on start: If None, uses first avaialable date for the ticker if minute bars,
            else uses 2020-01-01 for >=daily bars.
    :param callable writer: Callable to send the results queue to, with signature (q_results: mp.JoinableQueue, **kwargs).
        Objects in the queue passed to writer will contain a copy of `request_params`, in addition to
            - ticker
            - datatype
            - datasource = polygon
            - yyyymm if applicable
    :param dict writer_params: Any parameters to pass to the writer.
        For s3, this should contain s3_bucket.
    """

    # Default data parameters: insert where key is not specified
    data_params = {**{'data_type': 'bar', 'start': None, 'end': None, 'multiplier': 1, 'timespan': 'minute', 'unadjusted': 'true', 'sort': 'asc'}, **data_params}
    # All outstanding requests to take care of
    manager = mp.Manager()
    q_to_request = manager.JoinableQueue()
    q_to_write = manager.JoinableQueue()

    # Generate requests
    if data_params['data_type'] == 'bar':
        # Do one ticker at a time
        endpoint_name = 'historic_bars_v2'
        if data_params['timespan'] in ['hour', 'day', 'week', 'month', 'quarter', 'year']:
            for ticker in lst_tickers:
                # This will be a list of a list of length 1
                lst_lst_endpoints = make_requests_ticker(ticker, data_params['start'], data_params['end'],
                                                                data_params['multiplier'], data_params['timespan'], data_params['unadjusted'])
                
                # s3 information for if writer is to write to s3
                s3_key = f'price_data/bars/{data_params["timespan"]}/tkr={ticker}/data.parquet'
                writer_params.update({'s3_key': s3_key})
                kwargs = {
                    'ticker': ticker,
                    'lst_endpoints': lst_lst_endpoints[0],
                    'endpoint_name': endpoint_name,
                    'request_params': request_params,
                    'writer_params': writer_params,
                    'loop': False
                    }
                q_to_request.put(copy.deepcopy(kwargs))    # Recall that dictionaries are mutable
        elif data_params['timespan'] in ['minute']:
            # For minutely resolution, do one ticker * yyyymm combination in each go
            for ticker in lst_tickers:
                # This will be a list of lists of lenth ~30 (# days in a month)
                lst_lst_endpoints = make_requests_yyyymm(ticker, 'bar', data_params['start'], data_params['end'],
                                                                data_params['multiplier'], data_params['timespan'], data_params['unadjusted'])
                for lst_endpoints in lst_lst_endpoints:
                    # s3 information for if writer is to write to s3
                    yyyymm = lst_endpoints[0][-10:-6] + lst_endpoints[0][-5:-3]
                    s3_key = f'price_data/bars/{data_params["timespan"]}/tkr={ticker}/yyyymm={yyyymm}/data.parquet'
                    writer_params.update({'s3_key': s3_key})
                    kwargs = {
                        'ticker': ticker,
                        'lst_endpoints': lst_endpoints,
                        'endpoint_name': endpoint_name,
                        'request_params': request_params,
                        'writer_params': writer_params,
                        # As of recently, there is no longer a need to ping for multiple pages of results within a day. Instead all rows within a day are returned at the same time.
                        'loop': False 
                    }
                    q_to_request.put(copy.deepcopy(kwargs))    # Note that dictionaries are mutable
    elif data_params['data_type'] == 'quote':
        endpoint_name = 'historic_quotes_v2'
        # For minutely resolution, do one ticker * yyyymm combination in each go
        for ticker in lst_tickers:
            # This will be a list of lists of lenth ~30 (# days in a month)
            lst_lst_endpoints = make_requests_yyyymm(ticker, 'quote', data_params['start'], data_params['end'])
            for lst_endpoints in lst_lst_endpoints:
                # s3 information for if writer is to write to s3
                yyyymm = lst_endpoints[0][-10:-6] + lst_endpoints[0][-5:-3]
                s3_key = f'price_data/quotes/tkr={ticker}/yyyymm={yyyymm}/data.parquet'
                writer_params.update({'s3_key': s3_key})
                kwargs = {
                    'ticker': ticker,
                    'lst_endpoints': lst_endpoints,
                    'endpoint_name': endpoint_name,
                    'request_params': request_params,
                    'writer_params': writer_params,
                    'loop': True
                }
                q_to_request.put(copy.deepcopy(kwargs))    # Note that dictionaries are mutable
    elif data_params['data_type'] == 'tick':
        endpoint_name = 'historic_trades_v2'
        # For minutely resolution, do one ticker * yyyymm combination in each go
        for ticker in lst_tickers:
            # This will be a list of lists of lenth ~30 (# days in a month)
            lst_lst_endpoints = make_requests_yyyymm(ticker, 'tick', data_params['start'], data_params['end'])
            for lst_endpoints in lst_lst_endpoints:
                # s3 information for if writer is to write to s3
                yyyymm = lst_endpoints[0][-10:-6] + lst_endpoints[0][-5:-3]
                s3_key = f'price_data/ticks/tkr={ticker}/yyyymm={yyyymm}/data.parquet'
                writer_params.update({'s3_key': s3_key})
                kwargs = {
                    'ticker': ticker,
                    'lst_endpoints': lst_endpoints,
                    'endpoint_name': endpoint_name,
                    'request_params': request_params,
                    'writer_params': writer_params,
                    'loop': True
                }
                q_to_request.put(copy.deepcopy(kwargs))    # Note that dictionaries are mutable

    num_processes_requests = math.floor(mp.cpu_count() / 2)
    num_processes_write = math.floor(mp.cpu_count() / 2)
    with mp.Pool(num_processes_requests + num_processes_write) as pool:
        # logger = mp.log_to_stderr(logging.ERROR)
        for _ in range(num_processes_requests):
            res = pool.apply_async(load_historic_v2_batch, args = (q_to_request, q_to_write))
            res.get()
        for _ in range(num_processes_write):
            res = pool.apply_async(writer, args = (q_to_write,))
            res.get()
        pool.close()
        pool.join()
        # q_to_request.join()
        # q_to_write.join()

#============================================================================================
# Request helpers
#============================================================================================

def get_first_available_bar(ticker) -> datetime.datetime:
    '''
    Returns first date with available data for ticker.
    Returns datetime.datetime instance.
    '''
    endpoint = '/v2/aggs/ticker/{}/range/1/day/2000-01-01/{}'.format(
        ticker, datetime.datetime.now().strftime('%Y-%m-%d'))
    try:
        with requests.get(url + endpoint, params = {'apiKey': apiKey, 'limit': 1}) as resp:
            # convert millisecond to seconds
            return(datetime.datetime.utcfromtimestamp(resp.json()['results'][0]['t'] / 1000.0))
    except:
        return datetime.datetime(2000, 1, 1)

def make_requests_yyyymm(ticker, data_type = 'bar', start = None, end = None, multiplier = 1,
                            timespan = 'minute', unadjusted = True) -> list:
    '''
    Create the list of endpoints to request bars from for `ticker`.
    Returns a list of lists, with each sublist constituting a month's worth of dates.

    :param str ticker: ticker to request data for.
    :param str data_type: data type to request. Acceptable values are:
        - bar
        - tick
        - quote
    :param datetime.datetime start: start date to get data for. If none passed,
        loads from first available date. Default None.
    :param datetime.datetime end: last date to get minute bars for. If none passed,
        loads until last available date. Default None.
    :param int multiplier: Polygon request parameter; compression of candles. Default 1 (no compression).
    :param str timespan: Polygon request parameter; resolution of candles. Default 'minute'.
        Acceptable values are:
        - minute
        - hour
        - day
        - week
        - month
        - quarter
        - year
    :param bool unadjusted: Polygon request parameter; set to true if the results should NOT be adjusted for splits.
        Default True

    Returns list of lists:
        [['2019-01-01', ..., '2019-01-31'], ['2019-02-01', ..., '2019-02-28'], ..., ['2019-12-01', ..., '2019-12-31']]
    '''

    start = start or get_first_available_bar(ticker)
    if not start:
        return
    end = end or datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # Generate the first of the month for months between start and end, inclusive.
    date_bin_edges = [start.replace(day = 1)]
    while date_bin_edges[-1] < end.replace(day = 1):
        date_bin_edges.append((date_bin_edges[-1] + datetime.timedelta(days = 45)).replace(day = 1))
    # Generate list of lists:
    # datess = [['2019-01-01', ..., '2019-01-31'], ['2019-02-01', ..., '2019-02-28'], ..., ['2019-12-01', ..., '2019-12-31']]
    datess = [utils.generate_days(first, (first + datetime.timedelta(days = 45)).replace(day = 1) - datetime.timedelta(days = 1), 1) for first in date_bin_edges]
    # Type of request to generate:
    if data_type == 'bar':
        lst_lst_endpoints = [[url + f'/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{date.strftime("%Y-%m-%d")}/{date.strftime("%Y-%m-%d")}' for date in dates] for dates in datess]
    elif data_type == 'tick':
        lst_lst_endpoints = [[url + f'/v2/ticks/stocks/trades/{ticker}/{date.strftime("%Y-%m-%d")}' for date in dates] for dates in datess]
    elif data_type == 'quote':
        lst_lst_endpoints = [[url + f'/v2/ticks/stocks/nbbo/{ticker}/{date.strftime("%Y-%m-%d")}' for date in dates] for dates in datess]
    return lst_lst_endpoints

def make_requests_ticker(ticker, start = datetime.datetime(2000, 1, 1), end = None, multiplier = 1,
                            timespan = 'day', unadjusted = True) -> list:
    '''
    Create the list of endpoints to request bars from for `ticker`.
    Returns a list of a list of length 1, with each sublist containing endpoints for all requested dates for a ticker.

    :param datetime.datetime start: start date to get minute bars for. Default 2000-01-01.
    :param datetime.datetime end: last date to get minute bars for. If none passed,
        loads until last available date. Default None.
    :param int multiplier: Polygon request parameter; compression of candles. Default 1 (no compression).
    :param str timespan: Polygon request parameter; resolution of candles. Default 'day'.
        Acceptable values are:
        - minute
        - hour
        - day
        - week
        - month
        - quarter
        - year
    :param bool unadjusted: Polygon request parameter; set to true if the results should NOT be adjusted for splits.
        Default True
    Returns a list of a list of length 1:
        [[ticker 0 request]]
    '''
    start = start or datetime.datetime(2000, 1, 1)
    end = end or datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    lst_lst_endpoints = [[url + f'/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start.strftime("%Y-%m-%d")}/{end.strftime("%Y-%m-%d")}']]

    return lst_lst_endpoints

#============================================================================================
# Other helper functions
#============================================================================================
def get_polygon_names(endpoint_name):
    '''
    Provides the mapping as defined by Polygon.io
    from returned results key and the field names

    Note: Polygon.io results (the field in the response) don't actually have the key "T"
    '''
    dict_mappings = {
        'historic_trades_v2': { \
            # 'T': 'ticker',
            't': 'sip_timestamp',
            'y': 'participant_timestamp',
            # 'f': 'trf_timestamp',
            'q': 'sequence_number',
            'i': 'trade_id',
            'x': 'exchange_id',
            's': 'size',
            'c': 'conditions',
            'p': 'price',
            'z': 'tape'
        }
        ,
        'historic_quotes_v2': {	\
            # 'T': 'ticker',
            't': 'sip_timestamp',
            'y': 'participant_timestamp',
            # 'f': 'trf_timestamp',
            'q': 'sequence_number',
            'c': 'conditions',
            'z': 'tape',
            'p': 'bid',
            's': 'bid_size',
            'x': 'bid_exchange_id',
            'P': 'ask',
            'S': 'ask_size',
            'X': 'ask_exchange_id'
        }
        ,
        'historic_bars_v2': {	\
            # 'T': 'ticker',
            't': 'timestamp',
            'o': 'open',
            'h': 'high',
            'l': 'low',
            'c': 'close',
            'v': 'volume',
            'n': 'n_items',
            'vw': 'vwap'
        }
    }

    return dict_mappings[endpoint_name]

def get_polygon_schema(endpoint_name):
    '''
    Provides the schema by type of data.
    Using a hard-coded schema for stability
    '''
    # Define schema
    dict_schemas = {
        'historic_trades_v2': \
            pa.schema({
                'ticker': pa.string(),
                'sip_timestamp': pa.int64(),	# pa.timestamp(unit = 'ns', tz = None),
                'participant_timestamp': pa.int64(),	# pa.timestamp(unit = 'ns', tz = None),
                # 'trf_timestamp': pa.timestamp(unit = 'ns', tz = None),
                'sequence_number': pa.int64(),
                'trade_id': pa.string(),
                'exchange_id': pa.int16(),
                'size': pa.int64(),
                'conditions': pa.list_(value_type = pa.int8(), list_size = -1),
                'price': pa.float32(),
                'tape': pa.int8()
            })
        ,
        'historic_quotes_v2': \
            pa.schema({
                'ticker': pa.string(),
                'sip_timestamp': pa.int64(),	# pa.timestamp(unit = 'ns', tz = None),
                'participant_timestamp': pa.int64(),	# pa.timestamp(unit = 'ns', tz = None),
                # 'trf_timestamp': pa.timestamp(unit = 'ns', tz = None),
                'sequence_number': pa.int64(),
                'conditions': pa.list_(value_type = pa.int8(), list_size = -1),
                'tape': pa.int8() ,
                'bid': pa.float32(),
                'bid_size': pa.int64(),
                'bid_exchange_id': pa.int16(),
                'ask': pa.float32(),
                'ask_size': pa.int64(),
                'ask_exchange_id': pa.int16()
        })
        ,
        'historic_bars_v2': \
            pa.schema({
                'ticker': pa.string(),
                'timestamp': pa.int64(),
                'open': pa.float32(),
                'high': pa.float32(),
                'low': pa.float32(),
                'close': pa.float32(),
                'volume': pa.int64(),
                'n_items': pa.int64(),
                'vwap': pa.float32()
        })
    }

    return dict_schemas[endpoint_name]

def to_pa_tbl(lst_dicts, endpoint_name, additional_data = {}):
    """
    Convert list of rows (as dictionaries) to a pyarrow table using Polygon specific schema and names.

    :param dict additional_data: any additional data to add as a column. E.g. ticker name
    """
    # Get metadata
    data_name_mapping = get_polygon_names(endpoint_name)
    data_keys = list(data_name_mapping.keys())
    pa_schema = get_polygon_schema(endpoint_name)

    # Convert list of rows to columnar format
    columnar = utils.to_columnar(lst_dicts, data_keys)
    # Add ticker name (field is unpopulated)
    columnar['ticker'] = [additional_data['ticker']] * len(list(columnar.values())[0])
    # rename column names in columnar
    columnar = {data_name_mapping.get(key) or key: item for key, item in columnar.items()}
    # add additional data
    for key, item in additional_data.items():
        columnar[key] = [item] * len(columnar[list(columnar.keys())[0]])

    # Create pyarrow table
    pa_tbl = pa.Table.from_pydict(columnar, schema = pa_schema)

    return pa_tbl

def pq_to_s3(pa_tbl, s3_client, s3_bucket, s3_key, pq_params = {}):

    # Default params
    params = {'version': '2.0', 'use_dictionary': True, 'flavor': 'spark',
                'compression': 'snappy', 'use_deprecated_int96_timestamps': True,
                'allow_truncated_timestamps': True}
    params.update(pq_params)

    # Write parquet file to bytes stream
    writer = pa.BufferOutputStream()
    pq.write_table(pa_tbl,
                   writer,
                   **pq_params
                  )
    body = bytes(writer.getvalue())
    s3_client.put_object(Body = body, Bucket = s3_bucket, Key = s3_key)

# make a per process s3_client
s3_client = None
def initialize():
  global s3_client
  session = boto3.Session(profile_name='aws_credentials')
  s3_client = session.client('s3')

# bulk fetch: async, list of endpoints (1)

# bulk load to s3 (2)
# bulk load to snowflake (3)

# bulk fetch: multiprocessing, list of (2) or (3)