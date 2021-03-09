
import pandas as pd
import pprint
import re


def to_columnar(lst_dicts, data_keys) -> dict:
    '''
    Converts a list of dict to a dict of list.
    Missing values will be filled with None.
    The passed list_dict are rows with columns names as dict keys.
    The output is a dict with keys with column names and values as row values.

    :param list(dict) lst_dicts: a list of rows as dictionaries
    :param list data_keys: a list of column names to extract from the rows
    '''
    return {k: [dict_i.get(k) for dict_i in lst_dicts] for k in data_keys}

def to_isoformat(dict_data, endswiths = None):
    dict_data = {**dict_data}
    endswiths = endswiths or ['_at', '_timestamp', '_time', 'EVENT_TS']
    for key in dict_data.keys():
        for endswith in endswiths:
            if key.endswith(endswith):
                try:
                    dict_data[key] = pd.Timestamp(dict_data[key]).isoformat()
                except:
                    pass
    
    return dict_data

def to_timestamp(dict_data, endswiths = None):
    dict_data = {**dict_data}
    endswiths = endswiths or ['_at', '_timestamp', '_time', 'EVENT_TS']
    for key in dict_data.keys():
        for endswith in endswiths:
            if key.endswith(endswith):
                try:
                    dict_data[key] = pd.Timestamp(dict_data[key])
                except:
                    pass
    
    return dict_data


ISO8601YMD = re.compile(r'\d{4}-\d{2}-\d{2}T')
NY = 'America/New_York'


class Entity(object):
    """This helper class provides property access (the "dot notation")
    to the json object, backed by the original object stored in the _raw
    field.

    from https://github.com/alpacahq/alpaca-trade-api-python/blob/master/alpaca_trade_api/entity.py
    """

    def __init__(self, raw):
        self._raw = raw

    def __getattr__(self, key):
        if key in self._raw:
            val = self._raw[key]
            if (isinstance(val, str) and
                    (key.endswith('_at') or
                     key.endswith('_timestamp') or
                     key.endswith('_time')) and
                    ISO8601YMD.match(val)):
                return pd.Timestamp(val)
            else:
                return val
        return super().__getattribute__(key)

    def __repr__(self):
        return '{name}({raw})'.format(
            name=self.__class__.__name__,
            raw=pprint.pformat(self._raw, indent=4),
        )