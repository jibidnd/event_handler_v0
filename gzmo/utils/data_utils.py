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
    