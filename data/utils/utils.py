import datetime

def generate_days(start, end, step_in_days):
	'''
	start and end can be either datetime.datetime instances or string of yyyy-mm-dd format.
	Retunrs list of dates as datetime.datetime instances
	'''
	if isinstance(start, str):
		start = datetime.datetime.strptime(start, '%Y-%m-%d')
	if isinstance(end, str):
		end = datetime.datetime.strptime(end, '%Y-%m-%d')

	dates = [(start + datetime.timedelta(days = step_in_days * i)) for i in range((end - start).days + 1)]

	return dates

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