"""A Lines object is a collection of time series belonging to one asset (symbol)."""
import collections
import bisect
import itertools

import pandas as pd

from ..utils import constants as c

class lines(dict):
    """
    A Lines object is a collection of time series belonging to one asset (symbol).
    
    The lines object is primarily designed to quickly process incoming data (hence the use of deques).
    Each lines should consist of data from one symbol, for one specific resolution.
    On updating with new events, the lines object does an insert so that the resulting deques remain sorted,
    although it is much faster if sorting is not required (new event is later than all events).

    Slicing is supported by the lines object by specifiying beginning and the end of the slice.

    The lines object is subclasses `dict`, with the following methods overriden:
        __getattr__: collection of lines can be accessed through the dot notation.
            A case sensitive match is first attempted, before attempting a case insensitive
            match, which is much slower and not recommended.
        __len__: This method is overriden so len(lines_object) returns the length of the
            member datalines.

    Example:

        >>> # Create a `lines` object
        >>> aapl = lines()
        >>> # update with data
        >>> aapl.update_with_data({'EVENT_TS': 1548666060, 'OPEN': 39.3225, 'CLOSE': 39.35})
        >>> aapl.update_with_data({'EVENT_TS': 1548666120, 'OPEN': 39.35, 'CLOSE': 39.37})
        >>> aapl.update_with_data({'EVENT_TS': 1548666180, 'OPEN': 39.45, 'CLOSE': 39.55})
        >>> len(aapl)
        3
        >>> aapl.filter({'OPEN': lambda x: x >= 39.35}, as_df = False)
        {'Event_TS': [1548666120, 1548666180], 'OPEN': [39.35, 39.45], 'CLOSE': [39.37, 39.55]}
        >>> aapl.get_slice(beg = 1548666120, end = 1548666180)
        {'Event_TS': [1548666120, 1548666180], 'OPEN': [39.35, 39.45], 'CLOSE': [39.37, 39.55]}
    """
    def __init__(self, symbol, data_type = c.BAR, resolution = 'm', index_line = c.EVENT_TS, mark_line = c.CLOSE, tracked = None, maxlen = None):       
        """Inits a lines object.

        Args:
            symbol (str): An identifier for the underlying data. For reference only.
            data_type (str): The type of data (e.g. TICK, BAR, QUOTE, SIGNAL, STRATEGY).
            resolution (str): The resolution of data (e.g. D, h, m, s).
            index_line (str): The line to use as index. Default 'EVENT_TS'.
            mark_line (str): The line to use as mark. Default 'CLOSE'.
            tracked (None or set, optional): Track these fields in a data event. Defaults to OHLCV.
            maxlen (int, optional): The number of datapoints to keep. If None, keep all data. Default None.
        """        
        self.symbol = symbol
        self.data_type = data_type
        self.resolution = resolution
        self.index_line = index_line
        self.mark_line = mark_line
        self.tracked = tracked or [c.OPEN, c.HIGH, c.LOW, c.CLOSE, c.VOLUME]
        self.maxlen = maxlen

        self.index = collections.deque(maxlen = maxlen)
        for line in self.tracked:
            self[line] = collections.deque(maxlen = maxlen)

    def __getattr__(self, key):
        """Makes the dot notation available.

        Args:
            key (str): The key of the dictionary to access. A case-sensitive
                match is first attempted, before a case-insensitive attempt.
                A case-insensitive attempt is ~3 times slower.

        Raises:
            KeyError: If no matches are found.

        Returns:
            deque: The requested element of the dict; a deque object.
        """
        if key in self.keys():
            return self[key]
        elif (matching_keys := [k for k in self.keys() if k.lower() == key.lower()]) != []:
            # 3 times slower, try to avoid this
            _key = matching_keys[0]
            return self[_key]
        else:
            raise KeyError(key)

    @property
    def mark(self):
        """Convenient way to get the line designated as the mark line."""        
        return self[self.mark_line]
    
    def __len__(self):
        """Overrides parent method to provide lenth of the underlying deques.

        Returns:
            int: The length of the underlying deques.
        """        
        if len(self.keys()) == 0:
            return 0
        else:
            return len(self.index)

    def update_with_data(self, data):
        """Update each of the underlying deques with incoming data.

            The insert is performed such that the deques remain sorted
            according to self.index.
            
            If a particular data element is not avaialble in the incoming
            data, a None will be inserted in the corresponding line.

            Intermediate inserts are much slower than an append,
            so it is best to avoid updating with late-arriving events
            (event[self.index_line] > self.index[-1]).

        Args:
            data (dict): A dictionary with keys as the names of the lines
                to be updated.
        """        
        if (data_idx := data.get(self.index_line)) is None:
            pass
        elif (len(self.index) == 0) or (data_idx >= self.index[-1]):
            self.index.append(data.get(self.index_line))
            for line in self.tracked:
                self[line].append(data.get(line))
        else:
            # much slower, try to avoid intermediate inserts.
            if len(self.index) > self.maxlen:
                self.index.popleft()
                for line in self.tracked:
                    line.popleft()
            insert_position = bisect.bisect_right(self.index, data_idx)
            self.index.insert(insert_position, data_idx)
            for line in self.tracked:
                self[line].insert(data.get(line))
        return

    def get_slice(self, beg = None, end = None, iloc = False, as_df = False):
        """Slices the data by the index with closed bounds on both ends.

        Args:
            beg (any, optional): Indicates where the beginning of the slice is.
                If `iloc`, this indicates the beginning as a position in the deque; 
                otherwise it indicates a point in self.index. If None, the slice starts
                at the beginning. Defaults to None.
            end (any, optional): Indicates where the end of the slice is.
                If `iloc`, this indicates the end as a position in the deque; 
                otherwise it indicates a point in self.index. If None, the slice ends
                at the end. Defaults to None.
            iloc (boolean): Indicates whether `beg` and `end` specify a position in the
                deque as opposed to a point in terms of self.index.
            as_df (boolean): If True, returns a pandas Dataframe. Defaults to False.

        Returns:
            dict[deque] or pd.DataFrame.
        """        
        if beg is None:
            beg_position = 0
        else:
            if iloc:
                beg_position = beg
            else:
                beg_position = bisect.bisect_left(self.index, beg)
        
        if end is None:
            end_position = len(self.index)
        else:
            if iloc:
                end_position = end
            else:
                end_position = bisect.bisect_right(self.index, end)
        
        partial_copy = {}
        partial_copy[self.index_line] = list(itertools.islice(self.index, beg_position, end_position))
        for line in self.tracked:
            partial_copy[line] = list(itertools.islice(self[line], beg_position, end_position))
        
        if as_df:
            partial_copy = pd.DataFrame(partial_copy)
        
        return partial_copy
    
    def filter(self, func, as_df = False):
        """Filters the data by one or more conditions.

        Args:
            func (callable or dict[callable]): Used to create a mask to filter the data.
                If a callable is provided, expect pandas.DataFrame.apply method, where
                    the callable is called on each row of data and columns are accessible
                    as dictionary elements.
                    In this case, a pandas DataFrame is returned and the `as_df argument`
                    is ignored.
                    This method is much slower than providing a dict of callables as it involves
                    constructing the dataframe and applying DF.apply, which performs calculations
                    in a row-wise manner.
                If a dict of callable is provided, each callable is called on the indicated
                    dataline.

            as_df (boolean): If True, returns a pandas Dataframe. Defaults to False.

        Returns:
            dict[deque] or pd.DataFrame.
        """        
        # create the mask
        if callable(func):
            tempdf = self.as_pandas_df()
            tempdf = tempdf.loc[tempdf.apply(func, axis = 1)]
            return tempdf
        elif isinstance(func, dict):
            masks = []
            for k, f in func.items():
                if k == self.index_line:
                    masks.append(map(f, self.index))
                else:
                    masks.append(map(f, self[k]))
            mask = [all(m) for m in zip(*masks)]
        else:
            raise NotImplementedError('func must be either a callable or a dict of callables.')

        # apply compress to data
        filtered_copy = {}
        filtered_copy[self.index_line] = list(itertools.compress(self.index, mask))
        for line in self.tracked:
            filtered_copy[line] = list(itertools.compress(self[line], mask))

        if as_df:
            filtered_copy = pd.DataFrame(filtered_copy).set_index(self.index_line)

        return filtered_copy
    
    def as_pandas_df(self, columns = None):
        """Returns the lines as a pandas DataFrame.

        Args:
            columns (iterable, optional): The columns to return. If None, returns all columns.
                Defaults to None.

        Returns:
            pd.DataFrame.
        """        
        columns = columns or self.keys()
        df = pd.DataFrame(data = {k: v for k, v in self.items() if k in columns}, index = self.index)
        df.index.name = self.index_line
        return df

    def as_pandas_series(self, column = None):
        """Returns one line as a pandas Series.

        Args:
            column (str, optional): The column to return. If None, returns self.mark.
                Defaults to None.

        Returns:
            pd.Series.
        """       
        column = column or self.mark_line
        s = pd.Series(self[column], index = self.index)
        s.index.name = self.index_line
        return s