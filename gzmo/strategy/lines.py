class lines(dict):
    """
    A lines object is a collection of time series (deques) belonging to one asset (symbol).
    
    The lines object will keep track of fields from data events, as specified in the __init__
        method. `include_only` and `exclude` cannot be modified after initialization to keep
        all deques in sync.
    If `include_only` is None (default), the first data event will determine what
        fields are tracked.

    Args:
        symbol (str): An identifier for the underlying data. For reference only.
        data_type (str): The type of data (e.g. TICK, BAR, QUOTE, SIGNAL, STRATEGY).
        resolution (str): The resolution of data (e.g. D, h, m, s).
        index_line (str): The line to use as index. Default 'EVENT_TS'.
        mark_line (str): The line to use as mark. Default 'CLOSE'.
        tracked (None or set, optional): Track these fields in a data event. Defaults to OHLCV.
        maxlen (int, optional): The number of datapoints to keep. If None, keep all data. Default None.
        
    """
    def __init__(self, symbol, data_type = c.BAR, resolution = 'm', index_line = c.EVENT_TS, mark_line = c.CLOSE, tracked = None, maxlen = None):       
        
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
        return self[self.mark_line]
    
    def __len__(self):
        if len(self.keys()) == 0:
            return 0
        else:
            return len(self.index)

    def update_with_data(self, data):
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

    def get_slice(self, beg = None, end = None, as_df = False):
        if beg is None:
            beg_position = 0
        else:
            beg_position = bisect.bisect_left(self.index, beg)
        if end is None:
            end_position = len(self.index)
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
        # create the mask
        if callable(func):
            mask = list(map(func, self.index))
        elif isinstance(func, dict):
            masks = []
            for k, f in func.items():
                masks.append(map(f, self[k]))
            mask = [all(m) for m in zip(*masks)]
        else:
            raise NotImplementedError('func must be either a callable to be applied onto the index, or a dict of callables.')

        # apply compress to data
        filtered_copy = {}
        filtered_copy[self.index_line] = list(itertools.compress(self.index, mask))
        for line in self.tracked:
            filtered_copy[line] = list(itertools.compress(self[line], mask))

        if as_df:
            filtered_copy = pd.DataFrame(filtered_copy).set_index(self.index_line)

        return filtered_copy
    
    def as_pandas_df(self, columns = None):
        columns = columns or self.keys()
        df = pd.DataFrame(data = {k: v for k, v in self.items() if k in columns}, index = self.index)
        df.index.name = self.index_line
        return df

    def as_pandas_series(self, column = None):
        column = column or self.mark_line
        s = pd.Series(self[column], index = self.index)
        s.index.name = self.index_line
        return s