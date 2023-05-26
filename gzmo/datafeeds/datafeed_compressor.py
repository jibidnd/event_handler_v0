# # WIP
# import decimal

# import zmq

# from . import BaseDataFeed
# from .. import utils
# from ..utils import constants as c


# class DatafeedCompressor(BaseDataFeed):
    
#     def __init__(self, datafeed, size, compressor, zmq_context = None):
#         """Takes a datafeed and combine multiple data points into one.

#         Args:
#             datafeed (iterable): Datafeed to compress.
#             compressor (object): An object that takes datapoints and spits out a compressed datapoint when ready.
#         """
#         super().__init__(None, zmq_context)
#         self.datafeed = datafeed
#         # assert hasattr(compressor, 'process') and callable(getattr(compressor, 'process')), \
#         #     'compressor must have process method.'
#         # assert hasattr(compressor, 'reset') and callable(getattr(compressor, 'reset')), \
#         #     'compressor must have reset method.'
#         self.size = size
#         self.compressor = compressor
        

#     def execute_query(self):
#         """Execute the query and get ready to emit data."""      
#         self.datafeed.execute_query()
#         self.from_beginning = False

#     def fetch(self, limit = 1):
#         """Return the requested number of records.

#             If self.from_beginning is True (as it is set when the datafeed is instantiated),
#             the query will be executed when this method is called.


#         Args:
#             limit (int, optional): The number of records to return. Defaults to 1.

#         Returns:
#             list[dict] or dict: The queried data as record(s).
#         """    
#         # If need to get results from scratch
#         if self.from_beginning:
#             self.execute_query()
        
#         counter = 0
#         d_res = []

#         while counter < self.size:
            
#             if not self.datafeed.is_finished:
#                 if (datapoint := self.datafeed.fetch(1)) is not None:
#                     if (res := self.compressor.process(datapoint)) is not None:
#                         results.append(res)
#                         counter += 1
#                         self.compressor.reset()
        
#         if len(results) == 0:
#             self.is_finished = True
#             return

#         if limit > 1:
#             return results
#         elif limit == 1:
#             return results[0]

# class compressor:
#     def __init__(self, target_count, combiner):
#         self.target_count = target_count
#         self.combiner = combiner
#         self.reset()
    
#     def reset(self):
#         self.temp_res = {}
#         self.count = 0
    
#     def process(self, event):
#         self.temp_res = self.combiner(self.temp_res, event)
#         self.count += 1
#         if self.count >= self.target_count:
#             return self.temp_res
#         return


# def combine_bars(orig_bar, new_bar):
#     res = {**new_bar}
#     for key in [c.OPEN, c.HIGH, c.LOW, c.CLOSE, c.VOLUME]:
#         res[key] = (res.get(key) or decimal.Decimal(0.0)) + (orig_bar.get(key) or decimal.Decimal(0.0))
#     return res