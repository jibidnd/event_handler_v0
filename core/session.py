'''
A session coordinates instances of events, brokers, and strategies.
'''
import threading

import zmq

from . import data

class Session:
    def __init__(self):
        self.strategies = []    # Strategy instances
        self.datafeeds = []     # tuple(topic, datafeed generator)
        self.data_address = None   
        self.zmq_context = zmq.Context()
    
    def run(self):
        # Start strategies
        for strategy in self.strategies:
            strategy.start()

        self.bob.start()

        # self.console.is_alive?

        # Start datafeeds
        datafeed_threads = []
        for topic, datafeed in self.datafeeds:
            datafeed_threads.append(threading.Thread(target = data.publish, args = (self.data_address, topic, datafeed, zmq_context)), daemon = True)

        for data_thread in datafeed_threads:
            data_thread.start()
            

        # TODO
        # What to do?

        # start all strategies and make sure they're running
        # start bob and make sure it is running
        # ping console
        # start data
        

    def add_strategies(self, strategy):
        '''add strategies to the session'''
        self.strategies.append(strategy)
    
    def add_datafeed(self, topic, datafeed):
        '''add data feeds'''
        self.datafeeds.append((topic, datafeed))

    # def kill_strategy(self):
    #     '''Kill a strategy'''
    #     pass