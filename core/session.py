'''
A session coordinates instances of events, brokers, and strategies.
'''
import threading
import socket
import time
import zmq

from ..utils.util_functions import get_free_tcp_address

# from . import data

class Session:
    def __init__(self):
        self.strategies = []    # Strategy instances
        self.strategy_threads = []
        self.datafeeds = []     # datafeed instances, must have method "run"
        self.datafeed_threads = []
        self.data_address = None   
        self.zmq_context = zmq.Context()

        # datafeed proxy params
        self.datafeed_address_in = None    # datafeeds publish to this address
        self.datafeed_address_out = None   # strategies subscribe to this address for datafeeds
        self.datafeed_capture = None

        self.main_shutdown_flag = threading.Event()
    
    def run(self):

        # # Start brokers
        # self.bob.start()

        # # self.console.is_alive?

        # Find ports that are available
        # TODO: take care of race conditions where someone else grabs that port in between us finding it and binding to it
        self.datafeed_address_in, host_in, port_in = get_free_tcp_address()
        self.datafeed_address_out, host_out, port_out = get_free_tcp_address(exclude = (self.datafeed_address_in,))   # want to avoid taking datafeed_address_in (it is "free" right now)

        # Start proxy to relay datafeeds
        self.proxy_thread = threading.Thread(target = proxy, args = (self.datafeed_address_in, self.datafeed_address_out, self.datafeed_capture, self.zmq_context, self.main_shutdown_flag))
        self.proxy_thread.start()

        # Start strategies
        for strategy in self.strategies:
            strategy.connect_data_socket(self.datafeed_address_out)
            strategy_thread = threading.Thread(target = strategy.run)
            strategy_thread.daemon = True
            self.strategy_threads.append(strategy_thread)
            strategy_thread.start()

        # Start datafeeds
        for datafeed in self.datafeeds:
            # Tell the feeds where to publish to
            datafeed.publish_to(self.datafeed_address_in)
            # set the shutdown flag
            datafeed.shutdown_flag = self.main_shutdown_flag
            datafeed_thread = threading.Thread(target = datafeed.publish)
            datafeed_thread.daemon = True
            self.datafeed_threads.append(datafeed_thread)

        for data_thread in self.datafeed_threads:
            data_thread.start()

        # wait for datafeeds to finish
        for data_thread in self.datafeed_threads:
            data_thread.join()

        # exit gracefully
        self.shutdown()

    def add_strategy(self, strategy):
        '''add strategies to the session'''
        self.strategies.append(strategy)
    
    def add_datafeed(self, datafeed):
        '''add data feeds'''
        self.datafeeds.append(datafeed)

    # def kill_strategy(self):
    #     '''Kill a strategy'''
    #     pass

    def shutdown(self):
        # exiting gracefully
        self.main_shutdown_flag.set()

        # tell strategies to stop
        for strategy in self.strategies:
            strategy.stop()
        
        # wait for the strategies to exit clean
        for strategy_thread in self.strategy_threads:
            strategy_thread.join()

        # wait a second before terminating the context
        time.sleep(1)
        self.zmq_context.term()


def proxy(address_in, address_out, capture = None, context = None, shutdown_flag = None):
    '''
        Relays messages from backend (address_in) to frontend (address_out),
        so strategies can have one central place to subscribe data from.

        Note that we are binding on the subscriber end because we have a
        multiple publisher (datafeeds) - one subscriber (session) pattern
    '''
    # try:
    context = context or zmq.Context.instance()

    # publisher facing socket
    backend = context.socket(zmq.SUB)
    # no filtering here
    backend.setsockopt(zmq.SUBSCRIBE, b'')
    backend.bind(address_in)
    
    

    # client facing socket
    frontend = context.socket(zmq.PUB)
    frontend.bind(address_out)
    
    if capture:
        # bind to capture address
        capture_socket = context.socket(zmq.PUB)
        capture_socket.bind(capture)
    else:
        capture_socket = None

    try:
        zmq.proxy(frontend, backend, capture_socket)
    except zmq.ContextTerminated:
        backend.close(linger = 10)
        frontend.close(linger = 10)
    
    # exit gracefully
    if shutdown_flag:
        backend.close(linger = 10)
        frontend.close(linger = 10)

