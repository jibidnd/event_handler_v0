'''
A session coordinates instances of events, brokers, and strategies.
'''
import threading

import zmq

# from . import data

class Session:
    def __init__(self):
        self.strategies = []    # Strategy instances
        self.datafeeds = []     # tuple(topic, datafeed generator)
        self.data_address = None   
        self.zmq_context = zmq.Context()
    
    def run(self):

        # Start brokers
        self.bob.start()

        # Start strategies
        for strategy in self.strategies:
            strategy.run()

        

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
        

    def add_strategies(self, *strategies):
        '''add strategies to the session'''
        for strategy in strategies:
            self.strategies.append(strategy)
    
    def add_datafeed(self, topic, datafeed):
        '''add data feeds'''
        self.datafeeds.append((topic, datafeed))

    # def kill_strategy(self):
    #     '''Kill a strategy'''
    #     pass

    def kill(self):
        pass


def proxy(address_in, address_out, capture = None, context = None):

    try:
        context = context or zmq.Context.instance()

        # publisher facing client
        backend = context.socket(zmq.PUB)
        backend.bind(address_in)

        # socket facing client
        frontend = context.socket(zmq.SUB)
        frontend.bind(address_out)
        # no filtering here
        frontend.setsockopt(zmq.SUBSCRIBE, b'')

        # make socket if capture is an address
        if isinstance(capture, str):
            try:
                capture_socket = context.socket(zmq.PUB)
                capture_socket.bind(capture)
            except:
                raise
        else:
            capture_socket = capture

        zmq.proxy(frontend, backend, capture_socket)
    
    except Exception as exc:
        print(e)
    finally:
        frontend.close()
        backend.close()
        context.term()
