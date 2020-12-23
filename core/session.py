'''
A session coordinates instances of events, brokers, and strategies.
'''
import threading
import socket
import time
import zmq
import msgpack

from ..utils.util_functions import get_free_tcp_address, get_inproc_address

from . import constants as c

class Session:
    def __init__(self):

        # Use one context for all communication (thread safe)
        self.zmq_context = zmq.Context()

        # instances and threads
        # Strategies
        self.strategies = []        # Strategy instances
        self.strategy_threads = []
        # Datafeeds
        self.datafeeds = []         # datafeed instances, must have method "run"
        self.datafeed_threads = []
        self.datafeed_start_sync = threading.Event()
        # Brokers
        self.brokers = []
        self.broker_threads = []


        # Proxies
        self.proxy_threads = []
        self.data_address = None   

        # datafeed proxy params
        self.datafeed_publisher_address = None     # datafeeds publish to this address
        self.datafeed_subscriber_address = None    # strategies subscribe to this address for datafeeds
        self.datafeed_capture = None

        # broker proxy parans
        self.broker_broker_address = None   # brokers talk to this address. This will be a router socket.
        self.broker_strategy_address = None # strategies talk to this address. This will be a dealer socket.
        self.broker_capture = None
        
        # main shutdown flag
        self.main_shutdown_flag = threading.Event()
    
    def run(self):

        # Establish proxies to relay datafeed and broker messages
        # TODO: take care of race conditions where someone else grabs that port in between us finding it and binding to it

        # addresses to avoid
        # want to avoid taking datafeed_publisher_address (it is "free" right now because it's been released by get_free_tcp_address)
        addresses_used = []

        # # datafeed in
        self.datafeed_publisher_address, _, _ = get_free_tcp_address(exclude = addresses_used)
        addresses_used.append(self.datafeed_publisher_address)
        # datafeed out
        self.datafeed_subscriber_address, _, _ = get_free_tcp_address(exclude = addresses_used)
        addresses_used.append(self.datafeed_subscriber_address)
        # BOB to starategies
        self.broker_strategy_address, _, _ = get_free_tcp_address(exclude = addresses_used)
        addresses_used.append(self.broker_strategy_address)
        # BOB to brokers
        self.broker_broker_address, _, _ = get_free_tcp_address(exclude = addresses_used)
        addresses_used.append(self.broker_broker_address)

        # inproc was rather unstable for some reason.
        # self.datafeed_publisher_address = get_inproc_address(exclude = addresses_used)
        # addresses_used.append(self.datafeed_publisher_address)
        # # datafeed out
        # self.datafeed_subscriber_address = get_inproc_address(exclude = addresses_used)
        # addresses_used.append(self.datafeed_subscriber_address)
        # # BOB to starategies
        # self.broker_strategy_address = get_inproc_address(exclude = addresses_used)
        # addresses_used.append(self.broker_strategy_address)
        # # BOB to brokers
        # self.broker_broker_address = get_inproc_address(exclude = addresses_used)
        # addresses_used.append(self.broker_broker_address)


        # Add datafeed proxy to proxy threads
        self.proxy_threads.append(threading.Thread(target = pub_sub_proxy, args = (self.datafeed_publisher_address, self.datafeed_subscriber_address, 
                                                                                    self.datafeed_capture, self.zmq_context, self.main_shutdown_flag)))
        # Add broker proxy to proxy threads
        default_broker = self.brokers[0].name if len(self.brokers) > 0 else None
        self.proxy_threads.append(threading.Thread(target = broker_proxy, args = (self.broker_strategy_address, self.broker_broker_address,
                                                                                    self.broker_capture, self.zmq_context, self.main_shutdown_flag, default_broker)))

        # Start the proxies
        for proxy_thread in self.proxy_threads:
            proxy_thread.start()

        # Start datafeeds
        for datafeed in self.datafeeds:
            datafeed.zmq_context = self.zmq_context
            datafeed.publish_to(self.datafeed_publisher_address)
            datafeed.shutdown_flag = self.main_shutdown_flag
            # datafeed will wait for a signcal to all start together
            datafeed.start_sync.clear()
            datafeed_thread = threading.Thread(target = datafeed.publish)
            datafeed_thread.daemon = True
            self.datafeed_threads.append(datafeed_thread)
            # Start the datafeed. Publishing will be blocked until datafeed_start_sync is set.
            datafeed_thread.start()

        # Start brokers
        for broker in self.brokers:
            broker.zmq_context = self.zmq_context
            broker.connect_data_socket(self.datafeed_subscriber_address)
            broker.connect_order_socket(self.broker_broker_address)
            broker.main_shutdown_flag = self.main_shutdown_flag
            broker_thread = threading.Thread(target = broker.run)
            broker_thread.daemon = True
            self.broker_threads.append(broker_thread)
            broker_thread.start()

        # Start strategies
        # TODO: what if we want to multiprocess?
        for strategy in self.strategies:
            strategy.zmq_context = self.zmq_context
            strategy.connect_data_socket(self.datafeed_subscriber_address)
            strategy.connect_order_socket(self.broker_strategy_address)
            strategy_thread = threading.Thread(target = strategy.run)
            strategy_thread.daemon = True
            self.strategy_threads.append(strategy_thread)
            strategy_thread.start()


        # tell datafeeds to start publishing
        self.datafeed_start_sync.set()

        # wait for datafeeds to finish
        for data_thread in self.datafeed_threads:
            data_thread.join()

        time.sleep(1)
        print('shutting down...')
        # exit gracefully
        self.shutdown()

    def add_strategy(self, strategy):
        '''add strategies to the session'''
        self.strategies.append(strategy)
    
    def add_datafeed(self, datafeed):
        '''add data feeds'''
        self.datafeeds.append(datafeed)

    def add_broker(self, broker):
        '''add a broker to the session'''
        self.brokers.append(broker)

    # def kill_strategy(self):
    #     '''Kill a strategy'''
    #     pass

    def shutdown(self, linger = 0.1):
        # exiting gracefully
        self.main_shutdown_flag.set()

        # tell strategies to stop
        for strategy in self.strategies:
            strategy.stop()
        
        # wait for the strategies to exit clean
        for strategy_thread in self.strategy_threads:
            strategy_thread.join()

        # wait a second before terminating the context
        time.sleep(linger)
        self.zmq_context.destroy()


def pub_sub_proxy(address_in, address_out, capture = None, context = None, shutdown_flag = None):
    '''
        Relays messages from backend (address_in) to frontend (address_out),
        so strategies can have one central place to subscribe data from.

        Note that we are binding on the subscriber end because we have a
        multiple publisher (datafeeds) - one subscriber (session) pattern
    '''

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

    # start the proxy
    try:
        zmq.proxy(frontend, backend, capture_socket)
    except (zmq.ContextTerminated, zmq.ZMQError): # Not sure why it's not getting caught by ContextTerminated
        backend.close(linger = 10)
        frontend.close(linger = 10)
    except:
        frontend.close(linger = 10)
        backend.close(linger = 10)
        raise
    
    # exit gracefully
    if shutdown_flag:
        backend.close(linger = 10)
        frontend.close(linger = 10)


def broker_proxy(address_frontend, address_backend, capture = None, context = None, shutdown_flag = None, default_broker = None):
    '''broker of brokers
    
        The flow or order is as follow:

        strategy: dealer: sends order

        proxy: router: receives (strategy ident, order)
        proxy extracts broker name from order
        proxy adds the field SENDER_ID to the order
        proxy: router: sends (broker ident, order)

        broker: dealer: receives order
        broker processes order
        broker: dealer: sends back order

        proxy: router receives (broker ident, order)
        proxy extract sender ident from order
        proxy: router sends (strategy ident, order)

        strategy: dealer: receives order
    
    '''
    context = context or zmq.Context.Instance()

    # establish strategy facing socket
    frontend = context.socket(zmq.ROUTER)
    frontend.bind(address_frontend)
    # establish broker facing socket
    backend = context.socket(zmq.ROUTER)
    backend.bind(address_backend)
    # if there is a capture socket
    if capture:
        # bind to capture address
        capture_socket = context.socket(zmq.PUB)
        capture_socket.bind(capture)
    else:
        capture_socket = None

    poller = zmq.Poller()
    poller.register(frontend, zmq.POLLIN)
    poller.register(backend, zmq.POLLIN)

    while not shutdown_flag.is_set():

        try:
            socks = dict(poller.poll(timeout = 10))
            
            if socks.get(frontend) == zmq.POLLIN:
                # received order from strategy: (strategy ident, order)
                strategy_id, order = frontend.recv_multipart()
                order = msgpack.unpackb(order)
                order[c.SENDER_ID] = strategy_id
                # find out which broker to send to
                if (broker := order.get(c.BROKER)) is not None:
                    broker = broker
                else:
                    broker = default_broker

                if broker is None:
                    raise Exception('Either specify broker in order or specify default broker in session.')
                
                # send the order to the broker: (broker name, )
                backend.send_multipart([broker.encode('utf-8'), msgpack.packb(order)])

            elif socks.get(backend) == zmq.POLLIN:
                # received order from broker: (broker ident, (strategy ident, order))
                broker, order = backend.recv_multipart()
                order_unpacked = msgpack.unpackb(order)
                strategy_id = order_unpacked[c.SENDER_ID]
                frontend.send_multipart([strategy_id, order])

        except (zmq.ContextTerminated, zmq.ZMQError): # Not sure why it's not getting caught by ContextTerminated
            frontend.close(linger = 10)
            backend.close(linger = 10)
        except:
            frontend.close(linger = 10)
            backend.close(linger = 10)
            raise

    # exit gracefully
    if shutdown_flag.is_set():
        frontend.close(linger = 10)
        backend.close(linger = 10)




