'''
A session coordinates instances of events, brokers, and strategies.
'''
import threading
import socket
import time
import zmq
import msgpack

from ..utils.util_functions import get_free_tcp_address, get_inproc_address, default_conversion

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
            datafeed.start_sync = self.datafeed_start_sync
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

        time.sleep(0.1)
        # print('shutting down...')
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

def proxy(address_data_backend, address_data_frontend, address_broker_backend, address_broker_frontend, sync_key = 'EVENT_TS',\
            zmq_context = None, shutdown_flag = None, default_broker = None, data_capture_address = None, broker_capture_address = None):
    '''
        Relays messages from backend (address_in) to frontend (address_out),
        so strategies can have one central place to subscribe data from.

        Note that we are binding on the subscriber end because we have a
        multiple publisher (datafeeds) - one subscriber (session) pattern
    '''

    # set up context
    context = context or zmq.Context.instance()

    # datafeed proxy
    # ---------------------------------------------------------------
    # datafeed facing socket
    data_backend = context.socket(zmq.SUB)
    # no filtering here
    data_backend.setsockopt(zmq.SUBSCRIBE, b'')
    data_backend.bind(address_in)

    # strategy facing socket
    data_frontend = context.socket(zmq.PUB)
    data_frontend.bind(address_out)
    
    # if there is a capture socket
    if data_capture_address:
        # bind to capture address
        data_capture = context.socket(zmq.PUB)
        data_capture.bind(data_capture_address)
    else:
        data_capture = None


    # broker proxy
    # ---------------------------------------------------------------
    # broker facing socket
    broker_backend = context.socket(zmq.ROUTER)
    broker_backend.bind(address_backend)
    # strategy facing socket
    broker_frontend = context.socket(zmq.ROUTER)
    broker_frontend.bind(address_broker_frontend)

    # if there is a capture socket
    if broker_capture_address:
        # bind to capture address
        broker_capture = context.socket(zmq.PUB)
        broker_capture.bind(broker_capture_address)
    else:
        broker_capture = None

    # the event queue
    next_events = {}
    # socket orders for ties
    dict_tiebreaker = {'broker_frontend': 0, 'broker_backend': 1, 'data_backend': 2}

    while not shutdown_flag.is_set():
        
        try:

            # If any slot us empty, try to fill it
            for name, sock in zip(['data_backend', 'broker_backend', 'broker_frontend'],
                                    [data_backend, broker_backend, broker_frontend]):
                if next_events.get(name) is None:
                    try:
                        envelope_encoded, event_packed = sock.recv_multipart(zmq.NOBLOCK)
                        next_events[name] = (envelope_encoded, event_packed)
                    except:
                        zmq.ZMQError as exc:
                        if exc.errno == zmq.EAGAIN:
                            # nothing to get
                            pass
                        else:
                            shut_down()
                            raise                   

            # sort the events
            # take the first item (socket name) of the first item ((socket name, event)) of the sorted queue
            next_socket = sorted(next_events.items(), key = lambda x: (x[1][sync_key], dict_tiebreaker[x[0]]))[0][0]
            next_event = next_events.pop(next_socket)

            # emit the event
            if next_socket == 'data_backend':
                # if it is data, just pass it on
                data_frontend.send_multipart(next_event[0], next_event[1])
            elif next_socket == 'broker_backend':
                # if it is an order from backend, need to unpack it
                # and figure out which strategy to send it to
                order_packed = next_event[1]
                order_unpacked = msgpack.unpackb(next_event[1])
                # retrieve sender id from order
                strategy_id_encoded = order_unpacked[c.SENDER_ID].encode('utf-8')
                # send the order to frontend
                broker_frontend.send_multipart([strategy_id_encoded, order_packed])
            elif next_socket == 'broker_frontend':
                # if it is an order from front end, need to unpack it
                # and figure out which broker to send it to
                order_packed = next_event[1]
                order_unpacked = msgpack.unpackb(next_event[1])
                # add sender id to order as a string
                order_unpacked[c.SENDER_ID] = next_event[0].decode('utf-8')
                # repack order
                order_packed = msgpack.packb(order_unpacked, use_bin_type = True, default = default_conversion)
                # determine broker to send to
                if (broker := order.get(c.BROKER)) is not None:
                    broker = broker
                else:
                    broker = default_broker
                # send the order to backend
                broker_backend.send_multipart(broker.encode('utf-8'), order_packed)
        
        except (zmq.ContextTerminated, zmq.ZMQError):
            # Not sure why it's not getting caught by ContextTerminated
            shutdown()

    shutdown()


    def shutdown():
        data_backend.close(linger =10)
        data_frontend.close(linger =10)
        broker_backend.close(linger =10)
        broker_frontend.close(linger =10)