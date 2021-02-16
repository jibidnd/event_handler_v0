'''
A session coordinates instances of events, brokers, and strategies.
'''
from collections import deque
import threading
import queue
import operator
import time
import zmq

from . import utils
from .utils import constants as c
from .datafeeds.datafeed_synchronizer import DatafeedSynchronizer

class Session:
    def __init__(self, socket_mode = None):

        self.socket_mode = socket_mode

        # Use one context for all communication (thread safe)
        self.zmq_context = zmq.Context()

        # instances and threads
        # Strategies
        self.strategies = {}                        # Strategy instances
        self.strategy_threads = []

        # Datafeeds
        self.datafeeds = []                         # datafeed instances, must have method "run"
        self.datafeed_threads = []
        self.synced_datafeed = None
        self.data_subscriber_socket = None          # if syncing, the session will act as a datafeed

        self.datafeed_start_sync = threading.Event()
        self.datafeed_publisher_address = None      # datafeeds publish to this address
        self.datafeed_subscriber_address = None     # strategies subscribe to this address for datafeeds
        self.datafeed_capture_address = None        # not implemented
        
        # Brokers
        self.brokers = {}                           # name: broker instance
        self.broker_threads = []
        self.broker_strategy_socket = None          # if syncing, the session will act as a broker

        self.broker_broker_address = None           # brokers talk to this address. This will be a router socket.
        self.broker_strategy_address = None         # strategies talk to this address. This will be a dealer socket.
        self.broker_capture_address = None          # not implemented
        self.order_deque = deque()
        self.order_socket_emulator = SocketEmulator(self.order_deque)

        # Communication channel
        self.communication_address = None
        self.communication_deque = deque()
        self.communication_socket_emulator = SocketEmulator(self.communication_deque)
        self.communication_capture_address = None

        # Proxies
        self.proxy_threads = []
        
        # main shutdown flag
        self.main_shutdown_flag = threading.Event()
    
    def run(self, socket_mode = c.NONE):
        """Whether/how to use sockets for this session.

        Args:
            socket_mode (str, optional): Whether to use sockets.
                If ALL:
                    - The `publish` method of each datafeed is called in a separate thread,
                        and each datafeed publish independently to the session's pub_sub_proxy,
                        which relays messages in an unsynced manner.
                    - The `run` method of each broker is called in a separate thread,
                        and each broker communicates with strategies via the session's broker_proxy,
                        which relays messges in an unsynced manner.
                    - Strategies send/receive events via their respectiv ZMQ sockets.
                If STRATEGIES_FULL:
                    - The `fetch` method of each datafeed is called in the main thread.
                        Data events are consolidated and synced before being sent out from the session's data socket.
                    - The `take_order` and `try_fill_with_data` methods of each broker is called in the main thread.
                        Order events are consolidated and synced before being relayed through the session's order socket.
                    - The behaviour of strategies is the same as if `socket_mode` = c.ALL.
                If STRATEGIES_INTERNALONLY:
                    - Strategies continue to communicate among themselves via sockets
                    - All communications from the session to strategies, order or data, will be directly handled via the `_handle_event` method
                        of the strategies.
                If NONE:
                    - Strategies will send communication and orders to a queue, which will then be processed accordingly.
                    - Between data events, any actions started by any communication (including subsequent communication) will be resolved completely
                Defaults to NONE.
        """

        self.socket_mode = socket_mode
        
        self.setup_addresses(self.socket_mode)

        self.setup_proxies(self.socket_mode)

        self.setup_datafeeds(self.socket_mode)

        self.setup_brokers(self.socket_mode)

        self.setup_strategies(self.socket_mode)

        self.start()

        time.sleep(1)

        # exit gracefully
        self.shutdown()

        return

    def add_strategy(self, strategy):
        '''add strategies to the session
            Note that all child strategies should be added to the session as well.
        '''
        self.strategies[strategy.strategy_id] = strategy
    
    def add_datafeed(self, datafeed):
        '''add data feeds'''
        self.datafeeds.append(datafeed)

    def add_broker(self, broker):
        '''add a broker to the session'''
        self.brokers[broker.name] = broker

    def setup_addresses(self, socket_mode):

        # addresses to avoid
        # want to avoid taking datafeed_publisher_address (it is "free" right now because it's been released by get_free_tcp_address)
        # inproc was found to be rather unstable for some reason, so we stick with tcp.
        addresses_used = []

        if socket_mode == c.ALL:

            # broker backend
            self.broker_broker_address, _, _ = utils.get_free_tcp_address(exclude = addresses_used)
            addresses_used.append(self.broker_broker_address)

            # broker frontend
            self.broker_strategy_address, _, _ = utils.get_free_tcp_address(exclude = addresses_used)
            addresses_used.append(self.broker_strategy_address)
            
            # datafeed backend
            self.datafeed_publisher_address, _, _ = utils.get_free_tcp_address(exclude = addresses_used)
            addresses_used.append(self.datafeed_publisher_address)

            # datafeed frontend
            self.datafeed_subscriber_address, _, _ = utils.get_free_tcp_address(exclude = addresses_used)
            addresses_used.append(self.datafeed_subscriber_address)

            # communication channel
            self.communication_address, _, _ = utils.get_free_tcp_address(exclude = addresses_used)
            addresses_used.append(self.communication_address)
        
        elif socket_mode == c.STRATEGIES_FULL:

            # broker frontend
            self.broker_strategy_address, _, _ = utils.get_free_tcp_address(exclude = addresses_used)
            addresses_used.append(self.broker_strategy_address)

            # datafeed frontend
            self.datafeed_subscriber_address, _, _ = utils.get_free_tcp_address(exclude = addresses_used)
            addresses_used.append(self.datafeed_subscriber_address)

            # communication channel
            self.communication_address, _, _ = utils.get_free_tcp_address(exclude = addresses_used)
            addresses_used.append(self.communication_address)
        
        elif socket_mode == c.STRATEGIES_INTERNALONLY:
            
            # communication channel
            self.communication_address, _, _ = utils.get_free_tcp_address(exclude = addresses_used)
            addresses_used.append(self.communication_address)
        
        elif socket_mode == c.NONE:
            
            pass
        
        else:
            
            raise NotImplementedError(f'socket_mode {socket_mode} not implemented')

        return

    def setup_proxies(self, socket_mode):

        # clear proxies
        self.proxy_threads = []

        if socket_mode == c.ALL:
            self.proxy_threads.append(threading.Thread(target = pub_sub_proxy, args = (self.datafeed_publisher_address, self.datafeed_subscriber_address, 
                                                                                        self.datafeed_capture_address, self.zmq_context, self.main_shutdown_flag)))
            # Add broker proxy to proxy threads
            default_broker = next(iter(self.brokers.values())).name if len(self.brokers) > 0 else None
            self.proxy_threads.append(threading.Thread(target = broker_proxy, args = (self.broker_strategy_address, self.broker_broker_address,
                                                                                        self.broker_capture_address, self.zmq_context, self.main_shutdown_flag, default_broker)))
            # Add communication_proxy to proxy threads
            self.proxy_threads.append(threading.Thread(target = communication_proxy, args = (self.communication_address, self.communication_capture_address, self.zmq_context, self.main_shutdown_flag)))

            # Start the proxies
            for proxy_thread in self.proxy_threads:
                proxy_thread.start()
        
        elif socket_mode == c.STRATEGIES_FULL:
            pass
        elif socket_mode == c.STRATEGIES_INTERNALONLY:
            pass
        elif socket_mode == c.NONE:
            pass
        else:
            raise NotImplementedError(f'socket_mode {socket_mode} not implemented')
        
        return

    def setup_datafeeds(self, socket_mode):

        if socket_mode == c.ALL:
            # set up the datafeeds for publishing
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

        # If not running in full socket mode, set up the synced datafeed
        elif socket_mode == c.STRATEGIES_FULL:
            # if there is more than one datafeed, combine them with DatafeedSynchronizer
            if len(self.datafeeds) > 1:
                self.synced_datafeed = DatafeedSynchronizer(datafeeds = self.datafeeds)
            # else just use the datafeed as the sole datafeed
            elif len(self.datafeeds) == 1:
                self.synced_datafeed = self.datafeeds[0]
            else:
                raise Exception('No datafeed to set up.')
            
            self.data_subscriber_socket = self.zmq_context.socket(zmq.PUB)
            self.data_subscriber_socket.bind(self.datafeed_subscriber_address)

        elif socket_mode in [c.STRATEGIES_INTERNALONLY, c.NONE]:

            # if there is more than one datafeed, combine them with DatafeedSynchronizer
            if len(self.datafeeds) > 1:
                self.synced_datafeed = DatafeedSynchronizer(datafeeds = self.datafeeds)
            # else just use the datafeed as the sole datafeed
            elif len(self.datafeeds) == 1:
                self.synced_datafeed = self.datafeeds[0]
            else:
                raise Exception('No datafeed to set up.')

        else:
            raise NotImplementedError(f'socket_mode {socket_mode} not implemented')
        
        return

    def setup_brokers(self, socket_mode):
        

        # If running in full socket mode, set up the brokers
        if socket_mode == c.ALL:
            for broker in self.brokers.values():
                broker.zmq_context = self.zmq_context
                broker.connect_data_socket(self.datafeed_subscriber_address)
                broker.connect_order_socket(self.broker_broker_address)
                broker.main_shutdown_flag = self.main_shutdown_flag
                broker_thread = threading.Thread(target = broker.run)
                broker_thread.daemon = True
                self.broker_threads.append(broker_thread)
                broker_thread.start()
        
        # if strategies require sockets, also set them up
        elif socket_mode == c.STRATEGIES_FULL:
            self.broker_strategy_socket = self.zmq_context.socket(zmq.ROUTER)
            self.broker_strategy_socket.bind(self.broker_strategy_address)
        
        elif socket_mode == c.STRATEGIES_INTERNALONLY:
            pass

        elif socket_mode == c.NONE:
            pass

        else:
            raise NotImplementedError(f'socket_mode {socket_mode} not implemented')

        return

    def setup_strategies(self, socket_mode):

        for strategy in self.strategies.values():
            strategy.zmq_context = self.zmq_context
            strategy.shutdown_flag = self.main_shutdown_flag

            if socket_mode in [c.ALL, c.STRATEGIES_FULL]:
                
                # receiving data
                strategy.connect_data_socket(self.datafeed_subscriber_address)
                strategy_thread = threading.Thread(target = strategy.run)
                strategy_thread.daemon = True
                self.strategy_threads.append(strategy_thread)
                
                # communication with broker
                strategy.connect_order_socket(self.broker_strategy_address)
                
                # communication with other strategies
                strategy.connect_communication_socket(self.communication_address)

                # tell the threads to start listening
                strategy_thread.start()
            
            elif socket_mode == c.STRATEGIES_INTERNALONLY:
                
                # Communication with broker
                strategy.order_socket = self.order_socket_emulator

                # communication with other strategies
                strategy.connect_communication_socket(self.communication_address)

            elif socket_mode == c.NONE:
                
                # Communication with broker
                strategy.order_socket = self.order_socket_emulator

                # connect to the socket emulator
                strategy.communication_socket = self.communication_socket_emulator
            
            else:
                raise NotImplementedError(f'socket_mode {socket_mode} not implemented')

        return

    def start(self):
        
        for strategy in self.strategies.values():
            strategy.start()

        if self.socket_mode == c.ALL:
        # tell datafeeds to start publishing
        # proxies, brokers, and strategies should all be ready to process events
            self.datafeed_start_sync.set()
            # wait for datafeeds to finishfor data_thread in self.datafeed_threads:
            for datafeed_thread in self.datafeed_threads:
                datafeed_thread.join()
        
        elif self.socket_mode in [c.STRATEGIES_FULL, c.STRATEGIES_INTERNALONLY, c.NONE]:

            # handle any communication event before start
            # e.g. passing cash, etc
            while True:
                try:
                    communication_event = self.communication_deque.popleft()
                    try:
                        receiver = communication_event[c.RECEIVER_ID]
                    except KeyError:
                        raise Exception('No receiver specified for communication event\n', communication_event)
                    self.strategies[receiver]._handle_event(communication_event)
                except IndexError:
                    # nothing else to handle
                    break
                except:
                    raise
            
            self.process_events(self.socket_mode)
        else:
            raise NotImplementedError(f'socket_mode {self.socket_mode} not implemented')

    def shutdown(self, linger = 0.1):
        # exiting gracefully
        self.main_shutdown_flag.set()

        # close the sockets, if any
        for socket in [self.broker_strategy_socket, self.data_subscriber_socket]:
            if (socket is not None) and (~socket.closed):
                socket.close(linger = 10)

        # tell strategies to stop
        for strategy in self.strategies.values():
            strategy.stop()
        
        # # wait for the strategies to exit clean
        # for strategy_thread in self.strategy_threads:
        #     strategy_thread.join()

        # wait a second before terminating the context
        time.sleep(linger)
        self.zmq_context.destroy()
    
    def process_events(self, socket_mode):
        '''
            This is equivalent to pausing time to process all orders.
            - router socket to strategies
            - publish socket to strategies
            - continuous loop:
                - broker handle orders from strategies; send any response
                - get data
                - send data
                - broker handle data; send any response
        '''
        # check socket type
        if socket_mode == c.ALL:
            raise NotImplementedError('socket_mode = "ALL" is not implemented in process_events.')
        elif socket_mode not in [c.STRATEGIES_FULL, c.STRATEGIES_INTERNALONLY, c.NONE]:
            raise NotImplementedError(f'socket_mode {socket_mode} not implemented')

        while not self.main_shutdown_flag.is_set():
            # STEP 1: BROKER HANDLE ORDERS
            # -----------------------------------------------------------------------------------------------------
            # try to get (all) orders from strategies
            # get from order socket if we're using sockets
            if socket_mode == c.STRATEGIES_FULL:
                while True:
                    try:
                        strategy_id_encoded, order_packed = self.broker_strategy_socket.recv_multipart(zmq.NOBLOCK)
                        order_unpacked = utils.unpackb(order_packed)
                        
                        # find a broker
                        if (broker := order_unpacked.get(c.BROKER)) is None:
                            broker = next(iter(self.brokers.values()))
                        # have broker handle the order
                        if (response := broker.take_order(order_unpacked)) is not None:
                            # prepare the response
                            response_packed = utils.packb(response)
                            original_sender = response[c.STRATEGY_CHAIN][-1]
                            original_sender_encoded = original_sender.encode('utf-8')
                            # send the response
                            msg = self.broker_strategy_socket.send_multipart([original_sender_encoded, response_packed], copy = False, track = True)
                            msg.wait()
                    
                    except zmq.ZMQError as exc:
                        if exc.errno == zmq.EAGAIN:
                            # nothing to get
                            break
                        else:
                            raise
            
            # otherwise orders will be in the order deque
            # handle in FIFO manner
            else:
                while True:
                    try:
                        order = self.order_deque.popleft()

                        # find a broker
                        if (broker := order.get(c.BROKER)) is None:
                            broker = next(iter(self.brokers.values()))
                        # have broker handle the order
                        if (response := broker.take_order(order)) is not None:
                            # have the strategy handle the response
                            original_sender = response[c.STRATEGY_CHAIN][-1]
                            self.strategies[original_sender]._handle_event(response)
                    
                    except IndexError:
                        # nothing to get
                        break
                    
                    except:
                        raise
            
            # STEP 2: GET DATA
            # -----------------------------------------------------------------------------------------------------
            # Get next data event
            if (next_data := self.synced_datafeed.fetch(1)) is None:
                # if no more data, we're done.
                self.main_shutdown_flag.set()
                
            # STEP 3: BROKERS HANDLE DATA
            # -----------------------------------------------------------------------------------------------------
            # Brokers will use new data to fill any outstanding orders / update states
            else:
                for broker in self.brokers.values():
                    if (fills := broker.try_fill_with_data(next_data)) is not None:
                        for fill in fills:
                            # if STRATEGIES_FULL, send order response via the socket
                            if socket_mode == c.STRATEGIES_FULL:
                                # prepare the response
                                response_packed = utils.packb(fill)
                                original_sender = fill[c.STRATEGY_CHAIN][-1]
                                original_sender_encoded = original_sender.encode('utf-8')
                                msg = self.broker_strategy_socket.send_multipart([original_sender_encoded, response_packed], copy = False, track = True)
                                msg.wait()
                            # otherwise directly have the strategy handle the event
                            else:
                                self.strategies[fill[c.STRATEGY_CHAIN][-1]]._handle_event(fill)

            # STEP 4: STRATEGIES HANDLE DATA
            # -----------------------------------------------------------------------------------------------------
                # if STRATEGIES_FULL, send data via socket
                if socket_mode == c.STRATEGIES_FULL:
                    # prepare the data
                    topic_encoded = next_data[c.TOPIC].encode('utf-8')
                    data_packed = utils.packb(next_data)
                    msg = self.data_subscriber_socket.send_multipart([topic_encoded, data_packed], copy = False, track = True)
                    msg.wait()
                # otherwise have strategies directly handle the data event
                else:
                    for strategy_id, strategy in self.strategies.items():
                        if next_data[c.TOPIC] in strategy.data_subscriptions:
                            strategy._handle_event(next_data)
            # STEP 5: RESOLVE COMMUNICATION
            # -----------------------------------------------------------------------------------------------------
            # Send communication to each strategy. Any further communication will be added to self.communication_deque,
            #   and we will handle this in an infinite loop until there is no more communication (responses)
            # t0 = time.time()
            while True:
                try:
                    communication_event = self.communication_deque.popleft()
                    try:
                        receiver = communication_event[c.RECEIVER_ID]
                    except KeyError:
                        raise Exception('No receiver specified for communication event\n', communication_event)
                    self.strategies[receiver]._handle_event(communication_event)
                except IndexError:
                    # nothing else to handle
                    break
                except:
                    raise

        # Done processing all data events; close things
        self.shutdown()
        


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
    context = context or zmq.Context.instance()

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
                # Note that the strategy should already have placed its strategy_id in the STRATEGY_CHAIN in the order
                strategy_id_encoded, order_packed = frontend.recv_multipart()
                order_unpacked = utils.unpackb(order_packed)
                if (broker := order_unpacked.get(c.BROKER)) is not None:
                    broker = broker
                else:
                    broker = default_broker

                if broker is None:
                    raise Exception('Either specify broker in order or specify default broker in session.')
                
                # send the order to the broker: (broker name, )
                backend.send_multipart([broker.encode('utf-8'), order_packed])

            elif socks.get(backend) == zmq.POLLIN:
                # received order from broker: (broker ident, (strategy ident, order))
                broker_encoded, order_packed = backend.recv_multipart()
                order_unpacked = utils.unpackb(order_packed)
                send_to = order_unpacked[c.STRATEGY_CHAIN][-1]
                frontend.send_multipart([send_to.encode('utf-8'), utils.packb(order_unpacked)])
        except (zmq.ContextTerminated, zmq.ZMQError): # Not sure why it's not getting caught by ContextTerminated
            frontend.close(linger = 10)
            backend.close(linger = 10)
        except:
            raise

    # exit gracefully
    if shutdown_flag.is_set():
        frontend.close(linger = 10)
        backend.close(linger = 10)

def communication_proxy(address, capture = None, context = None, shutdown_flag = None):
    '''Proxy to facilitate inter-strategy communication.
        
        Instead of each parent having their own address that children can connect to,
            all strategies will connect to a proxy socket that relays messages between
            strategies.
        The proxy will add the identity of the sender to the message, and sends the (updated)
            mesage to the strategy indicated by the c.RECEIVER_ID field in the message.
        
        The proxy is a ZMQ_ROUTER socket that binds to the address.
        Strategies should connect to the address as ZMQ_DEALER sockets.

        A message will look like:
        {
            c.SENDER_ID: sender id,   # will be filled in by proxy
            c.RECEIVER_ID: receiver id,
            c.EVENT_TYPE: c.COMMUNICATION,
            c.EVENV_SUBTYPE: c.INFO or c.ACTION,
            c.MESSAGE: "xxxx..."
        }

    '''
    context = context or zmq.Context.instance()

    message_router = context.socket(zmq.ROUTER)
    message_router.bind(address)

    # # if there is a capture socket
    # if capture:
    #     # bind to capture address
    #     capture_socket = context.socket(zmq.PUB)
    #     capture_socket.bind(capture)
    # else:
    #     capture_socket = None

    poller = zmq.Poller()
    poller.register(message_router, zmq.POLLIN)

    while not shutdown_flag.is_set():
        try:
            socks = dict(poller.poll())
            if socks.get(message_router) == zmq.POLLIN:
                # received message from strategy: (strategy ident, order)
                strategy_id_encoded, message_packed = message_router.recv_multipart()
                message_unpacked = utils.unpackb(message_packed)
                # message_unpacked[c.SENDER_ID] = strategy_id_encoded.decode('utf-8')
                # find out which strategy to send to
                if (receiver := message_unpacked.get(c.RECEIVER_ID)) is None:
                    raise Exception('No receiver for message specified')
                # print('communications', receiver, message_unpacked, '\n')
                # send the message to the receiver
                message_router.send_multipart([receiver.encode('utf-8'), utils.packb(message_unpacked)])

        except (zmq.ContextTerminated, zmq.ZMQError): # Not sure why it's not getting caught by ContextTerminated
            message_router.close(linger = 10)
        except:
            raise

    # exit gracefully
    if shutdown_flag.is_set():
        message_router.close(linger = 10)

    return

class SocketEmulator:
    def __init__(self, deq = None):
        """A class to emulate strategy sockets for communication when zmq is not desired.
            Doesn't fully emulate dealer-router type sockets, as one cannot extract the sender identity.
            Sender should include SENDER_ID or RECEIVER_ID in the message in those cases.

        Args:
            deq (collection.deque instance, optional): The deque to append received events to. Defaults to None.
        """
        if deq is not None:
            self.deq = deq
        else:
            self.deq = deque()

    def send(self, item):
        self.deq.append(utils.unpackb(item))

    def send_multipart(self, items):
        item = items[1]
        self.deq.append(utils.unpackb(item))

    def recv(self):
        raise zmq.ZMQError(errno = zmq.EAGAIN, msg = 'Socket emulator: Nothing to receive.')
    
    def recv_multipart(self):
        raise zmq.ZMQError(errno = zmq.EAGAIN, msg = 'Socket emulator: Nothing to receive.')