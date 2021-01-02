'''
A session coordinates instances of events, brokers, and strategies.
'''
from collections import deque
import threading
import operator
import time
import zmq
import msgpack

from . import utils

from . import constants as c
from .data.datafeed_synchronizer import DatafeedSynchronizer

class Session:
    def __init__(self):

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

        # Proxies
        self.proxy_threads = []
        self.data_address = None
        
        # main shutdown flag
        self.main_shutdown_flag = threading.Event()
    
    def run(self, socket_mode = c.STRATEGIES_INTERNALONLY):
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
                If STRATEGIES_ORDERSONLY:
                    - The behaviours of datafeeds and brokers are the same as if `socket_mode` = c.STRATEGIES.
                    - The `_handle_event` method of strategies is called on each event.
                    - Sockets are only used to listen for orders from strategies.
                        i.e. for top level strategies (those with no parents), only the order socket is used, and it is only used to send orders.
                        Communication between strategies is not changed. This mode is primarily for the purpose for backtesting, and to gurantee
                        that events are received and processed in the correct order. However, there is no reason why it wouldn't also
                        work in live mode.
                If STRATEGIES_INTERNALONLY:
                    - Strategies continue to communicate among themselves via sockets
                    - All communications from the session to strategies, order or data, will be directly handled via the `_handle_event` method
                        of the strategies.
                Defaults to STRATEGIES_INTERNALONLY.
        """        
        
        self.setup_addresses(socket_mode)

        self.setup_proxies(socket_mode)
        
        self.setup_datafeeds(socket_mode)

        self.setup_brokers(socket_mode)

        self.setup_strategies(socket_mode)


        if socket_mode == c.ALL:
            # tell datafeeds to start publishing
            # proxies, brokers, and strategies should all be ready to process events
            self.datafeed_start_sync.set()

            # wait for datafeeds to finish
            for data_thread in self.datafeed_threads:
                data_thread.join()

        elif socket_mode in [c.STRATEGIES_FULL, c.STRATEGIES_ORDERSONLY, c.STRATEGIES_INTERNALONLY]:
            self.process_events(socket_mode)

        else:
            raise NotImplementedError(f'socket_mode {socket_mode} not implemented')

        time.sleep(0.1)

        # exit gracefully
        self.shutdown()



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

        if socket_mode in [c.ALL, c.STRATEGIES_FULL, c.STRATEGIES_ORDERSONLY]:
            self.broker_strategy_address, _, _ = utils.get_free_tcp_address(exclude = addresses_used)
            addresses_used.append(self.broker_strategy_address)
        
            if socket_mode in [c.ALL, c.STRATEGIES_FULL]:
                # datafeed frontend
                self.datafeed_subscriber_address, _, _ = utils.get_free_tcp_address(exclude = addresses_used)
                addresses_used.append(self.datafeed_subscriber_address)

                # Also need to connect to datafeeds and brokers if running async (full sockets) mode
                if socket_mode == c.ALL:
                    # datafeed backend
                    self.datafeed_publisher_address, _, _ = utils.get_free_tcp_address(exclude = addresses_used)
                    addresses_used.append(self.datafeed_publisher_address)
                    # broker backend
                    self.broker_broker_address, _, _ = utils.get_free_tcp_address(exclude = addresses_used)
                    addresses_used.append(self.broker_broker_address)
        elif socket_mode == c.STRATEGIES_INTERNALONLY:
            pass
        else:
            raise NotImplementedError(f'socket_mode {socket_mode} not implemented')

        return

    def setup_proxies(self, socket_mode):

        if socket_mode == c.ALL:
            self.proxy_threads.append(threading.Thread(target = pub_sub_proxy, args = (self.datafeed_publisher_address, self.datafeed_subscriber_address, 
                                                                                        self.datafeed_capture_address, self.zmq_context, self.main_shutdown_flag)))
            # Add broker proxy to proxy threads
            default_broker = next(iter(self.brokers.values())).name if len(self.brokers) > 0 else None
            self.proxy_threads.append(threading.Thread(target = broker_proxy, args = (self.broker_strategy_address, self.broker_broker_address,
                                                                                        self.broker_capture_address, self.zmq_context, self.main_shutdown_flag, default_broker)))
            # Start the proxies
            for proxy_thread in self.proxy_threads:
                proxy_thread.start()
        elif socket_mode in [c.STRATEGIES_FULL, c.STRATEGIES_ORDERSONLY, c.STRATEGIES_INTERNALONLY]:
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
        elif socket_mode in [c.STRATEGIES_FULL, c.STRATEGIES_ORDERSONLY, c.STRATEGIES_INTERNALONLY]:
            # if there is more than one datafeed, combine them with DatafeedSynchronizer
            if len(self.datafeeds) > 1:
                self.synced_datafeed = DatafeedSynchronizer(datafeeds = self.datafeeds)
            # else just use the datafeed as the sole datafeed
            elif len(self.datafeeds) == 1:
                self.synced_datafeed = self.datafeeds[0]
            else:
                raise Exception('No datafeed to set up.')
            
            # if strategies require sockets, also set them up
            if socket_mode == c.STRATEGIES_FULL:
                self.data_subscriber_socket = self.zmq_context.socket(zmq.PUB)
                self.data_subscriber_socket.bind(self.datafeed_subscriber_address)
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
        elif socket_mode in [c.STRATEGIES_FULL, c.STRATEGIES_ORDERSONLY]:
            self.broker_strategy_socket = self.zmq_context.socket(zmq.ROUTER)
            self.broker_strategy_socket.bind(self.broker_strategy_address)
        elif socket_mode == c.STRATEGIES_INTERNALONLY:
            pass
        else:
            raise NotImplementedError(f'socket_mode {socket_mode} not implemented')

        return

    def setup_strategies(self, socket_mode):

        for strategy in self.strategies.values():
            strategy.zmq_context = self.zmq_context

            if socket_mode in [c.ALL, c.STRATEGIES_FULL, c.STRATEGIES_ORDERSONLY]:
                strategy.connect_order_socket(self.broker_strategy_address)

                if socket_mode in [c.ALL, c.STRATEGIES_FULL]:
                    strategy.connect_data_socket(self.datafeed_subscriber_address)
                    strategy_thread = threading.Thread(target = strategy.run)
                    strategy_thread.daemon = True
                    self.strategy_threads.append(strategy_thread)
                    strategy_thread.start()
            
            elif socket_mode == c.STRATEGIES_INTERNALONLY:
                pass
            else:
                raise NotImplementedError(f'socket_mode {socket_mode} not implemented')

        return

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
        
        # wait for the strategies to exit clean
        for strategy_thread in self.strategy_threads:
            strategy_thread.join()

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


        if socket_mode == c.ALL:
            raise NotImplementedError('socket_mode = "ALL" is not implemented in process_events.')
        elif socket_mode not in [c.STRATEGIES_FULL, c.STRATEGIES_ORDERSONLY, c.STRATEGIES_INTERNALONLY]:
            raise NotImplementedError(f'socket_mode {socket_mode} not implemented')

        # the event queue
        # use a deque as we may need to pop and append from both ends
        next_events = deque()

        while not self.main_shutdown_flag.is_set():
            
            # try to get (all) orders from strategies
            if socket_mode in [c.ALL, c.STRATEGIES_FULL, c.STRATEGIES_ORDERSONLY]:
                while True:
                    try:
                        strategy_id_encoded, order_packed = self.broker_strategy_socket.recv_multipart(zmq.NOBLOCK)
                        order_unpacked = msgpack.unpackb(order_packed, ext_hook = utils.ext_hook)
                        order_unpacked[c.SENDER_ID] = strategy_id_encoded.decode('utf-8')
                        next_events.append(order_unpacked)
                    except zmq.ZMQError as exc:
                        if exc.errno == zmq.EAGAIN:
                            # nothing to get
                            break
                        else:
                            raise
            # otherwise order would have been returned via _handle_event, and would be already in next_events.
            elif socket_mode == c.STRATEGIES_INTERNALONLY:
                pass
            
            # Get next data event
            if (next_data := self.synced_datafeed.fetch(1)) is not None:
                next_events.append(next_data)
            else:
                # if no more data, we're done.
                self.main_shutdown_flag.set()

            # sort the events by timestamp
            # sorts are "stable"; i.e. in the case of a tie, the original order is preserved
            # since orders were inserted first, order will be taken care of first in the case of a tie.
            next_events = sorted(next_events, key = operator.itemgetter(c.EVENT_TS))

            # handle each event, in order
            # iterating over range because we may append events to next_events
            for i in range(len(next_events)):
                event = next_events.pop(0)

                # if the event is an order (from strategies)
                if event[c.EVENT_TYPE] == c.ORDER:
                    
                    # find a broker
                    if (broker := event.get(c.BROKER)) is None:
                        broker = next(iter(self.brokers.values()))
                    # have broker handle the order
                    if (response := broker.take_order(event)) is not None:
                        # if STRATEGIES_FULL, send order response to socket
                        if socket_mode == c.STRATEGIES_FULL:
                            # prepare the response
                            response_packed = msgpack.packb(response, default = utils.default_packer)
                            original_sender = event[c.SENDER_ID]
                            original_sender_encoded = original_sender.encode('utf-8')
                            # send the response
                            # print('order submitted in session')
                            msg = self.broker_strategy_socket.send_multipart([original_sender_encoded, response_packed], copy = False, track = True)
                            msg.wait()
                        # otherwise directly have the strategy handle the event
                        elif socket_mode == c.STRATEGIES_ORDERSONLY:
                            self.strategies[event[c.SENDER_ID]]._handle_event(event)
                        elif socket_mode == c.STRATEGIES_INTERNALONLY:
                            if (response := self.strategies[event[c.SENDER_ID]]._handle_event(event)) is not None:
                                next_events.append(response)
                        else:
                            raise NotImplementedError(f'socket_mode {socket_mode} not implemented')

                # else if the event is a data event
                elif event[c.EVENT_TYPE] == c.DATA:
                    
                    # first let the broker try to fill any outstanding orders
                    for broker in self.brokers.values():
                        if (fills := broker.try_fill_with_data(event)) is not None:
                            for fill in fills:
                                # if STRATEGIES_FULL, send order response via the socket
                                if socket_mode == c.STRATEGIES_FULL:
                                    # prepare the response
                                    response_packed = msgpack.packb(fill, default = utils.default_packer)
                                    original_sender = fill[c.SENDER_ID]
                                    original_sender_encoded = original_sender.encode('utf-8')
                                    msg = self.broker_strategy_socket.send_multipart([original_sender_encoded, response_packed], copy = False, track = True)
                                    msg.wait()
                                # otherwise directly have the strategy handle the event
                                elif socket_mode == c.STRATEGIES_ORDERSONLY:
                                    self.strategies[fill[c.SENDER_ID]]._handle_event(fill)
                                elif socket_mode == c.STRATEGIES_INTERNALONLY:
                                    if (response := self.strategies[fill[c.SENDER_ID]]._handle_event(fill)) is not None:
                                        next_events.append(response)
                                else:
                                    raise NotImplementedError(f'socket_mode {socket_mode} not implemented')

                    # Then we can send the data to the strategies
                    if socket_mode == c.STRATEGIES_FULL:
                        # prepare the data
                        topic_encoded = event[c.TOPIC].encode('utf-8')
                        event_packed = msgpack.packb(event, default = utils.default_packer)
                        # print('data event in session')
                        msg = self.data_subscriber_socket.send_multipart([topic_encoded, event_packed], copy = False, track = True)
                        msg.wait()
                    elif socket_mode == c.STRATEGIES_ORDERSONLY:
                        for strategy in self.strategies.values():
                            if event[c.TOPIC] in strategy.data_subscriptions:
                                strategy._handle_event(event)
                    elif socket_mode == c.STRATEGIES_INTERNALONLY:
                        for strategy_id, strategy in self.strategies.items():
                            if event[c.TOPIC] in strategy.data_subscriptions:
                                if (response := strategy._handle_event(event)) is not None:
                                    response.update({c.SENDER_ID: strategy_id})
                                    next_events.append(response)
                    else:
                        raise NotImplementedError(f'socket_mode {socket_mode} not implemented')
        
        # Done processing all data events; close things
        self.shutdown()
        
def strategy_proxy(address, capture = None, context = None, shutdown_flag = None):
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
    context = context or zmq.Context.Instance()

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
            socks = dict(poller.poll(timeout = 10))
            
            if socks.get(message_router) == zmq.POLLIN:
                # received message from strategy: (strategy ident, order)
                strategy_id_encoded, message_packed = message_router.recv_multipart()
                message_unpacked = msgpack.unpackb(message_packed, ext_hook = utils.ext_hook)
                message_unpacked[c.SENDER_ID] = strategy_id_encoded.decode('utf-8')
                
                # find out which strategy to send to
                if (receiver := message_unpacked.get(c.RECEIVER_ID)) is None:
                    raise Exception('No receiver for message specified')
                
                # send the message to the receiver
                message_router.send_multipart([receiver.encode('utf-8'), msgpack.packb(message_unpacked, default = utils.default_packer)])

        except (zmq.ContextTerminated, zmq.ZMQError): # Not sure why it's not getting caught by ContextTerminated
            message_router.close(linger = 10)
        except:
            raise

    # exit gracefully
    if shutdown_flag.is_set():
        message_router.close(linger = 10)

    return


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
                order = msgpack.unpackb(order, ext_hook = utils.ext_hook)
                order[c.SENDER_ID] = strategy_id
                # find out which broker to send to
                if (broker := order.get(c.BROKER)) is not None:
                    broker = broker
                else:
                    broker = default_broker

                if broker is None:
                    raise Exception('Either specify broker in order or specify default broker in session.')
                
                # send the order to the broker: (broker name, )
                backend.send_multipart([broker.encode('utf-8'), msgpack.packb(order, default = utils.default_packer)])

            elif socks.get(backend) == zmq.POLLIN:
                # received order from broker: (broker ident, (strategy ident, order))
                broker, order = backend.recv_multipart()
                order_unpacked = msgpack.unpackb(order, ext_hook = utils.ext_hook)
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
        DEPRECATED; untested.
        
        Combines pub_sub_proxy and broker_proxy, but also sync the events between the two entities.
    '''

    # set up context
    context = context or zmq.Context.instance()

    # datafeed proxy
    # ---------------------------------------------------------------
    # datafeed facing socket
    data_backend = context.socket(zmq.SUB)
    # no filtering here
    data_backend.setsockopt(zmq.SUBSCRIBE, b'')
    data_backend.bind(address_data_backend)

    # strategy facing socket
    data_frontend = context.socket(zmq.PUB)
    data_frontend.bind(address_data_frontend)
    
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
    broker_backend.bind(address_broker_backend)
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
                    except zmq.ZMQError as exc:
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
                order_unpacked = msgpack.unpackb(next_event[1], ext_hook = utils.ext_hook)
                # retrieve sender id from order
                strategy_id_encoded = order_unpacked[c.SENDER_ID].encode('utf-8')
                # send the order to frontend
                broker_frontend.send_multipart([strategy_id_encoded, order_packed])
            elif next_socket == 'broker_frontend':
                # if it is an order from front end, need to unpack it
                # and figure out which broker to send it to
                order_packed = next_event[1]
                order_unpacked = msgpack.unpackb(next_event[1], ext_hook = utils.ext_hook)
                # add sender id to order as a string
                order_unpacked[c.SENDER_ID] = next_event[0].decode('utf-8')
                # repack order
                order_packed = msgpack.packb(order_unpacked, use_bin_type = True, default = utils.default_packer)
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
