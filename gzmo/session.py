"""A session coordinates instances of events, brokers, and strategies."""

from collections import deque
import threading
import time
import zmq
from zmq.sugar.socket import Socket

from . import utils
from .utils import constants as c
from .datafeeds.datafeed_synchronizer import DatafeedSynchronizer

class Session:
    """The Session sets up any necessary infrastructure for strategies/datafeeds/brokers
        to run, and to communicate with each other. It can be used for both backtesting
        and live trading.

        Modular design
        ==============
        Strategies, datafeeds, and brokers are designed to be modular. The interfaces to
        interact with them are consistent, so they can be switched out for other instances
        easily. For example, to switch from a broker simulator to a live broker, simply
        remove the simulator from the session, and add the live broker.

            >>> session.brokers.pop('simulator_name')
            >>> session.brokers['live_broker_name'] = live_broker
        
        Socket modes
        ============
        Communication between strategies, datafeeds, and brokers can be done in one of a few different ways,
        from passing python variables to each other within the same program, to communicating via ZMQ
        sockets as different standalone programs running on different machines. All of these are possible
        with the Session class by sepcifying a socket mode. A few socket modes are available, as described below:
            ALL:
                All communication is done via ZMQ sockets.
                - The `publish` method of each datafeed is called in a separate thread,
                    and each datafeed publishes independently to the session's pub_sub_proxy,
                    which relays messages in an unsynced manner.
                - The `run` method of each broker is called in a separate thread,
                    and each broker communicates with strategies via the session's broker_proxy,
                    which relays messges in an unsynced manner.
                - Strategies send/receive events via their respective ZMQ sockets.
            STRATEGIES_FULL:
                Strategies interact with ZQM sockets (both among strategies and with datafeed/brokers); datafeed and brokers do not.
                - The `fetch` method of each datafeed is called in the main thread.
                    Data events are consolidated and synced before being sent out from the session's data socket.
                - The `take_order` and `try_fill_with_data` methods of each broker is called in the main thread.
                    Order events are consolidated and synced before being relayed through the session's order socket.
                - The behaviour of strategies is the same as if `socket_mode` = c.ALL.
            STRATEGIES_INTERNALONLY:
                Only inter-strategy communications are done via ZMQ sockets.
                - Strategies continue to communicate among themselves via sockets
                - All communications from the session to strategies, order or data, will be directly handled via the `_handle_event` method
                    of the strategies.
            NONE:
                Sockets are not used.
                - Events (data, order, communication) are queued and methods from parties are called directly to handle those events.
                - Events are handled in a synced manner.
                - Strategies will send communication and orders to a queue, which will then be processed accordingly.
                - Between data events, any actions started by any communication (including subsequent communication) will be resolved completely
            Defaults to NONE.

        Examples:

            >>> strat = gzmo.Strategy(name = 'example_strat')
            >>> datafeed_aapl = gzmo.SnowflakeDataFeed(topic = 'AAPL', query = ...)
            >>> sesh = gzmo.Session()
            >>> sesh.add_datafeed(datafeed_aapl)
            >>> sesh.add_strategy(strat)
            >>> sesh.run()
    """

    def __init__(self):
        """Inits the session instance.
        
        Strategies, datafeeds, and brokers can be added via the `add_...` methods.
        """

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
        """Sets up infrastructure and runs the strategies.
        
        See __init__ docstring for documentation on socket_mode.
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
        """Adds a strategy to the session.
            Note that any child strategies should be added to the session as well.
        """
        self.strategies[strategy.strategy_id] = strategy
    
    def add_datafeed(self, datafeed):
        """Adds data feeds to the session."""
        self.datafeeds.append(datafeed)

    def add_broker(self, broker):
        """Adds a broker to the session."""
        self.brokers[broker.name] = broker

    def setup_addresses(self, socket_mode):
        """Obtain free tcp addresses for setting up sockets."""

        # addresses to avoid
        # want to avoid taking other sockets' addresses.
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
        """Sets up proxies to relay data, orders, and communications.
        
        The session can have multiple datafeeds or brokers. All should connect
        to the proxies so intermediate handling of data/order events can be done.
        """

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
        """Sets up the datafeeds for the different socket_modes.

        Sets up the datafeeds so that they know how to emit the data.
        If socket_mode is ALL, all datafeeds are set to publish to an address in an
            unsynced manner. Datafeeds are started when this method is called,
            but a start_sync flag is in place so that the datafeeds do not start publishing
            until the flag is set (in self.start).
        If socket_mode is STRATEGIES_FULL, STRATEGIES_INTERNALONLY, or NONE,
            all strategies will be combined into a synced datafeed via DatafeedSynchronizer,
            and data events will be `fetch`ed from the synced datafeed.
        """        

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
        """Sets up brokers for the different socket_modes.

        If socket_mode is ALL, the brokers will connect to an order socket and/or
            a datasocket.
        If socket_mode is STRATEGIES_FULL, the session will set up a "broker socket"
            for strategies to connect to, but the brokers will not be connecting
            to a socket.
        If socket_mode is STRATEGIES_INTERNALONLY or NONE, events are handled by
            directly calling the brokers' methods, so no sockets are needed.
        """

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
        """Sets up strategies for the different socket_modes.
        
        If socket_mode is ALL or STRATEGIES_FULL, all of the strategies' communication
            with the outside world are through ZMQ sockets.
        If socket_mode is STRATEGIES_INTERNALONLY, the strategies communicate with other
            strategies via ZMQ sockets, but handle events by having the corresponding
            methods directly called.
        If socket_mode is NONE, the session creates socket emulators so that any outbound
            communication from the strategies will still have the same interface as if they
            were sent to ZMQ sockets.
        
        """
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
        """Runs things that need to happen at the start of a backtest/live session.
        
        If socket_mode is ALL, a sync flag `datafeed_start_sync` is set so that all datafeeds
            start publishing at the same time.
        If socket_mode is STRATEGIES_FULL, STRATEGIES_INTERNALONLY, or NONE, any existing communications
            are handled before the event loop is started. This may include parent-child set-ups, passing
            cash, etc.
        
        
        """
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
        """Exits gracefully."""

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
        """Performs the main event loop in a synced manner.

        A method for socket_mode in ['STRATEGIES_FULL', 'STRATEGIES_INTERNALONLY', 'NONE'].

        The event loop is as follows:
            - broker handle orders from strategies; send any response
            - get data
            - send data
            - broker handle data; send any response
        """
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
                    fills = broker.try_fill_with_data(next_data)
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
    """Relays messages from backend (address_in) to frontend (address_out).
        
        This is to cosolidate datafeeds so that strategies can have one central place to
        subscribe data from, and it is easier for the session to keep track of addresses.

        Note that we are binding on the subscriber end because we have a
        multiple publisher (datafeeds) - one subscriber (session) pattern
    """

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
    """Relays orders to the correct brokers/strategies.

        This is a broker of brokers that manages sender and receiver identities.
    
        The flow or order is as follow:

        strategy (dealer): sends order

        proxy (router): receives (strategy ident, order)
        proxy extracts broker name from order
        proxy adds the field SENDER_ID to the order
        proxy (router): sends (broker ident, order)

        broker (dealer): receives order
        broker processes order
        broker (dealer): sends back order

        proxy (router): receives (broker ident, order)
        proxy extract sender ident from order
        proxy (router): sends (strategy ident, order)

        strategy (dealer): receives order
    
    """
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
    """Proxy to facilitate inter-strategy communication.
        
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

    """
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

class BrokerProxyEmulator:
    def __init__(self, shutdown_flag = None, default_broker = None):

        self.shutdown_flag = shutdown_flag or threading.Event()
        self.sockets = {}
        self.default_broker = default_broker

    def add_party(self, ident):
        self.sockets[ident] = SocketEmulator(unpack = True)
    
    def clear_queues(self):
        for socket in self.sockets:
            if socket.deq_in:
                order_unpacked = socket.deq_in.popleft()
                # REQUESTED ==> from strategy
                if (order_type := order_unpacked[c.EVENT_SUBTYPE]) == c.REQUESTED:
                    if (broker := order_unpacked.get(c.BROKER)) is None:
                        broker = self.default_broker
                    if broker is None:
                        raise Exception('Either specify broker in order or specify default broker in session.')
                    self.sockets[broker].deque_out.append(utils.packb(order_unpacked))
                else:
                    strategy = order_unpacked[c.STRATEGY_CHAIN][-1]
                    self.sockets[strategy].deque_out.append(utils.packb(order_unpacked))
    
    def run(self):
        while not self.shutdown_flag.is_set():
            self.clear_queues()


class CommunicationProxyEmulator:
    def __init__(self, shutdown_flag = None):

        self.shutdown_flag = shutdown_flag or threading.Event()
        self.sockets = {}

    def add_party(self, ident):
        self.sockets[ident] = SocketEmulator(unpack = True)

    def clear_queues(self):
        for socket in self.sockets:
            if socket.deq_in:
                message_unpacked = socket.deq_in.popleft()
                if (receiver := message_unpacked.get(c.RECEIVER_ID)) is None:
                    raise Exception('No receiver for message specified')
                if receiver not in self.sockets.keys():
                    raise Exception(f'Receiver {receiver} not found.')
                # attach a packed message for receiving at the receiver's socket
                self.sockets[receiver].deq_out.append(utils.packb(message_unpacked))
    
    def run(self):
        while not self.shutdown_flag.is_set():
            self.clear_queues()


class SocketEmulator:
    def __init__(self, deq_in = None, deq_out = None, unpack = False):
        """A class to emulate strategy sockets for communication.
            For two-way communications, a proxy should be implemented to have two `SocketEmulator`s
            and pass events between the two.

        Args:
            deq_in (collection.deque instance, optional): The deque to append events sent to the socket.
                Defaults to None.
            deq_in (collection.deque instance, optional): The deque to append events to be received from the socket.
                Defaults to None.
        """
        self.unpack = unpack

        if deq_in is not None:
            self.deq_in = deq_in
        else:
            self.deq_in = deque()
        
        if deq_out is not None:
            self.deq_out = deq_out
        else:
            self.deq_out = deque()

    def send(self, item):
        if self.unpack:
            self.deq_in.append(utils.unpackb(item))
        else:
            self.deq_in.append(item)

    def send_multipart(self, items):
        if self.unpack:
            self.deq_in.append(utils.unpackb(items))
        else:
            self.deq_in.append(items)

    def recv(self):
        if self.deq_out:
            return self.deq_out.popleft()
        else:
            raise zmq.ZMQError(errno = zmq.EAGAIN, msg = 'Socket emulator: Nothing to receive.')
    
    def recv_multipart(self):
        if self.deq_out:
            return self.deq_out.popleft()
        else:
            raise zmq.ZMQError(errno = zmq.EAGAIN, msg = 'Socket emulator: Nothing to receive.')