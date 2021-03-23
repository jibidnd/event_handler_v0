"""A session coordinates instances of events, brokers, and strategies."""

from collections import deque
from os import sync
from gzmo.datafeeds import DataFeedQuery
import threading
import time
import zmq

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
        
        Session modes
        ============
        Communication can be done in one of several ways:

        `use_zmq`: Using ZMQ sockets vs using a socket emulator.
            ZMQ sockets allows programs to be run on different machines and still communicate with each other.
            However, there is less fine tuned control over the order in which messages are processed, so lags
            that are inconsequential in real life could cause events to arrive out-of-order in a back test.
            
            A socket emulator provides the same interface as a ZMQ socket (send, send_multipart, recv, and recv_multipart),
            but behind the scenes there is a proxy that directs messages from and to the different sockets that
            programs connect to. There is more fine tuned control of how messages are processed/cleared, which
            allows for the following two event processing modes:
        
        'synced`: Processing events in a synced manner vs in a as-events-arrive manner.
            In a backtest, it is possible for processing times that are insignificant in real life to mess up
            event processing. For example, perhaps sending an order normally takes the strategy 1/10 second. In
            real life trading, this is likely not an issue, but in a backtest, this could mean that a whole day of bars
            have passed by the time the order has reached the broker.
            Because of this, there is a `synced` mode, where each "cycle" of events is cleared before moving onto
            the next. This is only available when ZMQ sockets are not used.
        

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

    def __init__(self, use_zmq = False, synced = True):
        """Inits the session instance.
        
        Strategies, datafeeds, and brokers can be added via the `add_...` methods.
        """

        self.use_zmq = use_zmq
        self.synced = synced

        # Use one context for all communication (thread safe)
        self._zmq_context = zmq.Context()

        # instances and threads
        # Strategies
        self.strategies = {}                        # strategy_id: strategy instances
        self._strategy_threads = {}                  # strategy_id: straegy thread

        # Datafeeds
        self.datafeeds = {}                         # name: datafeed instances
        self._datafeed_threads = {}                  # name: datafeed thread
        self._synced_datafeed = None
        # self._data_subscriber_socket = None

        self._datafeed_start_barrier = None
        self._datafeed_publisher_address = None      # datafeeds publish to this address
        self._datafeed_subscriber_address = None     # strategies subscribe to this address for datafeeds
        self._datafeed_capture_address = None        # not implemented
        
        # Brokers
        self.brokers = {}                           # name: broker instance
        self._broker_threads = {}                    # name: broker thread
        # self._broker_strategy_socket = None

        self._broker_broker_address = None           # brokers talk to this address. This will be a router socket / socket emulator
        self._broker_strategy_address = None         # strategies talk to this address. This will be a router socket / socket emulator
        self._broker_capture_address = None          # not implemented

        # Communication channel
        self._communication_address = None
        self._communication_capture_address = None   # not implemented

        # Proxies
        self._proxies = {}                           # name: proxy instance
        self._proxy_threads = {}                     # name: proxy thread
        
        # main shutdown flag
        self._main_shutdown_flag = threading.Event()
        
        self.is_setup = False

    def add_strategy(self, name, strategy):
        """Adds a strategy to the session.
            Note that any child strategies should be added to the session as well.
        """
        self.strategies[name] = strategy
    
    def add_datafeed(self, name, datafeed):
        """Adds data feeds to the session."""
        self.datafeeds[name] = datafeed

    def add_broker(self, broker):
        """Adds a broker to the session."""
        self.brokers[broker.name] = broker
    
    def run(self):
        """Sets up infrastructure and runs the strategies.
        
        See __init__ docstring for different session modes
        """

        if self.use_zmq:
            assert not synced, 'Cannot use ZMQ in non-synced mode.'
        
        self.setup()

        self._start()

        # exit gracefully
        self._stop()

        return
    
    def setup(self, use_zmq = None, synced = None):
        """Sets up things."""

        use_zmq = use_zmq or self.use_zmq
        synced = synced or self.synced

        self._setup_addresses(use_zmq)

        self._setup_proxies(use_zmq, synced)

        self._setup_datafeeds(use_zmq, synced)

        self._setup_brokers(use_zmq, synced)

        self._setup_strategies(use_zmq, synced)

        self.is_setup = True

    def _setup_addresses(self, use_zmq):
        """Obtains free tcp addresses for setting up sockets."""

        # addresses to avoid
        # want to avoid taking other sockets' addresses.
        # inproc was found to be rather unstable for some reason, so we stick with tcp.
        addresses_used = []

        if use_zmq:

            # broker backend
            self._broker_broker_address, _, _ = utils.get_free_tcp_address(exclude = addresses_used)
            addresses_used.append(self._broker_broker_address)

            # broker frontend
            self._broker_strategy_address, _, _ = utils.get_free_tcp_address(exclude = addresses_used)
            addresses_used.append(self._broker_strategy_address)
            
            # datafeed backend
            self._datafeed_publisher_address, _, _ = utils.get_free_tcp_address(exclude = addresses_used)
            addresses_used.append(self._datafeed_publisher_address)

            # datafeed frontend
            self._datafeed_subscriber_address, _, _ = utils.get_free_tcp_address(exclude = addresses_used)
            addresses_used.append(self._datafeed_subscriber_address)

            # communication channel
            self._communication_address, _, _ = utils.get_free_tcp_address(exclude = addresses_used)
            addresses_used.append(self._communication_address)
        
        else:
            pass

        return

    def _setup_proxies(self, use_zmq, synced):
        """Sets up proxies to relay data, orders, and communications.
        
        The session can have multiple datafeeds or brokers. All should connect
        to the proxies so intermediate handling of data/order events can be done.
        """

        # clear proxies
        self._proxy_threads = {}
        # default broker if none specified in order
        default_broker = next(iter(self.brokers.keys()))

        if use_zmq:
            datafeed_proxy = zmq_datafeed_proxy(
                self._datafeed_publisher_address,
                self._datafeed_subscriber_address,
                self._datafeed_capture_address,
                self.zmq_context
            )
            broker_proxy = zmq_broker_proxy(
                self._broker_broker_address,
                self._broker_strategy_address,
                self._broker_capture_address,
                self.zmq_context,
                default_broker
            )
            communication_proxy = zmq_communication_proxy(
                self._communication_address,
                self._communication_capture_address,
                self.zmq_context
            )
        
        else:
            datafeed_proxy = DatafeedProxyEmulator()
            broker_proxy = BrokerProxyEmulator(default_broker = default_broker)
            communication_proxy = CommunicationProxyEmulator()

        self._proxies[c.DATA] = datafeed_proxy
        self._proxies[c.BROKER] = broker_proxy
        self._proxies[c.COMMUNICATION] = communication_proxy

        # start the proxies
        self._proxy_threads[c.DATA] = threading.Thread(name = 'data_proxy_thread', target = datafeed_proxy.run, args = (self._main_shutdown_flag,), daemon = True)
        self._proxy_threads[c.BROKER] = threading.Thread(name = 'broker_proxy_thread', target = broker_proxy.run, args = (self._main_shutdown_flag,), daemon = True)
        self._proxy_threads[c.COMMUNICATION] = threading.Thread(name = 'communication_proxy_thread', target = communication_proxy.run, args = (self._main_shutdown_flag,), daemon = True)

        # Start the proxies
        for proxy_thread in self._proxy_threads.values():
            proxy_thread.start()
            
        return

    def _setup_datafeeds(self, use_zmq, synced):
        """Sets up the datafeeds.

        Sets up the datafeeds so that they know how to emit the data.
        
        If not `synced`, all datafeeds are set to publish to an address in an
            unsynced manner. Datafeeds are started when this method is called,
            but a threading.Barrier is in place so that the datafeeds do not start publishing
            until the the barrier is passed (in self.start).

            Datafeeds will emit data as soon as the data is available, without regards to
            whether the brokers/strategies are done processing prior data.

        If `synced`, all strategies will be combined into a synced datafeed via
            DatafeedSynchronizer, and data events will be `fetch`ed from the synced datafeed.
        
        """
        # need all datafeeds + the session to be ready
        self._datafeed_start_barrier = threading.Barrier(len(self.datafeeds) + 1)

        for name, datafeed in self.datafeeds.items():
            
            # datafeed will wait for a signcal to all start together
            datafeed._start_barrier = self._datafeed_start_barrier
            
            # set up connections
            if use_zmq:
                datafeed.zmq_context = self.zmq_context
                datafeed.publish_to(self._datafeed_publisher_address)
            else:
                datafeed.publishing_socket = self._proxies[c.DATA].add_publisher(name)

            # Start the datafeed if not syncing
            # Publishing will be blocked until _datafeed_start_barrier is passed.
            if not synced:
                datafeed_thread = threading.Thread(name = f'datafeed_thread_{name}', target = datafeed._start, daemon = True)
                self._datafeed_threads[name] = datafeed_thread
                datafeed_thread.start()

            # else sync the feeds
            # `fetch` will be called later in `next`
            else:
                if len(self.datafeeds) > 1:
                    self._synced_datafeed = DatafeedSynchronizer(datafeeds = self.datafeeds)
                # else just use the datafeed as the sole datafeed
                elif len(self.datafeeds) == 1:
                    self._synced_datafeed = next(iter(self.datafeeds.values()))
                else:
                    raise Exception('No datafeed to set up.')
                
                self._synced_datafeed.publishing_socket = self._proxies[c.DATA].add_publisher('synced_feed')
        
        return

    def _setup_brokers(self, use_zmq, synced):
        """Sets up brokers for the different socket_modes."""
        for name, broker in self.brokers.items():
            # set up connections
            if use_zmq:
                broker.zmq_context = self.zmq_context
                broker.connect_data_socket(self._datafeed_subscriber_address)
                broker.connect_order_socket(self._broker_broker_address)
            else:
                broker.data_socket = self._proxies[c.DATA].add_subscriber(name, '')
                broker.order_socket = self._proxies[c.BROKER].add_party(name)    # returns socket
            
            if not synced:
                # start threads
                broker_thread = threading.Thread(name = f'broker_thread_name', target = broker.run, args = (self._main_shutdown_flag,), daemon = True)
                self._broker_threads[name] = broker_thread
                broker_thread.start()

        return

    def _setup_strategies(self, use_zmq, synced):
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
        for name, strategy in self.strategies.items():
            strategy.zmq_context = self._zmq_context
            strategy.shutdown_flag = self._main_shutdown_flag

            # set up sockets
            if use_zmq:
                strategy.connect_data_socket(self._datafeed_subscriber_address)
                strategy.connect_order_socket(self._broker_strategy_address)
                strategy.connect_communication_socket(self._communication_address)
            else:
                strategy.data_socket = self._proxies[c.DATA].add_subscriber(
                    strategy.strategy_id, strategy.data_subscriptions)
                strategy.order_socket = self._proxies[c.BROKER].add_party(strategy.strategy_id)
                strategy.communication_socket = self._proxies[c.COMMUNICATION].add_party(
                    strategy.strategy_id)
            
            if not synced:
                # start running the strategies so they are ready to receive data
                strategy_thread = threading.Thread(name = f'strategy_thread_{name}', target = strategy.run, args = (self._main_shutdown_flag,), daemon = True)
                self._strategy_threads[name] = strategy_thread
                # tell the threads to start listening
                strategy_thread.start()
            else:
                strategy._before_start()
        
        # allow _before_start communication to go through if syncing
        if synced:
            self._proxies[c.COMMUNICATION].clear_queues()

        return

    def _start(self):
        """Starts the session."""

        if not self.synced:
            # all data feeds will start publishing together when the barrier is passed by everyone
            # Usually this should be the last barrier to pass
            # proxies, brokers, and strategies are all already running
            self._datafeed_start_barrier.wait()
            # wait for datafeeds to finishfor data_thread in self._datafeed_threads:
            for datafeed_thread in self._datafeed_threads:
                datafeed_thread.join()
        else:
            while not self._main_shutdown_flag.is_set():
                had_activity = self.next()
                if not had_activity:
                    break

        return

    def next(self):
        """Iterates one cycle of the main event loop

            The event loops has strategies and broker process events,
            and proxies relay messages, until an iteration where all
            entities have no activities.
        """

        if not self.is_setup:
            self.setup()

        # Set to True if there is either data or activities from strategies/brokers
        had_activity = False

        # Assume starting with all clear queues: no unhandled data, orders, or communications
        # First get the next data event
        if (data := self._synced_datafeed.fetch()) is not None:
            had_activity = True
            topic = data[c.TOPIC]
            self._proxies[c.DATA].sockets_publisher['synced_feed'].send_multipart([topic.encode(), utils.packb(data)])
            # Broadcast the data
            self._proxies[c.DATA].clear_queues()
        
        # clear all activities form strategies and brokers
        while True:
            has_activity = False

            # Strategies handle data (place orders, etc)
            for strategy in self.strategies.values():
                has_activity |= strategy.next()
            
            # Communication proxy relays messages
            has_activity |= self._proxies[c.COMMUNICATION].clear_queues()
            
            # Broker proxy relays orders
            has_activity |= self._proxies[c.BROKER].clear_queues()

            # Broker handle data/orders (place orders by strategies, fill orders using data, etc)
            for broker in self.brokers.values():
                has_activity |= broker.next()
            
            # set flag if there was any activities
            had_activity |= has_activity

            # had a full round of no activities, we can release the loop
            if not has_activity:
                break

        return had_activity

    def _stop(self, linger = 0.1):
        """Exits gracefully."""

        self._main_shutdown_flag.set()
        # wait a second before terminating the context
        time.sleep(linger)
        self._zmq_context.destroy()
        
        if not self.synced:
            # wait for the strategies to exit clean
            for strategy_thread in self._strategy_threads.values():
                # _before_stop() and _stop() are called after shutdown flag is set
                strategy_thread.join()
        else:
            for strategy in self.strategies.values():
                strategy._before_stop()
                strategy._stop()
    
    def stop(self):
        self._stop()
        

class zmq_datafeed_proxy:
    """Relays messages from backend (datafeed producers) to frontend (strategies).
        
        This is to cosolidate datafeeds so that strategies can have one central place to
        subscribe data from, and it is easier for the session to keep track of addresses.

        Note that in pyzmq's documentation, publishers are "frontend" and subscribers are
        "backend", so the syntax maybe a little confusing here if the user is looking
        at ZMQ docs.
    """

    def __init__(self, address_backend, address_frontend, address_capture = None, context = None):

        self.address_backend = address_backend
        self.address_frontend = address_frontend
        self.address_capture = address_capture
        self.context = context or zmq.Context.instance()
        self._shutdown_flag = threading.Event()

        # publisher facing socket
        self.backend = context.socket(zmq.SUB)
        # no filtering here
        self.backend.setsockopt(zmq.SUBSCRIBE, b'')
        self.backend.bind(address_backend)

        # client facing socket
        self.frontend = context.socket(zmq.PUB)
        self.frontend.bind(address_frontend)
        
        if address_capture:
            # bind to capture address
            self.capture = context.socket(zmq.PUB)
            self.capture.bind(address_capture)
        else:
            self.capture = None
    
    def run(self):
        self._start()
        self._stop()

    def _start(self, session_shutdown_flag = None):

        if not self.is_setup:
            self.setup()

        if session_shutdown_flag is None:
            session_shutdown_flag = self._shutdown_flag
            session_shutdown_flag.clear()

        # start the proxy
        try:
            zmq.proxy(self.backend, self.frontend, self.capture)
        except (zmq.ContextTerminated):#, zmq.ZMQError): # Not sure why it's not getting caught by ContextTerminated
            self.backend.close(linger = 10)
            self.frontend.close(linger = 10)
        except:
            self.frontend.close(linger = 10)
            self.backend.close(linger = 10)
            raise
        
        # # exit gracefully
        # if self.shutdown_flag.is_set():
        #     self.backend.close(linger = 10)
        #     self.frontend.close(linger = 10)

    def _stop(self):
        # TODO: is this needed?
        self._hutdown_flag.set()

class zmq_broker_proxy:
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
    def __init__(self, address_backend, address_frontend, address_capture = None, context = None, default_broker = None):
        
        self.address_backend = address_backend
        self.address_frontend = address_frontend
        # self.address_capture = address_capture
        self.context = context or zmq.Context.instance()
        self._shutdown_flag = threading.Event()
        self.default_broker = default_broker
        
        self.context = context or zmq.Context.instance()

        # establish strategy facing socket
        self.frontend = context.socket(zmq.ROUTER)
        self.frontend.bind(address_frontend)
        # establish broker facing socket
        self.backend = context.socket(zmq.ROUTER)
        self.backend.bind(address_backend)
        # TODO: implement capture socket
        # if there is a capture socket
        # if address_capture:
        #     # bind to capture address
        #     self.capture = context.socket(zmq.PUB)
        #     self.capture.bind(address_capture)
        # else:
        #     capture_socket = None

        poller = zmq.Poller()
        poller.register(self.frontend, zmq.POLLIN)
        poller.register(self.backend, zmq.POLLIN)
    
    def run(self):
        self._start()
        self._stop()

    def _start(self, session_shutdown_flag = None):

        if session_shutdown_flag is None:
            session_shutdown_flag = self._shutdown_flag
            session_shutdown_flag.clear()

        while (not session_shutdown_flag.is_set()) and (not self._shutdown_flag.is_set()):

            try:
                socks = dict(self.poller.poll(timeout = 10))
                
                if socks.get(self.frontend) == zmq.POLLIN:
                    # received order from strategy: (strategy ident, order)
                    # Note that the strategy should already have placed its strategy_id in the STRATEGY_CHAIN in the order
                    strategy_id_encoded, order_packed = self.frontend.recv_multipart()
                    order_unpacked = utils.unpackb(order_packed)
                    if (broker := order_unpacked.get(c.BROKER)) is not None:
                        broker = broker
                    else:
                        broker = self.default_broker

                    if broker is None:
                        raise Exception('Either specify broker in order or specify default broker in session.')
                    
                    # send the order to the broker: (broker name, )
                    self.backend.send_multipart([broker.encode('utf-8'), order_packed])

                elif socks.get(self.backend) == zmq.POLLIN:
                    # received order from broker: (broker ident, (strategy ident, order))
                    broker_encoded, order_packed = self.backend.recv_multipart()
                    order_unpacked = utils.unpackb(order_packed)
                    send_to = order_unpacked[c.STRATEGY_CHAIN][-1]
                    self.frontend.send_multipart([send_to.encode('utf-8'), utils.packb(order_unpacked)])
            except (zmq.ContextTerminated, zmq.ZMQError): # Not sure why it's not getting caught by ContextTerminated
                self.frontend.close(linger = 10)
                self.backend.close(linger = 10)
            except:
                raise

        # exit gracefully
        self.frontend.close(linger = 10)
        self.backend.close(linger = 10)
    
        return

    def _stop(self):
        self.shutdown_flag.set()

class zmq_communication_proxy:
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
    def __init__(self, address, address_capture = None, context = None):
        
        self.address = address
        # self.address_capture = address_capture
        self.context = context or zmq.Context.instance()
        self._shutdown_flag = threading.Event()

        self.message_router = self.context.socket(zmq.ROUTER)
        self.message_router.bind(address)

        # TODO: implement capture socket
        # # if there is a capture socket
        # if capture:
        #     # bind to capture address
        #     capture_socket = context.socket(zmq.PUB)
        #     capture_socket.bind(capture)
        # else:
        #     capture_socket = None

        self.poller = zmq.Poller()
        self.poller.register(self.message_router, zmq.POLLIN)
    
    def run(self, session_shutdown_flag = None):
        self._start(session_shutdown_flag)
        self._stop()

    def _start(self, session_shutdown_flag= None):

        if session_shutdown_flag is None:
            session_shutdown_flag = self._shutdown_flag
            session_shutdown_flag.clear()

        while (not session_shutdown_flag.is_set()) and (not self._shutdown_flag.is_set()):
            try:
                socks = dict(self.poller.poll())
                if socks.get(self.message_router) == zmq.POLLIN:
                    # received message from strategy: (strategy ident, order)
                    strategy_id_encoded, message_packed = self.message_router.recv_multipart()
                    message_unpacked = utils.unpackb(message_packed)
                    # message_unpacked[c.SENDER_ID] = strategy_id_encoded.decode('utf-8')
                    # find out which strategy to send to
                    if (receiver := message_unpacked.get(c.RECEIVER_ID)) is None:
                        raise Exception('No receiver for message specified')
                    # send the message to the receiver
                    self.message_router.send_multipart([receiver.encode('utf-8'), utils.packb(message_unpacked)])

            except (zmq.ContextTerminated, zmq.ZMQError): # Not sure why it's not getting caught by ContextTerminated
                self.message_router.close(linger = 10)
            except:
                raise

        # exit gracefully
        self.message_router.close(linger = 10)

        return

    def _stop(self):
        self._shutdown_flag.set()

class DatafeedProxyEmulator:
    """Relays data events to each strategy's sockets, filtering topics."""
    def __init__(self):

        self.sockets_publisher = {}     # datafeeds send data here
        self.sockets_subscriber = {}    # strategies get data here
        self.subscriptions = {}         # strategy subscriptions
        self._shutdown_flag = threading.Event()

    def add_publisher(self, ident):
        self.sockets_publisher[ident] = SocketEmulator()
        return self.sockets_publisher[ident]
    
    def add_subscriber(self, ident, subscriptions):
        self.sockets_subscriber[ident] = SocketEmulator()
        self.subscriptions[ident] = subscriptions
        return self.sockets_subscriber[ident]
    
    def clear_queues(self):
        """Move next items from deq_in from all sockets to deq_out to any subscribing sockets."""
        for datafeed in self.sockets_publisher.values():
            if datafeed.deq_in:
                topic_encoded, data_packed = datafeed.deq_in.popleft()
                topic_decoded, data_unpacked = topic_encoded.decode(), utils.unpackb(data_packed)
                # send this data event to each strategies that subscribes to this topic
                for strategy, subscriptions in self.subscriptions.items():
                    if (topic_decoded in subscriptions) or ('' in subscriptions):
                        self.sockets_subscriber[strategy].deq_out.append([topic_encoded, data_packed])
                return True

            # if datafeed.deq_in:
            #     topic, data_unpacked = datafeed.deq_in.popleft()
            #     topic_encoded, data_packed = topic.encode(), utils.packb(data_unpacked)
            #     if topic is not None:
            #         # send this data event to each strategies that subscribes to this topic
            #         for strategy, subscriptions in self.subscriptions.items():
            #             if topic in subscriptions:
            #                 self.sockets_subscriber[strategy].deq_out.append([topic_encoded, data_packed])
    
    def run(self, session_shutdown_flag = None):
        self._start(session_shutdown_flag)
        self._stop()

    def _start(self, session_shutdown_flag = None, pause = 1):
        
        if session_shutdown_flag is None:
            session_shutdown_flag = self._shutdown_flag
            session_shutdown_flag.clear()
        
        while (not session_shutdown_flag.is_set()) and (not self._shutdown_flag.is_set()):
            if not self.clear_queues():
                time.sleep(pause)
    
    def _stop(self):
        self._shutdown_flag.set()

class BrokerProxyEmulator:
    """Relays orders from one socket emulator to another.
    
    If the order's EVENT_SUBTYPE is REQUESTED, use the BROKER field to identify
        the socket to send to.
    Otherwise use the last item in the STRATEGY_CHAIN to identify the socket
        to send to.
    """
    def __init__(self, default_broker = None):

        self.sockets = {}
        self.default_broker = default_broker
        self._shutdown_flag = threading.Event()

    def add_party(self, ident):
        self.sockets[ident] = SocketEmulator()
        return self.sockets[ident]
    
    def clear_queues(self, max_iter = 2):
        """Clears the order queues.
        
        Runs until the earlier of:
            - one full cycle where all sockets have no messages
            - `max_iter` cycles
        """

        has_activities = True   # for keeping track of individual iterations
        had_activities = False   # for keeping track of entire function call
        i = 0

        while has_activities and (i < max_iter):
            # assume there are no events
            has_activities = False
            i += 1
            for socket in self.sockets.values():
                if socket.deq_in:
                    order_packed = socket.deq_in.popleft()
                    order_unpacked = utils.unpackb(order_packed)
                    # REQUESTED ==> from strategy
                    if (order_type := order_unpacked[c.EVENT_SUBTYPE]) == c.REQUESTED:
                        if (broker := order_unpacked.get(c.BROKER)) is None:
                            broker = self.default_broker
                        if broker is None:
                            raise Exception('Either specify broker in order or specify default broker in session.')
                        self.sockets[broker].deq_out.append(utils.packb(order_unpacked))
                    else:
                        strategy = order_unpacked[c.STRATEGY_CHAIN][-1]
                        self.sockets[strategy].deq_out.append(order_packed)
                    # there was an event. Reset the flag.
                    has_activities = True
                    had_activities = True
        
        return had_activities

    
    def run(self, session_shutdown_flag = None):
        self._start(session_shutdown_flag)
        self._stop()

    def _start(self, session_shutdown_flag = None, pause = 1):

        if session_shutdown_flag is None:
            session_shutdown_flag = self._shutdown_flag
            session_shutdown_flag.clear()
        
        while (not session_shutdown_flag.is_set()) and (not self._shutdown_flag.is_set()):
            if not self.clear_queues():
                time.sleep(pause)

    def _stop(self):
        self._shutdown_flag.set()


class CommunicationProxyEmulator:
    """Relays messages from one socket emulator to another using RECEIVER_ID."""
    def __init__(self):

        self.sockets = {}
        self._shutdown_flag = threading.Event()

    def add_party(self, ident):
        self.sockets[ident] = SocketEmulator()
        return self.sockets[ident]

    def clear_queues(self, max_iter = 2):
        """Clears the order queues.
        
        Runs until the earlier of:
            - one full cycle where all sockets have no messages
            - `max_iter` cycles
        This is to prevent an infinite loop of two strategies talking to each other.
        """

        has_activities = True   # for keeping track of individual iterations
        had_activites = False   # for keeping track of entire function call
        i = 0
        while has_activities and (i < max_iter):
            # assume there are no events
            has_activities = False
            i += 1
            
            for socket in self.sockets.values():
                if socket.deq_in:
                    message_packed = socket.deq_in.popleft()
                    message_unpacked = utils.unpackb(message_packed)
                    if (receiver := message_unpacked.get(c.RECEIVER_ID)) is None:
                        raise Exception('No receiver for message specified')
                    if receiver not in self.sockets.keys():
                        raise Exception(f'Receiver {receiver} not found.')
                    # attach a packed message for receiving at the receiver's socket
                    self.sockets[receiver].deq_out.append(message_packed)
                    has_activities = True
                    had_activities = True

        return had_activites
    
    def run(self, session_shutdown_flag = None):
        self._start(session_shutdown_flag)
        self._stop()

    def _start(self, session_shutdown_flag = None, pause = 1):
        if session_shutdown_flag is None:
            session_shutdown_flag = self._shutdown_flag
            session_shutdown_flag.clear()

        while (not session_shutdown_flag.is_set()) and (not self._shutdown_flag.is_set()):
            if not self.clear_queues():
                time.sleep(pause)

    def _stop(self):
        self._shutdown_flag.set()



class SocketEmulator:
    def __init__(self):
        """A class to emulate strategy sockets for communication.
            For two-way communications, a proxy should be implemented to have two `SocketEmulator`s
            and pass events between the two.

        Args:
            deq (collection.deque instance, optional): The deque to keep track of events.
                Add from the right, take from the left.
        """
        # self.unpack = unpack

        self.deq_in = deque()
        self.deq_out = deque()

    def send(self, item, flags = zmq.NULL):
        """items are sent here to the socket."""
        # if self.unpack:
        #     self.deq.append(utils.unpackb(item))
        # else:
        #     self.deq.append(item)
        self.deq_in.append(item)
        

    def send_multipart(self, items, flags = zmq.NULL):
        """items are sent here to the socket."""
        self.deq_in.append(items)

    def recv(self, flags = zmq.NULL):
        """items are retrieved from the socket here."""
        if flags == zmq.NOBLOCK:
        # do not block and throw and error
            if self.deq_out:
                return self.deq_out.popleft()
            else:
                raise zmq.ZMQError(errno = zmq.EAGAIN, msg = 'Socket emulator: Nothing to receive.')
        else:
        # block and wait for message
            while True:
                if self.deq_out:
                    return self.deq_out.popleft()
    
    def recv_multipart(self, flags = zmq.NULL):
        return self.recv(flags)