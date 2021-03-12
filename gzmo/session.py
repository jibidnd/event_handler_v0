"""A session coordinates instances of events, brokers, and strategies."""

from collections import deque
from gzmo.datafeeds import DataFeedQuery
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

    def __init__(self):
        """Inits the session instance.
        
        Strategies, datafeeds, and brokers can be added via the `add_...` methods.
        """

        # Use one context for all communication (thread safe)
        self.zmq_context = zmq.Context()

        # instances and threads
        # Strategies
        self.strategies = {}                        # strategy_id: strategy instances
        self.strategy_threads = {}                  # strategy_id: straegy thread

        # Datafeeds
        self.datafeeds = {}                         # name: datafeed instances
        self.datafeed_threads = {}                  # name: datafeed thread
        self.synced_datafeed = None
        self.data_subscriber_socket = None

        self.datafeed_start_sync = threading.Event()
        self.datafeed_publisher_address = None      # datafeeds publish to this address
        self.datafeed_subscriber_address = None     # strategies subscribe to this address for datafeeds
        self.datafeed_capture_address = None        # not implemented
        
        # Brokers
        self.brokers = {}                           # name: broker instance
        self.broker_threads = {}                    # name: broker thread
        self.broker_strategy_socket = None

        self.broker_broker_address = None           # brokers talk to this address. This will be a router socket / socket emulator
        self.broker_strategy_address = None         # strategies talk to this address. This will be a router socket / socket emulator
        self.broker_capture_address = None          # not implemented

        # Communication channel
        self.communication_address = None
        self.communication_capture_address = None   # not implemented

        # Proxies
        self.proxies = {}                           # name: proxy instance
        self.proxy_threads = {}                     # name: proxy thread
        
        # main shutdown flag
        self.main_shutdown_flag = threading.Event()
    
    def run(self, use_zmq = False, synced = True):
        """Sets up infrastructure and runs the strategies.
        
        See __init__ docstring for different session modes
        """

        if use_zmq:
            assert not synced, 'Cannot use ZMQ in non-synced mode.'
        
        self.setup_addresses(use_zmq)

        self.setup_proxies(use_zmq)

        self.setup_datafeeds(use_zmq, synced)

        self.setup_brokers(use_zmq, synced)

        self.setup_strategies(use_zmq, synced)

        self.start()

        time.sleep(1)

        # exit gracefully
        self.shutdown()

        return

    def add_strategy(self, strategy):
        """Adds a strategy to the session.
            Note that any child strategies should be added to the session as well.
        """
        self.strategies[strategy.get(c.STRATEGY_ID)] = strategy
    
    def add_datafeed(self, name, datafeed):
        """Adds data feeds to the session."""
        self.datafeeds[name] = datafeed

    def add_broker(self, broker):
        """Adds a broker to the session."""
        self.brokers[broker.name] = broker

    def setup_addresses(self, use_zmq):
        """Obtain free tcp addresses for setting up sockets."""

        # addresses to avoid
        # want to avoid taking other sockets' addresses.
        # inproc was found to be rather unstable for some reason, so we stick with tcp.
        addresses_used = []

        if use_zmq:

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
        
        else:
            pass

        return

    def setup_proxies(self, use_zmq, synced):
        """Sets up proxies to relay data, orders, and communications.
        
        The session can have multiple datafeeds or brokers. All should connect
        to the proxies so intermediate handling of data/order events can be done.
        """

        # clear proxies
        self.proxy_threads = {}

        if use_zmq:
            datafeed_proxy = zmq_datafeed_proxy(
                self.datafeed_publisher_address,
                self.data_subscriber_socket,
                self.datafeed_capture_address,
                self.zmq_context,
                self.main_shutdown_flag
            )
            broker_proxy = zmq_broker_proxy(
                self.broker_broker_address,
                self.broker_strategy_address,
                self.broker_capture_address,
                self.zmq_context,
                self.main_shutdown_flag
            )
            communication_proxy = zmq_communication_proxy(
                self.communication_address,
                self.communication_capture_address,
                self.zmq_context,
                self.main_shutdown_flag
            )
        
        else:
            datafeed_proxy = DatafeedProxyEmulator(shutdown_flag = self.main_shutdown_flag)
            broker_proxy = BrokerProxyEmulator(shutdown_flag = self.main_shutdown_flag)
            communication_proxy = CommunicationProxyEmulator(shutdown_flag = self.main_shutdown_flag)

        self.proxies[c.DATA] = datafeed_proxy
        self.proxies[c.BROKER] = broker_proxy
        self.proxies[c.COMMUNICATION] = communication_proxy

        # start the proxies if we're not syncing
        if not synced:
            self.proxy_threads[c.DATA] = threading.Thread(target = datafeed_proxy.run)
            self.proxy_threads[c.BROKER] = threading.Thread(target = broker_proxy.run)
            self.proxy_threads[c.COMMUNICATION] = threading.Thread(target = communication_proxy.run)

            # Start the proxies
            for proxy_thread in self.proxy_threads.values():
                proxy_thread.start()
        else:
            pass
            
        return

    def setup_datafeeds(self, use_zmq, synced):
        """Sets up the datafeeds.

        Sets up the datafeeds so that they know how to emit the data.
        
        If not `synced`, all datafeeds are set to publish to an address in an
            unsynced manner. Datafeeds are started when this method is called,
            but a start_sync flag is in place so that the datafeeds do not start publishing
            until the flag is set (in self.start).
            Datafeeds will emit data as soon as the data is available, without regards to
            whether the brokers/strategies are done processing prior data.

        If `synced`, all strategies will be combined into a synced datafeed via
            DatafeedSynchronizer, and data events will be `fetch`ed from the synced datafeed.
        
        """
        for name, datafeed in self.datafeeds.items():
            
            datafeed.shutdown_flag = self.main_shutdown_flag
            # datafeed will wait for a signcal to all start together
            datafeed.start_sync = self.datafeed_start_sync
            
            # set up connections
            if use_zmq:
                datafeed.zmq_context = self.zmq_context
                datafeed.publish_to(self.datafeed_publisher_address)
            else:
                datafeed.publishing_socket = self.proxies[c.DATA].add_publisher(name)

            # Start the datafeed if not syncing
            # Publishing will be blocked until datafeed_start_sync is set.
            if not synced:
                datafeed_thread = threading.Thread(target = datafeed.publish)
                datafeed_thread.daemon = True
                self.datafeed_threads[name] = datafeed_thread
                datafeed_thread.start()        
            # else sync the feeds
            else:
                if len(self.datafeeds) > 1:
                    self.synced_datafeed = DatafeedSynchronizer(datafeeds = self.datafeeds)
                # else just use the datafeed as the sole datafeed
                elif len(self.datafeeds) == 1:
                    self.synced_datafeed = self.datafeeds[0]
                else:
                    raise Exception('No datafeed to set up.')
                
                self.synced_datafeed.publishing_socket = self.proxies[c.DATA].add_publisher('synced_feed')
        
        return

    def setup_brokers(self, use_zmq, synced):
        """Sets up brokers for the different socket_modes.

        If socket_mode is ALL, the brokers will connect to an order socket and/or
            a datasocket.
        If socket_mode is STRATEGIES_FULL, the session will set up a "broker socket"
            for strategies to connect to, but the brokers will not be connecting
            to a socket.
        If socket_mode is STRATEGIES_INTERNALONLY or NONE, events are handled by
            directly calling the brokers' methods, so no sockets are needed.
        """
        for name, broker in self.brokers.items():
            
            broker.main_shutdown_flag = self.main_shutdown_flag
            # set up connections
            if use_zmq:
                broker.zmq_context = self.zmq_context
                broker.connect_data_socket(self.datafeed_subscriber_address)
                broker.connect_order_socket(self.broker_broker_address)
            else:
                broker.data_socket = self.proxies[c.DATA].add_
                broker.order_socket = self.proxies[c.BROKER].add_party(name)    # returns socket
            
            # start threads if not syncing
            if not synced:
                broker_thread = threading.Thread(target = broker.run)
                broker_thread.daemon = True
                self.broker_threads[name] = broker_thread
                broker_thread.start()

        return

    def setup_strategies(self, use_zmq, synced):
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
        for strategy_id, strategy in self.strategies.items():
            strategy.zmq_context = self.zmq_context
            strategy.shutdown_flag = self.main_shutdown_flag

            # set up sockets
            if use_zmq:
                strategy.connect_data_socket(self.datafeed_subscriber_address)
                strategy.connect_order_socket(self.broker_strategy_address)
                strategy.connect_communication_socket(self.communication_address)
            else:
                strategy.data_socket = self.proxies[c.DATA].add_subscriber(
                    strategy.strategy_id, strategy.data_subscriptions)
                strategy.order_socket = self.proxies[c.ORDER].add_party(strategy.strategy_id)
                strategy.communication_socket = self.proxies[c.COMMUNICATION].add_party(
                    strategy.strategy_id)
            
            # Things to do before starting to handle events
            # Could be moving cash around, asking for information, etc.
            for strategy in self.strategies.values():
                strategy.start()
            
            # start running the strategies so they are ready to receive data
            strategy_thread = threading.Thread(target = strategy.run)
            strategy_thread.daemon = True
            self.strategy_threads[strategy_id] = strategy_thread
            # tell the threads to start listening
            strategy_thread.start()

        return

    def run(self, synced):
        """Starts the session."""

        if not synced:
            # simply tell datafeeds to start publishing
            # proxies, brokers, and strategies are all already running
            self.datafeed_start_sync.set()
            # wait for datafeeds to finishfor data_thread in self.datafeed_threads:
            for datafeed_thread in self.datafeed_threads:
                datafeed_thread.join()
        else:
            while not self.main_shutdown_flag.is_set():
                had_activity = self.next()
                if not had_activity:
                    break
        
        self.main_shutdown_flag.set()
        return

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
    
    def next(self):
        """Iterates one cycle of the main event loop

        The event loop is as follows:
            - clear communications queues
            - clear order queues
            - 
            - broker handle orders from strategies; send any response
            - get data
            - send data
            - broker handle data; send any response
        """

        # flag to keep track of activities
        had_data = False
        # Assume starting with all clear queues: no unhandled data, orders, or communications
        # First get the next data event
        if (data := self.synced_datafeed.fetch()) is not None:
            had_data = True
            topic = data[c.TOPIC]
            self.proxies[c.DATA].sockets_publisher['synced_feed'].deq_in.append([topic, data]) # proxy will take unpacked data
            # Pass down the data
            self.proxies[c.DATA].clear_queues()
            # At this point, brokers will have tried to fill orders with the data/cached the data,
            #   and strategies will have processed the data event and placed any orders
            # Allow strategies to pass on any communications/orders
            self.proxies[c.COMMUNICATION].clear_queues()
            # pass any orders to the broker
            self.proxies[c.BROKER].clear_queues()
            # If there are any responses, the parents will want to tell their children
            self.proxies[c.COMMUNICATION].clear_queues()

        return had_data

class zmq_datafeed_proxy:
    """Relays messages from backend (datafeed producers) to frontend (strategies).
        
        This is to cosolidate datafeeds so that strategies can have one central place to
        subscribe data from, and it is easier for the session to keep track of addresses.

        Note that in pyzmq's documentation, publishers are "frontend" and subscribers are
        "backend", so the syntax maybe a little confusing here if the user is looking
        at ZMQ docs.
    """

    def __init__(self, address_backend, address_frontend, address_capture = None, context = None, shutdown_flag = None):

        self.address_backend = address_backend
        self.address_frontend = address_frontend
        self.address_capture = address_capture
        self.context = context or zmq.Context.instance()
        self.shutdown_flag = shutdown_flag or threading.Event()

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
    def __init__(self, address_backend, address_frontend, address_capture = None, context = None, shutdown_flag = None, default_broker = None):
        
        self.address_backend = address_backend
        self.address_frontend = address_frontend
        # self.address_capture = address_capture
        self.context = context or zmq.Context.instance()
        self.shutdown_flag = shutdown_flag or threading.Event()
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
        while not self.shutdown_flag.is_set():

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
    def __init__(self, address, address_capture = None, context = None, shutdown_flag = None):
        
        self.address = address
        # self.address_capture = address_capture
        self.context = context or zmq.Context.instance()
        self.shutdown_flag = shutdown_flag or threading.Event()

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

    def run(self):
        while not self.shutdown_flag.is_set():
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
                    # print('communications', receiver, message_unpacked, '\n')
                    # send the message to the receiver
                    self.message_router.send_multipart([receiver.encode('utf-8'), utils.packb(message_unpacked)])

            except (zmq.ContextTerminated, zmq.ZMQError): # Not sure why it's not getting caught by ContextTerminated
                self.message_router.close(linger = 10)
            except:
                raise

        # exit gracefully
        self.message_router.close(linger = 10)

        return

class DatafeedProxyEmulator:
    """Relays data events to each strategy's sockets, filtering topics."""
    def __init__(self, shutdown_flag = None):

        self.shutdown_flag = shutdown_flag or threading.Event()
        self.sockets_publisher = {}     # datafeeds send data here
        self.sockets_subscriber = {}    # strategies get data here
        self.subscriptions = {}         # strategy subscriptions

    def add_publisher(self, ident):
        self.sockets_publisher[ident] = SocketEmulator(unpack = True)
        return self.sockets_publisher[ident]
    
    def add_subscriber(self, ident, subscriptions):
        self.sockets_subscriber[ident] = SocketEmulator()
        self.subscriptions[ident] = subscriptions
        return self.sockets_subscriber[ident]
    
    def clear_queues(self):
        """Relays the next item from each datafeed."""
        for datafeed in self.sockets_publisher.values():
            topic, data_unpacked = datafeed.deq_in.popleft()
            topic_encoded, data_packed = topic.encode(), utils.packb(data_unpacked)
            if (topic := data_unpacked.get(c.TOPIC)) is not None:
                # send this data event to each strategies that subscribes to this topic
                for strategy, subscriptions in self.subscriptions.items():
                    if topic in subscriptions:
                        self.sockets_subscriber[strategy].deq_out.append([topic_encoded, data_packed])
    
    def run(self):
        while not self.shutdown_flag.is_set():
            self.clear_queues()

class BrokerProxyEmulator:
    """Relays orders from one socket emulator to another.
    
    If the order's EVENT_SUBTYPE is REQUESTED, use the BROKER field to identify
        the socket to send to.
    Otherwise use the last item in the STRATEGY_CHAIN to identify the socket
        to send to.
    """
    def __init__(self, shutdown_flag = None, default_broker = None):

        self.shutdown_flag = shutdown_flag or threading.Event()
        self.sockets = {}
        self.default_broker = default_broker

    def add_party(self, ident):
        self.sockets[ident] = SocketEmulator(unpack = True)
        return self.sockets[ident]
    
    def clear_queues(self, max_iter = 2):
        """Clears the order queues.
        
        Runs until the earlier of:
            - one full cycle where all sockets have no messages
            - `max_iter` cycles
        """
        has_activities = True
        i = 0
        while has_activities and (i < max_iter):
            # assume there are no events
            has_activities = False
            i += 1
            for socket in self.sockets:
                if socket.deq_in:
                    # there was an event. Reset the flag.
                    has_activities = True
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
    """Relays messages from one socket emulator to another using RECEIVER_ID."""
    def __init__(self, shutdown_flag = None):

        self.shutdown_flag = shutdown_flag or threading.Event()
        self.sockets = {}

    def add_party(self, ident):
        self.sockets[ident] = SocketEmulator(unpack = True)
        return self.sockets[ident]

    def clear_queues(self, max_iter = 2):
        """Clears the order queues.
        
        Runs until the earlier of:
            - one full cycle where all sockets have no messages
            - `max_iter` cycles
        """
        has_activities = True
        i = 0
        while has_activities and (i < max_iter):
            # assume there are no events
            has_activities = False
            i += 1
            for socket in self.sockets:
                if socket.deq_in:
                    has_activities = True
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
        """items are sent here to the socket."""
        if self.unpack:
            self.deq_in.append(utils.unpackb(item))
        else:
            self.deq_in.append(item)

    def send_multipart(self, items):
        """items are sent here to the socket."""
        if self.unpack:
            self.deq_in.append([items[0].decode(), utils.unpackb(items[1])])
        else:
            self.deq_in.append(items)

    def recv(self):
        """items are retrieved from the socket here."""
        if self.deq_out:
            return self.deq_out.popleft()
        else:
            raise zmq.ZMQError(errno = zmq.EAGAIN, msg = 'Socket emulator: Nothing to receive.')
    
    def recv_multipart(self):
        """items are retrieved from the socket here."""
        if self.deq_out:
            return self.deq_out.popleft()
        else:
            raise zmq.ZMQError(errno = zmq.EAGAIN, msg = 'Socket emulator: Nothing to receive.')