"""A session coordinates instances of events, brokers, and strategies."""

from collections import deque
import threading
import time
import zmq
import logging

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
        The following settings are available for communication between objects:

        `use_zmq`: Whether or not to use ZMQ sockets.
            ZMQ sockets allows programs to be run on different machines and still communicate with each other.
            However, there is less fine tuned control over the order in which messages are processed, so lags
            that are inconsequential in real life could cause events to arrive out-of-order in a back test.
            
            Alternatively, a socket emulator can be used.
            A socket emulator provides the same interface as a ZMQ socket (send, send_multipart, recv, and recv_multipart),
            but behind the scenes it is really just an object that directs messages from and to the different sources that
            the object is connected to. There is more fine tuned control of how messages are processed/cleared, which
            allows for the following two event processing modes: synced and not synced.
        
        'synced`: Processing events in a synced manner, instead of an as-events-arrive manner.
            In a backtest, it is possible for processing times that are insignificant in real life to mess up
            event processing. For example, perhaps sending an order normally takes the strategy 1/100 second. In
            real life trading, this is likely not an issue, but in a backtest, this could mean that a whole day of bars
            have passed by the time the order has reached the broker.
            Because of this, there is a `synced` mode, where each "cycle" of events is cleared before moving onto
            the next. This is only available when ZMQ sockets are not used.

        Examples:

            >>> strat = gzmo.Strategy(name = 'example_strat')
            >>> datafeed_aapl = gzmo.SnowflakeDataFeed(topic = 'AAPL', query = ...)
            >>> sesh = gzmo.Session()
            >>> sesh.add_datafeed(datafeed_aapl)
            >>> sesh.add_strategy(strat)
            >>> sesh.run()
    """

    def __init__(self, use_zmq = False):
        """Inits the session instance.
        
        Strategies, datafeeds, and brokers can be added via the `add_...` methods.
        """

        self.use_zmq = use_zmq
        if self.use_zmq:
            # Use one context for all communication (thread safe)
            self._zmq_context = zmq.Context()

        # instances and threads
        # Strategies
        self.strategies = {}                         # strategy_id: strategy instances
        self._strategy_threads = {}                  # strategy_id: straegy thread

        # Datafeeds
        self.datafeeds = {}                         # name: datafeed instances
        self._datafeed_threads = {}                 # name: datafeed thread
        self._synced_datafeed = None
        # self._data_subscriber_socket = None

        self._datafeed_start_barrier = None
        self._datafeed_publisher_address = None     # datafeeds publish to this address
        self._datafeed_subscriber_address = None    # strategies subscribe to this address for datafeeds
        self._datafeed_capture_address = None       # for logging; currently not implemented
        
        # Brokers
        self.brokers = {}                           # name: broker instance
        self._broker_threads = {}                   # name: broker thread

        self._broker_broker_address = None          # brokers talk to this address. This will be a router socket / socket emulator
        self._broker_strategy_address = None        # strategies talk to this address. This will be a router socket / socket emulator
        self._broker_capture_address = None         # for logging; currently not implemented

        # Communication channel
        self._communication_address = None          # strategies can communicate to each other through this address.
        self._communication_capture_address = None  # for logging; currently not implemented

        # Proxies
        # Proxies consolidate datafeeds/brokers/etc so strategies have one central place to get/post data.
        self._proxies = {}                           # name: proxy instance
        self._proxy_threads = {}                     # name: proxy thread
        
        # main shutdown flag
        self._main_shutdown_flag = threading.Event()
        
        self.is_setup = False

        # catch child thread exceptions
        threading.excepthook = self.excepthook

    def excepthook(self, args):
        self.stop()
        raise Exception(f'caught {args.exc_type} with value {args.exc_value} in thread {args.thread}\n')

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
        """Runs session."""

        if not self.is_setup:
            # sets up addresses, proxies, brokers, datafeeds, and strategies
            self.setup()

        # start things
        self.start()

        # exit gracefully
        self.stop()
    
    # -----------------------------------------------------------------
    # set up stuff
    # -----------------------------------------------------------------

    def setup(self):
        """Sets up things."""

        self._setup_addresses(self.use_zmq)

        self._setup_proxies(self.use_zmq)

        self._setup_datafeeds(self.use_zmq) # starts the datafeed threads

        self._setup_brokers(self.use_zmq) # starts the broker threads

        self._setup_strategies(self.use_zmq)

        self.is_setup = True

    def _setup_addresses(self):
        """Obtains free tcp addresses for setting up sockets."""

        # addresses to avoid
        # want to avoid taking other sockets' addresses.
        # inproc was found to be rather unstable for some reason, so we stick with tcp.
        addresses_used = []

        if self.use_zmq:

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

            # logger
            self._logging_address, _, _ = utils.get_free_tcp_address(exclude = addresses_used)
            addresses_used.append(self._logging_address)
        
        else:
            pass

        return

    def _setup_proxies(self):
        """Sets up proxies to relay data, orders, and communications.
        
        This is so that strategies can have one central place to receive/post data.
        The session can have multiple datafeeds or brokers. All should connect
        to the proxies so intermediate handling of data/order events can be done.

        This will set up a proxy each for data, brokers, and inter-strategy communication.
        if `use_zmq`, a zmq proxy is created. Otherwise proxy emulators are used.

        All proxies are attached to `self._proxies`, and threads are started for each proxy.
        """

        # clear proxies
        self._proxy_threads = {}
        # default broker if none specified in order
        if self.brokers:
            default_broker = next(iter(self.brokers.keys()))
        else:
            default_broker = None

        if self.use_zmq:
            datafeed_proxy = zmq_datafeed_proxy(
                self._datafeed_publisher_address,
                self._datafeed_subscriber_address,
                self._datafeed_capture_address,
                self._zmq_context
            )
            # The broker proxy designates a default broker to send orders to
            broker_proxy = zmq_broker_proxy(
                self._broker_broker_address,
                self._broker_strategy_address,
                self._broker_capture_address,
                self._zmq_context,
                default_broker
            )
            communication_proxy = zmq_communication_proxy(
                self._communication_address,
                self._communication_capture_address,
                self._zmq_context
            )
            logging_proxy = zmq_logging_proxy(
                self._logging_address,
                self._zmq_context
            )
        
            # set up the proxy threads
            self._proxy_threads[c.DATA] = \
                threading.Thread(name = 'data_proxy_thread', target = datafeed_proxy.run, daemon = True)
            self._proxy_threads[c.BROKER] = \
                threading.Thread(name = 'broker_proxy_thread', target = broker_proxy.run, daemon = True)
            self._proxy_threads[c.COMMUNICATION] = \
                threading.Thread(name = 'communication_proxy_thread', target = communication_proxy.run, daemon = True)
            self._proxy_threads['LOGGING'] = \
                threading.Thread(name = 'logging_proxy_thread', target = logging_proxy.run, daemon = True)
        
        else:
            datafeed_proxy = DatafeedProxyEmulator()
            broker_proxy = BrokerProxyEmulator(default_broker = default_broker)
            communication_proxy = CommunicationProxyEmulator()
            logging_proxy = LoggingProxyEmulator()

            # set up the proxy threads
            self._proxy_threads[c.DATA] = \
                threading.Thread(name = 'data_proxy_thread', target = datafeed_proxy.run, args = (self._main_shutdown_flag,), daemon = True)
            self._proxy_threads[c.BROKER] = \
                threading.Thread(name = 'broker_proxy_thread', target = broker_proxy.run, args = (self._main_shutdown_flag,), daemon = True)
            self._proxy_threads[c.COMMUNICATION] = \
                threading.Thread(name = 'communication_proxy_thread', target = communication_proxy.run, args = (self._main_shutdown_flag,), daemon = True)
            self._proxy_threads['LOGGING'] = \
                threading.Thread(name = 'logging_proxy_thread', target = logging_proxy.run, args = (self._main_shutdown_flag,), daemon = True)

        self._proxies[c.DATA] = datafeed_proxy
        self._proxies[c.BROKER] = broker_proxy
        self._proxies[c.COMMUNICATION] = communication_proxy
        self._proxies['LOGGING'] = logging_proxy

        # Start the proxies
        for proxy_thread in self._proxy_threads.values():
            proxy_thread.start()
            
        return
            

    def _setup_datafeeds(self):
        """Sets up the datafeeds.

        Sets up the datafeeds so that they know how to emit the data.
        
        If not `synced`, all datafeeds are set to publish to an address in an
            unsynced manner. Datafeeds are started when this method is called,
            # TODO: is this true?
            but a threading.Barrier is in place so that the datafeeds do not start publishing
            until the the barrier is passed (in self.start).

            Datafeeds will emit data as soon as the data is available, without regards to
            whether the brokers/strategies are done processing prior data.

        If `synced`, all strategies will be combined into a synced datafeed via
            DatafeedSynchronizer, and data events will be `fetch`ed from the synced datafeed.
        
        """

        # set up connections
        if self.use_zmq:
            # need to wait for all datafeeds + the session to be ready
            self._datafeed_start_barrier = threading.Barrier(len(self.datafeeds) + 1)
        
            # If using ZMQ, let everyone know where to publish to
            for name, datafeed in self.datafeeds.items():
                datafeed.zmq_context = self._zmq_context
                datafeed.publish_to(self._datafeed_publisher_address)
                
                # datafeeds will wait for a signal to all start together
                datafeed._start_barrier = self._datafeed_start_barrier
                
                # Execute the queries of the datafeeds (starts caching data in `datafeed.results`)
                datafeed.execute_query()
            
                # set up threads
                datafeed_thread = threading.Thread(name = f'data_thread_{name}', target = datafeed.run, daemon = True)
                self._datafeed_threads[name] = datafeed_thread
        
        else:
            # if not using zmq, use `DatafeedSynchronizer` to sync different datafeeds
            self._synced_datafeed = DatafeedSynchronizer(datafeeds = self.datafeeds)
            #  give the synced datafeed a 'socket' to publish to
            #   (not using zmq, so self._proxies[c.DATA] is a proxy emulator)
            self._synced_datafeed.publishing_socket = \
                self._proxies[c.DATA].add_publisher('synced_datafeed')
                
            # execute the queries for the underlying datafeeds
            self._synced_datafeed.execute_query()
        
        return

    def _setup_brokers(self):
        """Sets up brokers for the different socket_modes."""
        # TODO: how would a `simplebroker` get data?
        for name, broker in self.brokers.items():
            # set up connections
            if self.use_zmq:
                broker.zmq_context = self._zmq_context
                # broker.connect_data_socket(self._datafeed_subscriber_address)
                broker.connect_order_socket(self._broker_broker_address)
                
                # set up threads
                broker_thread = threading.Thread(name = f'broker_thread_{name}', target = broker.run, daemon = True)
                self._broker_threads[name] = broker_thread
                
            else:
                # broker.data_socket = self._proxies[c.DATA].add_subscriber(name, '')
                broker.order_socket = self._proxies[c.BROKER].add_party(name)    # returns socket
                
        return

    def _setup_strategies(self, use_zmq):
        """Sets up strategies for the different socket_modes.

        If not synced, can start the strategies (they're just listening for data)      
        """
        for name, strategy in self.strategies.items():
            strategy.zmq_context = self._zmq_context
            strategy.shutdown_flag = self._main_shutdown_flag

            # set up sockets
            if use_zmq:
                strategy.connect_data_socket(self._datafeed_subscriber_address)
                strategy.connect_order_socket(self._broker_strategy_address)
                strategy.connect_communication_socket(self._communication_address)
                strategy.connect_logging_socket(self._logging_address)
                
                # set up the threads
                strategy_thread = threading.Thread(name = f'strategy_thread_{name}', target = strategy.run, daemon = True)
                self._strategy_threads[name] = strategy_thread
                
            else:
                strategy.data_socket = self._proxies[c.DATA].add_subscriber(
                    strategy.strategy_id, strategy.data_subscriptions)
                strategy.order_socket = self._proxies[c.BROKER].add_party(strategy.strategy_id)
                strategy.communication_socket = self._proxies[c.COMMUNICATION].add_party(
                    strategy.strategy_id)
                strategy.logger.addHandler(utils.ZMQHandler(self._proxies['LOGGING'].logging_socket))
            
                strategy.setup()

                # allow _before_start communication to go through
                # loop through the following until there are no more activities
                #   (datafeeds are not running yet)
                self.clear_queues()
        return


    # -----------------------------------------------------------------
    # start stuff
    # -----------------------------------------------------------------

    def start(self):
        """Starts the session.
        
        Starts the datafeed threads
        """

        if self.use_zmq:
            # these threads call the "run" methods of each object
            for broker_thread in self._broker_threads.values():
                broker_thread.start()
            for strategy_thread in self._strategy_threads.values():
                strategy_thread.start()
            for datafeed_thread in self._datafeed_threads.values():
                datafeed_thread.start()

            # all data feeds will start publishing together when the barrier is passed by everyone
            # Usually this should be the last barrier to pass
            # proxies, brokers, and strategies should all already running
            self._datafeed_start_barrier.wait()
            
            # run until all datafeeds finish:
            try:
                for datafeed_thread in self._datafeed_threads.values():
                    datafeed_thread.join()
            except KeyboardInterrupt:
                self.stop()
        else:
            while not self._main_shutdown_flag.is_set():
                try:
                    had_activity = self.next()
                    if not had_activity:
                        break
                except KeyboardInterrupt:
                    self._main_shutdown_flag.set()

        return

    def next(self):
        """Iterates one cycle of the main event loop

            The event loops has strategies and broker process events,
            and proxies relay messages, until an iteration where all
            entities have no activities.

            To use `next`, zmq cannot be used.
        """

        if not self.is_setup:
            self.setup()

        # Set to True if there is either data or activities from strategies/brokers
        had_data = False

        # Assume starting with all clear queues: no unhandled data, orders, or communications
        # First get the next data event
        if (datas := self._synced_datafeed.fetch()):    # return [] if no results
            data = datas[0]
            had_data = True
            topic = data[c.TOPIC]
            self._proxies[c.DATA].sockets_publisher['synced_datafeed'].send_multipart([topic.encode(), utils.packb(data)])
            # Broadcast the data
            self._proxies[c.DATA].clear_queues()
        
        had_activity = self.clear_queues()
        had_data_or_activity = had_data | had_activity

        return had_data_or_activity

    def clear_queues(self):
        
        had_activity = False
        # clear all activities form strategies and brokers
        for i in range(999):
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
            
            self._proxies['LOGGING'].clear_queues()
            
            had_activity |= has_activity

            if not has_activity:
                print('all clear; moving onto the next round!')
                return had_activity
                
        raise RecursionError('Too many rounds of activities for 1 datapoint.')
        
        


    def stop(self, linger = 0.1):
        """Exit gracefully."""

        if self.use_zmq:
            self._zmq_context.destroy()
            # wait for the threads to exit clean
            # _before_stop() and _stop() are called after shutdown flag is set
            for datafeed_thread in self._datafeed_threads.values():
                datafeed_thread.join()
            for strategy_thread in self._strategy_threads.values():
                # _before_stop() and _stop() are called after shutdown flag is set
                strategy_thread.join()
            for strategy_thread in self._strategy_threads.values():
                # _before_stop() and _stop() are called after shutdown flag is set
                strategy_thread.join()
        else:
            self._main_shutdown_flag.set()
            for strategy in self.strategies.values():
                strategy._stop()
        
class zmq_datafeed_proxy:
    """Relays messages from backend (datafeed producers) to frontend (strategies).
        
        This is to cosolidate datafeeds so that strategies can have one central place to
        subscribe data from, and it is easier for the session to keep track of addresses.

        Note that in pyzmq's documentation, publishers are "frontend" and subscribers are
        "backend", so the syntax maybe a little confusing here if the user is looking
        at ZMQ docs.
    """

    def __init__(self, address_backend, address_frontend, address_capture, zmq_context):

        self.address_backend = address_backend
        self.address_frontend = address_frontend
        self.address_capture = address_capture          
        self.zmq_context = zmq_context

        # publisher facing socket
        self.backend = self.zmq_context.socket(zmq.SUB)
        # no filtering here (subscribe to everything)
        self.backend.setsockopt(zmq.SUBSCRIBE, b'')
        self.backend.bind(address_backend)

        # client facing socket
        self.frontend = self.zmq_context.socket(zmq.PUB)
        self.frontend.bind(address_frontend)
        
        if address_capture is not None:
            # bind to capture address
            self.capture = self.zmq_context.socket(zmq.PUB)
            self.capture.bind(address_capture)
        else:
            self.capture = None
    
    def run(self):
        self._start()
        self._stop()

    def _start(self):

        if not self.is_setup:
            self.setup()

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

    def _stop(self):
        print('Shutting down zmq datafeed proxy.')

class zmq_broker_proxy:
    """Relays orders to the correct brokers/strategies.

        This is a broker of brokers that manages sender and receiver identities.
    
        The flow of an order is as follow:

        strategy (dealer): sends order

        proxy (router): receives (strategy ident, order)
        proxy extracts broker name from order
        proxy adds the field SENDER_ID = strategy ident to the order
        proxy (router): sends (broker ident, order)

        broker (dealer): receives order
        broker processes order
        broker (dealer): sends order response

        proxy (router): receives (broker ident, order response)
        proxy extract SENDER_ID = strategy ident from order response
        proxy (router): sends (strategy ident, order response)

        strategy (dealer): receives order response
    
    """
    def __init__(self, address_backend, address_frontend, address_capture, zmq_context, default_broker):
        
        self.address_backend = address_backend
        self.address_frontend = address_frontend
        # self.address_capture = address_capture
        self.zmq_context = zmq_context
        self.default_broker = default_broker

        # establish strategy facing socket
        self.frontend = self.zmq_context.socket(zmq.ROUTER)
        self.frontend.bind(address_frontend)
        # establish broker facing socket
        self.backend = self.zmq_context.socket(zmq.ROUTER)
        self.backend.bind(address_backend)
        # TODO: implement capture socket
        # if there is a capture socket
        if address_capture:
            # bind to capture address
            self.capture = self.zmq_context.socket(zmq.PUB)
            self.capture.bind(address_capture)
        else:
            capture_socket = None

        poller = zmq.Poller()
        poller.register(self.frontend, zmq.POLLIN)
        poller.register(self.backend, zmq.POLLIN)
    
    def run(self):
        self._start()
        self._stop()

    def _start(self):

        while True:

            try:
                socks = dict(self.poller.poll(timeout = 10))
                
                if socks.get(self.frontend) == zmq.POLLIN:
                    # received order from strategy: (strategy ident, order)
                    # Note that the strategy should already have placed its strategy_id in the STRATEGY_CHAIN in the order
                    strategy_id_encoded, order_packed = self.frontend.recv_multipart()
                    order_unpacked = utils.unpackb(order_packed)
                    
                    # Figure out which broker to send orders from
                    if (broker := order_unpacked.get(c.BROKER)) is not None:
                        broker = broker
                    else:
                        broker = self.default_broker

                    if broker is None:
                        # this really shouldn't happen unless user doesn't know how to set up the proxy
                        raise Exception('Either specify broker in order or specify default broker in session.')
                    
                    # send the order to the broker: (broker name, )
                    self.backend.send_multipart([broker.encode('utf-8'), order_packed])

                elif socks.get(self.backend) == zmq.POLLIN:
                    # received order response from broker: (broker ident, (strategy ident, order))
                    broker_encoded, order_packed = self.backend.recv_multipart()
                    # find out which strategy this order came from in the first place
                    order_unpacked = utils.unpackb(order_packed)
                    send_to = order_unpacked[c.STRATEGY_CHAIN][-1]
                    self.frontend.send_multipart([send_to.encode('utf-8'), order_packed])
            except (zmq.ContextTerminated, zmq.ZMQError): # Not sure why it's not getting caught by ContextTerminated
                self._stop()
            except:
                raise


    def _stop(self):
        print('shutting down broker proxy!')
        # exit gracefully
        self.frontend.close(linger = 10)
        self.backend.close(linger = 10)
        # self.capture.close(linger = 10)

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
    def __init__(self, address, address_capture, zmq_context):
        
        self.address = address
        # self.address_capture = address_capture
        self.zmq_context = zmq_context

        self.message_router = self.self.zmq_context.socket(zmq.ROUTER)
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
        self._start()
        self._stop()

    def _start(self):

        while True:
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
                    self.message_router.send_multipart([receiver.encode('utf-8'), message_packed])

            except (zmq.ContextTerminated, zmq.ZMQError): # Not sure why it's not getting caught by ContextTerminated
                self._stop
            except:
                raise

    def _stop(self):
        self.message_router.close(linger = 10)

class zmq_logging_proxy:
    """Proxy to handle logging.
        Simply receives log records and have the session logger handle them.
    """
    def __init__(self, address, zmq_context):

        self.address = address
        self.zmq_context = zmq_context

        self.socket = self.zmq_context.socket(zmq.SUB)
        # no filtering here
        self.socket.setsockopt(zmq.SUBSCRIBE, b'')
        self.socket.bind(address)

        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
    
    def run(self):
        self._start()
        self._stop()

    def _start(self):

        while True:
            try:
                socks = dict(self.poller.poll())
                if socks.get(self.socket) == zmq.POLLIN:
                    # received log record
                    topic_encoded, logrecord_packed = self.socket.recv_multipart()
                    logrecord_unpacked = utils.unpackb(logrecord_packed)
                    logging.getLogger().handle(logrecord_unpacked)
            except (zmq.ContextTerminated, zmq.ZMQError): # Not sure why it's not getting caught by ContextTerminated
                self._stop
            except:
                raise

        return

    def _stop(self):
        self.socket.close(linger = 10)

class DatafeedProxyEmulator:
    """Relays data events to each strategy's sockets, filtering topics."""
    def __init__(self, session_shutdown_flag):

        self.sockets_publisher = {}     # datafeeds send data here
        self.sockets_subscriber = {}    # strategies get data here
        self.subscriptions = {}         # strategy subscriptions (}name: list of subscriptions})
        self.session_shutdown_flag = session_shutdown_flag

    def add_publisher(self, ident):
        self.sockets_publisher[ident] = SocketEmulator()
        return self.sockets_publisher[ident]
    
    def add_subscriber(self, ident, subscriptions):
        self.sockets_subscriber[ident] = SocketEmulator()
        self.subscriptions[ident] = subscriptions
        return self.sockets_subscriber[ident]
    
    def clear_queues(self):
        """Move next items from deq from all sockets to deq to any subscribing sockets."""
        # loop through each datafeed (`SocketEmulator`s)
        for datafeed in self.sockets_publisher.values():
            # Check if the datafeeds have produced anything
            if datafeed.deq:
                topic_encoded, data_packed = datafeed.deq.popleft()
                topic_decoded, data_unpacked = topic_encoded.decode(), utils.unpackb(data_packed)
                # If so, queue it up to be sent as a data event to each strategy that subscribes to this topic
                for strategy, subscriptions in self.subscriptions.items():
                    if (topic_decoded in subscriptions) or ('' in subscriptions):
                        self.sockets_subscriber[strategy].deq.append([topic_encoded, data_packed])
                return True
    
    def run(self):
        self._start()
        self._stop()

    def _start(self, pause = 0.1):
        
        while not self.session_shutdown_flag.is_set():
            if not self.clear_queues():
                time.sleep(pause)
    
    def _stop(self):
        pass

class BrokerProxyEmulator:
    """Relays orders between strategies and brokers.
    
    If the order's EVENT_SUBTYPE is REQUESTED, use the BROKER field to identify
        the socket to send to.
    Otherwise use the last item in the STRATEGY_CHAIN to identify the socket
        to send to.
    """
    def __init__(self, default_broker, session_shutdown_flag):

        self.sockets = {}
        self.default_broker = default_broker
        self.session_shutdown_flag = session_shutdown_flag

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
                if socket.deq:
                    order_packed = socket.deq.popleft()
                    order_unpacked = utils.unpackb(order_packed)
                    # REQUESTED ==> from strategy
                    if (order_type := order_unpacked[c.EVENT_SUBTYPE]) == c.REQUESTED:
                        if (broker := order_unpacked.get(c.BROKER)) is None:
                            broker = self.default_broker
                        if broker is None:
                            raise Exception('Either specify broker in order or specify default broker in session.')
                        self.sockets[broker].deq.append(utils.packb(order_unpacked))
                    else:
                        strategy = order_unpacked[c.STRATEGY_CHAIN][-1]
                        self.sockets[strategy].deq.append(order_packed)
                    # there was an event. Reset the flag.
                    has_activities = True
                    had_activities = True
        
        return had_activities

    
    def run(self):
        self._start()
        self._stop()

    def _start(self, pause = 1):
        
        while not self.session_shutdown_flag.is_set():
            if not self.clear_queues():
                time.sleep(pause)

    def _stop(self):
        pass


class CommunicationProxyEmulator:
    """Relays messages from one socket emulator to another using RECEIVER_ID."""
    def __init__(self, session_shutdown_flag):

        self.sockets = {}
        self.session_shutdown_flag = session_shutdown_flag

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
        had_activities = False   # for keeping track of entire function call
        i = 0
        while has_activities and (i < max_iter):
            # assume there are no events
            has_activities = False
            i += 1
            
            for socket in self.sockets.values():
                if socket.deq:
                    message_packed = socket.deq.popleft()
                    message_unpacked = utils.unpackb(message_packed)
                    if (receiver := message_unpacked.get(c.RECEIVER_ID)) is None:
                        raise Exception('No receiver for message specified')
                    if receiver not in self.sockets.keys():
                        raise Exception(f'Receiver {receiver} not found.')
                    # attach a packed message for receiving at the receiver's socket
                    self.sockets[receiver].deq.append(message_packed)
                    has_activities = True
                    had_activities = True

        return had_activities
    
    def run(self):
        self._start()
        self._stop()

    def _start(self,pause = 1):
        
        while not self.session_shutdown_flag.is_set():
            if not self.clear_queues():
                time.sleep(pause)

    def _stop(self):
        pass

class LoggingProxyEmulator:
    """Receives logging records."""
    def __init__(self, session_shutdown_flag):
        self.session_shutdown_flag = session_shutdown_flag
        self.logging_socket = SocketEmulator()
    
    def clear_queues(self):
        """Have root logger handle all log records."""
        has_activities = False
        while (log_deq := self.logging_socket.deq):
            has_activities = True
            # fetch and unpack the next log record
            topic_encoded, logrecord_packed = log_deq.popleft()
            logrecord_unpacked = utils.unpackb(logrecord_packed)
            # have the root logger handle this
            logging.getLogger().handle(logrecord_unpacked)
        return has_activities

    def run(self):
        self._start()
        self._stop()

    def _start(self, pause = 1):
                
        while not self.session_shutdown_flag.is_set():
            if not self.clear_queues():
                time.sleep(pause)
    
    def _stop(self):
        pass

class SocketEmulator:
    def __init__(self):
        """A class to emulate sockets for communication.
            For two-way communications, a proxy should be implemented to have two `SocketEmulator`s
            and pass events between the two.

        """
        # self.unpack = unpack

        self.deq = deque()
        self.closed = False

    def send(self, item, flags = zmq.NULL):
        """items are sent here to the socket."""
        # if self.unpack:
        #     self.deq.append(utils.unpackb(item))
        # else:
        #     self.deq.append(item)
        self.deq.append(item)
        

    def send_multipart(self, items, flags = zmq.NULL):
        """items are sent here to the socket."""
        self.deq.append(items)

    def recv(self, flags = zmq.NULL):
        """items are retrieved from the socket here."""
        if flags == zmq.NOBLOCK:
        # do not block and throw and error
            if self.deq:
                return self.deq.popleft()
            else:
                raise zmq.ZMQError(errno = zmq.EAGAIN, msg = 'Socket emulator: Nothing to receive.')
        else:
        # block and wait for message
            while True:
                if self.deq:
                    return self.deq.popleft()
    
    def recv_multipart(self, flags = zmq.NULL):
        return self.recv(flags)
    
    def close(self, *args, **kwargs):
        return