"""Base datafeed class."""
import configparser
import threading
import abc
import datetime

import pytz
import zmq

from ..event_handler import Event
from .. import utils
from ..utils import constants as c

class BaseDataFeed(abc.ABC):
    """Base datafeed class.

        ALL BARS MUST BE RIGHT-EDGE-ALIGNED (timestamp for bar signifies end of bar period).

        A datafeed provides data in one of two ways: `publish` and `fetch`.

        `publish` publishes data to the data socket, which can be specified after initialization.
        `fetch` returns the next record when called.

        Attributes:
            topic (str): The topic the datafeed will be published under (in a PUB socket).
            zmq_context (ZMQ Context): ZMQ context instance for publishing.
            from_beginning (bool): Indicates whether to re-execute the query.
            _start_barrier (threading.Barrier): can be used to sync starting times between multiple datafeeds.
                To use it, simply assign `datafeed._start_barrier` to a threading.Barrier instance,
                with the number of parties as the number of entities to wait for to start publishing.
            shutdown_flag (threading.Event): If threading is used, this event can be set to signal the datafeed to shut down.
        
        Methods:
            format_query: formats the query to the format accepted by the external datafeed.
            execute_query: makes the connection and execute the query for results.
            format_result: formats the received results to internal format.
            fetch: returns records from the retrieved results (called by publishfetch).
    """

    def __init__(self, topic, query, auth = None, zmq_context = None):
        """ Inits a datafeed object.

        Args:
            topic (str): The topic the datafeed will be published under (in a PUB socket).
            zmq_context (zmq.Context, optional): ZMQ context instance for publishing. Defaults to None.
        """
        self.topic = topic
        self.query = query
        self.zmq_context = zmq_context or zmq.Context.instance()
        self.from_beginning = True
        self._start_barrier = threading.Barrier(1)  # default to not wait for anyone else to start publishing
        self.is_finished = False
        self._shutdown_flag = threading.Event()
        self.publishing_socket = None

        # Get auth file if a path if provided
        if isinstance(auth, str):
            # read the file
            config = configparser.ConfigParser()
            config.read_file(open(auth))
            self.auth = config
        else:
            self.auth = auth

    def publish_to(self, address):
        """ Tells the datafeed where to publish to (if sockets are used).

        Args:
            address (str): A ZMQ address string in the form of 'protocol://interface:port’.
        """
        self.address = address
        # Connect to a port
        self.publishing_socket = self.zmq_context.socket(zmq.PUB)
        # Note here we connect to this addres (instead of bind) because we have
        #    a multiple publisher (datafeeds) - one subscriber (session) pattern
        self.publishing_socket.connect(address)

    @abc.abstractmethod
    def format_query(self, query):
        """Translate the query from standard format to datafeed specific format."""
        return

    @abc.abstractmethod
    def execute_query(self):
        """Authentication and connections should be made here."""
        pass

    @abc.abstractmethod
    def format_result(self, result):
        """Formats the datafeed result to internal standards."""
        return

    @abc.abstractmethod
    def fetch(self, limit = 1):
        pass
    
    def run(self, session_shutdown_flag = None, pause = 1):
        return self._start(session_shutdown_flag = session_shutdown_flag, pause = pause)

    def _start(self, session_shutdown_flag = None, pause = 1):
        """Publishes the queried data to the socket.

            The queried data will be published to the socket, record by record, until
            all records have been published.

            When called, it will wait for the `_start_barrier` barrier to be passed, if not already.
            Then, it will fetch one record at a time from the connector cursor, and publish
            the record under the `topic` of the datafeed. The record will be packed as a
            msgpack message.

            If self.from_beginning is True (as it is set when the datafeed is instantiated),
            the query will be executed when this method is called.

            Note that if the ZMQ queue is full, DATA WILL BE (SILENTLY) DROPPED.
        """        
        # if starting over
        if self.from_beginning:
            self.execute_query()
        
        # wait for the starting signal
        self._start_barrier.wait()

        # Keep going?
        while (not session_shutdown_flag.is_set()) and \
                (not self._shutdown_flag.is_set()) and \
                (not self.is_finished):
            
            # get one row of result everytime
            # maybe slower but won't have to worry about size of results
            if (res := self.fetch(limit = 1)) is not None:
                try:
                    # send the event with a topic
                    res_packed = utils.packb(res)
                    self.publishing_socket.send_multipart([self.topic.encode(), res_packed], flag = zmq.NOBLOCK)
                except zmq.ZMQError as exc:
                    # Drop messages if queue is full
                    if exc.errno == zmq.EAGAIN:
                        pass
                    else:
                        # unexpected error: shutdown and raise
                        self._stop()
                        raise
                except zmq.ContextTerminated:
                    # context is being closed by session
                    self._stop()
                except:
                    raise
            else:
                # no more results
                self.is_finished = True
                self._stop()

                break
        
        # shut down gracefully
        self._stop()

        return

    def _stop(self):
        """Shuts down gracefully."""
        self._shutdown_flag.set()
        self.publishing_socket.close(linger = 10)


class DataFeedQuery:
    """A dictionary providing the necessary parameters for a query for data."""

    @staticmethod
    def BarQuery(
            symbol,
            start = pytz.timezone('America/New_York').localize(datetime.datetime(2020, 1, 1, 0, 0)).isoformat(),
            end = pytz.timezone('America/New_York').localize(datetime.datetime(2020, 1, 31, 0, 0)).isoformat(),
            multiplier = 1,
            resolution = 'm',
            alignment = c.LEFT):
            
        q = {
            c.DATA_TYPE: c.BAR,
            c.SYMBOL: symbol,
            c.START: start,
            c.END: end,
            c.MULTIPLIER: multiplier,
            c.RESOLUTION: resolution,
            c.ALIGNMENT: alignment
            }
        return q

    @staticmethod
    def QuoteQuery(
            symbol,
            start = pytz.timezone('America/New_York').localize(datetime.datetime(2020, 1, 1, 0, 0)).isoformat(),
            end = pytz.timezone('America/New_York').localize(datetime.datetime(2020, 1, 31, 0, 0)).isoformat()):
            
        q = {
            c.DATA_TYPE: c.QUOTE,
            c.SYMBOL: symbol,
            c.START: start,
            c.END: end
            }
        return q

    @staticmethod
    def TickQuery(
            symbol,
            start = pytz.timezone('America/New_York').localize(datetime.datetime(2020, 1, 1, 0, 0)).isoformat(),
            end = pytz.timezone('America/New_York').localize(datetime.datetime(2020, 1, 31, 0, 0)).isoformat()):
            
        q = {
            c.DATA_TYPE: c.TICK,
            c.SYMBOL: symbol,
            c.START: start,
            c.END: end
            }
        return q

class DataEvent:
    """Subclasses Event to have data-specific events.
    
    Each datafeed should return DataEvent objects to keep a consistent
    format for consumption.
    """
    
    class base_data_event(Event):

        def __init__(
                self,
                event_subtype,
                event_ts):

            super().__init__(
                event_type = c.DATA,
                event_subtype = event_subtype,
                event_ts = event_ts
            )

            return
    
    class bar(base_data_event):
        def __init__(
            self,
            event_ts = None,
            symbol = None,
            open_ = None,
            high = None,
            low = None,
            close = None,
            volume = None):

            super().__init__(event_ts, c.BAR)

            self.update({
                c.SYMBOL: symbol,
                c.OPEN: open_,
                c.HIGH: high,
                c.LOW: low,
                c.CLOSE: close,
                c.VOLUME: volume
            })

            return

    class quote(base_data_event):
        def __init__(
            self,
            event_ts = None,
            symbol = None,
            ask_exchange = None,
            ask_price = None,
            ask_size = None,
            bid_exchange = None,
            bid_price = None,
            bid_size = None,
            quote_conditions = None):
        
            super().__init__(event_ts, c.QUOTE)

            self.update({
                c.SYMBOL: symbol,
                c.ASK_EXCHANGE: ask_exchange,
                c.ASK_PRICE: ask_price,
                c.ASK_SIZE: ask_size,
                c.BID_EXCHANGE: bid_exchange,
                c.BID_PRICE: bid_price,
                c.BID_SIZE: bid_size,
                c.QUOTE_CONDITIONS: quote_conditions
            })

            return
    
    class tick(base_data_event):
        def __init__(
            self,
            event_ts = None,
            symbol = None,
            exchange = None,
            price = None,
            size = None,
            conditions = None,
            trade_id = None,
            tape = None):
        
            super().__init__(event_ts, c.TICK)

            self.update({
                c.SYMBOL: symbol,
                c.EXCHANGE: exchange,
                c.PRICE: price,
                c.SIZE: size,
                c.CONDITIONS: conditions,
                c.TRADE_ID: trade_id,
                c.TAPE: tape
            })

            return