"""Base datafeed class."""

import zmq
import threading
import abc

class BaseDataFeed:
    """Base datafeed class.

        ALL BARS MUST BE RIGHT-EDGE-ALIGNED (timestamp for bar signifies end of bar period).

        A datafeed provides data in one of two ways: `publish` and `fetch`.

        `publish` publishes data to the data socket, which can be specified after initialization.
        `fetch` returns the next record when called.

        Attributes:
            topic (str): The topic the datafeed will be published under (in a PUB socket).
            zmq_context (ZMQ Context): ZMQ context instance for publishing.
            from_beginning (bool): Indicates whether to re-execute the query.
            start_sync (threading.Event): can be used to sync starting times between multiple datafeeds.
                To use it, simply assign `datafeed.start_sync` to a common threading.Event() instance,
                start all the datafeeds, and `set` the threading.Event() instance.
            main_shutdown_flag (threading.Event): If threading is used, this event can be set to signal that the main session has shut down.
            shutdown_flag (threading.Event): If threading is used, this event can be set to signal the datafeed to shut down.
    """

    def __init__(self, topic, zmq_context = None):
        """ Inits a datafeed object.

        Args:
            topic (str): The topic the datafeed will be published under (in a PUB socket).
            zmq_context (zmq.Context, optional): ZMQ context instance for publishing. Defaults to None.
        """
        self.topic = topic
        self.zmq_context = zmq_context or zmq.Context.instance()
        self.from_beginning = True
        self.start_sync = threading.Event(); self.start_sync.set()  # default to not block for a sync
        self.is_finished = False
        self.main_shutdown_flag = threading.Event()
        self.shutdown_flag = threading.Event()

    def publish_to(self, address):
        """ Tells the datafeed where to publish to (if sockets are used).

        Args:
            address (str): A ZMQ address string in the form of 'protocol://interface:port’.
        """
        self.address = address
        # Connect to a port
        self.sock_out = self.zmq_context.socket(zmq.PUB)
        # Note here we connect to this addres (instead of bind) because we have
        #    a multiple publisher (datafeeds) - one subscriber (session) pattern
        self.sock_out.connect(address)

    @abc.abstractmethod
    def execute_query(self):
        pass

    @abc.abstractmethod
    def fetch(self, limit = 1):
        pass
    
    def publish(self):
        """Publishes the queried data to the socket.

            The queried data will be published to the socket, record by record, until
            all records have been published.

            When called, it will wait for the `start_sync` flag to be set, if not already.
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
        self.start_sync.wait()

        # Keep going?
        while (not self.main_shutdown_flag.is_set()) and \
                (not self.shutdown_flag.is_set()) and \
                (not self.is_finished):
            
            # get one row of result everytime
            # maybe slower but won't have to worry about size of results
            if (res := self.fetch(limit = 1)) is not None:
                try:
                    # send the event with a topic
                    res_packed = utils.packb(res)
                    self.sock_out.send_multipart([self.topic.encode(), res_packed], flag = zmq.NOBLOCK)
                except zmq.ZMQError as exc:
                    # Drop messages if queue is full
                    if exc.errno == zmq.EAGAIN:
                        pass
                    else:
                        # unexpected error: shutdown and raise
                        self.shutdown()
                        raise
                except zmq.ContextTerminated:
                    # context is being closed by session
                    self.shutdown()
                except:
                    raise
            else:
                # no more results
                self.is_finished = True
                self.shutdown()

                break
        
        # shut down gracefully
        self.shutdown()

        return

    def shutdown(self):
        """Shuts down gracefully."""
        self.shutdown_flag.set()
        self.sock_out.close(linger = 10)