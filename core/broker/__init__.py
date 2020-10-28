'''
Broker takes orders from a queue and handle them.
'''
import abc


class BOB(ABC):
    '''
    Broker of brokers: accepts order events and route them to the appropriate broker.
    Also responsible for publishing fill events.
    '''
    def __init__(self):
        pass

    @abc.abstractmethod
    def add_broker(self):
        pass