'''
A session coordinates instances of events, brokers, and strategies.
'''

import abc

def Session(abc.ABC):
    def __init__(self):
        self.strategies = {}
    
    def run(self):
        # TODO
        # What to do?

        # start all strategies and make sure they're running
        # start bob and make sure it is running
        # ping console
        # start data
        

    @property
    @abc.abstractmethod
    def console(self):
        '''The console from which the session will receive commands/send messages'''
        pass
    
    @property
    @abc.abstractmethod
    def bob(self):
        pass

    @abc.abstractmethod
    def add_strategies(self):
        '''add strategies to the session'''
        pass
    
    @abc.abstractmethod
    def add_data(self):
        '''add data feeds'''

    @abc.abstractmethod
    def start_data(self):
        '''start data feeds'''
        pass

    @abc.abstractmethod
    def start_bob(self):
        '''start broker of brokers instance'''
        pass

    @abc.abstractmethod
    def start_strategy(self):
        '''start an added strategy'''
        pass

    @abc.abstractmethod
    def kill_strategy(self):
        '''Kill a strategy'''
        pass