'''
A strategy contains the business logic for making actions given events.
'''


# Portfolio
# Position
# Dispatcher
# Strategies
# parent
# Child

import abc
import collections

from constants import *


class EventHandler(abc.ABC):
    '''
    Event handler is the base class for the two main classes: Strategy and Position.

    A Strategy is really an event handler with added logic and communication infrastructure,
    whereas a Position is an event handler that updates attributes given order and data events.
    '''

    def __init__(self):
        pass

    def handle_event(self, event):
        '''process the next event'''
        try:
            event_type = event['type']
        except KeyError:
            raise 'Event type not specified'
        
        if event_type == DATA:
            self._handle_data(event)
        elif event_type == ORDER:
            self._handle_order(event)
        elif event_type == COMMAND:
            self._handle_command(event)
        else:
            raise 'Event type {} not supported.'.format(event_type)
    
    @abc.abstractmethod
    def _handle_data(self, event):
        '''Handle a data event'''
        return
    
    @abc.abstractmethod
    def _handle_order(self, order):
        '''Handle an order event.'''
        return

    @abc.abstractmethod
    def _handle_command(self, command_event):
        return


class Trade:
    '''A trade consists of a group of transactions that should be viewed together'''
    def __init__(self):
        # TODO: get name of strategy from which it's called
        # Define the following
        # self.strategy_id
        # self.trade_id
        # self.trade_name
        # self.status
        # self.risk
        # self.realized
        # self.unrealized
        # self.commission
        # self.positions
        pass
    
    def update(self, event):
        # Handle events: Order, Fill, Flow, Command
        pass
    
    def close(self, params):
        # liquidate the position
        pass

