'''Getting data from various places then sending them to a data dispatch agent'''
import abc
import collections

import zmq
import msgpack

from constants import *

def publish(address, topic, datafeed, zmq_context = None):

    context = zmq_context or zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind(address)
    # TODO: wait for start request
    for data_event in datafeed:
        socket.send_multipart([msgpack.packb(topic), msgpack.packb(data_event)])

        