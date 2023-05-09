"""Functions for sending things around."""
import random
import string
import socket
import datetime
import warnings
import decimal
import logging

import pandas as pd
import pytz
import msgpack


def get_free_tcp_address(port = 1234, max_port = 1300, exclude = None):
    """Gets a random free tcp port using sockets.

    Attempts to find a free tcp port between `port` and `max_port`, inclusive.
    Any ports to be avoided can be specified in `exclude`.

    Args:
        port (int, optional): Port to start trying. Defaults to 1234.
        max_port (int, optional): Port to stop trying. Defaults to 1300.
        exclude ([type], optional): Ports to exclude. Defaults to None.

    Returns:
        str, str, int: The free tcp address.
    """    
    exclude = exclude or []
    while port <= max_port:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(('', port))
            host, free_port = sock.getsockname()
            
            address = f'tcp://{host}:{free_port}'
            if (address not in exclude):
                sock.close()
                return address, host, free_port
            else:
                port += 1
                sock.close()
        except OSError as exc:
            # sock.close()
            port += 1
        except:
            raise
    raise IOError('No free ports.')

def get_inproc_address(exclude = None):
    """Generates a random inproc address.

    Args:
        exclude ([type], optional): Ports to exclude. Defaults to None.

    Returns:
        str: The generated inproc address.
    """    
    
    exclude = exclude or []
    while True:
        random_string = ''.join(random.choice(string.ascii_lowercase) for i in range(12))
        inproc_address = 'inproc://' + random_string
        if inproc_address not in exclude:
            return inproc_address
        else:
            pass

def default_pre_packer(obj):
    """ "Prepacks" certain data types before passing to msgpack.

    msgpack's "default packer" option kicks in after the module attempts
    to handle the known datatypes. i.e. custom handling of float is not
    handled.

    This prepacker should be applied before objects are passed to msgpack. A
    corresponding ext_hook must be applied when unpacking the object.

    This prepacker does the following conversions:
        - converts datetime.datetime objects to a msgpack extended type with typecode 5,
            with tzinfo attached. If a naive datetime object is passed, assumes system
            local timezone.
        - converts decimal objects to a msgpack extended type with typecode 10.

    Args:
        obj: The object to be prepacked.
    """    
    if isinstance(obj, datetime.datetime):
        if (tzinfo := obj.tzinfo) is None:
            obj = pd.Timestamp(obj).tz_localize('UTC')
            warnings.warn('Naive datetime object passed. Assuming UTC time.')
        processed = msgpack.ExtType(5, msgpack.packb(obj.isoformat(), default = str))
    elif isinstance(obj, decimal.Decimal):
        processed = msgpack.ExtType(10, str(obj).encode('utf-8'))
    elif isinstance(obj, logging.LogRecord):
        # Issue #25685: delete 'message' if present: redundant with 'msg'
        processed = obj.__dict__
        processed.pop('message', None)
        processed = msgpack.ExtType(15, msgpack.packb(processed))
    else:
        processed = obj
    return processed

def packb(obj):
    """Convenient way to set the default pre packer in packing."""
    return msgpack.packb(obj, default = default_pre_packer)

def ext_hook(ext_type_code, data):
    """Applies conversion to msgpack ext_types defind in default_pre_packer.

    Any msgpack ext_type defined in default_pre_packer must be unpacked by a
    corresponding ext_hook. See `default_pre_packer` for details of the object
    types that are dealt with here.
    """
    # Handle it if it is one of the pre-defined ext_types
    if ext_type_code == 5:
        return pd.Timestamp(msgpack.unpackb(data))
    elif ext_type_code == 10:
        return decimal.Decimal(data.decode('utf-8'))
    elif ext_type_code == 15:
        return logging.makeLogRecord(msgpack.unpackb(data))
    # otherwise let msgpack do the default handling of ext_types
    else:
        return msgpack.ExtType(ext_type_code, data)  

def unpackb(obj):
    """Convenient way to set the ext_hook in unapcking."""
    return msgpack.unpackb(obj, ext_hook = ext_hook)

class ZMQHandler(logging.Handler):
    def __init__(self, socket):
        logging.Handler.__init__(self)
        self.socket = socket

    def emit(self, record):
        record_packed = packb(record)
        try:
            self.socket.send_multipart([''.encode(), record_packed])
            print('sent!')
        except:
            logging.getLogger().handle(record)