"""Functions for sending things around."""
import random
import string
import socket
import datetime
import warnings
import decimal

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
            obj = obj.astimezone(tz = None) # assumes system local timezone
            warnings.warn('Naive datetime object passed. Assuming system local timezone.')
        processed = msgpack.ExtType(5, msgpack.packb((obj.isoformat(), obj.tzinfo), default = str))
    else:
        if isinstance(obj, decimal.Decimal):
            processed = msgpack.ExtType(10, str(obj).encode('utf-8'))
        # elif (s := str(obj)).isnumeric():
        #     processed = obj
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
        isoformat, tzinfo = msgpack.unpackb(data)
        try:
            # add tzinfo if it is recognized by pytz
            # Note that some timezone names can be ambiguous (see https://pypi.org/project/pytz/)
            #   In such cases (e.g. PST), the tzname will be dropped.
            tzinfo = pytz.timezone(tzinfo)
            return tzinfo.localize(datetime.datetime.fromisoformat(isoformat).replace(tzinfo = None))
        except:
            return datetime.datetime.fromisoformat(isoformat)
    elif ext_type_code == 10:
        return decimal.Decimal(data.decode('utf-8'))
    # otherwise let msgpack do the default handling of ext_types
    else:
        return msgpack.ExtType(ext_type_code, data)  

def unpackb(obj):
    """Convenient way to set the ext_hook in unapcking."""
    return msgpack.unpackb(obj, ext_hook = ext_hook)