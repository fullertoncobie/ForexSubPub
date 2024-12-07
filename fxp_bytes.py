"""
Forex Provider
(c) all rights reserved

This module contains useful marshalling functions for manipulating Forex Provider packet contents.
"""
import ipaddress
from array import array
from datetime import datetime

MAX_QUOTES_PER_MESSAGE = 50
MICROS_PER_SECOND = 1_000_000


def serialize_price(x: float) -> bytes:
    """
    Convert a float to a byte array used in the price feed messages.

    # d5e9f642 in hex on a little-endian ieee754 machine
    >>> serialize_price(123.4567)
    b'\xd5\xe9\xf6B'

    :param x: number to be converted
    :return: bytes suitable to be sent in a Forex Provider message
    """
    a = array('f', [x])  # array of 8-byte floating-point numbers
    return a.tobytes()


def deserialize_address(b: bytes) -> (str, int):
    """
    Get the host, port address that the client wants us to publish to.

    >>> deserialize_address(b'\\x7f\\x00\\x00\\x01\\xff\\xfe')
    ('127.0.0.1', 65534)

    :param b: 6-byte sequence in subscription request
    :return: ip address and port pair
    """
    ip = ipaddress.ip_address(b[0:4])
    p = array('H')
    p.frombytes(b[4:6])
    p.byteswap()  # to big-endian
    return str(ip), p[0]


def serialize_utcdatetime(utc: datetime) -> bytes:
    """
    Convert a UTC datetime into a byte stream for a Forex Provider message.
    A 64-bit integer number of microseconds that have passed since 00:00:00 UTC on 1 January 1970
    (excluding leap seconds). Sent in big-endian network format.

    >>> serialize_utcdatetime(datetime(1971, 12, 10, 1, 2, 3, 64000))
    b'\\x00\\x007\\xa3e\\x8e\\xf2\\xc0'

    :param utc: timestamp to convert to desired byte format
    :return: 8-byte stream
    """
    epoch = datetime(1970, 1, 1)
    micros = (utc - epoch).total_seconds() * MICROS_PER_SECOND
    a = array('Q', [int(micros)])
    a.byteswap()  # convert to big-endian
    return a.tobytes()


def marshal_message(quote_sequence) -> bytes:
    """
    Construct the byte stream for a message with given quote_sequence.

    >>> q1 = {'cross': 'GBP/USD', 'price': 1.22041, 'time': datetime(2006,1,2)}
    >>> q2 = {'cross': 'USD/JPY', 'price': 108.2755, 'time': datetime(2006,1,1)}
    >>> b = marshal_message([q1, q2])
    >>> len(b)  # each record is 32 bytes, so 2 of those in this test
    64
    >>> r1 = b[:32]  # first record is first 32 bytes
    >>> r2 = b[32:]  # second record (but all sent together in UDP datagram)
    >>> r1[:18]  # first quote's data
    b'GBPUSDe6\\x9c?\\x00\\x04\\tT\\xdd5@\\x00'
    >>> r1[18:]  # 14 bytes of zero-padding
    b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'
    >>> r2[:18]  # second quote's data
    b'USDJPY\\x0e\\x8d\\xd8B\\x00\\x04\\t@\\xbf]\\xe0\\x00'
    >>> r2[18:] == r1[18:]  # second quote's 14 bytes of zero-padding
    True

    :param quote_sequence: list of quote structures ('cross' and 'price', may also have 'timestamp')
    :return: byte stream to send in UDP message
    """
    if len(quote_sequence) > MAX_QUOTES_PER_MESSAGE:
        raise ValueError('max quotes exceeded for a single message')
    message = bytes()
    default_time = serialize_utcdatetime(datetime.utcnow())
    padding = b'\x00' * 14  # 10 bytes of zeros
    for quote in quote_sequence:
        message += quote['cross'][0:3].encode('ascii')
        message += quote['cross'][4:7].encode('ascii')
        message += serialize_price(quote['price'])
        if 'time' in quote:
            message += serialize_utcdatetime(quote['time'])
        else:
            message += default_time
        message += padding
    return message
