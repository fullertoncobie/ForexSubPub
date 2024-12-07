"""
This module contains useful marshalling functions for manipulating Forex Provider packet contents.
All functions handle binary data conversion for the Forex Provider protocol.
"""

import ipaddress
from array import array
from datetime import datetime

MICROS_PER_SECOND = 1_000_000

def serialize_address(address) -> bytes:
    """
    Serialize an IP address and port into a 6-byte sequence.
    
    Example:
    >>> serialize_address('127.0.0.1', 65534)
    b'\\x7f\\x00\\x00\\x01\\xff\\xfe'
    
    Args:
        address (tuple): Tuple containing IP address string and port number
    
    Returns:
        bytes: 6-byte sequence combining IP (4 bytes) and port (2 bytes) in big-endian order
    """
    ip, port = address
    ip_bytes = ipaddress.ip_address(ip).packed
    port_array = array('H', [port])
    port_array.byteswap() 
    port_bytes = port_array.tobytes()
    return ip_bytes + port_bytes

def deserialize_price(byte_data: bytes) -> float:
    """
    Convert a byte array from the price feed messages back to a float.
    
    Example:
    >>> deserialize_price(b'\\xd5\\xe9\\xf6B')
    123.4567
    
    Args:
        byte_data (bytes): 4-byte sequence representing a float in IEEE 754 format
    
    Returns:
        float: Deserialized price value
    """
    a = array('f')
    a.frombytes(byte_data)
    return a[0]

def deserialize_utcdatetime(byte_data: bytes) -> int:
    """
    Convert a byte stream from a Forex Provider message back into seconds since epoch.
    Expects an 8-byte stream representing microseconds since Unix epoch in big-endian format.
    
    Example:
    >>> deserialize_utcdatetime(b'\\x00\\x007\\xa3e\\x8e\\xf2\\xc0')
    60549723.064000
    
    Args:
        byte_data (bytes): 8-byte sequence representing microseconds since epoch in big-endian
    
    Returns:
        int: Seconds since Unix epoch (January 1, 1970)
    """
    a = array('Q')
    a.frombytes(byte_data)
    a.byteswap()
    micros = a[0]
    return micros / MICROS_PER_SECOND

def split_quotes(binary_string: bytes) -> list:
    """
    Split a binary string containing multiple forex quotes into fixed-size chunks.
    
    Args:
        binary_string (bytes): Raw binary data containing multiple forex quotes
    
    Returns:
        list: List of 32-byte chunks, each containing a single forex quote
        
    Note:
        Each quote consists of:
        - 6 bytes: ISO currency pair (ASCII)
        - 4 bytes: Price (float)
        - 8 bytes: Timestamp (microseconds since epoch)
        - 14 bytes: Reserved/padding
    """
    quotes_size = 32
    quotes_count = len(binary_string) // quotes_size
    
    quotes_list = []
    for i in range(quotes_count):
        start = i * quotes_size
        end = start + quotes_size
        chunk = binary_string[start:end]
        quotes_list.append(chunk)
        
    if len(binary_string) % quotes_size:
        remaining = binary_string[quotes_count * quotes_size:]
        quotes_list.append(remaining)
        
    return quotes_list

def parse_quotes(binary_quotes_list: list) -> dict:
    """
    Parse a list of binary forex quotes into a dictionary of currency pairs with prices and timestamps.
    
    Args:
        binary_quotes_list (list): List of 32-byte chunks containing forex quotes
        
    Returns:
        dict: Dictionary mapping currency pairs to tuples of (price, timestamp)
              Example: {'EURUSD': (1.2345, 1634567890.123456)}
              
    Note:
        Each binary quote should contain:
        - Currency pair (bytes 0-5)
        - Price (bytes 6-9)
        - Timestamp (bytes 10-17)
    """
    quotes_dict = {}
    for quote in binary_quotes_list:
        iso_pair = (quote[0:3].decode('ascii'), quote[3:6].decode('ascii'))
        price = deserialize_price(quote[6:10])
        timestamp = deserialize_utcdatetime(quote[10:18])
        quotes_dict[iso_pair] = (price, timestamp)
    return quotes_dict