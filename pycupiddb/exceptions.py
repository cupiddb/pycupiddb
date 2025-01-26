class CupidDBError(Exception):
    """Base exception for CupidDB errors"""
    pass


class InvalidArrowData(CupidDBError):
    """Raised when the arrow data is invalid"""
    pass


class InvalidPickleData(CupidDBError):
    """Raised when the data cannot be unpickled"""
    pass


class InvalidDataType(CupidDBError):
    """Raised when the data type is invalid"""
    pass


class InvalidQuery(CupidDBError):
    """Raised when the query is invalid"""
    pass


class ProtocolVersionError(CupidDBError):
    """Raised when the protocol version is incorrect"""
    pass


class KeyTooLongError(CupidDBError):
    """Raised when key length exceeds 65535 bytes"""
    pass


class ConnectionError(CupidDBError):
    """Raised when connection fails"""
    pass


class DeserializationError(CupidDBError):
    """Raised when data cannot be deserialized"""
    pass
