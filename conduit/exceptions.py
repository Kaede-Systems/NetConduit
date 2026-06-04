"""
Conduit Custom Exceptions

All custom exceptions used by the library.
"""


class ConduitError(Exception):
    """Base exception for all Conduit errors."""
    pass


class ConnectionError(ConduitError):
    """Connection-related errors."""
    pass


class AuthenticationError(ConduitError):
    """Authentication failed."""
    pass


class ProtocolError(ConduitError):
    """Protocol-level errors."""
    pass


class TimeoutError(ConduitError):
    """Operation timed out."""
    pass


class RPCError(ConduitError):
    """RPC call failed."""
    
    def __init__(self, message: str, code: int = None, details: dict = None):
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}

    def __str__(self) -> str:
        if self.code:
            return f"[{self.code}] {self.message}"
        return self.message



class ValidationError(ConduitError):
    """Data validation failed."""
    
    def __init__(self, message: str, field: str = None, errors: list = None):
        super().__init__(message)
        self.field = field
        self.errors = errors or []


class BackpressureError(ConduitError):
    """Backpressure-related errors."""
    pass


class QueueFullError(BackpressureError):
    """Queue is full."""
    pass


class NotConnectedError(ConnectionError):
    """Not connected."""
    pass


class AlreadyConnectedError(ConnectionError):
    """Already connected."""
    pass


class ServerError(ConduitError):
    """Server-side errors."""
    pass


class ClientError(ConduitError):
    """Client-side errors."""
    pass
