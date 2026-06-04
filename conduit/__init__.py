"""
Conduit - High-Performance Async Bidirectional QUIC Communication Library

A Python library for secure, high-performance, asynchronous bidirectional communication
built on QUIC (Quinn/Rust), with features including:
  - ED25519 self-signed TLS certificates for transport security
  - UDP hole-punching via STUN for NAT traversal
  - mDNS-based local and remote peer discovery
  - Mesh network routing with zero-copy relay
  - Blake3 checksums for payload integrity
  - Sled-backed persistent route cache (session-bound)
  - IPv4 and IPv6 support
  - Password authentication with Pydantic-validated RPC
  - Gating: role/permission/custom callback guards
  - Binary stream send/receive over QUIC bidir streams
  - Backpressure and rate limiting

Usage:
    from conduit import Server, Client, ServerDescriptor, ClientDescriptor
    from conduit import RPC, data, Response, Error
    
    # Server
    server = Server(ServerDescriptor(password="secret"))
    
    @server.on("hello")
    async def handle_hello(client, data):
        return {"message": f"Hello, {data['name']}!"}
    
    @server.rpc
    async def add(a: int, b: int) -> int:
        return a + b
    
    await server.run()
    
    # Client
    client = Client(ClientDescriptor(
        server_host="localhost",
        server_port=8080,
        password="secret"
    ))
    
    await client.connect()
    result = await client.rpc.call("add", args=data(a=10, b=20))
"""

# Version info
from .__version__ import __version__, __protocol_version__

# Main classes
from .server import Server
from .client import Client

# Configuration
from .data.descriptors import ServerDescriptor, ClientDescriptor

# Messages
from .messages import Message

# RPC
from .rpc import RPC, data

# Response helpers
from .response import Response, Error

# Connection
from .connection import Connection, ConnectionPool

# Data models
from .data import (
    MessageData,
    RPCRequest,
    RPCResponse,
    RPCError,
    AuthRequest,
    AuthSuccess,
    AuthFailure,
    RPCMethodInfo,
    RPCListResponse,
    ConnectionInfo,
    ConnectionHealth,
)

# Protocol (for advanced usage)
from .protocol import (
    MessageType,
    MessageFlags,
    MessageHeader,
    ProtocolEncoder,
    ProtocolDecoder,
    MAGIC,
    HEADER_SIZE,
    PROTOCOL_VERSION,
)

# Transport (for advanced usage)
from .transport import ConnectionState

# File Transfer
from .transfer import FileTransfer, FileTransferHandler, TransferProgress

# Streaming
from .streaming import Stream, BidirectionalStream, StreamManager

# Client Connection Pool
from .pool import ClientPool, PoolStats

# Exceptions
from .exceptions import (
    ConduitError,
    ConnectionError,
    AuthenticationError,
    ProtocolError,
    TimeoutError,
    RPCError as RPCException,
    ValidationError,
    BackpressureError,
    QueueFullError,
    NotConnectedError,
    AlreadyConnectedError,
    ServerError,
    ClientError,
)

__all__ = [
    # Version
    "__version__",
    "__protocol_version__",
    
    # Main classes
    "Server",
    "Client",
    
    # Configuration
    "ServerDescriptor",
    "ClientDescriptor",
    
    # Messages
    "Message",
    
    # RPC
    "RPC",
    "data",
    
    # Response helpers
    "Response",
    "Error",
    
    # Connection
    "Connection",
    "ConnectionPool",
    
    # File Transfer
    "FileTransfer",
    "TransferProgress",
    
    # Streaming
    "Stream",
    "StreamManager",
    
    # Client Pool
    "ClientPool",
    "PoolStats",
    
    # Data models
    "MessageData",
    "RPCRequest",
    "RPCResponse",
    "RPCError",
    "AuthRequest",
    "AuthSuccess",
    "AuthFailure",
    "RPCMethodInfo",
    "RPCListResponse",
    "ConnectionInfo",
    "ConnectionHealth",
    
    # Protocol
    "MessageType",
    "MessageFlags",
    "MessageHeader",
    "ProtocolEncoder",
    "ProtocolDecoder",
    "MAGIC",
    "HEADER_SIZE",
    "PROTOCOL_VERSION",
    
    # Transport
    "ConnectionState",
    
    # Exceptions
    "ConduitError",
    "ConnectionError",
    "AuthenticationError",
    "ProtocolError",
    "TimeoutError",
    "RPCException",
    "ValidationError",
    "BackpressureError",
    "QueueFullError",
    "NotConnectedError",
    "AlreadyConnectedError",
    "ServerError",
    "ClientError",
]
