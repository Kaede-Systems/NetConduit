"""
Connection Class for QUIC.

Represents a connection on top of the Rust QUIC client or server connection.
"""

import asyncio
import time
import uuid
import logging
from typing import Any, Optional, Dict, Callable
from dataclasses import dataclass, field

from ..protocol import ProtocolEncoder, ProtocolDecoder, DecodedMessage, MessageType

logger = logging.getLogger(__name__)


@dataclass
class ConnectionStats:
    """Connection statistics."""
    connected_at: float = field(default_factory=time.time)
    messages_sent: int = 0
    messages_received: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    errors: int = 0


class Connection:
    """
    Represents a connection (client or server side).
    
    Delegates to the compiled Rust QUIC client/server underneath.
    """
    
    def __init__(
        self,
        rust_transport: Any,
        client_id: str = "",
        encoder: Optional[ProtocolEncoder] = None,
        decoder: Optional[ProtocolDecoder] = None,
        send_queue_size: int = 1000,
        receive_queue_size: int = 1000,
        heartbeat_interval: float = 30.0,
        heartbeat_timeout: float = 90.0,
        enable_backpressure: bool = True,
    ):
        self._rust = rust_transport
        self._client_id = client_id  # If server side, this is remote client ID. If client side, empty.
        self._id = client_id or str(uuid.uuid4())
        self._encoder = encoder or ProtocolEncoder()
        self._decoder = decoder or ProtocolDecoder()
        
        self._authenticated: bool = False
        self._session: Optional[Any] = None
        self._stats = ConnectionStats()
        
        # Pending RPC responses
        self._pending_rpcs: Dict[int, asyncio.Future] = {}
        
        # Callbacks
        self._on_message: Optional[Callable] = None
        self._on_disconnect: Optional[Callable] = None
        
        logger.debug(f"QUIC Connection wrapper created (ID: {self._id}, Client ID: {self._client_id})")
        
    @property
    def id(self) -> str:
        """Connection ID."""
        return self._id
    
    @property
    def state(self) -> Any:
        """Mock State for compatibility."""
        from ..transport import ConnectionState
        return ConnectionState.ACTIVE if self.is_connected else ConnectionState.DISCONNECTED
    
    @property
    def is_connected(self) -> bool:
        """Check if connected."""
        return True
    
    @property
    def is_authenticated(self) -> bool:
        """Check if authenticated."""
        return self._session is not None or self._authenticated
    
    @property
    def session(self) -> Optional[Any]:
        """Get session."""
        return self._session
    
    @property
    def remote_address(self) -> str:
        """Get remote address."""
        return self._client_id or "QUIC-Client"
    
    @property
    def stats(self) -> ConnectionStats:
        """Get connection stats."""
        return self._stats
    
    def set_session(self, session: Any) -> None:
        """Set session after authentication."""
        self._session = session
        self._authenticated = True
    
    def mark_authenticated(self) -> None:
        """Mark connection as authenticated."""
        self._authenticated = True
    
    def set_message_handler(self, handler: Callable) -> None:
        """Set message handler callback."""
        self._on_message = handler
    
    def set_disconnect_handler(self, handler: Callable) -> None:
        """Set disconnect handler callback."""
        self._on_disconnect = handler
    
    async def start(self) -> None:
        """Mock start."""
        pass
    
    async def stop(self) -> None:
        """Stop connection/disconnect."""
        if self._on_disconnect:
            try:
                handler = self._on_disconnect
                self._on_disconnect = None
                await handler(self)
            except Exception as e:
                logger.error(f"Error in disconnect callback: {e}")
    
    async def send_message(self, message_type: str, data: Any) -> None:
        """Send a regular message."""
        encoded = self._encoder.encode_message(message_type, data)
        self._stats.bytes_sent += len(encoded)
        self._stats.messages_sent += 1
        if self._client_id:
            # Server-to-client
            self._rust.send_message(self._client_id, encoded)
        else:
            # Client-to-server
            self._rust.send_message(encoded)
    
    async def send_rpc_request(self, method: str, params: dict) -> int:
        """Send an RPC request."""
        encoded, corr_id = self._encoder.encode_rpc_request(method, params)
        self._stats.bytes_sent += len(encoded)
        self._stats.messages_sent += 1
        
        future = asyncio.get_running_loop().create_future()
        self._pending_rpcs[corr_id] = future
        
        if self._client_id:
            self._rust.send_message(self._client_id, encoded)
        else:
            self._rust.send_message(encoded)
        return corr_id
    
    async def wait_for_rpc_response(self, correlation_id: int) -> Any:
        """Wait for RPC response."""
        future = self._pending_rpcs.get(correlation_id)
        if future is None:
            raise ValueError(f"No pending RPC for correlation ID {correlation_id}")
        try:
            return await future
        finally:
            self._pending_rpcs.pop(correlation_id, None)
    
    async def send_rpc_response(self, result: Any, correlation_id: int) -> None:
        """Send RPC response."""
        encoded = self._encoder.encode_rpc_response(result, correlation_id)
        self._stats.bytes_sent += len(encoded)
        self._stats.messages_sent += 1
        if self._client_id:
            self._rust.send_message(self._client_id, encoded)
        else:
            self._rust.send_message(encoded)
    
    async def send_rpc_error(self, error: str, correlation_id: int, code: int = None) -> None:
        """Send RPC error response."""
        encoded = self._encoder.encode_rpc_error(error, correlation_id, code)
        self._stats.bytes_sent += len(encoded)
        self._stats.messages_sent += 1
        # Fix: actually send the error response (was previously missing!)
        if self._client_id:
            self._rust.send_message(self._client_id, encoded)
        else:
            self._rust.send_message(encoded)

    async def send_raw(self, encoded: bytes) -> None:
        """Send raw bytes."""
        self._stats.bytes_sent += len(encoded)
        self._stats.messages_sent += 1
        if self._client_id:
            self._rust.send_message(self._client_id, encoded)
        else:
            self._rust.send_message(encoded)
            
    def handle_decoded_message(self, message: DecodedMessage) -> bool:
        """
        Handle a decoded message and resolve any pending RPCs on this connection.
        
        Returns True if the message was handled/intercepted internally (e.g. RPC response),
        False if it should be bubbled up to external handlers.
        """
        self._stats.messages_received += 1
        self._stats.bytes_received += len(message.raw_payload)
        
        msg_type = message.message_type
        if msg_type in (MessageType.RPC_RESPONSE, MessageType.RPC_ERROR):
            corr_id = message.correlation_id
            future = self._pending_rpcs.get(corr_id)
            if future and not future.done():
                future.set_result(message.payload)
                return True
        return False
