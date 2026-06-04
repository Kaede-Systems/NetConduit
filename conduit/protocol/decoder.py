"""
Conduit Protocol Decoder — Protobuf + length-prefix framing with multi-codec decompression.
"""

from typing import Any, Optional, Union
import json

try:
    from netconduit_core import decompress_payload as _rust_decompress
    _RUST_COMPRESSION = True
except ImportError:
    _RUST_COMPRESSION = False

from .protocol_pb2 import (
    Packet,
    AuthRequestPayload,
    AuthResponsePayload,
    RPCRequestPayload,
    RPCResponsePayload,
    MessagePayload,
    StreamDataPayload,
    FileChunkPayload,
)
from .format import (
    MessageHeader,
    MessageType,
    MessageFlags,
)


class DecodeError(Exception):
    """Error during message decoding."""
    pass


class IncompleteMessageError(Exception):
    """Message is incomplete, need more data."""
    def __init__(self, bytes_needed: int):
        self.bytes_needed = bytes_needed
        super().__init__(f"Need {bytes_needed} more bytes")


def deserialize_data(data_bytes: bytes) -> Any:
    if not data_bytes:
        return None
    try:
        return json.loads(data_bytes.decode('utf-8'))
    except Exception:
        try:
            return data_bytes.decode('utf-8')
        except Exception:
            return data_bytes


class DecodedMessage:
    """Represents a decoded message wrapper compatible with the TCP codebase."""
    
    def __init__(
        self,
        header: MessageHeader,
        payload: Any,
        raw_payload: bytes,
        route_src: str = "",
        route_dst: str = "",
        is_mesh: bool = False,
    ):
        self.header = header
        self.payload = payload
        self.raw_payload = raw_payload
        self.route_src = route_src
        self.route_dst = route_dst
        self.is_mesh = is_mesh
    
    @property
    def message_type(self) -> MessageType:
        return self.header.message_type
    
    @property
    def correlation_id(self) -> int:
        return self.header.correlation_id
    
    @property
    def timestamp(self) -> int:
        return self.header.timestamp
    
    @property
    def flags(self) -> MessageFlags:
        return self.header.flags
    
    def is_compressed(self) -> bool:
        return bool(self.flags & MessageFlags.COMPRESSED)
    
    def get_message_type_str(self) -> Optional[str]:
        """Get string message type for MESSAGE types."""
        if self.message_type == MessageType.MESSAGE and isinstance(self.payload, dict):
            return self.payload.get("type")
        return None
    
    def get_data(self) -> Any:
        """Get message data for MESSAGE types."""
        if self.message_type == MessageType.MESSAGE and isinstance(self.payload, dict):
            return self.payload.get("data")
        return self.payload
    
    def get_rpc_method(self) -> Optional[str]:
        """Get RPC method name for RPC_REQUEST."""
        if self.message_type == MessageType.RPC_REQUEST and isinstance(self.payload, dict):
            return self.payload.get("method")
        return None
    
    def get_rpc_params(self) -> dict:
        """Get RPC parameters for RPC_REQUEST."""
        if self.message_type == MessageType.RPC_REQUEST and isinstance(self.payload, dict):
            res = self.payload.get("params")
            return res if isinstance(res, dict) else {}
        return {}
    
    def get_rpc_result(self) -> Any:
        """Get RPC result for RPC_RESPONSE."""
        if self.message_type in (MessageType.RPC_RESPONSE, MessageType.RPC_ERROR):
            if isinstance(self.payload, dict):
                return self.payload.get("result")
        return None
    
    def get_rpc_error(self) -> Optional[str]:
        """Get RPC error message for RPC_ERROR."""
        if self.message_type == MessageType.RPC_ERROR and isinstance(self.payload, dict):
            return self.payload.get("error")
        return None
    
    def is_success(self) -> bool:
        """Check if RPC response indicates success."""
        if isinstance(self.payload, dict):
            return self.payload.get("success", True)
        return True
    
    def __repr__(self) -> str:
        return f"DecodedMessage(type={self.message_type.name}, corr_id={self.correlation_id})"


class ProtocolDecoder:
    """Decodes messages from binary protocol format using Protobuf."""
    
    def __init__(self):
        """Initialize decoder."""
        self._buffer = bytearray()
    
    def feed(self, data: bytes) -> None:
        """Add data to the internal buffer."""
        self._buffer.extend(data)
    
    def decode_one(self) -> Optional[DecodedMessage]:
        """Try to decode one complete message from buffer."""
        if len(self._buffer) < 4:
            return None
        length = int.from_bytes(self._buffer[:4], byteorder='big')
        # Sanity check to avoid huge allocations/incorrect parsing
        if length > 100 * 1024 * 1024:
            self._buffer.clear()
            return None
        if len(self._buffer) < 4 + length:
            return None
        packet_bytes = bytes(self._buffer[4:4+length])
        del self._buffer[:4+length]
        try:
            return self.decode_single(packet_bytes)
        except Exception as e:
            raise DecodeError(f"Failed to decode message: {e}") from e
            
    def decode_all(self) -> list[DecodedMessage]:
        """Decode all complete messages from buffer."""
        messages = []
        while True:
            msg = self.decode_one()
            if msg is None:
                break
            messages.append(msg)
        return messages
    
    @staticmethod
    def decode_single(data: bytes) -> DecodedMessage:
        """
        Decode a single complete message from bytes.
        """
        packet = Packet()
        parsed = False
        
        # Try direct parse first
        try:
            packet.ParseFromString(data)
            parsed = True
        except Exception:
            pass
            
        # If direct parse failed, check if length-prefixed
        if not parsed:
            if len(data) < 4:
                raise IncompleteMessageError(4 - len(data))
            length = int.from_bytes(data[:4], byteorder='big')
            if len(data) < 4 + length:
                raise IncompleteMessageError(4 + length - len(data))
            try:
                packet.ParseFromString(data[4:4+length])
            except Exception as e:
                raise DecodeError(f"Failed to decode message: {e}") from e
                
        try:
            payload_bytes = packet.payload
            flags = packet.flags

            # Decompress based on which codec flag is set
            if flags & MessageFlags.CODEC_LZ4:
                if _RUST_COMPRESSION:
                    payload_bytes = _rust_decompress(bytes([1]) + payload_bytes)
                # If Rust not available, try to pass through (best effort)
            elif flags & MessageFlags.CODEC_ZSTD:
                if _RUST_COMPRESSION:
                    payload_bytes = _rust_decompress(bytes([2]) + payload_bytes)
            elif flags & MessageFlags.COMPRESSED:
                # Legacy zlib path (backward compatibility)
                import zlib
                payload_bytes = zlib.decompress(payload_bytes)

            msg_type = MessageType(packet.type)
            payload = None
            
            if msg_type == MessageType.MESSAGE:
                p = MessagePayload()
                p.ParseFromString(payload_bytes)
                payload = {
                    "type": p.type,
                    "data": deserialize_data(p.data),
                }
            elif msg_type == MessageType.RPC_REQUEST:
                p = RPCRequestPayload()
                p.ParseFromString(payload_bytes)
                payload = {
                    "method": p.method,
                    "params": deserialize_data(p.params),
                }
            elif msg_type in (MessageType.RPC_RESPONSE, MessageType.RPC_ERROR):
                p = RPCResponsePayload()
                p.ParseFromString(payload_bytes)
                payload = {
                    "success": p.success,
                    "result": deserialize_data(p.result) if p.success else None,
                    "error": p.error if not p.success else None,
                    "code": p.code,
                }
            elif msg_type == MessageType.AUTH_REQUEST:
                p = AuthRequestPayload()
                p.ParseFromString(payload_bytes)
                payload = {
                    "password_hash": p.password_hash,
                    "client_info": dict(p.client_info),
                }
            elif msg_type in (MessageType.AUTH_SUCCESS, MessageType.AUTH_FAILURE):
                p = AuthResponsePayload()
                p.ParseFromString(payload_bytes)
                payload = {
                    "success": p.success,
                    "session_token": p.session_token,
                    "server_info": dict(p.server_info),
                    "reason": p.reason,
                }
            else:
                # Heartbeats, Pause, Resume, Close, CloseAck
                payload = deserialize_data(payload_bytes)
                
            header = MessageHeader(
                magic=b'CNDT',
                version=packet.version,
                message_type=msg_type,
                flags=MessageFlags(packet.flags),
                reserved=0,
                content_length=len(payload_bytes),
                correlation_id=packet.correlation_id,
                timestamp=packet.timestamp
            )
            return DecodedMessage(
                header,
                payload,
                payload_bytes,
                route_src=getattr(packet, "route_src", ""),
                route_dst=getattr(packet, "route_dst", ""),
                is_mesh=getattr(packet, "is_mesh", False),
            )
            
        except Exception as e:
            if isinstance(e, IncompleteMessageError):
                raise
            raise DecodeError(f"Failed to decode message: {e}") from e
    
    def buffer_size(self) -> int:
        """Get current buffer size."""
        return len(self._buffer)
    
    def clear(self) -> None:
        """Clear the internal buffer."""
        self._buffer.clear()
    
    def peek_header(self) -> Optional[MessageHeader]:
        """Peek at the header without consuming it."""
        if len(self._buffer) < 4:
            return None
        try:
            length = int.from_bytes(self._buffer[:4], byteorder='big')
            if len(self._buffer) < 4 + length:
                return None
            packet = Packet()
            packet.ParseFromString(bytes(self._buffer[4:4+length]))
            return MessageHeader(
                magic=b'CNDT',
                version=packet.version,
                message_type=MessageType(packet.type),
                flags=MessageFlags(packet.flags),
                reserved=0,
                content_length=len(packet.payload),
                correlation_id=packet.correlation_id,
                timestamp=packet.timestamp
            )
        except Exception:
            return None
