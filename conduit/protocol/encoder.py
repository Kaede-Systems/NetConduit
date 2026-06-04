"""
Conduit Protocol Encoder — Protobuf + length-prefix framing with Rust-backed compression.

Codec selection thresholds:
  < 64 bytes  : no compression (overhead dominates)
  64–255 bytes: LZ4 (fast, good on small payloads)
  > 256 bytes : Zstd level 1 (better ratio, competitive latency)
  > 4 KB      : Zstd level 3 (worth the extra CPU for ratio gains)
"""

import time
import json
from typing import Any, Optional

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
from .format import MessageType, MessageFlags, PROTOCOL_VERSION

try:
    from netconduit import compress_payload, decompress_payload
    _RUST_COMPRESSION = True
except ImportError:
    _RUST_COMPRESSION = False


def serialize_data(data: Any) -> bytes:
    if data is None:
        return b''
    if isinstance(data, bytes):
        return data
    if isinstance(data, str):
        return data.encode('utf-8')
    return json.dumps(data).encode('utf-8')


def _compress(payload: bytes, flags: int) -> tuple[bytes, int]:
    """Compress payload using Rust Zstd/LZ4 based on size. Returns (compressed, updated_flags)."""
    if not _RUST_COMPRESSION or len(payload) < 64:
        return payload, flags

    compressed = compress_payload(payload)
    if len(compressed) >= len(payload):
        return payload, flags

    # Codec byte is prepended by compress_payload: 0=raw, 1=LZ4, 2=Zstd
    codec = compressed[0]
    if codec == 1:
        return compressed[1:], flags | MessageFlags.CODEC_LZ4
    elif codec == 2:
        return compressed[1:], flags | MessageFlags.CODEC_ZSTD
    return payload, flags


class ProtocolEncoder:
    """Encodes messages into binary protocol format using Protobuf with length-prefix framing."""

    def __init__(self, enable_compression: bool = False):
        self.enable_compression = enable_compression
        self._correlation_counter = 0

    def _next_correlation_id(self) -> int:
        self._correlation_counter += 1
        return self._correlation_counter

    def encode(
        self,
        message_type: MessageType,
        payload_bytes: bytes = b'',
        correlation_id: Optional[int] = None,
        flags: int = 0,
        route_src: Optional[str] = None,
        route_dst: Optional[str] = None,
        is_mesh: bool = False,
    ) -> bytes:
        """Encode a message into binary format."""
        if self.enable_compression and payload_bytes:
            payload_bytes, flags = _compress(payload_bytes, flags)

        if correlation_id is None and message_type == MessageType.RPC_REQUEST:
            correlation_id = self._next_correlation_id()

        packet = Packet(
            version=PROTOCOL_VERSION,
            type=int(message_type),
            flags=flags,
            correlation_id=correlation_id or 0,
            timestamp=int(time.time() * 1000),
            payload=payload_bytes,
            route_src=route_src or "",
            route_dst=route_dst or "",
            is_mesh=is_mesh,
        )

        serialized = packet.SerializeToString()
        return len(serialized).to_bytes(4, byteorder='big') + serialized

    def encode_message(self, message_type_str: str, data: Any, correlation_id: Optional[int] = None) -> bytes:
        """Encode a regular message."""
        payload = MessagePayload(type=message_type_str, data=serialize_data(data))
        return self.encode(MessageType.MESSAGE, payload.SerializeToString(), correlation_id=correlation_id)

    def encode_rpc_request(self, method: str, params: Optional[dict] = None, correlation_id: Optional[int] = None) -> tuple[bytes, int]:
        """Encode an RPC request, returning (bytes, correlation_id)."""
        if correlation_id is None:
            correlation_id = self._next_correlation_id()
        payload = RPCRequestPayload(method=method, params=serialize_data(params))
        encoded = self.encode(MessageType.RPC_REQUEST, payload.SerializeToString(), correlation_id=correlation_id)
        return encoded, correlation_id

    def encode_rpc_response(self, result: Any, correlation_id: int, success: bool = True) -> bytes:
        """Encode an RPC response."""
        if isinstance(result, dict) and "success" in result:
            success = result["success"]
            if success:
                actual = result.get("data") if result.get("data") is not None else result.get("result")
                payload = RPCResponsePayload(success=True, result=serialize_data(actual))
                msg_type = MessageType.RPC_RESPONSE
            else:
                payload = RPCResponsePayload(success=False, error=str(result.get("error", "Unknown error")), code=result.get("code", 0))
                msg_type = MessageType.RPC_ERROR
        else:
            if success:
                payload = RPCResponsePayload(success=True, result=serialize_data(result))
                msg_type = MessageType.RPC_RESPONSE
            else:
                payload = RPCResponsePayload(success=False, error=str(result))
                msg_type = MessageType.RPC_ERROR
        return self.encode(msg_type, payload.SerializeToString(), correlation_id=correlation_id)

    def encode_rpc_error(self, error_message: str, correlation_id: int, error_code: Optional[int] = None) -> bytes:
        """Encode an RPC error response."""
        payload = RPCResponsePayload(success=False, error=error_message, code=error_code or 0)
        return self.encode(MessageType.RPC_ERROR, payload.SerializeToString(), correlation_id=correlation_id)

    def encode_auth_request(self, password_hash: str, client_info: dict) -> bytes:
        """Encode an authentication request."""
        payload = AuthRequestPayload(
            password_hash=password_hash,
            client_info={k: str(v) for k, v in client_info.items()},
            protocol_version="1.0"
        )
        return self.encode(MessageType.AUTH_REQUEST, payload.SerializeToString())

    def encode_auth_success(self, session_token: str, server_info: dict) -> bytes:
        """Encode an authentication success response."""
        payload = AuthResponsePayload(
            success=True,
            session_token=session_token,
            server_info={k: str(v) for k, v in server_info.items()},
            heartbeat_interval=30,
        )
        return self.encode(MessageType.AUTH_SUCCESS, payload.SerializeToString())

    def encode_auth_failure(self, reason: str) -> bytes:
        """Encode an authentication failure response."""
        payload = AuthResponsePayload(success=False, reason=reason)
        return self.encode(MessageType.AUTH_FAILURE, payload.SerializeToString())

    def encode_heartbeat_ping(self) -> bytes:
        """Encode a heartbeat ping."""
        return self.encode(MessageType.HEARTBEAT_PING)

    def encode_heartbeat_pong(self) -> bytes:
        """Encode a heartbeat pong."""
        return self.encode(MessageType.HEARTBEAT_PONG)

    def encode_pause(self) -> bytes:
        return self.encode(MessageType.PAUSE)

    def encode_resume(self) -> bytes:
        return self.encode(MessageType.RESUME)

    def encode_ack(self, correlation_id: int) -> bytes:
        return self.encode(MessageType.ACK, correlation_id=correlation_id)

    def encode_nack(self, correlation_id: int, reason: str = "") -> bytes:
        payload = AuthResponsePayload(reason=reason)
        return self.encode(MessageType.NACK, payload.SerializeToString(), correlation_id=correlation_id)

    def encode_close(self, reason: str = "") -> bytes:
        payload = AuthResponsePayload(reason=reason)
        return self.encode(MessageType.CLOSE, payload.SerializeToString())

    def encode_close_ack(self) -> bytes:
        return self.encode(MessageType.CLOSE_ACK)

    def encode_rpc_list(self, methods: list[dict]) -> bytes:
        return self.encode(MessageType.RPC_LIST, serialize_data(methods))
