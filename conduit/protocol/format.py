"""
Conduit Protocol Format

Wire framing layout (per message):

  [4B frame_length BE] [protobuf Packet bytes]

  frame_length is always big-endian regardless of payload byte order.
  The Packet.byte_order field describes the byte order of raw binary
  payload data (0 = big-endian / network order, 1 = little-endian).

Packet fields relevant to ordering:
  sequence_id  uint64  — monotonically increasing per (connection, stream_id)
  stream_id    uint32  — logical channel; 0 = unordered (bypass reorder buffer)
  byte_order   uint32  — 0 = BE (default), 1 = LE

MessageHeader (legacy 32-byte struct, big-endian):
  Offset  Size  Field
  0       4     Magic b'CNDT'
  4       2     Protocol version (0x0100 = 1.0)
  6       2     Message type
  8       2     Flags
  10      2     Reserved
  12      4     Content length
  16      8     Correlation ID
  24      8     Timestamp (ms)
"""

import struct
from enum import IntEnum, IntFlag
from dataclasses import dataclass
from typing import Optional
import time


# ─── Protocol constants ───────────────────────────────────────────────────────

MAGIC            = b'CNDT'
MAGIC_INT        = 0x434E4454       # 'CNDT' as big-endian uint32
HEADER_SIZE      = 32
MAX_PAYLOAD_SIZE = 100 * 1024 * 1024
PROTOCOL_VERSION = 0x0100           # 1.0

# ─── Byte order ───────────────────────────────────────────────────────────────

class ByteOrder(IntEnum):
    """
    Byte order for raw binary payload fields.
    The framing layer (frame_length) is always big-endian.
    """
    BE = 0   # Big-endian / network order (default)
    LE = 1   # Little-endian (x86/ARM native)

    @staticmethod
    def native() -> 'ByteOrder':
        """Return the host's native byte order."""
        import sys
        return ByteOrder.LE if sys.byteorder == 'little' else ByteOrder.BE

    def to_struct_prefix(self) -> str:
        """Return the struct module prefix character for this byte order."""
        return '<' if self == ByteOrder.LE else '>'

    def pack_u32(self, value: int) -> bytes:
        fmt = '<I' if self == ByteOrder.LE else '>I'
        return struct.pack(fmt, value)

    def pack_u64(self, value: int) -> bytes:
        fmt = '<Q' if self == ByteOrder.LE else '>Q'
        return struct.pack(fmt, value)

    def unpack_u32(self, data: bytes, offset: int = 0) -> int:
        fmt = '<I' if self == ByteOrder.LE else '>I'
        return struct.unpack_from(fmt, data, offset)[0]

    def unpack_u64(self, data: bytes, offset: int = 0) -> int:
        fmt = '<Q' if self == ByteOrder.LE else '>Q'
        return struct.unpack_from(fmt, data, offset)[0]


# ─── Message types ────────────────────────────────────────────────────────────

class MessageType(IntEnum):
    """Message type identifiers."""

    MESSAGE        = 0x0001
    RPC_REQUEST    = 0x0002
    RPC_RESPONSE   = 0x0003
    RPC_ERROR      = 0x0004
    HEARTBEAT_PING = 0x0005
    HEARTBEAT_PONG = 0x0006
    AUTH_REQUEST   = 0x0007
    AUTH_SUCCESS   = 0x0008
    AUTH_FAILURE   = 0x0009
    PAUSE          = 0x000A
    RESUME         = 0x000B
    ACK            = 0x000C
    NACK           = 0x000D
    CLOSE          = 0x000E
    CLOSE_ACK      = 0x000F
    RPC_LIST       = 0x0010


# ─── Message flags ────────────────────────────────────────────────────────────

class MessageFlags(IntFlag):
    """Message flags carried in Packet.flags."""

    NONE          = 0x0000
    COMPRESSED    = 0x0001   # Legacy zlib (backward compat only)
    ENCRYPTED     = 0x0002   # Payload is encrypted (beyond TLS)
    REQUIRE_ACK   = 0x0004   # Sender expects an ACK
    PRIORITY      = 0x0008   # High-priority message
    FRAGMENT      = 0x0010   # This packet is a fragment
    LAST_FRAGMENT = 0x0020   # Last fragment of a fragmented message
    BINARY        = 0x0040   # Payload is raw binary (not text/JSON)
    CODEC_LZ4     = 0x0080   # Payload compressed with LZ4
    CODEC_ZSTD    = 0x0100   # Payload compressed with Zstd
    ORDERED       = 0x0200   # Receiver must use reorder buffer for this packet
    BYTE_ORDER_LE = 0x0400   # Payload binary data is little-endian


# ─── Ordering helpers ─────────────────────────────────────────────────────────

# stream_id=0 means "no ordering" (bypass the reorder buffer).
STREAM_UNORDERED = 0


def make_stream_id(category: int, index: int) -> int:
    """
    Compose a stream_id from a category (upper 16 bits) and an index
    (lower 16 bits). Helps namespacing streams without collisions.

    Example:
        STREAM_RPC    = make_stream_id(1, 0)   # ordered RPC replies
        STREAM_EVENTS = make_stream_id(2, 0)   # ordered event stream
    """
    return ((category & 0xFFFF) << 16) | (index & 0xFFFF)


# ─── MessageHeader (legacy / framing reference) ───────────────────────────────

@dataclass
class MessageHeader:
    """
    In-memory representation of a decoded Packet header.
    The on-wire format is the protobuf Packet; this struct exists for
    compatibility with code that inspects headers before full decode.
    """

    magic:          bytes
    version:        int
    message_type:   MessageType
    flags:          MessageFlags
    reserved:       int
    content_length: int
    correlation_id: int
    timestamp:      int
    sequence_id:    int = 0
    stream_id:      int = 0
    byte_order:     ByteOrder = ByteOrder.BE

    # Struct is big-endian (network byte order) — framing layer only.
    STRUCT_FORMAT = '>4sHHHHIQQ'

    @classmethod
    def create(
        cls,
        message_type: MessageType,
        content_length: int,
        correlation_id: int = 0,
        flags: MessageFlags = MessageFlags.NONE,
        timestamp: Optional[int] = None,
        sequence_id: int = 0,
        stream_id: int = 0,
        byte_order: ByteOrder = ByteOrder.BE,
    ) -> 'MessageHeader':
        if timestamp is None:
            timestamp = int(time.time() * 1000)
        return cls(
            magic=MAGIC,
            version=PROTOCOL_VERSION,
            message_type=message_type,
            flags=flags,
            reserved=0,
            content_length=content_length,
            correlation_id=correlation_id,
            timestamp=timestamp,
            sequence_id=sequence_id,
            stream_id=stream_id,
            byte_order=byte_order,
        )

    def to_bytes(self) -> bytes:
        """Serialize to the 32-byte legacy header (always big-endian)."""
        return struct.pack(
            self.STRUCT_FORMAT,
            self.magic,
            self.version,
            int(self.message_type),
            int(self.flags),
            self.reserved,
            self.content_length,
            self.correlation_id,
            self.timestamp,
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> 'MessageHeader':
        if len(data) < HEADER_SIZE:
            raise ValueError(f"Header too short: expected {HEADER_SIZE}, got {len(data)}")
        unpacked = struct.unpack(cls.STRUCT_FORMAT, data[:HEADER_SIZE])
        magic = unpacked[0]
        if magic != MAGIC:
            raise ValueError(f"Invalid magic: expected {MAGIC!r}, got {magic!r}")
        return cls(
            magic=magic,
            version=unpacked[1],
            message_type=MessageType(unpacked[2]),
            flags=MessageFlags(unpacked[3]),
            reserved=unpacked[4],
            content_length=unpacked[5],
            correlation_id=unpacked[6],
            timestamp=unpacked[7],
        )

    def validate(self) -> None:
        if self.magic != MAGIC:
            raise ValueError(f"Invalid magic: {self.magic!r}")
        if self.version != PROTOCOL_VERSION:
            raise ValueError(f"Unsupported version: {self.version:#06x}")
        if self.content_length > MAX_PAYLOAD_SIZE:
            raise ValueError(f"Payload too large: {self.content_length}")

    def is_control_message(self) -> bool:
        return self.message_type in (
            MessageType.HEARTBEAT_PING, MessageType.HEARTBEAT_PONG,
            MessageType.PAUSE, MessageType.RESUME,
            MessageType.ACK, MessageType.NACK,
            MessageType.CLOSE, MessageType.CLOSE_ACK,
        )

    def is_rpc(self) -> bool:
        return self.message_type in (
            MessageType.RPC_REQUEST, MessageType.RPC_RESPONSE,
            MessageType.RPC_ERROR, MessageType.RPC_LIST,
        )

    def is_auth(self) -> bool:
        return self.message_type in (
            MessageType.AUTH_REQUEST, MessageType.AUTH_SUCCESS,
            MessageType.AUTH_FAILURE,
        )

    def is_ordered(self) -> bool:
        """True if the receiver should route through the reorder buffer."""
        return bool(self.flags & MessageFlags.ORDERED) and self.stream_id != STREAM_UNORDERED

    def payload_byte_order(self) -> ByteOrder:
        """Byte order of raw binary payload content."""
        if self.byte_order == ByteOrder.LE or bool(self.flags & MessageFlags.BYTE_ORDER_LE):
            return ByteOrder.LE
        return ByteOrder.BE


# Sanity check
assert struct.calcsize(MessageHeader.STRUCT_FORMAT) == HEADER_SIZE, \
    f"Header struct size mismatch: {struct.calcsize(MessageHeader.STRUCT_FORMAT)} != {HEADER_SIZE}"
