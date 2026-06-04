# Protocol Specification

Technical reference for the NetConduit wire protocol.

---

## Overview

NetConduit uses QUIC as the transport layer. All application data is carried in Protobuf-encoded `Packet` messages. Three delivery modes are available on each QUIC connection:

| Mode | QUIC primitive | Guarantee |
|------|---------------|-----------|
| Message | uni-stream | Reliable, ordered per-stream |
| BinaryStream / DuplexStream | bi-stream | Reliable, ordered |
| Datagram | QUIC datagram | Unreliable, unordered, ~1200 B max |

---

## Wire Frame Format

### Stream frames (uni-stream and bi-stream)

```
 ┌─────────────────────────────────────────────────┐
 │  PROTO_MAGIC     5 bytes   "NCON\x01"           │
 │  pkt_len         4 bytes   big-endian uint32     │
 │  Packet          pkt_len   protobuf-encoded      │
 └─────────────────────────────────────────────────┘
```

`PROTO_MAGIC = 0x4E 0x43 0x4F 0x4E 0x01` — last byte encodes protocol version.

Bi-streams open with one full frame (magic + len + Packet). Long-lived duplex streams send subsequent messages as `len + Packet` only (no repeated magic).

### Datagram frames

```
 ┌─────────────────────────────────────────────────┐
 │  PROTO_MAGIC     5 bytes   "NCON\x01"           │
 │  Packet          remaining  protobuf-encoded     │
 └─────────────────────────────────────────────────┘
```

No length prefix — the datagram boundary is the packet boundary.

---

## Packet Message

Defined in `src/protocol.proto`:

```protobuf
message Packet {
    uint32    version        = 1;   // Must equal PROTO_VERSION (1)
    PacketType type          = 2;   // Payload type discriminator
    uint32    flags          = 3;   // Bitfield — see below
    uint64    correlation_id = 4;   // RPC request/response matching; file transfer group
    uint64    timestamp      = 5;   // Unix milliseconds at sender
    bytes     payload        = 6;   // Application data (may be compressed)
    string    src_id         = 7;   // Sender peer ID (32-char hex)
    string    dst_id         = 8;   // Destination peer ID for mesh routing
    bool      is_mesh        = 9;   // True when packet was relayed through a mesh node
    bytes     checksum       = 10;  // Blake3-128 hex of uncompressed payload (FLAG_HAS_CHECKSUM)
    uint64    sequence_id    = 11;  // Monotonic per (connection, stream_id)
    uint32    stream_id      = 12;  // Logical channel (0 = default)
    string    channel_name   = 13;  // Channel name for STREAM_DATA; empty for MESSAGE
    bytes     signature      = 14;  // Ed25519 64-byte signature (FLAG_SIGNED)
    uint32    ttl            = 15;  // Mesh hop limit; decremented at each relay; drop at 0
}
```

---

## Packet Types

| Value | Name | Description |
|-------|------|-------------|
| 0 | `UNKNOWN` | Invalid / unset |
| 1 | `MESSAGE` | Raw application message |
| 2 | `STREAM_DATA` | Named binary stream chunk |
| 3 | `FILE_CHUNK` | File transfer chunk (correlation_id groups chunks) |
| 4 | `RPC_REQUEST` | RPC call |
| 5 | `RPC_RESPONSE` | RPC result |
| 6 | `RPC_ERROR` | RPC failure |
| 7 | `PING` | Keep-alive probe |
| 8 | `PONG` | Keep-alive reply |
| 9 | `AUTH_REQUEST` | Authentication handshake |
| 10 | `AUTH_RESPONSE` | Authentication result |
| 11 | `CLOSE` | Graceful connection close |
| 12 | `ACK` | Explicit acknowledgment |
| 13 | `NACK` | Negative acknowledgment |
| 14 | `PAUSE` | Flow control pause |
| 15 | `RESUME` | Flow control resume |

---

## Flags Field

`Packet.flags` is a 32-bit bitfield:

| Bit | Constant | Value | Meaning |
|-----|----------|-------|---------|
| 0 | `FLAG_COMPRESS_LZ4` | `0x01` | Payload is LZ4-compressed (`lz4_flex::compress_prepend_size`) |
| 1 | `FLAG_COMPRESS_ZSTD` | `0x02` | Payload is Zstd-compressed |
| 2 | `FLAG_UNRELIABLE` | `0x04` | Sent via QUIC datagram (unreliable channel) |
| 3 | `FLAG_HAS_CHECKSUM` | `0x08` | `checksum` field is populated |
| 4 | `FLAG_LITTLE_ENDIAN` | `0x10` | Payload byte order is little-endian (default: big-endian) |
| 5 | `FLAG_DUPLEX` | `0x20` | Opening packet of a long-lived bidirectional stream |
| 6 | `FLAG_SIGNED` | `0x40` | `signature` field is populated with a 64-byte Ed25519 signature |

Bits 0 and 1 are mutually exclusive (LZ4 and Zstd cannot both be set).

---

## Compression Pipeline

```
  payload (raw bytes)
        │
        ▼
  select_codec(len):
    < 64 B   → NONE  (pass through)
    64–255 B → LZ4
    ≥ 256 B  → ZSTD
        │
        ▼  (for ZSTD, Auto policy only)
  is_likely_compressible? (4 KB LZ4 entropy probe)
    no  → return raw (skip zstd entirely)
    yes → continue
        │
        ▼
  compress:
    LZ4  → lz4_flex::compress_prepend_size
    ZSTD → zstd level-1  (~400 MB/s single-thread)
          → zstd MT (≥ 32 MB only, up to 8 threads)
        │
        ▼
  if compressed >= original → send raw (no expansion)
  if compressed < original  → send compressed, set flag
```

### Codec selection thresholds

| Payload size | Codec | Rationale |
|-------------|-------|-----------|
| < 64 B | None | Compression overhead exceeds savings |
| 64–255 B | LZ4 | Fast, in-place, minimal overhead |
| ≥ 256 B | Zstd level 1 | Better ratio for larger data; level 1 ≈ 400 MB/s |
| ≥ 32 MB | Zstd MT | Multi-threaded; below this MT dispatch overhead dominates |

---

## Framing Detail: Duplex Streams

A duplex stream uses a QUIC bi-directional stream that stays open for the lifetime of the logical channel.

**Opening frame** (client → server, from `open_duplex_stream()`):
```
PROTO_MAGIC[5] + pkt_len[4] + Packet {
    type: STREAM_DATA,
    flags: FLAG_DUPLEX,
    channel_name: "<name>",
    ...
}
```

**Subsequent messages** (either direction, from `send_data()`):
```
pkt_len[4] + Packet {
    type: STREAM_DATA,
    flags: <compress flags> | FLAG_DUPLEX,
    payload: <compressed>,
    ...
}
```

No magic prefix on continuation frames. The stream is closed by calling `finish()` on the send half.

---

## Ed25519 Packet Signing

Signing covers `Packet.payload` only. The signature does NOT cover other fields (flags, timestamp, etc.) — those are transport metadata, not application content.

**Sign:**
```
pkt.src_id    = identity.peer_id    // 32-char hex (first 16 bytes of public key)
pkt.signature = Ed25519.sign(pkt.payload)
pkt.flags    |= FLAG_SIGNED
```

**Verify:**
```
if FLAG_SIGNED not set → accept (unsigned packets are not rejected)
else → Ed25519.verify(public_key, pkt.payload, pkt.signature)
```

`KeyStore.verify()` additionally rejects packets with `FLAG_SIGNED` if the `src_id` is not in the trusted-peer map.

---

## Mesh Routing

The server relays packets when `Packet.dst_id` is non-empty and names a different connected peer.

```
Client A → Server → Client B
  Packet { dst_id: "peer_id_B", ttl: 16, ... }

Server:
  if dst_id != "" and dst_id != src_conn_id:
      if ttl == 0: drop
      lookup connections[dst_id]
      fwd = pkt.clone()
      fwd.is_mesh = true
      fwd.ttl     = ttl - 1
      forward via open_uni() to dst connection
      return   ← do NOT deliver locally
```

`MESH_DEFAULT_TTL = 16`. Set `Packet.ttl` explicitly to control routing depth.

---

## Other Protobuf Messages

### RpcRequest / RpcResponse

```protobuf
message RpcRequest {
    string method = 1;
    bytes  params = 2;   // application-defined serialization
}

message RpcResponse {
    bool   success = 1;
    bytes  result  = 2;
    string error   = 3;
    int32  code    = 4;
}
```

Encode an `RpcRequest` into `Packet.payload` with `type = RPC_REQUEST`. Echo `correlation_id` in the response.

### AuthRequest / AuthResponse

```protobuf
message AuthRequest {
    string password_hash    = 1;
    string protocol_version = 2;
    map<string, string> client_info = 3;
}

message AuthResponse {
    bool   success            = 1;
    string session_token      = 2;
    string reason             = 3;
    uint32 heartbeat_interval = 4;
    map<string, string> server_info = 5;
}
```

### FileChunk

```protobuf
message FileChunk {
    uint32 chunk_index  = 1;
    uint32 total_chunks = 2;
    bytes  data         = 3;
    string filename     = 4;
    uint64 file_size    = 5;
}
```

Group chunks with a shared `correlation_id` in the outer `Packet`.

---

## Protocol Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `PROTO_VERSION` | 1 | Protocol version encoded in every `Packet.version` |
| `PROTO_MAGIC` | `"NCON\x01"` | 5-byte stream prefix |
| `MAX_MSG_SIZE` | 10 MB | Maximum uni-stream message size |
| `MAX_STREAM_SIZE` | 100 MB | Maximum bi-stream payload |
| `MAX_CONNECTIONS` | 1024 | Default server connection cap |
| `MESH_DEFAULT_TTL` | 16 | Default hop limit for routed packets |
| `ZSTD_MT_THRESHOLD` | 32 MB | Minimum size for multi-threaded Zstd |

---

## TLS / QUIC Layer

- TLS 1.3 (enforced by QUIC)
- ALPN protocol ID: `"netconduit"`
- Server generates a self-signed Ed25519 certificate at startup (`rcgen`)
- Clients default to `DummyVerifier` (skip cert verification) — suitable for LAN/dev
- Production use: call `ConduitClient::connect_pinned()` with the server's DER cert bytes
- Session resumption (0-RTT) enabled client-side via `ClientSessionMemoryCache` (256 entries)
- Keep-alive: 15-second interval, 60-second idle timeout
- Max concurrent bi-streams per connection: 4096
- Max concurrent uni-streams per connection: 4096
- Stream receive window: 16 MB per stream, 64 MB connection-wide
