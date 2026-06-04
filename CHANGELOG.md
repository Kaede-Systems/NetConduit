# Changelog

All notable changes to NetConduit are documented here.

---

## [4.0.0] — 2026-06-05

### Complete rewrite: Python TCP → Rust QUIC core

This release replaces the Python TCP implementation with a native Rust library
built on QUIC (quinn), Protobuf (prost), and hardware-accelerated cryptography.

### Added

**Transport**
- QUIC over UDP via `quinn` — multiplexed streams + datagrams on a single UDP connection
- TLS 1.3 with auto-generated self-signed Ed25519 certificates (`rcgen`)
- `connect_pinned()` for production cert-pinning without a CA
- `send_datagram()` — unreliable, ~1200 B max, lowest-latency path
- 0-RTT session resumption on reconnect (client-side session cache)

**Serialization**
- Full Protobuf protocol (`prost`); JSON completely removed
- `Packet` message: version, type, flags, correlation_id, timestamp, payload, src_id, dst_id, checksum, sequence_id, stream_id, channel_name, signature, ttl
- `RpcRequest` / `RpcResponse` / `AuthRequest` / `AuthResponse` / `FileChunk` messages

**Compression**
- Adaptive codec selection: none < 64 B · LZ4 64–255 B · Zstd ≥ 256 B
- Entropy check (`is_likely_compressible`): 4 KB LZ4 probe skips Zstd on incompressible data — avoids ~20 ms wasted on random/encrypted/pre-compressed payloads
- Zstd level 1 (~400 MB/s) over level 3 (~200 MB/s); ~5% ratio penalty but 2× speed
- Multi-threaded Zstd only above 32 MB (MT dispatch overhead dominates below that threshold)
- Single-allocation frame building via `prost::encoded_len()` + direct encode

**Security**
- `NodeIdentity` — Ed25519 keypair via `aws-lc-rs` (VAES, SHA-NI accelerated)
- `sign_packet` / `verify_packet` — payload-covering signature, `FLAG_SIGNED` bit
- `KeyStore` — in-memory map of trusted peer public keys; `verify()` rejects unknown signers
- PKCS#8 serialization for persistent node identity across restarts

**Mesh Networking**
- Server-side routing: `Packet.dst_id` → forward to that peer's `Connection`
- `Packet.ttl` (default 16) — decremented at each relay; packet dropped at 0
- `Packet.is_mesh` flag set on forwarded packets
- `METRICS.mesh_forwards` counter

**Resource Management**
- `ResourcePool` — `Arc<Semaphore>` bounded task pool (default: `min(cpu_count × 64, 4096)`)
- Uni-streams and datagrams use `try_acquire()` for load shedding under pressure
- Bi-streams (duplex) use `acquire().await` — never shed
- `METRICS.messages_dropped` counter for shed packets

**Full-Duplex Streams**
- `ConduitDuplexStream` — wraps `quinn::SendStream` + `RecvStream`
- `send_data()` / `recv_data()` — compression + framing per message; stream stays open
- `close()` — graceful `SendStream::finish()`, recv half remains until remote closes
- `FLAG_DUPLEX = 0x20` marks opening packet; server emits `ConduitEvent::DuplexStreamOpen`

**Hardware Acceleration**
- `.cargo/config.toml`: `target-cpu=native` enables VAES, AVX2, SHA-NI, VPCLMULQDQ
- `aws-lc-rs` replaces `ring` — VAES-accelerated AES-GCM for QUIC, 2.1× faster Ed25519
- `mimalloc` global allocator — 15–40% faster on allocation-heavy workloads
- `zstdmt` feature — Zstd multi-threaded encoder for ≥ 32 MB payloads

**Named Channels**
- `ChannelConfig` — named logical channel with `ChannelMode` and `CompressionPolicy`
- `ChannelMode`: `ReliableOrdered` · `ReliableUnordered` · `Unreliable`
- `CompressionPolicy`: `Auto` · `Never` · `Always`
- `ConduitServer::register_channel()` for custom per-channel policies

**Metrics**
- `METRICS` global: bytes_sent, bytes_received, messages_sent, messages_received, connections_active, connections_total, connections_rejected, messages_dropped, mesh_forwards
- `metrics_snapshot()` for atomic point-in-time reads

**Utilities**
- `compute_checksum` / `verify_checksum` — Blake3-128 hex digest
- `stun_punch_hole()` — ICE-like NAT traversal via STUN XOR-MAPPED-ADDRESS
- `ConduitRouteCache` — sled-backed persistent route table
- `ConduitReorderBuffer` — per-stream reorder buffer with configurable gap tolerance
- `ConduitSequenceCounter` — per-stream monotonic sequence numbers

**Flutter Bindings**
- `flutter_rust_bridge` v2 FFI API in `src/api/mod.rs`
- `NetConduitServerHandle` / `NetConduitClientHandle` as opaque Dart handles
- `NetConduitEvent` sealed class hierarchy pushed via `StreamSink`

**Python Bindings**
- PyO3 / maturin extension module (`feature = "python"`)

**Tests**
- 91 integration tests covering: connect/disconnect, messaging, binary streams, duplex streams, mesh routing, Ed25519 security, ResourcePool, compression, checksums

### Removed

- Python TCP server/client (`conduit/` package remains as legacy; not the primary API)
- mDNS discovery (`mdns-sd` dependency removed)
- JSON serialization (`serde_json` removed)
- SHA256 password authentication (replaced by TLS + Ed25519 node identity)
- `ring` crate (replaced by `aws-lc-rs` for hardware acceleration)

---

## [3.0.0] — 2024-12-15

### Added (Python TCP era)
- File transfer (`conduit.transfer`) — chunked upload/download with SHA256 checksum
- Streaming API (`conduit.streaming`) — continuous streams with subscribers
- Client pool (`conduit.pool`) — multiple connections with round-robin/random/least-latency

---

## [2.0.0] — 2024-12-15

### Fixed
- Connection state machine invalid transitions
- RPC response routing bug
- Authentication flag not being set

---

## [0.1.0] — 2024-12-14

### Added
- Initial release: binary TCP protocol, RPC, heartbeat, backpressure
