# NetConduit Documentation

NetConduit is a QUIC-based networking library with a native Rust core, Ed25519 mesh security, and bindings for Flutter and Python.

## Quick Navigation

| Guide | Description |
|-------|-------------|
| [Quick Start](quickstart.md) | Get a server and client running in 5 minutes |
| [Server Guide](server/README.md) | `ConduitServer` — lifecycle, events, channels, mesh |
| [Client Guide](client/README.md) | `ConduitClient` — connect, send, duplex streams, datagrams |
| [Protocol Spec](protocol/README.md) | Wire format, Packet fields, flags, compression, security |
| [Examples](examples.md) | Echo server, chat, file transfer, duplex streaming |

## Key Concepts

**QUIC transport** — all communication runs over UDP via QUIC. Each connection multiplexes:
- **Uni-streams** — reliable, ordered, one-shot messages
- **Bi-streams** — reliable, ordered, either ephemeral (`BinaryStream`) or long-lived (`DuplexStream`)
- **Datagrams** — unreliable, lowest-latency, ~1200 B max

**Protobuf framing** — every frame is `PROTO_MAGIC[5] + pkt_len_u32_be[4] + Packet[pkt_len]`. The `Packet` message carries type, flags, payload, routing fields, and an optional Ed25519 signature.

**Adaptive compression** — payloads below 64 B are sent raw; 64–255 B use LZ4; ≥256 B use Zstd level 1. An entropy check (4 KB LZ4 probe) skips the Zstd pass for incompressible data.

**Mesh routing** — a server relays packets when `Packet.dst_id` names another connected peer. The TTL field caps hop count (default 16).

**ResourcePool** — a bounded semaphore (`Arc<Semaphore>`) limits concurrent stream-processing tasks to `min(cpu_count × 64, 4096)`. Non-critical streams shed load via `try_acquire()`; duplex streams always acquire.

**Ed25519 security** — `NodeIdentity` generates a keypair; `sign_packet` attaches a 64-byte signature; `KeyStore` verifies against registered peers.

## Crate Features

| Feature | Activation | What it adds |
|---------|-----------|--------------|
| _(none)_ | `cargo build` | Rust library only |
| `flutter` | `--features flutter` | `flutter_rust_bridge` v2 FFI API |
| `python` | `--features python` | PyO3 extension module |

## Support

- **GitHub**: [DarsheeeGamer/NetConduit](https://github.com/DarsheeeGamer/NetConduit)
- **Email**: cleaverdeath@gmail.com

## License

MIT License — Kaede Dev - Kento Hinode
