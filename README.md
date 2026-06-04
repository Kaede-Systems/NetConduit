# NetConduit

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)

**High-performance peer-to-peer networking library built on QUIC, with Protobuf framing, adaptive compression, Ed25519 mesh security, and bindings for Flutter and Python.**

Developed by **Kaede Dev - Kento Hinode**

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│              Application Layer                      │
│     Flutter (FFI)    Python (PyO3)    Rust crate    │
├─────────────────────────────────────────────────────┤
│               netconduit_core (Rust)                │
│  ConduitServer / ConduitClient / ConduitDuplexStream│
│  Mesh routing · ResourcePool · Ed25519 security     │
├─────────────────────────────────────────────────────┤
│             Transport & Serialization               │
│   QUIC (quinn)  ·  Protobuf (prost)  ·  TLS 1.3    │
├─────────────────────────────────────────────────────┤
│          Compression (auto-selected)                │
│   LZ4 (64–255 B)  ·  Zstd level-1 (≥ 256 B)       │
│   Entropy check: skips compress if incompressible   │
└─────────────────────────────────────────────────────┘
```

---

## Features

| Feature | Details |
|---------|---------|
| **Transport** | QUIC over UDP — multiplexed streams, datagrams, 0-RTT reconnect |
| **Serialization** | Protobuf (prost) — compact binary, schema-versioned |
| **Compression** | LZ4 for small payloads, Zstd level-1 for large; entropy check skips compress on incompressible data |
| **Security** | Ed25519 node identity, packet signing/verification, KeyStore for trusted peers |
| **Mesh routing** | Server-mediated — packets with `dst_id` are forwarded with TTL decrement |
| **Full-duplex streams** | `ConduitDuplexStream` — bidirectional bi-streams that stay open |
| **Resource pool** | `ResourcePool` (bounded semaphore) — load-sheds uni/datagram streams under pressure |
| **Hardware accel** | `target-cpu=native` enables VAES, AVX2, SHA-NI, VPCLMULQDQ on supported CPUs |
| **Flutter** | `flutter_rust_bridge` v2 FFI bindings |
| **Python** | PyO3 / maturin extension module |

---

## Performance (Intel i5-12450HX, loopback)

| Payload | Throughput |
|---------|-----------|
| 16 KB messages | 1.09 GB/s |
| 256 KB messages | 4.15 GB/s |
| 1 MB messages | 3.93 GB/s |
| 10 MB compressible stream | 1.70 GB/s |
| 10 MB random stream | 1.59 GB/s |
| Ed25519 sign | 105 k/s |

---

## Quick Start (Rust)

### Server

```rust
use tokio::sync::mpsc;
use netconduit_core::core::{ConduitServer, ConduitEvent};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel(8192);
    let server = ConduitServer::start("0.0.0.0", 9000, tx, 0).await?;

    while let Some(event) = rx.recv().await {
        match event {
            ConduitEvent::Connect    { client_id } => println!("+ {client_id}"),
            ConduitEvent::Message    { client_id, payload } => {
                server.send_message(client_id, payload);   // echo
            }
            ConduitEvent::Disconnect { client_id } => println!("- {client_id}"),
            _ => {}
        }
    }
    Ok(())
}
```

### Client

```rust
use tokio::sync::mpsc;
use netconduit_core::core::{ConduitClient, ConduitEvent};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel(8192);
    let client = ConduitClient::connect("127.0.0.1", 9000, 10, tx, None).await?;

    client.send_message(b"hello world").await?;

    if let Some(ConduitEvent::Message { payload, .. }) = rx.recv().await {
        println!("echo: {}", String::from_utf8_lossy(&payload));
    }
    Ok(())
}
```

### Full-Duplex Stream

```rust
// Client side — open and hold a long-lived bi-stream
let mut stream = client.open_duplex_stream("chat").await?;
stream.send_data(b"hello".to_vec()).await?;
if let Some(reply) = stream.recv_data().await? {
    println!("reply: {}", String::from_utf8_lossy(&reply));
}
stream.close().await?;

// Server side — receives ConduitEvent::DuplexStreamOpen { stream, .. }
```

### Signed Messages (Ed25519)

```rust
use netconduit_core::security::{NodeIdentity, sign_packet, KeyStore};
use netconduit_core::protocol::Packet;
use netconduit_core::core::PROTO_VERSION;

let identity = NodeIdentity::generate()?;
let mut pkt = Packet { version: PROTO_VERSION, payload: b"secure".to_vec(), ..Default::default() };
sign_packet(&mut pkt, &identity);  // sets FLAG_SIGNED + src_id + signature

let store = KeyStore::new();
store.add(identity.peer_id.clone(), identity.public_key.clone());
assert!(store.verify(&pkt));
```

### Mesh Routing

```rust
// Set dst_id to route through the server to another peer
use netconduit_core::core::MESH_DEFAULT_TTL;
use netconduit_core::protocol::Packet;

let mut pkt = Packet {
    dst_id: "target_peer_id_here".to_string(),
    ttl:    MESH_DEFAULT_TTL,
    payload: b"routed message".to_vec(),
    ..Default::default()
};
// Server forwards automatically; TTL decremented each hop
```

---

## Project Layout

```
netconduit_core/        Rust library crate
  src/
    core.rs             Server, Client, DuplexStream, ResourcePool, compression
    security.rs         Ed25519 NodeIdentity, KeyStore, sign/verify helpers
    lib.rs              Crate root — module re-exports, global allocator
    protocol.proto      Protobuf schema (compiled at build time via prost-build)
    python.rs           PyO3 bindings (feature = "python")
    api/mod.rs          Flutter Rust Bridge v2 API (feature = "flutter")
    bin/bench.rs        Throughput and latency benchmarks
  tests/
    integration.rs      91 integration tests

conduit/                Python TCP library (legacy, not QUIC-based)
flutter/                Flutter/Dart project
documentation/          User-facing guides and protocol specification
diagrams/               Architecture and flow diagrams
```

---

## Building

```bash
# Rust library (dev)
cd netconduit_core
cargo build

# Rust library (release, native-optimized)
cargo build --release                  # uses .cargo/config.toml target-cpu=native

# Run benchmarks
cargo run --release --bin bench

# Run all tests (91 tests)
cargo test

# Python extension (requires maturin)
maturin develop --features python

# Flutter bindings (requires flutter_rust_bridge_codegen)
make codegen
flutter build
```

---

## Documentation

- [Quick Start](documentation/quickstart.md)
- [Server Guide](documentation/server/README.md)
- [Client Guide](documentation/client/README.md)
- [Protocol Specification](documentation/protocol/README.md)
- [Examples](documentation/examples.md)
- [Architecture Diagrams](diagrams/README.md)

---

## License

MIT License — Kaede Dev - Kento Hinode

**GitHub**: [DarsheeeGamer/NetConduit](https://github.com/DarsheeeGamer/NetConduit)
