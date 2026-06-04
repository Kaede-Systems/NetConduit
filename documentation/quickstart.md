# Quick Start

Get a NetConduit server and client running in under 5 minutes.

## Prerequisites

- Rust 1.75+
- `cargo` in PATH

```bash
cd netconduit_core
cargo build --release
```

---

## Step 1: Server

```rust
// src/bin/server_example.rs
use tokio::sync::mpsc;
use netconduit_core::core::{ConduitServer, ConduitEvent};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel(8192);
    let server = ConduitServer::start("0.0.0.0", 9000, tx, 0).await?;

    println!("Listening on :9000");

    while let Some(event) = rx.recv().await {
        match event {
            ConduitEvent::Connect { client_id } => {
                println!("connected: {client_id}");
            }
            ConduitEvent::Message { client_id, payload } => {
                println!("message from {client_id}: {} bytes", payload.len());
                // Echo back
                server.send_message(client_id, payload);
            }
            ConduitEvent::BinaryStream { client_id, name, payload } => {
                println!("stream '{name}' from {client_id}: {} bytes", payload.len());
            }
            ConduitEvent::DuplexStreamOpen { client_id, mut stream } => {
                // Handle duplex stream in a background task
                tokio::spawn(async move {
                    while let Ok(Some(data)) = stream.recv_data().await {
                        let _ = stream.send_data(data).await; // echo
                    }
                });
            }
            ConduitEvent::Disconnect { client_id } => {
                println!("disconnected: {client_id}");
            }
        }
    }

    Ok(())
}
```

---

## Step 2: Client

```rust
// src/bin/client_example.rs
use tokio::sync::mpsc;
use netconduit_core::core::{ConduitClient, ConduitEvent};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel(8192);
    let client = ConduitClient::connect("127.0.0.1", 9000, 10, tx, None).await?;

    println!("Connected");

    // Send a raw message
    client.send_message(b"hello world").await?;

    // Receive the echo
    if let Some(ConduitEvent::Message { payload, .. }) = rx.recv().await {
        println!("Echo: {}", String::from_utf8_lossy(&payload));
    }

    // Send a named binary stream
    client.send_binary_stream("sensor_data", vec![0u8; 1024]).await?;

    Ok(())
}
```

---

## Step 3: Run

```bash
# Terminal 1 — server
cargo run --release --bin server_example

# Terminal 2 — client
cargo run --release --bin client_example
```

**Output (client):**
```
Connected
Echo: hello world
```

---

## Step 4: Full-Duplex Stream

```rust
// Open a persistent bidirectional stream
let mut stream = client.open_duplex_stream("realtime").await?;

for i in 0..5 {
    stream.send_data(format!("message {i}").into_bytes()).await?;
    if let Some(reply) = stream.recv_data().await? {
        println!("reply: {}", String::from_utf8_lossy(&reply));
    }
}

stream.close().await?;
```

---

## Step 5: Signed Messages

```rust
use netconduit_core::security::{NodeIdentity, sign_packet, KeyStore};
use netconduit_core::protocol::Packet;
use netconduit_core::core::{PROTO_VERSION, FLAG_SIGNED};

// Generate node identity (persist pkcs8_bytes() to disk for reuse)
let identity = NodeIdentity::generate()?;

let mut pkt = Packet {
    version: PROTO_VERSION,
    payload: b"authenticated payload".to_vec(),
    ..Default::default()
};
sign_packet(&mut pkt, &identity);
assert!(pkt.flags & FLAG_SIGNED != 0);

// On receiver: register the sender's public key and verify
let store = KeyStore::new();
store.add(identity.peer_id.clone(), identity.public_key.clone());
assert!(store.verify(&pkt));
```

---

## Step 6: Low-Latency Datagram

```rust
// Unreliable, ~1200 B max, no ordering guarantee
// Use for game state, audio, periodic telemetry
client.send_datagram(b"tick:42:x=100:y=200").await?;
```

---

## Next Steps

- [Server Guide](server/README.md) — channels, broadcasting, mesh, metrics
- [Client Guide](client/README.md) — cert pinning, duplex streams, datagrams
- [Protocol Spec](protocol/README.md) — wire format, Packet fields, compression pipeline
- [Examples](examples.md) — echo server, chat, file transfer, signed mesh messages
