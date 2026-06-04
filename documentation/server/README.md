# Server Guide

Complete reference for `ConduitServer`.

---

## Start a Server

```rust
use tokio::sync::mpsc;
use netconduit::core::{ConduitServer, ConduitEvent};

let (tx, mut rx) = mpsc::channel::<ConduitEvent>(8192);
// 0 = use MAX_CONNECTIONS default (1024)
let server = ConduitServer::start("0.0.0.0", 9000, tx, 0).await?;
```

The server binds immediately. `rx` receives all connection events asynchronously.

---

## Event Loop

```rust
while let Some(event) = rx.recv().await {
    match event {
        ConduitEvent::Connect { client_id } => {
            // client_id = remote socket address string, e.g. "192.168.1.5:54321"
            println!("connected: {client_id}");
        }
        ConduitEvent::Message { client_id, payload } => {
            // payload is already decompressed
            handle_message(client_id, payload, &server);
        }
        ConduitEvent::BinaryStream { client_id, name, payload } => {
            // named ephemeral stream (bi-stream, already decompressed)
            println!("stream '{name}' from {client_id}: {} bytes", payload.len());
        }
        ConduitEvent::DuplexStreamOpen { client_id, mut stream } => {
            // long-lived bidirectional stream — must be handled in a task
            tokio::spawn(async move {
                while let Ok(Some(data)) = stream.recv_data().await {
                    let _ = stream.send_data(data).await;
                }
            });
        }
        ConduitEvent::Disconnect { client_id } => {
            println!("disconnected: {client_id}");
        }
    }
}
```

---

## Sending Messages

### Unicast

```rust
// Returns false if the send queue is full (8192 capacity) — backpressure signal
let queued = server.send_message(client_id.clone(), b"hello".to_vec());
```

### Broadcast (arc-shared frame, no per-client copy)

```rust
server.broadcast(b"announcement".to_vec());
```

### Named binary stream to a specific client

```rust
server.send_binary_stream(&client_id, "sensor_data", payload).await?;
```

### QUIC datagram (unreliable, lowest latency, ~1200 B max)

```rust
server.send_datagram(&client_id, b"tick".to_vec())?;
```

---

## Named Channels

Register a logical channel with custom reliability and compression settings:

```rust
use netconduit::core::{ChannelConfig, ChannelMode, CompressionPolicy};

// Reliable ordered channel with auto compression (default behaviour)
server.register_channel(ChannelConfig {
    name: "telemetry".to_string(),
    mode: ChannelMode::ReliableOrdered,
    compression: CompressionPolicy::Auto,
});

// Unreliable channel, no compression (for game state, audio)
server.register_channel(ChannelConfig::unreliable("game_state"));

// Reliable unordered, never compress (already-compressed images)
server.register_channel(ChannelConfig {
    name: "images".to_string(),
    mode: ChannelMode::ReliableUnordered,
    compression: CompressionPolicy::Never,
});
```

`ChannelMode` options:

| Mode | Description |
|------|-------------|
| `ReliableOrdered` | QUIC bi-stream — ordered, reliable. Default. |
| `ReliableUnordered` | QUIC uni-stream — reliable but no ordering across messages. |
| `Unreliable` | QUIC datagram — fire-and-forget, lowest latency. |

`CompressionPolicy` options:

| Policy | Description |
|--------|-------------|
| `Auto` | None < 64 B · LZ4 64–255 B · Zstd ≥ 256 B + entropy check |
| `Never` | Send raw. Use for pre-compressed data. |
| `Always` | Always compress, even tiny payloads. |

---

## Connection Management

```rust
// List all currently connected client IDs
let clients: Vec<String> = server.connected_clients();

// Check if a specific peer is connected
let alive = server.is_connected("192.168.1.5:54321");

// Total number of active connections
let count = server.connection_count();

// Stop the server (closes QUIC endpoint, disconnects all peers)
server.stop();
```

---

## Certificate Pinning

By default the server generates a self-signed Ed25519 certificate at startup. Share the DER bytes with clients to enable cert pinning:

```rust
// Get DER bytes after starting server
let cert_der: Vec<u8> = server.cert_der.clone();

// Distribute cert_der to clients (e.g. via out-of-band channel, config file)
// Client then connects with:
//   ConduitClient::connect_pinned(..., cert_der).await?
```

---

## Metrics

Process-wide atomic counters, updated by every server and client connection:

```rust
use netconduit::core::{METRICS, metrics_snapshot};
use std::sync::atomic::Ordering;

// Snapshot (all counters at once, Relaxed ordering)
let (bytes_sent, bytes_recv, msg_sent, msg_recv, conn_active, conn_total, conn_rejected)
    = metrics_snapshot();

// Individual fields
let dropped  = METRICS.messages_dropped.load(Ordering::Relaxed);
let forwarded = METRICS.mesh_forwards.load(Ordering::Relaxed);
```

| Metric | Description |
|--------|-------------|
| `bytes_sent` | Total bytes written across all connections |
| `bytes_received` | Total bytes read across all connections |
| `messages_sent` | Total messages sent |
| `messages_received` | Total messages received |
| `connections_active` | Currently connected peers |
| `connections_total` | All-time connections accepted |
| `connections_rejected` | Connections dropped due to capacity limit |
| `messages_dropped` | Packets shed when ResourcePool was at capacity |
| `mesh_forwards` | Packets relayed to another peer via mesh routing |

---

## ResourcePool

The server creates a `ResourcePool` automatically (`default_for_machine()`). It limits concurrent stream-processing tasks to `min(cpu_count × 64, 4096)`.

- **Uni-streams and datagrams**: `try_acquire()` — shed load when full, increment `messages_dropped`
- **Bi-streams (duplex)**: `acquire().await` — always accepted, backpressure via QUIC flow control

To use a custom pool size:

```rust
use netconduit::core::ResourcePool;

let pool = ResourcePool::new(512);
println!("Active tasks: {}", pool.active_tasks());
println!("Available slots: {}", pool.available());
```

---

## Mesh Routing

The server automatically relays packets when `Packet.dst_id` is set to another connected peer's `client_id`. No application code needed on the server side.

```rust
// Client A sends a packet routed through the server to Client B:
use netconduit::protocol::Packet;
use netconduit::core::{PROTO_VERSION, MESH_DEFAULT_TTL};

let pkt = Packet {
    version: PROTO_VERSION,
    dst_id:  "192.168.1.10:44444".to_string(),  // Client B's client_id
    ttl:     MESH_DEFAULT_TTL,
    payload: b"hello client B".to_vec(),
    ..Default::default()
};
// Serialize pkt into a uni-stream and send to server
```

The server:
1. Checks `dst_id` on every incoming uni-stream packet
2. If non-empty and not its own address: look up `connections[dst_id]`
3. Decrement TTL, set `is_mesh = true`, forward via `open_uni()`
4. Increments `METRICS.mesh_forwards`
5. Drops packet when TTL reaches 0

---

## Complete Example

```rust
use tokio::sync::mpsc;
use netconduit::core::{ConduitServer, ConduitEvent, ChannelConfig};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type PeerMap = Arc<Mutex<HashMap<String, String>>>;  // client_id → username

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel(8192);
    let server = ConduitServer::start("0.0.0.0", 9000, tx, 0).await?;
    let peers: PeerMap = Arc::new(Mutex::new(HashMap::new()));

    println!("Server listening on :9000");
    println!("Certificate DER: {} bytes", server.cert_der.len());

    while let Some(event) = rx.recv().await {
        match event {
            ConduitEvent::Connect { client_id } => {
                peers.lock().unwrap().insert(client_id.clone(), client_id.clone());
                server.broadcast(format!("JOIN:{client_id}").into_bytes());
            }
            ConduitEvent::Message { client_id, payload } => {
                // Echo to sender, broadcast to others
                let msg = format!("{client_id}: {}", String::from_utf8_lossy(&payload));
                server.broadcast(msg.into_bytes());
            }
            ConduitEvent::DuplexStreamOpen { client_id, mut stream } => {
                let server_clone = server.clone();
                tokio::spawn(async move {
                    while let Ok(Some(data)) = stream.recv_data().await {
                        let _ = stream.send_data(data).await;
                    }
                });
            }
            ConduitEvent::Disconnect { client_id } => {
                peers.lock().unwrap().remove(&client_id);
                server.broadcast(format!("LEAVE:{client_id}").into_bytes());
            }
            _ => {}
        }
    }

    Ok(())
}
```

---

## Python

```python
import netconduit

server = netconduit.RustQUICServer()

def on_event(event: str, client_id: str, payload: bytes):
    if event == "connect":
        print(f"Connected: {client_id}")
    elif event == "message":
        print(f"Message from {client_id}: {payload.decode()}")
        server.send_message(client_id, payload)   # echo
    elif event == "binary_stream":
        print(f"Stream from {client_id}: {len(payload)} bytes")
    elif event == "disconnect":
        print(f"Disconnected: {client_id}")

server.start("0.0.0.0", 9000, on_event)

import time; time.sleep(60)   # keep alive

server.stop()
```

See [Python Guide](../examples.md#python) for more examples.
