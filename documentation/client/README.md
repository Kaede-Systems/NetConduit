# Client Guide

Complete reference for `ConduitClient`.

---

## Connect

### Development (accept any certificate)

```rust
use tokio::sync::mpsc;
use netconduit_core::core::{ConduitClient, ConduitEvent};

let (tx, mut rx) = mpsc::channel::<ConduitEvent>(8192);
let client = ConduitClient::connect(
    "127.0.0.1",  // host
    9000,          // port
    10,            // connect timeout (seconds)
    tx,            // event sink
    None,          // local port (None = OS assigns)
).await?;
```

### Production (certificate pinning)

```rust
let cert_der: Vec<u8> = /* DER bytes from server.cert_der or out-of-band */ vec![...];

let client = ConduitClient::connect_pinned(
    "192.168.1.1", 9000, 10, tx, None, cert_der
).await?;
```

### Bind to a specific local port (for hole-punching)

```rust
let client = ConduitClient::connect(
    "peer.example.com", 9000, 10, tx, Some(44444)
).await?;
```

---

## Event Loop

```rust
while let Some(event) = rx.recv().await {
    match event {
        ConduitEvent::Connect { client_id } => {
            // client_id is empty string for client-side events
            println!("connected");
        }
        ConduitEvent::Message { payload, .. } => {
            println!("received: {} bytes", payload.len());
        }
        ConduitEvent::BinaryStream { name, payload, .. } => {
            println!("stream '{name}': {} bytes", payload.len());
        }
        ConduitEvent::DuplexStreamOpen { mut stream, .. } => {
            tokio::spawn(async move {
                while let Ok(Some(data)) = stream.recv_data().await {
                    println!("duplex: {}", String::from_utf8_lossy(&data));
                }
            });
        }
        ConduitEvent::Disconnect { .. } => {
            println!("disconnected");
            break;
        }
    }
}
```

---

## Sending

### Raw message (uni-stream, reliable ordered)

```rust
client.send_message(b"hello server").await?;
```

### Named binary stream (bi-stream, ephemeral)

```rust
let data = std::fs::read("sensor.bin")?;
client.send_binary_stream("sensor_data", data).await?;
```

### QUIC datagram (unreliable, ~1200 B max, lowest latency)

```rust
// Good for: game state, audio packets, periodic telemetry
client.send_datagram(b"x=100:y=200:t=42").await?;
```

---

## Full-Duplex Stream

Open a persistent bidirectional channel. Both sides can send and receive simultaneously without the stream closing between messages.

```rust
// Client opens the stream
let mut stream = client.open_duplex_stream("chat").await?;

// Send without closing
stream.send_data(b"hello".to_vec()).await?;

// Receive (blocks until data arrives)
if let Some(reply) = stream.recv_data().await? {
    println!("server said: {}", String::from_utf8_lossy(&reply));
}

// Close the send half when done (server can still send)
stream.close().await?;
```

On the server side, the stream arrives as `ConduitEvent::DuplexStreamOpen { stream, .. }`.

### Ping-pong example

```rust
let mut stream = client.open_duplex_stream("ping").await?;
for i in 0u64..10 {
    stream.send_data(i.to_be_bytes().to_vec()).await?;
    let reply = stream.recv_data().await?.unwrap();
    let n = u64::from_be_bytes(reply.try_into().unwrap());
    println!("pong: {n}");
}
stream.close().await?;
```

---

## Signed Messages (Ed25519)

```rust
use netconduit_core::security::{NodeIdentity, sign_packet, KeyStore};
use netconduit_core::protocol::Packet;
use netconduit_core::core::{PROTO_VERSION, FLAG_SIGNED};

// Generate once, persist pkcs8_bytes() to disk
let identity = NodeIdentity::generate()?;

// Restore from disk
let identity = NodeIdentity::from_pkcs8(stored_pkcs8_bytes)?;

// Sign a packet before sending
let mut pkt = Packet {
    version: PROTO_VERSION,
    payload: b"authenticated payload".to_vec(),
    ..Default::default()
};
sign_packet(&mut pkt, &identity);
// pkt.src_id    = identity.peer_id (32-char hex)
// pkt.signature = 64-byte Ed25519 signature
// pkt.flags    |= FLAG_SIGNED

// Verify on the receiving side
let store = KeyStore::new();
store.add(peer.peer_id.clone(), peer.public_key.clone());
assert!(store.verify(&received_pkt));
```

---

## Mesh Routing

Send a packet through the server to another peer:

```rust
use netconduit_core::protocol::Packet;
use netconduit_core::core::{PROTO_VERSION, MESH_DEFAULT_TTL, frame_packet};

let pkt = Packet {
    version: PROTO_VERSION,
    dst_id:  "192.168.1.10:55555".to_string(),  // target peer's client_id
    ttl:     MESH_DEFAULT_TTL,
    payload: b"routed message".to_vec(),
    ..Default::default()
};

// Frame and send via uni-stream to server
let frame = frame_packet(&pkt);
let mut s = client.conn.open_uni().await?;
s.write_all(&frame).await?;
s.finish()?;
```

---

## STUN / NAT Traversal

Discover the external address and punch a UDP hole simultaneously:

```rust
use netconduit_core::core::stun_punch_hole;

let external_addr = stun_punch_hole(
    "stun.l.google.com:19302".to_string(),
    44444,          // local UDP port
    "".to_string(), // peer addr (empty = discovery only)
)?;
println!("External address: {external_addr}");

// Punch hole to peer
let mapped = stun_punch_hole(
    "stun.l.google.com:19302".to_string(),
    44444,
    "peer_external_addr:port".to_string(),
)?;
```

---

## Connection Stats

```rust
let stats = client.conn.stats();
println!("UDP TX datagrams: {}", stats.udp_tx.datagrams);
println!("UDP RX datagrams: {}", stats.udp_rx.datagrams);
println!("UDP TX bytes:     {}", stats.udp_tx.bytes);
println!("UDP RX bytes:     {}", stats.udp_rx.bytes);
```

---

## Disconnect

```rust
client.disconnect().await;
// Sends QUIC close frame; server receives ConduitEvent::Disconnect
```

---

## Python (netconduit_core extension)

```python
import netconduit_core

client = netconduit_core.RustQUICClient()

def on_event(event: str, client_id: str, payload: bytes):
    if event == "connect":
        print("Connected")
    elif event == "message":
        print(f"Received: {payload.decode()}")
    elif event == "binary_stream":
        print(f"Stream received: {len(payload)} bytes")
    elif event == "disconnect":
        print("Disconnected")

connected = client.connect("127.0.0.1", 9000, 10, on_event)
if connected:
    client.send_message(b"hello from Python")
    client.send_binary_stream("data", bytes(range(256)))

# Check connection health
print(f"Alive: {client.is_alive()}")
print(f"Stats: {client.stats()}")

client.disconnect()
```

See [Python Guide](../examples.md#python) for more complete examples.

---

## Complete Rust Example

```rust
use tokio::sync::mpsc;
use netconduit_core::core::{ConduitClient, ConduitEvent};
use netconduit_core::security::NodeIdentity;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let identity = NodeIdentity::generate()?;
    println!("My peer ID: {}", identity.peer_id);

    let (tx, mut rx) = mpsc::channel(8192);
    let client = ConduitClient::connect("127.0.0.1", 9000, 10, tx, None).await?;

    // Fire off a message
    client.send_message(b"ping").await?;

    // Open a duplex stream concurrently
    let mut stream = client.open_duplex_stream("bidirectional").await?;
    tokio::spawn(async move {
        for i in 0..3u32 {
            let _ = stream.send_data(format!("msg-{i}").into_bytes()).await;
            if let Ok(Some(reply)) = stream.recv_data().await {
                println!("stream reply: {}", String::from_utf8_lossy(&reply));
            }
        }
        let _ = stream.close().await;
    });

    // Drain events
    let mut count = 0;
    while let Some(event) = rx.recv().await {
        match event {
            ConduitEvent::Message { payload, .. } => {
                println!("msg: {}", String::from_utf8_lossy(&payload));
                count += 1;
                if count >= 3 { break; }
            }
            ConduitEvent::Disconnect { .. } => break,
            _ => {}
        }
    }

    Ok(())
}
```
