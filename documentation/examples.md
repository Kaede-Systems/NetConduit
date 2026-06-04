# Examples

Real-world usage patterns for NetConduit.

---

## Rust Examples

### Echo Server

```rust
use tokio::sync::mpsc;
use netconduit_core::core::{ConduitServer, ConduitEvent};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel(8192);
    let server = ConduitServer::start("0.0.0.0", 9000, tx, 0).await?;

    while let Some(event) = rx.recv().await {
        if let ConduitEvent::Message { client_id, payload } = event {
            server.send_message(client_id, payload);
        }
    }
    Ok(())
}
```

---

### Chat Server (broadcast)

```rust
use tokio::sync::mpsc;
use netconduit_core::core::{ConduitServer, ConduitEvent};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (tx, mut rx) = mpsc::channel(8192);
    let server = ConduitServer::start("0.0.0.0", 9000, tx, 0).await?;

    while let Some(event) = rx.recv().await {
        match event {
            ConduitEvent::Connect { client_id } => {
                server.broadcast(format!("JOIN {client_id}").into_bytes());
            }
            ConduitEvent::Message { client_id, payload } => {
                let msg = format!("{client_id}: {}", String::from_utf8_lossy(&payload));
                server.broadcast(msg.into_bytes());
            }
            ConduitEvent::Disconnect { client_id } => {
                server.broadcast(format!("LEAVE {client_id}").into_bytes());
            }
            _ => {}
        }
    }
    Ok(())
}
```

---

### File Transfer (named binary stream)

```rust
// Sender (client side)
let data = std::fs::read("large_file.bin")?;
client.send_binary_stream("file:large_file.bin", data).await?;

// Receiver (server side)
ConduitEvent::BinaryStream { client_id, name, payload } => {
    if let Some(filename) = name.strip_prefix("file:") {
        std::fs::write(format!("received/{filename}"), &payload)?;
        println!("Saved {}: {} bytes", filename, payload.len());
    }
}
```

---

### Duplex Streaming (real-time bidirectional)

```rust
// Server side — handle each duplex stream in its own task
ConduitEvent::DuplexStreamOpen { client_id, mut stream } => {
    tokio::spawn(async move {
        loop {
            match stream.recv_data().await {
                Ok(Some(data)) => {
                    // Process and reply
                    let reply = process(data);
                    if stream.send_data(reply).await.is_err() { break; }
                }
                Ok(None) | Err(_) => break,  // stream closed
            }
        }
    });
}

// Client side
let mut stream = client.open_duplex_stream("realtime").await?;
loop {
    let reading = get_sensor_reading();
    stream.send_data(reading).await?;
    if let Some(command) = stream.recv_data().await? {
        apply_command(command);
    }
}
stream.close().await?;
```

---

### Signed Mesh Message

```rust
use netconduit_core::security::{NodeIdentity, sign_packet};
use netconduit_core::protocol::Packet;
use netconduit_core::core::{PROTO_VERSION, MESH_DEFAULT_TTL, frame_packet};

// Node A sends an authenticated message to Node C via server relay
let identity = NodeIdentity::generate()?;

let mut pkt = Packet {
    version: PROTO_VERSION,
    dst_id:  "node_c_client_id".to_string(),
    ttl:     MESH_DEFAULT_TTL,
    payload: b"authenticated routed message".to_vec(),
    ..Default::default()
};
sign_packet(&mut pkt, &identity);

let frame = frame_packet(&pkt);
let mut s = client.conn.open_uni().await?;
s.write_all(&frame).await?;
s.finish()?;
```

---

### Low-Latency Game State (datagrams)

```rust
// Server broadcasts game state every frame (~60 Hz)
loop {
    let state = serialize_game_state();
    for cid in server.connected_clients() {
        let _ = server.send_datagram(&cid, state.clone());
    }
    tokio::time::sleep(Duration::from_millis(16)).await;
}

// Client sends input
client.send_datagram(&serialize_input(keys_pressed)).await?;
```

---

### Checksum Verification

```rust
use netconduit_core::core::{compute_checksum, verify_checksum};

let data = std::fs::read("important.bin")?;
let checksum = compute_checksum(&data);  // Blake3-128 hex

// Later, verify integrity
assert!(verify_checksum(&received_data, &checksum));
```

---

## Python Examples

Install the extension module:

```bash
pip install netconduit-core   # pre-built wheel from PyPI
# or build from source:
cd netconduit_core && maturin develop --features python
```

### Echo Server

```python
import netconduit_core
import time

server = netconduit_core.RustQUICServer()

def on_event(event: str, client_id: str, payload: bytes) -> None:
    if event == "connect":
        print(f"[+] {client_id}")
    elif event == "message":
        server.send_message(client_id, payload)   # echo
    elif event == "binary_stream":
        print(f"stream from {client_id}: {len(payload)} bytes")
    elif event == "disconnect":
        print(f"[-] {client_id}")

server.start("0.0.0.0", 9000, on_event)
print("Echo server on :9000")

try:
    while True:
        time.sleep(1)
        print(f"Connections: {server.connection_count()}")
except KeyboardInterrupt:
    server.stop()
```

---

### Client

```python
import netconduit_core

client = netconduit_core.RustQUICClient()

received: list[bytes] = []

def on_event(event: str, client_id: str, payload: bytes) -> None:
    if event == "message":
        received.append(payload)
    elif event == "disconnect":
        print("disconnected")

ok = client.connect("127.0.0.1", 9000, 10, on_event)
assert ok, "Connection failed"

# Send a raw message
client.send_message(b"hello from Python")

# Send a named binary stream
client.send_binary_stream("data_channel", bytes(range(256)))

# Check connection health
print(f"Alive: {client.is_alive()}")
print(f"Stats: {client.stats()}")

import time; time.sleep(0.5)   # let messages arrive
print(f"Received: {received}")

client.disconnect()
```

---

### Compression Utilities

```python
import netconduit_core

data = b"hello " * 10000

# Compress (codec byte prepended: 0=none, 1=LZ4, 2=Zstd)
compressed = netconduit_core.compress_payload(data)
print(f"{len(data)} → {len(compressed)} bytes")

# Decompress
original = netconduit_core.decompress_payload(compressed)
assert original == data
```

---

### Checksum

```python
import netconduit_core

data = open("file.bin", "rb").read()
checksum = netconduit_core.compute_checksum(data)   # Blake3-128 hex string

# Verify later
ok = netconduit_core.verify_checksum(data, checksum)
print(f"Integrity: {'OK' if ok else 'CORRUPTED'}")
```

---

### STUN Hole Punching

```python
import netconduit_core

# Discover external address
external = netconduit_core.stun_punch_hole(
    "stun.l.google.com:19302",
    44444,   # local port
    "",      # empty = discovery only
)
print(f"External address: {external}")

# Punch hole to peer and get external address simultaneously
external = netconduit_core.stun_punch_hole(
    "stun.l.google.com:19302",
    44444,
    "peer.external.addr:port",
)
```

---

### Route Cache (persistent routing table)

```python
import netconduit_core

cache = netconduit_core.RouteCache("/tmp/netconduit_routes")

cache.set_route("node-B", "192.168.1.10:9000")
cache.set_route("node-C", "192.168.1.11:9000")

hop = cache.get_route("node-B")
print(f"Next hop for node-B: {hop}")

print(f"All routes: {cache.list_routes()}")
cache.track_connection("node-B")
cache.flush()
```

---

### Reorder Buffer

```python
import netconduit_core

buf = netconduit_core.ReorderBuffer(max_gap=64, max_buf=1024)

# Out-of-order delivery
buf.push(stream_id=1, sequence_id=2, payload=b"second")
buf.push(stream_id=1, sequence_id=0, payload=b"first")
buf.push(stream_id=1, sequence_id=1, payload=b"middle")

# Drain in order
for seq, data in buf.drain_ready(stream_id=1):
    print(f"seq={seq}: {data.decode()}")
# seq=0: first
# seq=1: middle
# seq=2: second
```

---

### Pack/Unpack Integers

```python
import netconduit_core

be = netconduit_core.BYTE_ORDER_BE
le = netconduit_core.BYTE_ORDER_LE

packed = netconduit_core.pack_u64(1234567890, be)
value  = netconduit_core.unpack_u64(packed, be)
assert value == 1234567890

host_order = netconduit_core.host_byte_order()  # returns BE or LE constant
```

---

### Generate Ed25519 Certificate (PEM)

```python
import netconduit_core

cert_pem, key_pem = netconduit_core.generate_ed25519_cert_pem()
# Use cert_pem/key_pem with TLS libraries or save to disk
with open("server.crt", "w") as f: f.write(cert_pem)
with open("server.key", "w") as f: f.write(key_pem)
```

---

### Server Statistics

```python
server = netconduit_core.RustQUICServer()
# ... start server ...

clients = server.connected_clients()
print(f"Connected ({len(clients)}): {clients}")
print(f"Is 127.0.0.1:12345 connected? {server.is_connected('127.0.0.1:12345')}")
```
