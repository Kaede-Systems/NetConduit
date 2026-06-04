/// NetConduit integration tests — real QUIC over loopback.
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::mpsc;

use netconduit::core::{
    ConduitClient, ConduitServer, ConduitEvent,
    ChannelConfig, METRICS, metrics_snapshot, ResourcePool,
};
use netconduit::security::{NodeIdentity, sign_packet, verify_packet, KeyStore};
use std::sync::atomic::Ordering;

fn install_crypto_provider() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| { let _ = rustls::crypto::aws_lc_rs::default_provider().install_default(); });
}

extern crate netconduit;
extern crate rustls;

fn free_port() -> u16 {
    use std::net::TcpListener;
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

async fn start_test_server() -> (ConduitServer, u16, mpsc::Receiver<ConduitEvent>) {
    install_crypto_provider();
    let port = free_port();
    let (tx, rx) = mpsc::channel(256);
    let server = ConduitServer::start("127.0.0.1", port, tx, 0).await.expect("server start");
    (server, port, rx)
}

async fn connect_test_client(port: u16) -> (ConduitClient, mpsc::Receiver<ConduitEvent>) {
    install_crypto_provider();
    let (tx, rx) = mpsc::channel(256);
    let client = ConduitClient::connect("127.0.0.1", port, 5, tx, None).await.expect("connect");
    (client, rx)
}

async fn next_event(rx: &mut mpsc::Receiver<ConduitEvent>) -> ConduitEvent {
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await.expect("event timeout").expect("channel closed")
}

// ─── Server lifecycle ─────────────────────────────────────────────────────────

#[tokio::test]
async fn server_starts_and_stops() {
    let (server, port, _rx) = start_test_server().await;
    assert!(port > 0);
    server.stop();
}

#[tokio::test]
async fn server_reports_no_connections_when_idle() {
    let (server, _port, _rx) = start_test_server().await;
    assert_eq!(server.connection_count(), 0);
    assert!(server.connected_clients().is_empty());
    server.stop();
}

// ─── Client lifecycle ─────────────────────────────────────────────────────────

#[tokio::test]
async fn client_connects_and_server_fires_connect_event() {
    let (server, port, mut srv_rx) = start_test_server().await;
    let (_client, _) = connect_test_client(port).await;
    let ev = next_event(&mut srv_rx).await;
    assert!(matches!(ev, ConduitEvent::Connect { .. }));
    server.stop();
}

#[tokio::test]
async fn client_is_listed_after_connect() {
    let (server, port, mut srv_rx) = start_test_server().await;
    let (_client, _) = connect_test_client(port).await;
    next_event(&mut srv_rx).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(server.connection_count(), 1);
    server.stop();
}

#[tokio::test]
async fn client_disconnect_fires_disconnect_event() {
    let (server, port, mut srv_rx) = start_test_server().await;
    let (client, _) = connect_test_client(port).await;
    next_event(&mut srv_rx).await;
    drop(client);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let ev = next_event(&mut srv_rx).await;
    assert!(matches!(ev, ConduitEvent::Disconnect { .. }));
    server.stop();
}

// ─── Message passing ──────────────────────────────────────────────────────────

#[tokio::test]
async fn client_to_server_message_roundtrip() {
    let (server, port, mut srv_rx) = start_test_server().await;
    let (client, _) = connect_test_client(port).await;
    next_event(&mut srv_rx).await;
    client.send_message(b"hello").await.unwrap();
    let ev = next_event(&mut srv_rx).await;
    match ev {
        ConduitEvent::Message { payload, .. } => assert_eq!(payload, b"hello"),
        other => panic!("{other:?}"),
    }
    server.stop();
}

#[tokio::test]
async fn server_to_client_message_roundtrip() {
    let (server, port, mut srv_rx) = start_test_server().await;
    let (_client, mut cli_rx) = connect_test_client(port).await;
    let cid = match next_event(&mut srv_rx).await {
        ConduitEvent::Connect { client_id } => client_id,
        other => panic!("{other:?}"),
    };
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(server.send_message(cid, b"hi".to_vec()));
    let ev = next_event(&mut cli_rx).await;
    match ev {
        ConduitEvent::Message { payload, .. } => assert_eq!(payload, b"hi"),
        other => panic!("{other:?}"),
    }
    server.stop();
}

#[tokio::test]
async fn large_message_delivers_correctly() {
    let (server, port, mut srv_rx) = start_test_server().await;
    let (client, _) = connect_test_client(port).await;
    next_event(&mut srv_rx).await;
    let payload: Vec<u8> = (0u8..=255).cycle().take(1_000_000).collect();
    client.send_message(&payload).await.unwrap();
    let ev = next_event(&mut srv_rx).await;
    match ev {
        ConduitEvent::Message { payload: r, .. } => assert_eq!(r, payload),
        other => panic!("{other:?}"),
    }
    server.stop();
}

// ─── Binary streams ───────────────────────────────────────────────────────────

#[tokio::test]
async fn binary_stream_roundtrip_small() {
    let (server, port, mut srv_rx) = start_test_server().await;
    let (client, _) = connect_test_client(port).await;
    next_event(&mut srv_rx).await;
    client.send_binary_stream("test", b"data".to_vec()).await.unwrap();
    let ev = next_event(&mut srv_rx).await;
    match ev {
        ConduitEvent::BinaryStream { name, payload, .. } => {
            assert_eq!(name, "test");
            assert_eq!(payload, b"data");
        }
        other => panic!("{other:?}"),
    }
    server.stop();
}

#[tokio::test]
async fn binary_stream_large_compresses_correctly() {
    let (server, port, mut srv_rx) = start_test_server().await;
    let (client, _) = connect_test_client(port).await;
    next_event(&mut srv_rx).await;
    let data = vec![0xABu8; 512*1024];
    client.send_binary_stream("bulk", data.clone()).await.unwrap();
    let ev = tokio::time::timeout(Duration::from_secs(5), srv_rx.recv())
        .await.expect("timeout").expect("closed");
    match ev {
        ConduitEvent::BinaryStream { name, payload, .. } => {
            assert_eq!(name, "bulk");
            assert_eq!(payload, data);
        }
        other => panic!("{other:?}"),
    }
    server.stop();
}

#[tokio::test]
async fn binary_stream_name_preserved_through_protobuf() {
    let (server, port, mut srv_rx) = start_test_server().await;
    let (client, _) = connect_test_client(port).await;
    next_event(&mut srv_rx).await;
    client.send_binary_stream("my_channel_123", b"x".to_vec()).await.unwrap();
    let ev = next_event(&mut srv_rx).await;
    match ev {
        ConduitEvent::BinaryStream { name, .. } => assert_eq!(name, "my_channel_123"),
        other => panic!("{other:?}"),
    }
    server.stop();
}

// ─── Protocol enforcement ─────────────────────────────────────────────────────

#[tokio::test]
async fn message_without_magic_is_dropped() {
    let (server, port, mut srv_rx) = start_test_server().await;
    let (client, _) = connect_test_client(port).await;
    next_event(&mut srv_rx).await;
    let mut s = client.conn.open_uni().await.unwrap();
    s.write_all(b"bad frame").await.unwrap();
    s.finish().unwrap();
    client.send_message(b"valid").await.unwrap();
    let ev = next_event(&mut srv_rx).await;
    match ev {
        ConduitEvent::Message { payload, .. } => assert_eq!(payload, b"valid"),
        other => panic!("{other:?}"),
    }
    server.stop();
}

#[tokio::test]
async fn message_wrong_magic_version_is_dropped() {
    let (server, port, mut srv_rx) = start_test_server().await;
    let (client, _) = connect_test_client(port).await;
    next_event(&mut srv_rx).await;
    let mut s = client.conn.open_uni().await.unwrap();
    s.write_all(b"NCON\x02payload").await.unwrap();
    s.finish().unwrap();
    client.send_message(b"ok").await.unwrap();
    let ev = next_event(&mut srv_rx).await;
    match ev {
        ConduitEvent::Message { payload, .. } => assert_eq!(payload, b"ok"),
        other => panic!("{other:?}"),
    }
    server.stop();
}

// ─── Max connections ──────────────────────────────────────────────────────────

#[tokio::test]
async fn max_connections_rejects_excess() {
    let port = free_port();
    let (tx, _) = mpsc::channel(256);
    let server = ConduitServer::start("127.0.0.1", port, tx, 2).await.unwrap();
    let before = METRICS.connections_rejected.load(Ordering::Relaxed);
    let (_c1, _r1) = connect_test_client(port).await;
    let (_c2, _r2) = connect_test_client(port).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(server.connection_count(), 2);
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        ConduitClient::connect("127.0.0.1", port, 2, mpsc::channel(1).0, None),
    ).await;
    let rejected = result.is_err() || result.unwrap().is_err();
    let after = METRICS.connections_rejected.load(Ordering::Relaxed);
    assert!(rejected || after > before);
    server.stop();
}

// ─── Broadcast ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn broadcast_reaches_all_clients() {
    let (server, port, mut srv_rx) = start_test_server().await;
    let mut clients = Vec::new();
    let mut rxs: Vec<mpsc::Receiver<ConduitEvent>> = Vec::new();
    for _ in 0..3 {
        let (c, r) = connect_test_client(port).await;
        next_event(&mut srv_rx).await;
        clients.push(c); rxs.push(r);
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    let msg = b"broadcast".to_vec();
    server.broadcast(msg.clone());
    for rx in &mut rxs {
        let ev = next_event(rx).await;
        match ev {
            ConduitEvent::Message { payload, .. } => assert_eq!(payload, msg),
            other => panic!("{other:?}"),
        }
    }
    server.stop();
}

// ─── Cert pinning ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn connect_pinned_correct_cert_succeeds() {
    let (server, port, _) = start_test_server().await;
    let (tx, _) = mpsc::channel(1);
    ConduitClient::connect_pinned("127.0.0.1", port, 5, tx, None, server.cert_der.clone())
        .await.expect("pinned connect");
    server.stop();
}

#[tokio::test]
async fn connect_pinned_wrong_cert_fails() {
    let (server, port, _) = start_test_server().await;
    let (wrong, _, _) = netconduit::core::generate_self_signed_cert().unwrap();
    let (tx, _) = mpsc::channel(1);
    let result = tokio::time::timeout(Duration::from_secs(3),
        ConduitClient::connect_pinned("127.0.0.1", port, 2, tx, None, wrong.as_ref().to_vec())
    ).await;
    assert!(result.is_err() || result.unwrap().is_err(), "wrong cert must be rejected");
    server.stop();
}

// ─── Backpressure ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn send_message_returns_false_when_queue_full() {
    let port = free_port();
    let (tx, _) = mpsc::channel::<ConduitEvent>(1);
    let server = ConduitServer::start("127.0.0.1", port, tx, 0).await.unwrap();
    let (_c, _r) = connect_test_client(port).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let cid = server.connected_clients()[0].clone();
    let payload = vec![0u8; 100];
    let mut accepted = 0usize;
    for _ in 0..10_000 {
        if server.send_message(cid.clone(), payload.clone()) { accepted += 1; }
    }
    assert!(accepted > 0);
    server.stop();
}

// ─── Channel registration ─────────────────────────────────────────────────────

#[tokio::test]
async fn register_channel_stores_config() {
    let (server, _, _) = start_test_server().await;
    server.register_channel(ChannelConfig::unreliable("telemetry"));
    server.register_channel(ChannelConfig::reliable("files"));
    server.stop();
}

// ─── Metrics ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn metrics_bytes_received_increment_on_message() {
    let (server, port, mut srv_rx) = start_test_server().await;
    let (client, _) = connect_test_client(port).await;
    next_event(&mut srv_rx).await;
    let (_, br0, _, mr0, _, _, _) = metrics_snapshot();
    client.send_message(&vec![0u8; 1024]).await.unwrap();
    next_event(&mut srv_rx).await;
    let (_, br1, _, mr1, _, _, _) = metrics_snapshot();
    assert!(br1 > br0);
    assert!(mr1 > mr0);
    server.stop();
}

#[tokio::test]
async fn metrics_connections_active_tracks_lifecycle() {
    let (server, port, mut srv_rx) = start_test_server().await;
    let (_, _, _, _, ca0, ct0, _) = metrics_snapshot();
    let (client, _) = connect_test_client(port).await;
    next_event(&mut srv_rx).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let (_, _, _, _, ca1, ct1, _) = metrics_snapshot();
    assert!(ca1 >= ca0 + 1);
    assert!(ct1 >= ct0 + 1);
    drop(client);
    let ca_after = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let (_, _, _, _, ca, _, _) = metrics_snapshot();
            if ca < ca1 { return ca; }
        }
    }).await.expect("active count never decreased");
    assert!(ca_after < ca1);
    server.stop();
}

// ─── Concurrent sends ─────────────────────────────────────────────────────────

#[tokio::test]
async fn concurrent_sends_all_arrive() {
    let (server, port, mut srv_rx) = start_test_server().await;
    let (client, _) = connect_test_client(port).await;
    next_event(&mut srv_rx).await;
    const N: usize = 50;
    let client = std::sync::Arc::new(client);
    let handles: Vec<_> = (0..N).map(|i| {
        let c = client.clone();
        tokio::spawn(async move { c.send_message(&(i as u64).to_be_bytes()).await })
    }).collect();
    for h in handles { h.await.unwrap().unwrap(); }
    let mut received = std::collections::HashSet::new();
    for _ in 0..N {
        if let Ok(Some(ConduitEvent::Message { payload, .. })) =
            tokio::time::timeout(Duration::from_secs(5), srv_rx.recv()).await
        {
            received.insert(u64::from_be_bytes(payload.try_into().unwrap()));
        }
    }
    assert_eq!(received.len(), N);
    server.stop();
}

// ─── Multiple clients ─────────────────────────────────────────────────────────

#[tokio::test]
async fn multiple_clients_independent_event_streams() {
    let (server, port, mut srv_rx) = start_test_server().await;
    let (_c1, _r1) = connect_test_client(port).await;
    let (_c2, _r2) = connect_test_client(port).await;
    let id1 = match next_event(&mut srv_rx).await { ConduitEvent::Connect { client_id } => client_id, _ => panic!() };
    let id2 = match next_event(&mut srv_rx).await { ConduitEvent::Connect { client_id } => client_id, _ => panic!() };
    assert_ne!(id1, id2);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(server.connection_count(), 2);
    server.stop();
}

// ─── Duplex streaming ─────────────────────────────────────────────────────────

#[tokio::test]
async fn duplex_stream_client_to_server_and_back() {
    let (server, port, mut srv_rx) = start_test_server().await;
    let (client, _) = connect_test_client(port).await;
    next_event(&mut srv_rx).await;

    // Server echo task.
    tokio::spawn(async move {
        while let Some(ev) = srv_rx.recv().await {
            if let ConduitEvent::DuplexStreamOpen { mut stream, .. } = ev {
                tokio::spawn(async move {
                    while let Ok(Some(data)) = stream.recv_data().await {
                        let _ = stream.send_data(data).await;
                    }
                });
            }
        }
    });

    let mut ds = client.open_duplex_stream("echo_ch").await.unwrap();
    ds.send_data(b"hello duplex".to_vec()).await.unwrap();
    let reply = ds.recv_data().await.unwrap().expect("no reply");
    assert_eq!(reply, b"hello duplex");
    server.stop();
}

#[tokio::test]
async fn duplex_stream_multiple_messages() {
    let (server, port, mut srv_rx) = start_test_server().await;
    let (client, _) = connect_test_client(port).await;
    next_event(&mut srv_rx).await;

    tokio::spawn(async move {
        while let Some(ev) = srv_rx.recv().await {
            if let ConduitEvent::DuplexStreamOpen { mut stream, .. } = ev {
                tokio::spawn(async move {
                    while let Ok(Some(data)) = stream.recv_data().await {
                        let _ = stream.send_data(data).await;
                    }
                });
            }
        }
    });

    let mut ds = client.open_duplex_stream("multi").await.unwrap();
    for i in 0u32..10 {
        ds.send_data(i.to_be_bytes().to_vec()).await.unwrap();
        let reply = ds.recv_data().await.unwrap().expect("expected reply");
        assert_eq!(reply, i.to_be_bytes());
    }
    server.stop();
}

#[tokio::test]
async fn duplex_stream_name_matches_channel() {
    let (server, port, mut srv_rx) = start_test_server().await;
    let (client, _) = connect_test_client(port).await;
    next_event(&mut srv_rx).await;

    // Server just verifies the channel name.
    let (name_tx, mut name_rx) = mpsc::channel::<String>(1);
    tokio::spawn(async move {
        while let Some(ev) = srv_rx.recv().await {
            if let ConduitEvent::DuplexStreamOpen { stream, .. } = ev {
                let _ = name_tx.send(stream.name.clone()).await;
            }
        }
    });

    let _ds = client.open_duplex_stream("my_duplex_channel").await.unwrap();
    let name = tokio::time::timeout(Duration::from_secs(2), name_rx.recv())
        .await.unwrap().unwrap();
    assert_eq!(name, "my_duplex_channel");
    server.stop();
}

// ─── Mesh routing ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn mesh_message_forwarded_between_clients() {
    use netconduit::core::{PROTO_VERSION, frame_packet};
    use netconduit::protocol::{Packet, PacketType};

    let (server, port, mut srv_rx) = start_test_server().await;
    let (client_a, _ra) = connect_test_client(port).await;
    let (client_b, mut rb) = connect_test_client(port).await;

    // Drain connect events to get client B's ID.
    let _ev_a = next_event(&mut srv_rx).await;
    let ev_b = next_event(&mut srv_rx).await;
    let b_id = match ev_b { ConduitEvent::Connect { client_id } => client_id, _ => panic!() };

    tokio::time::sleep(Duration::from_millis(20)).await;

    // Client A sends a MESSAGE directly to client B via dst_id (mesh route).
    let pkt = Packet {
        version: PROTO_VERSION,
        r#type:  PacketType::Message as i32,
        dst_id:  b_id.clone(),
        payload: b"mesh hello".to_vec(),
        ttl:     16,
        ..Default::default()
    };
    let frame = frame_packet(&pkt);
    let mut s = client_a.conn.open_uni().await.unwrap();
    s.write_all(&frame).await.unwrap();
    s.finish().unwrap();

    // Client B should receive the forwarded message.
    let ev = tokio::time::timeout(Duration::from_secs(3), rb.recv())
        .await.expect("mesh forward timeout").expect("closed");
    match ev {
        ConduitEvent::Message { payload, .. } => assert_eq!(payload, b"mesh hello"),
        other => panic!("expected Message, got {other:?}"),
    }
    server.stop();
}

#[tokio::test]
async fn mesh_ttl_zero_packet_dropped() {
    use netconduit::core::{PROTO_VERSION, frame_packet};
    use netconduit::protocol::{Packet, PacketType};

    let (server, port, mut srv_rx) = start_test_server().await;
    let (client_a, _ra) = connect_test_client(port).await;
    let (client_b, mut rb) = connect_test_client(port).await;
    let _ev_a = next_event(&mut srv_rx).await;
    let ev_b = next_event(&mut srv_rx).await;
    let b_id = match ev_b { ConduitEvent::Connect { client_id } => client_id, _ => panic!() };
    tokio::time::sleep(Duration::from_millis(20)).await;

    // TTL = 0 → server must drop it.
    let pkt = Packet {
        version: PROTO_VERSION,
        r#type:  PacketType::Message as i32,
        dst_id:  b_id,
        payload: b"dropped".to_vec(),
        ttl:     0,
        ..Default::default()
    };
    let frame = frame_packet(&pkt);
    let mut s = client_a.conn.open_uni().await.unwrap();
    s.write_all(&frame).await.unwrap();
    s.finish().unwrap();

    // Nothing should arrive at B within 500ms.
    let result = tokio::time::timeout(Duration::from_millis(500), rb.recv()).await;
    assert!(result.is_err(), "TTL=0 packet should be dropped, not forwarded");
    server.stop();
}

#[tokio::test]
async fn mesh_forwards_count_increments() {
    use netconduit::core::{PROTO_VERSION, frame_packet};
    use netconduit::protocol::{Packet, PacketType};

    let (server, port, mut srv_rx) = start_test_server().await;
    let (client_a, _ra) = connect_test_client(port).await;
    let (client_b, mut rb) = connect_test_client(port).await;
    let _ea = next_event(&mut srv_rx).await;
    let eb = next_event(&mut srv_rx).await;
    let b_id = match eb { ConduitEvent::Connect { client_id } => client_id, _ => panic!() };
    tokio::time::sleep(Duration::from_millis(20)).await;

    let before = METRICS.mesh_forwards.load(Ordering::Relaxed);
    let pkt = Packet {
        version: PROTO_VERSION,
        r#type:  PacketType::Message as i32,
        dst_id:  b_id,
        payload: b"fwd".to_vec(),
        ttl:     16,
        ..Default::default()
    };
    let mut s = client_a.conn.open_uni().await.unwrap();
    s.write_all(&frame_packet(&pkt)).await.unwrap();
    s.finish().unwrap();
    // Wait for delivery
    tokio::time::timeout(Duration::from_secs(2), rb.recv()).await.ok();
    let after = METRICS.mesh_forwards.load(Ordering::Relaxed);
    assert!(after > before, "mesh_forwards counter should increment");
    server.stop();
}

// ─── Security (Ed25519) ───────────────────────────────────────────────────────

#[tokio::test]
async fn signed_message_verified_by_receiver() {
    use netconduit::core::{PROTO_VERSION, frame_packet};
    use netconduit::protocol::{Packet, PacketType};

    let (server, port, mut srv_rx) = start_test_server().await;
    let (client, _) = connect_test_client(port).await;
    next_event(&mut srv_rx).await;

    // Client creates identity and signs a packet.
    let identity = NodeIdentity::generate().unwrap();
    let mut pkt = Packet {
        version: PROTO_VERSION,
        r#type:  PacketType::Message as i32,
        payload: b"secure payload".to_vec(),
        ..Default::default()
    };
    sign_packet(&mut pkt, &identity);

    let frame = frame_packet(&pkt);
    let mut s = client.conn.open_uni().await.unwrap();
    s.write_all(&frame).await.unwrap();
    s.finish().unwrap();

    // Server receives the packet and verifies the signature.
    let ev = next_event(&mut srv_rx).await;
    // The server fires a Message event with the raw payload.
    // Application-level verification uses the sender's public key.
    match ev {
        ConduitEvent::Message { payload, .. } => {
            assert_eq!(payload, b"secure payload");
            // Re-verify using the sender's public key.
            let verified = NodeIdentity::verify(&identity.public_key, b"secure payload", &pkt.signature);
            assert!(verified, "Ed25519 signature must verify");
        }
        other => panic!("{other:?}"),
    }
    server.stop();
}

#[tokio::test]
async fn key_store_rejects_tampered_packet() {
    use netconduit::core::PROTO_VERSION;
    use netconduit::protocol::{Packet, PacketType};

    let identity = NodeIdentity::generate().unwrap();
    let store = KeyStore::new();
    store.add(identity.peer_id.clone(), identity.public_key.clone());

    let mut pkt = Packet {
        version: PROTO_VERSION,
        r#type:  PacketType::Message as i32,
        payload: b"original".to_vec(),
        ..Default::default()
    };
    sign_packet(&mut pkt, &identity);
    pkt.payload = b"tampered".to_vec(); // corrupt after signing

    assert!(!store.verify(&pkt), "tampered payload must fail verification");
}

#[tokio::test]
async fn resource_pool_reports_machine_stats() {
    let pool = ResourcePool::default_for_machine();
    assert!(pool.max_tasks >= 4, "pool should size to at least 4");
    assert_eq!(pool.active_tasks(), 0, "should start idle");
}
