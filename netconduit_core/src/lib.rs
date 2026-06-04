use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use std::sync::{Arc, Mutex};
use std::collections::{HashMap, BTreeMap};
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::runtime::Runtime;
use quinn::{Endpoint, ServerConfig, ClientConfig, Connection, VarInt};

pub mod protocol {
    include!(concat!(env!("OUT_DIR"), "/netconduit.rs"));
}

// ─── Byte Order ───────────────────────────────────────────────────────────────
// The framing layer (4-byte length prefix) is always big-endian.
// BYTE_ORDER_BE / BYTE_ORDER_LE describe the byte order of the *payload* content
// inside a Packet, for raw binary messages where the receiver needs to know.

/// Payload byte order: big-endian (network order, default).
pub const BYTE_ORDER_BE: u32 = 0;
/// Payload byte order: little-endian (x86/ARM native).
pub const BYTE_ORDER_LE: u32 = 1;

// ─── Codec identifiers (wire protocol) ────────────────────────────────────────

const CODEC_NONE: u8 = 0;
const CODEC_LZ4:  u8 = 1;
const CODEC_ZSTD: u8 = 2;

/// Zstd level decision threshold.
const THRESHOLD_ZSTD_FAST: usize = 4096;

// ─── Compression ──────────────────────────────────────────────────────────────

fn select_codec(payload: &[u8]) -> u8 {
    match payload.len() {
        0..=63   => CODEC_NONE,
        64..=255  => CODEC_LZ4,
        _         => CODEC_ZSTD,
    }
}

/// Compress bytes using the optimal codec for the payload size.
/// Wire format: [1B codec_id][compressed or raw payload]
#[pyfunction]
fn compress_payload<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    let codec = select_codec(data);
    let out = match codec {
        CODEC_NONE => {
            let mut out = Vec::with_capacity(1 + data.len());
            out.push(CODEC_NONE);
            out.extend_from_slice(data);
            out
        }
        CODEC_LZ4 => {
            let compressed = lz4_flex::compress_prepend_size(data);
            let mut out = Vec::with_capacity(1 + compressed.len());
            out.push(CODEC_LZ4);
            out.extend_from_slice(&compressed);
            out
        }
        _ => {
            let level = if data.len() >= THRESHOLD_ZSTD_FAST { 3 } else { 1 };
            let compressed = zstd::encode_all(data, level)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            let mut out = Vec::with_capacity(1 + compressed.len());
            out.push(CODEC_ZSTD);
            out.extend_from_slice(&compressed);
            out
        }
    };
    Ok(PyBytes::new_bound(py, &out))
}

/// Decompress a payload compressed by `compress_payload`.
#[pyfunction]
fn decompress_payload<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    if data.is_empty() {
        return Ok(PyBytes::new_bound(py, &[]));
    }
    let decompressed = match data[0] {
        CODEC_NONE => data[1..].to_vec(),
        CODEC_LZ4  => {
            lz4_flex::decompress_size_prepended(&data[1..])
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        }
        CODEC_ZSTD => {
            zstd::decode_all(&data[1..])
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
        }
        other => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("Unknown codec id: {}", other)
        )),
    };
    Ok(PyBytes::new_bound(py, &decompressed))
}

// ─── Byte-order aware integer helpers (exposed to Python) ─────────────────────

/// Pack a u64 as 8 bytes in the requested byte order (0=BE, 1=LE).
#[pyfunction]
fn pack_u64<'py>(py: Python<'py>, value: u64, byte_order: u32) -> Bound<'py, PyBytes> {
    let bytes = if byte_order == BYTE_ORDER_LE {
        value.to_le_bytes()
    } else {
        value.to_be_bytes()
    };
    PyBytes::new_bound(py, &bytes)
}

/// Pack a u32 as 4 bytes in the requested byte order.
#[pyfunction]
fn pack_u32<'py>(py: Python<'py>, value: u32, byte_order: u32) -> Bound<'py, PyBytes> {
    let bytes = if byte_order == BYTE_ORDER_LE {
        value.to_le_bytes()
    } else {
        value.to_be_bytes()
    };
    PyBytes::new_bound(py, &bytes)
}

/// Unpack a u64 from 8 bytes in the requested byte order.
#[pyfunction]
fn unpack_u64(data: &[u8], byte_order: u32) -> PyResult<u64> {
    if data.len() < 8 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Need 8 bytes"));
    }
    let arr: [u8; 8] = data[..8].try_into().unwrap();
    Ok(if byte_order == BYTE_ORDER_LE { u64::from_le_bytes(arr) } else { u64::from_be_bytes(arr) })
}

/// Unpack a u32 from 4 bytes in the requested byte order.
#[pyfunction]
fn unpack_u32(data: &[u8], byte_order: u32) -> PyResult<u32> {
    if data.len() < 4 {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Need 4 bytes"));
    }
    let arr: [u8; 4] = data[..4].try_into().unwrap();
    Ok(if byte_order == BYTE_ORDER_LE { u32::from_le_bytes(arr) } else { u32::from_be_bytes(arr) })
}

/// Return the native host byte order (0=BE, 1=LE).
#[pyfunction]
fn host_byte_order() -> u32 {
    if cfg!(target_endian = "little") { BYTE_ORDER_LE } else { BYTE_ORDER_BE }
}

// ─── Reorder Buffer ────────────────────────────────────────────────────────────
//
// Per-stream gap-aware buffer. Holds out-of-order packets in a BTreeMap keyed
// by sequence_id. `drain_ready()` returns a contiguous run starting from the
// next expected sequence, so the application always processes packets in order.
//
// Design:
//   - stream_id 0 means "unordered" — drain_ready() returns immediately
//   - Multiple streams are tracked independently inside one ReorderBuffer
//   - `max_gap`: if the gap ahead of the next expected sequence exceeds this,
//     the buffer skips to the next available packet (loss recovery / skip-ahead)

#[pyclass]
struct ReorderBuffer {
    // stream_id → (next_expected_seq, BTreeMap<seq, payload>)
    streams:  HashMap<u32, (u64, BTreeMap<u64, Vec<u8>>)>,
    max_gap:  u64,
    max_buf:  usize,
}

#[pymethods]
impl ReorderBuffer {
    #[new]
    #[pyo3(signature = (max_gap=64, max_buf=1024))]
    fn new(max_gap: u64, max_buf: usize) -> Self {
        ReorderBuffer {
            streams: HashMap::new(),
            max_gap,
            max_buf,
        }
    }

    /// Push a packet into the buffer.
    /// stream_id=0 → bypass ordering (always immediately drained).
    /// Returns True if the packet was accepted, False if the buffer is full.
    fn push(&mut self, stream_id: u32, sequence_id: u64, payload: Vec<u8>) -> bool {
        if stream_id == 0 {
            return true; // unordered — caller drains immediately via drain_unordered
        }
        let entry = self.streams.entry(stream_id).or_insert((0u64, BTreeMap::new()));
        let (next_expected, queue) = entry;

        // Already seen or too far behind — drop duplicate
        if sequence_id < *next_expected {
            return true;
        }
        if queue.len() >= self.max_buf {
            return false;
        }
        queue.insert(sequence_id, payload);
        true
    }

    /// Drain all packets that are ready (contiguous from next_expected).
    /// Returns list of (sequence_id, payload) tuples in order.
    /// If a gap is detected and the gap exceeds max_gap, skip-ahead to next known.
    fn drain_ready<'py>(&mut self, py: Python<'py>, stream_id: u32) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty_bound(py);
        let entry = self.streams.entry(stream_id).or_insert((0u64, BTreeMap::new()));
        let (next_expected, queue) = entry;

        loop {
            if let Some(payload) = queue.remove(next_expected) {
                let seq = *next_expected;
                *next_expected += 1;
                let item = pyo3::types::PyTuple::new_bound(
                    py,
                    &[seq.into_py(py).into_bound(py), PyBytes::new_bound(py, &payload).into_any()]
                );
                list.append(item)?;
            } else {
                // Gap detected — check if we should skip ahead
                if let Some((&first_available, _)) = queue.iter().next() {
                    if first_available > *next_expected + self.max_gap {
                        // Skip ahead to the next available packet
                        *next_expected = first_available;
                        continue;
                    }
                }
                break;
            }
        }
        Ok(list)
    }

    /// Get the next expected sequence_id for a stream.
    fn next_expected(&self, stream_id: u32) -> u64 {
        self.streams.get(&stream_id).map(|(n, _)| *n).unwrap_or(0)
    }

    /// Number of buffered (out-of-order) packets for a stream.
    fn buffered_count(&self, stream_id: u32) -> usize {
        self.streams.get(&stream_id).map(|(_, q)| q.len()).unwrap_or(0)
    }

    /// Reset a stream's state (e.g. on reconnect).
    fn reset_stream(&mut self, stream_id: u32) {
        self.streams.remove(&stream_id);
    }

    /// Reset all streams.
    fn reset_all(&mut self) {
        self.streams.clear();
    }

    /// List all active stream IDs.
    fn stream_ids<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty_bound(py);
        for id in self.streams.keys() {
            list.append(*id)?;
        }
        Ok(list)
    }
}

// ─── Sequence Counter ──────────────────────────────────────────────────────────
// Per-connection, per-stream monotonic sequence number generator.

#[pyclass]
struct SequenceCounter {
    counters: HashMap<u32, u64>, // stream_id → next_seq
}

#[pymethods]
impl SequenceCounter {
    #[new]
    fn new() -> Self {
        SequenceCounter { counters: HashMap::new() }
    }

    /// Get and increment the sequence number for a stream.
    fn next(&mut self, stream_id: u32) -> u64 {
        let counter = self.counters.entry(stream_id).or_insert(0);
        let seq = *counter;
        *counter += 1;
        seq
    }

    /// Peek at the next sequence number without incrementing.
    fn peek(&self, stream_id: u32) -> u64 {
        *self.counters.get(&stream_id).unwrap_or(&0)
    }

    /// Reset a single stream counter.
    fn reset_stream(&mut self, stream_id: u32) {
        self.counters.remove(&stream_id);
    }

    /// Reset all counters.
    fn reset_all(&mut self) {
        self.counters.clear();
    }
}

// ─── Blake3 Checksum ──────────────────────────────────────────────────────────

#[pyfunction]
fn compute_checksum(data: &[u8]) -> String {
    let hash = blake3::hash(data);
    hex::encode(&hash.as_bytes()[..16])
}

#[pyfunction]
fn verify_checksum(data: &[u8], expected: &str) -> bool {
    let hash = blake3::hash(data);
    hex::encode(&hash.as_bytes()[..16]) == expected
}

// ─── TLS Certificate (ED25519) ────────────────────────────────────────────────

#[derive(Debug)]
struct DummyVerifier;

impl rustls::client::danger::ServerCertVerifier for DummyVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(&self, _: &[u8], _: &rustls::pki_types::CertificateDer<'_>, _: &rustls::DigitallySignedStruct) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(&self, _: &[u8], _: &rustls::pki_types::CertificateDer<'_>, _: &rustls::DigitallySignedStruct) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
        ]
    }
}

fn generate_self_signed_cert() -> Result<(rustls::pki_types::CertificateDer<'static>, rustls::pki_types::PrivateKeyDer<'static>), anyhow::Error> {
    let params = rcgen::CertificateParams::new(vec![
        "localhost".to_string(), "127.0.0.1".to_string(), "::1".to_string(),
    ])?;
    let key_pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519)?;
    let cert = params.self_signed(&key_pair)?;
    Ok((
        rustls::pki_types::CertificateDer::from(cert.der().to_vec()),
        rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(key_pair.serialize_der()))
    ))
}

#[pyfunction]
fn generate_ed25519_cert_pem() -> PyResult<(String, String)> {
    let params = rcgen::CertificateParams::new(vec![
        "localhost".to_string(), "127.0.0.1".to_string(), "::1".to_string(),
    ]).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    let key_pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    let cert = params.self_signed(&key_pair)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    Ok((cert.pem(), key_pair.serialize_pem()))
}

// ─── Address Resolution ────────────────────────────────────────────────────────

fn resolve_addr(host: &str, port: u16) -> Result<SocketAddr, anyhow::Error> {
    let addr_str = if host.contains(':') && !host.starts_with('[') {
        format!("[{}]:{}", host, port)
    } else {
        format!("{}:{}", host, port)
    };
    if let Ok(addr) = addr_str.parse::<SocketAddr>() {
        return Ok(addr);
    }
    for addr in addr_str.to_socket_addrs()? {
        return Ok(addr);
    }
    Err(anyhow::anyhow!("Could not resolve: {}", addr_str))
}

// ─── UDP Hole Punching ────────────────────────────────────────────────────────

#[pyfunction]
fn stun_punch_hole(stun_server: String, local_port: u16, peer_addr: String) -> PyResult<String> {
    let addr: std::net::SocketAddr = format!("0.0.0.0:{}", local_port).parse()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid address: {}", e)))?;
    
    let domain = if addr.is_ipv6() { socket2::Domain::IPV6 } else { socket2::Domain::IPV4 };
    let sock = socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("Socket creation failed: {}", e)))?;
    
    sock.set_reuse_address(true)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("Set reuse address failed: {}", e)))?;
    
    #[cfg(not(windows))]
    sock.set_reuse_port(true)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("Set reuse port failed: {}", e)))?;
        
    sock.bind(&addr.into())
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("Socket bind failed to {}: {}", addr, e)))?;
        
    let socket: std::net::UdpSocket = sock.into();

    let mut mapped_addr: Option<SocketAddr> = None;
    
    if !stun_server.is_empty() {
        let mut request = [0u8; 20];
        request[0..2].copy_from_slice(&0x0001u16.to_be_bytes());
        request[4..8].copy_from_slice(&0x2112A442u32.to_be_bytes());
        for i in 8..20 { request[i] = (i as u8).wrapping_mul(17); }

        let stun_sock = if let Ok(addr) = stun_server.parse::<SocketAddr>() {
            addr
        } else {
            let (host, port) = if let Some(pos) = stun_server.rfind(':') {
                let (h, p_str) = stun_server.split_at(pos);
                let p = p_str[1..].parse::<u16>().unwrap_or(19302);
                (h, p)
            } else {
                (stun_server.as_str(), 19302)
            };
            resolve_addr(host, port)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Could not resolve STUN host: {}", e)))?
        };
        let _ = socket.send_to(&request, stun_sock);
        let _ = socket.set_read_timeout(Some(Duration::from_secs(2)));

        let mut buf = [0u8; 1024];
        if let Ok((len, _)) = socket.recv_from(&mut buf) {
            let mut idx = 20usize;
            while idx + 4 <= len {
                let attr_type = u16::from_be_bytes([buf[idx], buf[idx+1]]);
                let attr_len  = u16::from_be_bytes([buf[idx+2], buf[idx+3]]) as usize;
                idx += 4;
                if idx + attr_len > len { break; }

                if attr_type == 0x0020 && attr_len >= 8 {
                    let family = buf[idx + 1];
                    let port = u16::from_be_bytes([buf[idx+2], buf[idx+3]]) ^ 0x2112;
                    if family == 1 {
                        let mut ip = [0u8; 4];
                        ip.copy_from_slice(&buf[idx+4..idx+8]);
                        let xor = 0x2112A442u32.to_be_bytes();
                        for i in 0..4 { ip[i] ^= xor[i]; }
                        mapped_addr = Some(SocketAddr::new(std::net::IpAddr::V4(ip.into()), port));
                        break;
                    } else if family == 2 && attr_len >= 20 {
                        let mut ip = [0u8; 16];
                        ip.copy_from_slice(&buf[idx+4..idx+20]);
                        let xor = 0x2112A442u32.to_be_bytes();
                        for i in 0..4 { ip[i] ^= xor[i]; }
                        for i in 4..16 { ip[i] ^= request[8 + (i - 4)]; }
                        mapped_addr = Some(SocketAddr::new(std::net::IpAddr::V6(ip.into()), port));
                        break;
                    }
                }

                idx += attr_len + (if attr_len % 4 != 0 { 4 - attr_len % 4 } else { 0 });
            }
        }
    }

    if !peer_addr.is_empty() {
        if let Ok(peer) = peer_addr.parse::<SocketAddr>() {
            for _ in 0..5 {
                let _ = socket.send_to(b"NETCONDUIT_PUNCH\x00\x01", peer);
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }

    Ok(mapped_addr.map(|a| a.to_string()).unwrap_or_default())
}

// ─── Route Cache (sled-backed, session-bound) ─────────────────────────────────

#[pyclass]
struct RouteCache {
    db:   sled::Db,
    path: String,
}

#[pymethods]
impl RouteCache {
    #[new]
    fn new(path: String) -> PyResult<Self> {
        let db = sled::Config::new()
            .path(&path)
            .cache_capacity(64 * 1024 * 1024)
            .flush_every_ms(Some(500))
            .open()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("RouteCache open failed: {}", e)))?;
        db.clear().map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(RouteCache { db, path })
    }

    fn set_route(&self, destination: String, next_hop: String) -> PyResult<()> {
        let key = format!("r:{}", destination);
        self.db.insert(key.as_bytes(), next_hop.as_bytes())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(())
    }

    fn get_route(&self, destination: &str) -> PyResult<Option<String>> {
        let key = format!("r:{}", destination);
        Ok(self.db.get(key.as_bytes())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
            .map(|v| String::from_utf8_lossy(&v).to_string()))
    }

    fn remove_route(&self, destination: &str) -> PyResult<()> {
        let key = format!("r:{}", destination);
        self.db.remove(key.as_bytes())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(())
    }

    fn list_routes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty_bound(py);
        for item in self.db.scan_prefix(b"r:") {
            let (k, v) = item.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            let dest = String::from_utf8_lossy(&k[2..]).to_string();
            let hop  = String::from_utf8_lossy(&v).to_string();
            list.append(pyo3::types::PyTuple::new_bound(py, &[dest, hop]))?;
        }
        Ok(list)
    }

    fn route_count(&self) -> usize {
        self.db.scan_prefix(b"r:").count()
    }

    fn clear(&self) -> PyResult<()> {
        self.db.clear().map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(())
    }

    fn set_meta(&self, key: String, value: Vec<u8>) -> PyResult<()> {
        let k = format!("m:{}", key);
        self.db.insert(k.as_bytes(), value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(())
    }

    fn get_meta(&self, key: &str) -> PyResult<Option<Vec<u8>>> {
        let k = format!("m:{}", key);
        Ok(self.db.get(k.as_bytes())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
            .map(|v| v.to_vec()))
    }

    fn track_connection(&self, client_id: &str) -> PyResult<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let k = format!("c:{}", client_id);
        self.db.insert(k.as_bytes(), &now.to_be_bytes())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(())
    }

    fn untrack_connection(&self, client_id: &str) -> PyResult<()> {
        let k = format!("c:{}", client_id);
        self.db.remove(k.as_bytes())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(())
    }

    fn get_last_seen(&self, client_id: &str) -> PyResult<Option<u64>> {
        let k = format!("c:{}", client_id);
        Ok(self.db.get(k.as_bytes())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
            .and_then(|v| {
                if v.len() == 8 {
                    Some(u64::from_be_bytes([v[0],v[1],v[2],v[3],v[4],v[5],v[6],v[7]]))
                } else { None }
            }))
    }

    fn list_connections<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty_bound(py);
        for item in self.db.scan_prefix(b"c:") {
            let (k, _) = item.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            let id = String::from_utf8_lossy(&k[2..]).to_string();
            list.append(id)?;
        }
        Ok(list)
    }

    fn path(&self) -> &str { &self.path }

    fn flush(&self) -> PyResult<()> {
        self.db.flush().map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(())
    }
}

// ─── QUIC transport config ─────────────────────────────────────────────────────

fn make_transport_config(idle_secs: u64, keepalive_secs: u64) -> Arc<quinn::TransportConfig> {
    let mut t = quinn::TransportConfig::default();
    t.max_idle_timeout(Some(Duration::from_secs(idle_secs).try_into().unwrap()));
    t.keep_alive_interval(Some(Duration::from_secs(keepalive_secs)));
    t.max_concurrent_bidi_streams(VarInt::from_u32(1024));
    t.max_concurrent_uni_streams(VarInt::from_u32(1024));
    t.stream_receive_window(VarInt::from_u32(8 * 1024 * 1024));
    t.receive_window(VarInt::from_u32(32 * 1024 * 1024));
    t.send_window(8 * 1024 * 1024);
    Arc::new(t)
}

const READ_CHUNK:     usize = 65536;
const MAX_MSG_SIZE:   usize = 10 * 1024 * 1024;
const MAX_STREAM_SIZE: usize = 100 * 1024 * 1024;

// ─── QUIC Server ──────────────────────────────────────────────────────────────

#[pyclass]
struct RustQUICServer {
    rt:          Option<Runtime>,
    tx_stop:     Option<mpsc::Sender<()>>,
    tx_send:     Option<mpsc::Sender<(String, Vec<u8>)>>,
    connections: Arc<Mutex<HashMap<String, Connection>>>,
}

#[pymethods]
impl RustQUICServer {
    #[new]
    fn new() -> Self {
        RustQUICServer {
            rt: None, tx_stop: None, tx_send: None,
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn start(&mut self, py: Python<'_>, host: String, port: u16, callback: PyObject) -> PyResult<()> {
        let addr = resolve_addr(&host, port)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        let rt = Runtime::new()?;

        let (cert, key) = generate_self_signed_cert()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(e.to_string()))?;
        let mut server_crypto = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(e.to_string()))?;
        server_crypto.alpn_protocols = vec![b"netconduit".to_vec()];

        let quic_cfg = quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(e.to_string()))?;
        let mut server_cfg = ServerConfig::with_crypto(Arc::new(quic_cfg));
        server_cfg.transport_config(make_transport_config(60, 15));

        let endpoint = rt.block_on(async {
            Endpoint::server(server_cfg, addr)
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("Failed to bind server to {}: {}", addr, e)))?;

        let (tx_stop, mut rx_stop)   = mpsc::channel::<()>(1);
        let (tx_send, mut rx_send)   = mpsc::channel::<(String, Vec<u8>)>(4096);
        let (tx_event, mut rx_event) = mpsc::channel::<(String, String, Vec<u8>)>(8192);

        let connections  = self.connections.clone();
        let py_callback  = callback.clone_ref(py);

        rt.spawn(async move {
            println!("[Rust Server] QUIC Server listening on {}", addr);

            tokio::spawn(async move {
                while let Some((evt, cid, payload)) = rx_event.recv().await {
                    Python::with_gil(|py| {
                        if let Err(e) = py_callback.call1(py, (evt, cid, PyBytes::new_bound(py, &payload))) {
                            e.print(py);
                        }
                    });
                }
            });

            let conns_send = connections.clone();
            tokio::spawn(async move {
                while let Some((client_id, payload)) = rx_send.recv().await {
                    let conn = conns_send.lock().unwrap().get(&client_id).cloned();
                    if let Some(conn) = conn {
                        tokio::spawn(async move {
                            if let Ok(mut s) = conn.open_uni().await {
                                let _ = s.write_all(&payload).await;
                                let _ = s.finish();
                            }
                        });
                    }
                }
            });

            loop {
                tokio::select! {
                    incoming = endpoint.accept() => {
                        let Some(attempt) = incoming else { break };
                        let tx_event  = tx_event.clone();
                        let connections = connections.clone();

                        tokio::spawn(async move {
                            let Ok(conn) = attempt.await else { return };
                            let cid = conn.remote_address().to_string();
                            connections.lock().unwrap().insert(cid.clone(), conn.clone());
                            let _ = tx_event.send(("connect".into(), cid.clone(), vec![])).await;

                            loop {
                                tokio::select! {
                                    uni = conn.accept_uni() => match uni {
                                        Ok(mut recv) => {
                                            let tx  = tx_event.clone();
                                            let cid = cid.clone();
                                            tokio::spawn(async move {
                                                let mut buf = Vec::with_capacity(4096);
                                                loop {
                                                    match recv.read_chunk(READ_CHUNK, true).await {
                                                        Ok(Some(c)) => { buf.extend_from_slice(&c.bytes); if buf.len() > MAX_MSG_SIZE { return; } }
                                                        Ok(None)    => break,
                                                        Err(_)      => return,
                                                    }
                                                }
                                                if !buf.is_empty() { let _ = tx.send(("message".into(), cid, buf)).await; }
                                            });
                                        }
                                        Err(_) => break,
                                    },
                                    bi = conn.accept_bi() => match bi {
                                        Ok((_, mut recv)) => {
                                            let tx  = tx_event.clone();
                                            let cid = cid.clone();
                                            tokio::spawn(async move {
                                                let mut buf = Vec::with_capacity(65536);
                                                loop {
                                                    match recv.read_chunk(READ_CHUNK, true).await {
                                                        Ok(Some(c)) => { buf.extend_from_slice(&c.bytes); if buf.len() > MAX_STREAM_SIZE { return; } }
                                                        Ok(None)    => break,
                                                        Err(_)      => return,
                                                    }
                                                }
                                                if !buf.is_empty() { let _ = tx.send(("binary_stream".into(), cid, buf)).await; }
                                            });
                                        }
                                        Err(_) => break,
                                    },
                                }
                            }

                            connections.lock().unwrap().remove(&cid);
                            let _ = tx_event.send(("disconnect".into(), cid, vec![])).await;
                        });
                    }
                    _ = rx_stop.recv() => {
                        endpoint.close(VarInt::from_u32(0), b"shutdown");
                        break;
                    }
                }
            }
        });

        self.rt      = Some(rt);
        self.tx_stop = Some(tx_stop);
        self.tx_send = Some(tx_send);
        Ok(())
    }

    fn stop(&mut self) -> PyResult<()> {
        if let Some(tx) = self.tx_stop.take() { let _ = tx.blocking_send(()); }
        if let Some(rt) = self.rt.take() { rt.shutdown_timeout(Duration::from_millis(500)); }
        self.connections.lock().unwrap().clear();
        Ok(())
    }

    fn send_message(&self, client_id: String, payload: Vec<u8>) -> PyResult<()> {
        if let Some(tx) = &self.tx_send { let _ = tx.try_send((client_id, payload)); }
        Ok(())
    }

    fn relay_raw(&self, client_id: String, raw_payload: Vec<u8>) -> PyResult<()> {
        if let Some(tx) = &self.tx_send { let _ = tx.try_send((client_id, raw_payload)); }
        Ok(())
    }

    fn send_binary_stream(&self, client_id: String, stream_name: String, data: Vec<u8>) -> PyResult<()> {
        let conn = self.connections.lock().unwrap().get(&client_id).cloned();
        if let (Some(conn), Some(rt)) = (conn, &self.rt) {
            rt.block_on(async move {
                if let Ok((mut send, _)) = conn.open_bi().await {
                    let name = stream_name.as_bytes();
                    let _ = send.write_all(&(name.len() as u32).to_be_bytes()).await;
                    let _ = send.write_all(name).await;
                    let _ = send.write_all(&data).await;
                    let _ = send.finish();
                }
            });
        }
        Ok(())
    }

    fn connection_count(&self) -> usize {
        self.connections.lock().unwrap().len()
    }

    fn is_connected(&self, client_id: &str) -> bool {
        self.connections.lock().unwrap().contains_key(client_id)
    }

    fn connected_clients<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty_bound(py);
        for key in self.connections.lock().unwrap().keys() {
            list.append(key.clone())?;
        }
        Ok(list)
    }
}

// ─── QUIC Client ──────────────────────────────────────────────────────────────

#[pyclass]
struct RustQUICClient {
    rt:      Option<Runtime>,
    conn:    Option<Connection>,
    tx_stop: Option<mpsc::Sender<()>>,
}

#[pymethods]
impl RustQUICClient {
    #[new]
    fn new() -> Self {
        RustQUICClient { rt: None, conn: None, tx_stop: None }
    }

    #[pyo3(signature = (host, port, connect_timeout_secs, callback, local_port=None))]
    fn connect(
        &mut self,
        py: Python<'_>,
        host: String,
        port: u16,
        connect_timeout_secs: u64,
        callback: PyObject,
        local_port: Option<u16>,
    ) -> PyResult<bool> {
        let peer_addr = resolve_addr(&host, port)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

        let rt = Runtime::new()?;
        let (tx_stop, mut rx_stop) = mpsc::channel::<()>(1);

        let client_res: Result<Connection, anyhow::Error> = rt.block_on(async {
            let bind_port = local_port.unwrap_or(0);
            let local = if peer_addr.is_ipv6() {
                format!("[::]:{}", bind_port).parse()?
            } else {
                format!("0.0.0.0:{}", bind_port).parse()?
            };

            let mut crypto = rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth();
            crypto.dangerous().set_certificate_verifier(Arc::new(DummyVerifier));
            crypto.alpn_protocols = vec![b"netconduit".to_vec()];

            let quic_cfg = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?;
            let mut client_cfg = ClientConfig::new(Arc::new(quic_cfg));
            client_cfg.transport_config(make_transport_config(60, 15));

            let mut endpoint = Endpoint::client(local)?;
            endpoint.set_default_client_config(client_cfg);

            let conn = tokio::time::timeout(
                Duration::from_secs(connect_timeout_secs),
                endpoint.connect(peer_addr, "localhost")?
            ).await??;
            Ok(conn)
        });

        match client_res {
            Ok(conn) => {
                let conn_clone = conn.clone();
                let py_callback = callback.clone_ref(py);

                rt.spawn(async move {
                    let (tx_event, mut rx_event) = mpsc::channel::<(String, String, Vec<u8>)>(8192);
                    tokio::spawn(async move {
                        while let Some((evt, cid, payload)) = rx_event.recv().await {
                            Python::with_gil(|py| {
                                if let Err(e) = py_callback.call1(py, (evt, cid, PyBytes::new_bound(py, &payload))) {
                                    e.print(py);
                                }
                            });
                        }
                    });

                    loop {
                        tokio::select! {
                            uni = conn_clone.accept_uni() => match uni {
                                Ok(mut recv) => {
                                    let tx = tx_event.clone();
                                    tokio::spawn(async move {
                                        let mut buf = Vec::with_capacity(4096);
                                        loop {
                                            match recv.read_chunk(READ_CHUNK, true).await {
                                                Ok(Some(c)) => { buf.extend_from_slice(&c.bytes); if buf.len() > MAX_MSG_SIZE { return; } }
                                                Ok(None)    => break,
                                                Err(_)      => return,
                                            }
                                        }
                                        if !buf.is_empty() { let _ = tx.send(("message".into(), "".into(), buf)).await; }
                                    });
                                }
                                Err(_) => break,
                            },
                            bi = conn_clone.accept_bi() => match bi {
                                Ok((_, mut recv)) => {
                                    let tx = tx_event.clone();
                                    tokio::spawn(async move {
                                        let mut buf = Vec::with_capacity(65536);
                                        loop {
                                            match recv.read_chunk(READ_CHUNK, true).await {
                                                Ok(Some(c)) => { buf.extend_from_slice(&c.bytes); if buf.len() > MAX_STREAM_SIZE { return; } }
                                                Ok(None)    => break,
                                                Err(_)      => return,
                                            }
                                        }
                                        if !buf.is_empty() { let _ = tx.send(("binary_stream".into(), "".into(), buf)).await; }
                                    });
                                }
                                Err(_) => break,
                            },
                            _ = rx_stop.recv() => {
                                conn_clone.close(VarInt::from_u32(0), b"disconnect");
                                break;
                            }
                        }
                    }
                    let _ = tx_event.send(("disconnect".into(), "".into(), vec![])).await;
                });

                self.rt      = Some(rt);
                self.conn    = Some(conn);
                self.tx_stop = Some(tx_stop);
                Ok(true)
            }
            Err(e) => {
                eprintln!("[Rust Client] QUIC Connection failed: {}", e);
                Ok(false)
            }
        }
    }

    fn disconnect(&mut self) -> PyResult<()> {
        if let Some(tx) = self.tx_stop.take() { let _ = tx.blocking_send(()); }
        self.conn.take();
        if let Some(rt) = self.rt.take() { rt.shutdown_timeout(Duration::from_millis(200)); }
        Ok(())
    }

    fn send_message(&self, payload: Vec<u8>) -> PyResult<()> {
        if let (Some(conn), Some(rt)) = (&self.conn, &self.rt) {
            let conn = conn.clone();
            rt.block_on(async move {
                if let Ok(mut s) = conn.open_uni().await {
                    let _ = s.write_all(&payload).await;
                    let _ = s.finish();
                }
            });
        }
        Ok(())
    }

    fn send_binary_stream(&self, stream_name: String, data: Vec<u8>) -> PyResult<()> {
        if let (Some(conn), Some(rt)) = (&self.conn, &self.rt) {
            let conn = conn.clone();
            rt.block_on(async move {
                if let Ok((mut send, _)) = conn.open_bi().await {
                    let name = stream_name.as_bytes();
                    let _ = send.write_all(&(name.len() as u32).to_be_bytes()).await;
                    let _ = send.write_all(name).await;
                    let _ = send.write_all(&data).await;
                    let _ = send.finish();
                }
            });
        }
        Ok(())
    }

    fn is_alive(&self) -> bool {
        self.conn.as_ref().map(|c| c.close_reason().is_none()).unwrap_or(false)
    }

    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new_bound(py);
        if let Some(conn) = &self.conn {
            let s = conn.stats();
            d.set_item("udp_tx_datagrams", s.udp_tx.datagrams)?;
            d.set_item("udp_rx_datagrams", s.udp_rx.datagrams)?;
            d.set_item("udp_tx_bytes",     s.udp_tx.bytes)?;
            d.set_item("udp_rx_bytes",     s.udp_rx.bytes)?;
        }
        Ok(d)
    }
}

// ─── Module Registration ───────────────────────────────────────────────────────

#[pymodule]
fn netconduit_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Classes
    m.add_class::<RustQUICServer>()?;
    m.add_class::<RustQUICClient>()?;
    m.add_class::<RouteCache>()?;
    m.add_class::<ReorderBuffer>()?;
    m.add_class::<SequenceCounter>()?;

    // Byte-order constants
    m.add("BYTE_ORDER_BE", BYTE_ORDER_BE)?;
    m.add("BYTE_ORDER_LE", BYTE_ORDER_LE)?;

    // Functions
    m.add_function(wrap_pyfunction!(stun_punch_hole, m)?)?;
    m.add_function(wrap_pyfunction!(generate_ed25519_cert_pem, m)?)?;
    m.add_function(wrap_pyfunction!(compute_checksum, m)?)?;
    m.add_function(wrap_pyfunction!(verify_checksum, m)?)?;
    m.add_function(wrap_pyfunction!(compress_payload, m)?)?;
    m.add_function(wrap_pyfunction!(decompress_payload, m)?)?;
    m.add_function(wrap_pyfunction!(pack_u64, m)?)?;
    m.add_function(wrap_pyfunction!(pack_u32, m)?)?;
    m.add_function(wrap_pyfunction!(unpack_u64, m)?)?;
    m.add_function(wrap_pyfunction!(unpack_u32, m)?)?;
    m.add_function(wrap_pyfunction!(host_byte_order, m)?)?;
    Ok(())
}
