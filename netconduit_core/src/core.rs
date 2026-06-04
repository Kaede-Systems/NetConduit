use std::sync::{Arc, RwLock};
use std::collections::{HashMap, BTreeMap};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Semaphore, OwnedSemaphorePermit};
use tokio::task::JoinSet;
use quinn::{Endpoint, ServerConfig, ClientConfig, Connection, VarInt};
use prost::Message as ProstMessage;
use crate::protocol::{Packet, PacketType};

// ─── Byte Order & Codec Constants ──────────────────────────────────────────────

pub const BYTE_ORDER_BE: u32 = 0;
pub const BYTE_ORDER_LE: u32 = 1;

pub const CODEC_NONE: u8 = 0;
pub const CODEC_LZ4:  u8 = 1;
pub const CODEC_ZSTD: u8 = 2;

const THRESHOLD_ZSTD_FAST: usize = 4096;

pub const MAX_MSG_SIZE:      usize = 10 * 1024 * 1024;
pub const MAX_STREAM_SIZE:   usize = 100 * 1024 * 1024;
pub const MAX_CONNECTIONS:   usize = 1024;

/// Protocol version encoded in every Packet.version field.
pub const PROTO_VERSION: u32 = 1;

/// 5-byte frame prefix on every QUIC stream and datagram.
/// Encodes the magic + version byte — receivers reject non-matching prefixes.
pub const PROTO_MAGIC: [u8; 5] = *b"NCON\x01";

// ─── Packet flags ─────────────────────────────────────────────────────────────

pub const FLAG_COMPRESS_LZ4:  u32 = 0x01;
pub const FLAG_COMPRESS_ZSTD: u32 = 0x02;
pub const FLAG_UNRELIABLE:    u32 = 0x04;
pub const FLAG_HAS_CHECKSUM:  u32 = 0x08;
pub const FLAG_LITTLE_ENDIAN: u32 = 0x10;
/// Marks a bi-stream as long-lived duplex (both sides can keep sending).
pub const FLAG_DUPLEX:        u32 = 0x20;
/// Packet carries an Ed25519 signature in `Packet.signature`.
pub const FLAG_SIGNED:        u32 = 0x40;

/// Default mesh TTL — packets are forwarded at most this many hops.
pub const MESH_DEFAULT_TTL: u32 = 16;

// ─── Channel system ────────────────────────────────────────────────────────────

/// Reliability/ordering guarantee for a named logical channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChannelMode {
    /// QUIC bi-stream — ordered, reliable.  Default for binary streams.
    ReliableOrdered,
    /// QUIC uni-stream — reliable but no ordering guarantee across messages.
    ReliableUnordered,
    /// QUIC datagram — fire-and-forget, lowest latency, no ordering/delivery guarantee.
    Unreliable,
}

/// Per-channel compression policy, overriding the global auto-selection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompressionPolicy {
    /// Choose codec by payload size (none < 64 B, lz4 < 256 B, zstd >= 256 B).
    Auto,
    /// Never compress. Use when data is already compressed (images, video).
    Never,
    /// Always compress, even for tiny payloads.
    Always,
}

/// Configuration for a named logical channel.
#[derive(Clone, Debug)]
pub struct ChannelConfig {
    pub name: String,
    pub mode: ChannelMode,
    pub compression: CompressionPolicy,
}

impl ChannelConfig {
    pub fn reliable(name: impl Into<String>) -> Self {
        Self { name: name.into(), mode: ChannelMode::ReliableOrdered, compression: CompressionPolicy::Auto }
    }
    pub fn unreliable(name: impl Into<String>) -> Self {
        Self { name: name.into(), mode: ChannelMode::Unreliable, compression: CompressionPolicy::Never }
    }
    pub fn unordered(name: impl Into<String>) -> Self {
        Self { name: name.into(), mode: ChannelMode::ReliableUnordered, compression: CompressionPolicy::Auto }
    }
}

// ─── Global atomic metrics ────────────────────────────────────────────────────

pub struct ConduitMetrics {
    pub bytes_sent:           AtomicU64,
    pub bytes_received:       AtomicU64,
    pub messages_sent:        AtomicU64,
    pub messages_received:    AtomicU64,
    pub connections_active:   AtomicU64,
    pub connections_total:    AtomicU64,
    pub connections_rejected: AtomicU64,
    /// Messages shed when the ResourcePool was at capacity.
    pub messages_dropped:     AtomicU64,
    /// Mesh routing forwards (packet relayed to another peer).
    pub mesh_forwards:        AtomicU64,
}

impl ConduitMetrics {
    const fn new() -> Self {
        Self {
            bytes_sent:           AtomicU64::new(0),
            bytes_received:       AtomicU64::new(0),
            messages_sent:        AtomicU64::new(0),
            messages_received:    AtomicU64::new(0),
            connections_active:   AtomicU64::new(0),
            connections_total:    AtomicU64::new(0),
            connections_rejected: AtomicU64::new(0),
            messages_dropped:     AtomicU64::new(0),
            mesh_forwards:        AtomicU64::new(0),
        }
    }
}

/// Process-wide metrics for all NetConduit connections.
pub static METRICS: ConduitMetrics = ConduitMetrics::new();

/// Snapshot all metrics atomically (Relaxed ordering — counters only).
pub fn metrics_snapshot() -> (u64, u64, u64, u64, u64, u64, u64) {
    (
        METRICS.bytes_sent.load(Ordering::Relaxed),
        METRICS.bytes_received.load(Ordering::Relaxed),
        METRICS.messages_sent.load(Ordering::Relaxed),
        METRICS.messages_received.load(Ordering::Relaxed),
        METRICS.connections_active.load(Ordering::Relaxed),
        METRICS.connections_total.load(Ordering::Relaxed),
        METRICS.connections_rejected.load(Ordering::Relaxed),
    )
}

// ─── Resource Pool ────────────────────────────────────────────────────────────

/// Bounded concurrent task pool for all stream/message processing.
///
/// Instead of spawning one unbounded task per incoming stream, every processing
/// task acquires a permit from this pool. When the pool is at capacity, new
/// non-critical work is shed (tracked via `METRICS.messages_dropped`).
///
/// Default size: `min(cpu_count × 64, 4096)` — handles large burst workloads
/// while capping memory and scheduling overhead.
pub struct ResourcePool {
    semaphore: Arc<Semaphore>,
    pub max_tasks: usize,
}

impl ResourcePool {
    pub fn new(max_tasks: usize) -> Arc<Self> {
        Arc::new(Self {
            semaphore: Arc::new(Semaphore::new(max_tasks)),
            max_tasks,
        })
    }

    /// Auto-size for this machine (cpu_count × 64, capped at 4096).
    pub fn default_for_machine() -> Arc<Self> {
        let cpus = std::thread::available_parallelism().map(|p| p.get()).unwrap_or(4);
        Self::new((cpus * 64).min(4096))
    }

    /// Acquire a permit, waiting if the pool is at capacity.
    /// Use for critical streams where dropping is unacceptable.
    pub async fn acquire(&self) -> OwnedSemaphorePermit {
        self.semaphore.clone().acquire_owned().await
            .expect("semaphore closed — this is a bug")
    }

    /// Try to acquire without blocking. Returns `None` and increments
    /// `METRICS.messages_dropped` when at capacity (load-shedding path).
    pub fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        match self.semaphore.clone().try_acquire_owned() {
            Ok(p) => Some(p),
            Err(_) => {
                METRICS.messages_dropped.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Current number of free slots.
    pub fn available(&self) -> usize { self.semaphore.available_permits() }

    /// Current number of active tasks.
    pub fn active_tasks(&self) -> usize { self.max_tasks.saturating_sub(self.available()) }
}

// ─── Protobuf frame helpers ───────────────────────────────────────────────────

/// Current unix milliseconds — embedded in every Packet.timestamp.
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn flags_to_codec(flags: u32) -> u8 {
    if flags & FLAG_COMPRESS_ZSTD != 0 { CODEC_ZSTD }
    else if flags & FLAG_COMPRESS_LZ4 != 0 { CODEC_LZ4 }
    else { CODEC_NONE }
}

/// Encode a Packet to a length-prefixed frame ready to write to a QUIC stream.
/// Format: PROTO_MAGIC[5] + pkt_len_u32_be[4] + Packet[pkt_len]
///
/// Uses `encoded_len()` + direct encode to avoid the extra allocation that
/// `encode_to_vec()` + `Vec::extend` would require.
pub fn frame_packet(pkt: &Packet) -> Vec<u8> {
    let pkt_len = pkt.encoded_len();
    let mut frame = Vec::with_capacity(PROTO_MAGIC.len() + 4 + pkt_len);
    frame.extend_from_slice(&PROTO_MAGIC);
    frame.extend_from_slice(&(pkt_len as u32).to_be_bytes());
    // Encode directly into the pre-allocated frame buffer — no intermediate Vec.
    pkt.encode(&mut frame).expect("prost encode into Vec never fails");
    frame
}

/// Decode a Packet from a length-prefixed stream buffer.
/// Validates PROTO_MAGIC prefix and version field.
pub fn decode_stream_frame(buf: &[u8]) -> anyhow::Result<Packet> {
    let hdr = PROTO_MAGIC.len() + 4;
    if buf.len() < hdr {
        anyhow::bail!("frame too short: {} bytes", buf.len());
    }
    if &buf[..PROTO_MAGIC.len()] != &PROTO_MAGIC {
        anyhow::bail!("bad magic");
    }
    let len = u32::from_be_bytes([buf[5], buf[6], buf[7], buf[8]]) as usize;
    if buf.len() < hdr + len {
        anyhow::bail!("frame truncated: need {} bytes, got {}", hdr + len, buf.len());
    }
    let pkt = Packet::decode(&buf[hdr..hdr + len])?;
    if pkt.version != PROTO_VERSION {
        anyhow::bail!("version mismatch: got {}, expected {}", pkt.version, PROTO_VERSION);
    }
    Ok(pkt)
}

/// Decode a Packet from a QUIC datagram buffer (no length prefix).
/// Validates PROTO_MAGIC prefix and version field.
pub fn decode_datagram_frame(buf: &[u8]) -> anyhow::Result<Packet> {
    if buf.len() < PROTO_MAGIC.len() {
        anyhow::bail!("datagram too short");
    }
    if &buf[..PROTO_MAGIC.len()] != &PROTO_MAGIC {
        anyhow::bail!("bad magic");
    }
    let pkt = Packet::decode(&buf[PROTO_MAGIC.len()..])?;
    if pkt.version != PROTO_VERSION {
        anyhow::bail!("version mismatch: got {}, expected {}", pkt.version, PROTO_VERSION);
    }
    Ok(pkt)
}

/// Read one length-prefixed Packet from a QUIC RecvStream (no magic check).
/// Used for: opening handshake of bi-streams (after magic) and subsequent
/// duplex messages (no magic on continuation frames).
///
/// Returns `Ok(None)` when the stream was cleanly closed by the remote side.
pub async fn read_framed_packet(
    recv: &mut quinn::RecvStream,
    max_size: usize,
) -> anyhow::Result<Option<Packet>> {
    let mut len_buf = [0u8; 4];
    match recv.read_exact(&mut len_buf).await {
        Ok(()) => {}
        Err(quinn::ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(quinn::ReadExactError::ReadError(e)) => return Err(e.into()),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len == 0 { return Ok(None); }
    if len > max_size {
        anyhow::bail!("packet too large: {} bytes (max {})", len, max_size);
    }
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf).await.map_err(|e| match e {
        quinn::ReadExactError::FinishedEarly(_) => anyhow::anyhow!("stream ended mid-packet"),
        quinn::ReadExactError::ReadError(e)     => e.into(),
    })?;
    let pkt = Packet::decode(&buf[..])?;
    if pkt.version != PROTO_VERSION {
        anyhow::bail!("version mismatch: got {}, expected {}", pkt.version, PROTO_VERSION);
    }
    Ok(Some(pkt))
}

/// Decompress a Packet payload using the codec indicated by its flags.
pub fn decompress_payload(flags: u32, data: &[u8]) -> anyhow::Result<Vec<u8>> {
    match flags_to_codec(flags) {
        CODEC_NONE => Ok(data.to_vec()),
        CODEC_LZ4  => {
            lz4_flex::decompress_size_prepended(data)
                .map_err(|e| anyhow::anyhow!("LZ4 decompression failed: {}", e))
        }
        CODEC_ZSTD => {
            zstd::decode_all(data)
                .map_err(|e| anyhow::anyhow!("Zstd decompression failed: {}", e))
        }
        other => anyhow::bail!("unknown codec id: {}", other),
    }
}

/// Compress payload for embedding in a Packet. Returns (flags, compressed_bytes).
/// Offloads zstd (CPU-bound) to a blocking thread pool.

// Multi-threaded zstd only worthwhile at 32 MB+.
// Below that, MT job dispatch overhead exceeds the parallelism gain.
const ZSTD_MT_THRESHOLD: usize = 32 * 1024 * 1024;

// Entropy sample size for the compressibility check.
const ENTROPY_SAMPLE: usize = 4096;

/// Fast compressibility probe: sample up to 4 KB with lz4 (µs overhead).
/// Returns false for random/encrypted/pre-compressed data so we skip the
/// full zstd pass and send raw — avoids ~20 ms wasted on 10 MB random data.
fn is_likely_compressible(data: &[u8]) -> bool {
    let n = data.len().min(ENTROPY_SAMPLE);
    if n < 64 { return true; }
    let compressed = lz4_flex::compress_prepend_size(&data[..n]);
    // If sample doesn't compress to < 92% of original, data is probably incompressible.
    compressed.len() < (n as f64 * 0.92) as usize
}

pub(crate) async fn compress_for_packet(
    data: Vec<u8>,
    policy: CompressionPolicy,
) -> anyhow::Result<(u32, Vec<u8>)> {
    match policy {
        CompressionPolicy::Never => Ok((0, data)),
        CompressionPolicy::Auto | CompressionPolicy::Always => {
            let codec = select_codec(&data);
            match codec {
                CODEC_NONE => Ok((0, data)),
                CODEC_LZ4 => {
                    let compressed = lz4_flex::compress_prepend_size(&data);
                    if compressed.len() < data.len() {
                        Ok((FLAG_COMPRESS_LZ4, compressed))
                    } else {
                        Ok((0, data))
                    }
                }
                _ => {
                    // Entropy check: bail out early for incompressible data.
                    // Saves ~20 ms compress + decompress-to-recover on 10 MB random input.
                    if policy == CompressionPolicy::Auto && !is_likely_compressible(&data) {
                        return Ok((0, data));
                    }

                    let orig_len = data.len();
                    // Level 1 = ~400 MB/s, level 3 = ~200 MB/s with only ~5% better ratio.
                    // For QUIC transport where bandwidth >> CPU, speed wins.
                    let level = 1i32;
                    let use_mt = orig_len >= ZSTD_MT_THRESHOLD;

                    // Arc: keep original alive so if compressed > original we return
                    // the original directly — no expensive decompress-to-recover step.
                    let data_arc = Arc::new(data);
                    let data_for_compress = Arc::clone(&data_arc);

                    let compressed = tokio::task::spawn_blocking(move || {
                        if use_mt {
                            // Multi-threaded zstd for very large payloads (≥ 32 MB).
                            let threads = std::thread::available_parallelism()
                                .map(|p| p.get().min(8) as u32)
                                .unwrap_or(4);
                            let mut enc = zstd::Encoder::new(Vec::new(), level)?;
                            enc.multithread(threads).map_err(anyhow::Error::from)?;
                            use std::io::Write as _;
                            enc.write_all(&*data_for_compress).map_err(anyhow::Error::from)?;
                            enc.finish().map_err(anyhow::Error::from)
                        } else {
                            zstd::encode_all(&data_for_compress[..], level)
                                .map_err(anyhow::Error::from)
                        }
                    })
                    .await
                    .map_err(|e| anyhow::anyhow!("spawn_blocking panicked: {e}"))??;

                    if compressed.len() < orig_len {
                        Ok((FLAG_COMPRESS_ZSTD, compressed))
                    } else {
                        // Compressed expanded (edge case after entropy check).
                        // Arc::try_unwrap succeeds here — the spawn_blocking clone was dropped.
                        let raw = Arc::try_unwrap(data_arc)
                            .unwrap_or_else(|a| (*a).clone());
                        Ok((0, raw))
                    }
                }
            }
        }
    }
}

/// Build a complete stream frame for a MESSAGE packet. Shared by client and API layer.
/// Uses `encoded_len()` to pre-size the buffer — single allocation, no intermediate Vec.
pub async fn make_message_frame(payload: Vec<u8>) -> anyhow::Result<Vec<u8>> {
    let (flags, body) = compress_for_packet(payload, CompressionPolicy::Auto).await?;
    let pkt = Packet {
        version: PROTO_VERSION,
        r#type: PacketType::Message as i32,
        flags,
        timestamp: now_ms(),
        payload: body,
        ..Default::default()
    };
    let pkt_len = pkt.encoded_len();
    let mut frame = Vec::with_capacity(PROTO_MAGIC.len() + 4 + pkt_len);
    frame.extend_from_slice(&PROTO_MAGIC);
    frame.extend_from_slice(&(pkt_len as u32).to_be_bytes());
    pkt.encode(&mut frame).expect("prost encode into Vec never fails");
    Ok(frame)
}

// ─── Async compression (legacy, codec-prefixed) ────────────────────────────────
// Used by compress/decompress_bytes public API; kept for Python/Flutter consumers.

/// Compress bytes returning [codec_byte][compressed_data]. Offloads zstd to blocking pool.
pub async fn compress_bytes_async(data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
    let codec = select_codec(&data);
    match codec {
        CODEC_NONE => {
            let mut out = Vec::with_capacity(1 + data.len());
            out.push(CODEC_NONE);
            out.extend_from_slice(&data);
            Ok(out)
        }
        CODEC_LZ4 => {
            let compressed = lz4_flex::compress_prepend_size(&data);
            let mut out = Vec::with_capacity(1 + compressed.len());
            out.push(CODEC_LZ4);
            out.extend_from_slice(&compressed);
            Ok(out)
        }
        _ => {
            tokio::task::spawn_blocking(move || {
                let level = if data.len() >= THRESHOLD_ZSTD_FAST { 3 } else { 1 };
                let compressed = zstd::encode_all(&data[..], level)?;
                let mut out = Vec::with_capacity(1 + compressed.len());
                out.push(CODEC_ZSTD);
                out.extend_from_slice(&compressed);
                Ok::<Vec<u8>, anyhow::Error>(out)
            })
            .await
            .map_err(|e| anyhow::anyhow!("spawn_blocking panicked: {e}"))?
        }
    }
}

/// Channel-aware binary stream framing using protobuf.
/// Builds STREAM_DATA Packet, compresses payload per policy, writes length-prefixed frame.
pub async fn write_framed_stream(
    send: &mut quinn::SendStream,
    stream_name: &str,
    data: Vec<u8>,
    policy: CompressionPolicy,
) -> anyhow::Result<()> {
    let (flags, payload) = compress_for_packet(data, policy).await?;
    let pkt = Packet {
        version: PROTO_VERSION,
        r#type: PacketType::StreamData as i32,
        flags,
        channel_name: stream_name.to_string(),
        timestamp: now_ms(),
        payload,
        ..Default::default()
    };
    let pkt_len = pkt.encoded_len();
    let mut frame = Vec::with_capacity(PROTO_MAGIC.len() + 4 + pkt_len);
    frame.extend_from_slice(&PROTO_MAGIC);
    frame.extend_from_slice(&(pkt_len as u32).to_be_bytes());
    pkt.encode(&mut frame).expect("prost encode into Vec never fails");
    let n = frame.len() as u64;
    send.write_all(&frame).await?;
    send.finish()?;
    METRICS.bytes_sent.fetch_add(n, Ordering::Relaxed);
    METRICS.messages_sent.fetch_add(1, Ordering::Relaxed);
    Ok(())
}

// ─── Compression logic ─────────────────────────────────────────────────────────

pub fn select_codec(payload: &[u8]) -> u8 {
    match payload.len() {
        0..=63   => CODEC_NONE,
        64..=255  => CODEC_LZ4,
        _         => CODEC_ZSTD,
    }
}

pub fn compress_bytes(data: &[u8]) -> Result<Vec<u8>, anyhow::Error> {
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
            let compressed = zstd::encode_all(data, level)?;
            let mut out = Vec::with_capacity(1 + compressed.len());
            out.push(CODEC_ZSTD);
            out.extend_from_slice(&compressed);
            out
        }
    };
    Ok(out)
}

pub fn decompress_bytes(data: &[u8]) -> Result<Vec<u8>, anyhow::Error> {
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let decompressed = match data[0] {
        CODEC_NONE => data[1..].to_vec(),
        CODEC_LZ4  => {
            lz4_flex::decompress_size_prepended(&data[1..])
                .map_err(|e| anyhow::anyhow!("LZ4 decompression failed: {}", e))?
        }
        CODEC_ZSTD => {
            zstd::decode_all(&data[1..])
                .map_err(|e| anyhow::anyhow!("Zstd decompression failed: {}", e))?
        }
        other => anyhow::bail!("Unknown codec id: {}", other),
    };
    Ok(decompressed)
}

// ─── Byte-order pack/unpack helpers ───────────────────────────────────────────

pub fn pack_u64_bytes(value: u64, byte_order: u32) -> [u8; 8] {
    if byte_order == BYTE_ORDER_LE { value.to_le_bytes() } else { value.to_be_bytes() }
}

pub fn pack_u32_bytes(value: u32, byte_order: u32) -> [u8; 4] {
    if byte_order == BYTE_ORDER_LE { value.to_le_bytes() } else { value.to_be_bytes() }
}

pub fn unpack_u64_bytes(data: &[u8], byte_order: u32) -> Result<u64, anyhow::Error> {
    if data.len() < 8 { anyhow::bail!("Need 8 bytes"); }
    let arr: [u8; 8] = data[..8].try_into().unwrap();
    Ok(if byte_order == BYTE_ORDER_LE { u64::from_le_bytes(arr) } else { u64::from_be_bytes(arr) })
}

pub fn unpack_u32_bytes(data: &[u8], byte_order: u32) -> Result<u32, anyhow::Error> {
    if data.len() < 4 { anyhow::bail!("Need 4 bytes"); }
    let arr: [u8; 4] = data[..4].try_into().unwrap();
    Ok(if byte_order == BYTE_ORDER_LE { u32::from_le_bytes(arr) } else { u32::from_be_bytes(arr) })
}

// ─── Reorder Buffer ────────────────────────────────────────────────────────────

pub struct ConduitReorderBuffer {
    streams: HashMap<u32, (u64, BTreeMap<u64, Vec<u8>>)>,
    max_gap: u64,
    max_buf: usize,
}

impl ConduitReorderBuffer {
    pub fn new(max_gap: u64, max_buf: usize) -> Self {
        ConduitReorderBuffer { streams: HashMap::new(), max_gap, max_buf }
    }

    pub fn push(&mut self, stream_id: u32, sequence_id: u64, payload: Vec<u8>) -> bool {
        if stream_id == 0 { return true; }
        let entry = self.streams.entry(stream_id).or_insert((0u64, BTreeMap::new()));
        let (next_expected, queue) = entry;
        if sequence_id < *next_expected { return true; }
        if queue.len() >= self.max_buf { return false; }
        queue.insert(sequence_id, payload);
        true
    }

    pub fn drain_ready(&mut self, stream_id: u32) -> Vec<(u64, Vec<u8>)> {
        let mut list = Vec::new();
        let entry = self.streams.entry(stream_id).or_insert((0u64, BTreeMap::new()));
        let (next_expected, queue) = entry;
        loop {
            if let Some(payload) = queue.remove(next_expected) {
                let seq = *next_expected;
                *next_expected += 1;
                list.push((seq, payload));
            } else {
                if let Some((&first_available, _)) = queue.iter().next() {
                    if first_available > *next_expected + self.max_gap {
                        *next_expected = first_available;
                        continue;
                    }
                }
                break;
            }
        }
        list
    }

    pub fn next_expected(&self, stream_id: u32) -> u64 {
        self.streams.get(&stream_id).map(|(n, _)| *n).unwrap_or(0)
    }

    pub fn buffered_count(&self, stream_id: u32) -> usize {
        self.streams.get(&stream_id).map(|(_, q)| q.len()).unwrap_or(0)
    }

    pub fn reset_stream(&mut self, stream_id: u32) { self.streams.remove(&stream_id); }
    pub fn reset_all(&mut self) { self.streams.clear(); }
    pub fn stream_ids(&self) -> Vec<u32> { self.streams.keys().cloned().collect() }
}

// ─── Sequence Counter ──────────────────────────────────────────────────────────

pub struct ConduitSequenceCounter {
    counters: HashMap<u32, u64>,
}

impl ConduitSequenceCounter {
    pub fn new() -> Self { ConduitSequenceCounter { counters: HashMap::new() } }

    pub fn next(&mut self, stream_id: u32) -> u64 {
        let counter = self.counters.entry(stream_id).or_insert(0);
        let seq = *counter;
        *counter += 1;
        seq
    }

    pub fn peek(&self, stream_id: u32) -> u64 {
        *self.counters.get(&stream_id).unwrap_or(&0)
    }

    pub fn reset_stream(&mut self, stream_id: u32) { self.counters.remove(&stream_id); }
    pub fn reset_all(&mut self) { self.counters.clear(); }
}

// ─── Checksums ─────────────────────────────────────────────────────────────────

pub fn compute_checksum(data: &[u8]) -> String {
    let hash = blake3::hash(data);
    hex::encode(&hash.as_bytes()[..16])
}

pub fn verify_checksum(data: &[u8], expected: &str) -> bool {
    let hash = blake3::hash(data);
    hex::encode(&hash.as_bytes()[..16]) == expected
}

// ─── Route Cache (sled-backed, session-bound) ─────────────────────────────────

pub struct ConduitRouteCache {
    db:   sled::Db,
    path: String,
}

impl ConduitRouteCache {
    pub fn new(path: String) -> Result<Self, anyhow::Error> {
        let db = sled::Config::new()
            .path(&path)
            .cache_capacity(64 * 1024 * 1024)
            .flush_every_ms(Some(500))
            .open()?;
        db.clear()?;
        Ok(ConduitRouteCache { db, path })
    }

    pub fn set_route(&self, destination: &str, next_hop: &str) -> Result<(), anyhow::Error> {
        self.db.insert(format!("r:{}", destination).as_bytes(), next_hop.as_bytes())?;
        Ok(())
    }

    pub fn get_route(&self, destination: &str) -> Result<Option<String>, anyhow::Error> {
        Ok(self.db.get(format!("r:{}", destination).as_bytes())?
            .map(|v| String::from_utf8_lossy(&v).to_string()))
    }

    pub fn remove_route(&self, destination: &str) -> Result<(), anyhow::Error> {
        self.db.remove(format!("r:{}", destination).as_bytes())?;
        Ok(())
    }

    pub fn list_routes(&self) -> Result<Vec<(String, String)>, anyhow::Error> {
        let mut routes = Vec::new();
        for item in self.db.scan_prefix(b"r:") {
            let (k, v) = item?;
            routes.push((
                String::from_utf8_lossy(&k[2..]).to_string(),
                String::from_utf8_lossy(&v).to_string(),
            ));
        }
        Ok(routes)
    }

    pub fn route_count(&self) -> usize { self.db.scan_prefix(b"r:").count() }

    pub fn clear(&self) -> Result<(), anyhow::Error> { self.db.clear()?; Ok(()) }

    pub fn set_meta(&self, key: &str, value: &[u8]) -> Result<(), anyhow::Error> {
        self.db.insert(format!("m:{}", key).as_bytes(), value)?;
        Ok(())
    }

    pub fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>, anyhow::Error> {
        Ok(self.db.get(format!("m:{}", key).as_bytes())?.map(|v| v.to_vec()))
    }

    pub fn track_connection(&self, client_id: &str) -> Result<(), anyhow::Error> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        self.db.insert(format!("c:{}", client_id).as_bytes(), &now.to_be_bytes())?;
        Ok(())
    }

    pub fn untrack_connection(&self, client_id: &str) -> Result<(), anyhow::Error> {
        self.db.remove(format!("c:{}", client_id).as_bytes())?;
        Ok(())
    }

    pub fn get_last_seen(&self, client_id: &str) -> Result<Option<u64>, anyhow::Error> {
        Ok(self.db.get(format!("c:{}", client_id).as_bytes())?
            .and_then(|v| {
                if v.len() == 8 {
                    Some(u64::from_be_bytes([v[0],v[1],v[2],v[3],v[4],v[5],v[6],v[7]]))
                } else { None }
            }))
    }

    pub fn list_connections(&self) -> Result<Vec<String>, anyhow::Error> {
        let mut conns = Vec::new();
        for item in self.db.scan_prefix(b"c:") {
            let (k, _) = item?;
            conns.push(String::from_utf8_lossy(&k[2..]).to_string());
        }
        Ok(conns)
    }

    pub fn path(&self) -> &str { &self.path }

    pub fn flush(&self) -> Result<(), anyhow::Error> { self.db.flush()?; Ok(()) }
}

// ─── QUIC Config Helpers ───────────────────────────────────────────────────────

pub fn make_transport_config(idle_secs: u64, keepalive_secs: u64) -> Arc<quinn::TransportConfig> {
    let mut t = quinn::TransportConfig::default();
    t.max_idle_timeout(Some(Duration::from_secs(idle_secs).try_into().unwrap()));
    t.keep_alive_interval(Some(Duration::from_secs(keepalive_secs)));
    t.max_concurrent_bidi_streams(VarInt::from_u32(4096));
    t.max_concurrent_uni_streams(VarInt::from_u32(4096));
    t.stream_receive_window(VarInt::from_u32(16 * 1024 * 1024));
    t.receive_window(VarInt::from_u32(64 * 1024 * 1024));
    t.send_window(16 * 1024 * 1024);
    t.datagram_receive_buffer_size(Some(4 * 1024 * 1024));
    Arc::new(t)
}

pub fn resolve_addr(host: &str, port: u16) -> Result<SocketAddr, anyhow::Error> {
    let addr_str = if host.contains(':') && !host.starts_with('[') {
        format!("[{}]:{}", host, port)
    } else {
        format!("{}:{}", host, port)
    };
    if let Ok(addr) = addr_str.parse::<SocketAddr>() { return Ok(addr); }
    for addr in addr_str.to_socket_addrs()? { return Ok(addr); }
    Err(anyhow::anyhow!("Could not resolve: {}", addr_str))
}

#[derive(Debug)]
pub struct DummyVerifier {}

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

/// Returns (cert_der_for_rustls, private_key, raw_cert_bytes).
pub fn generate_self_signed_cert() -> Result<
    (rustls::pki_types::CertificateDer<'static>, rustls::pki_types::PrivateKeyDer<'static>, Vec<u8>),
    anyhow::Error,
> {
    let params = rcgen::CertificateParams::new(vec![
        "localhost".to_string(), "127.0.0.1".to_string(), "::1".to_string(),
    ])?;
    let key_pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519)?;
    let cert = params.self_signed(&key_pair)?;
    let raw = cert.der().to_vec();
    Ok((
        rustls::pki_types::CertificateDer::from(raw.clone()),
        rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(
            key_pair.serialize_der(),
        )),
        raw,
    ))
}

/// Build a ClientConfig that pins to the given DER certificate.
pub fn make_pinned_client_config(cert_der: &[u8]) -> Result<ClientConfig, anyhow::Error> {
    let cert = rustls::pki_types::CertificateDer::from(cert_der.to_vec());
    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(cert)?;
    let mut crypto = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![b"netconduit".to_vec()];
    let quic_cfg = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?;
    let mut cfg = ClientConfig::new(Arc::new(quic_cfg));
    cfg.transport_config(make_transport_config(60, 15));
    Ok(cfg)
}

pub fn stun_punch_hole(stun_server: String, local_port: u16, peer_addr: String) -> Result<String, anyhow::Error> {
    let addr: std::net::SocketAddr = format!("0.0.0.0:{}", local_port).parse()?;
    let domain = if addr.is_ipv6() { socket2::Domain::IPV6 } else { socket2::Domain::IPV4 };
    let sock = socket2::Socket::new(domain, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))?;
    sock.set_reuse_address(true)?;
    #[cfg(not(windows))]
    sock.set_reuse_port(true)?;
    sock.bind(&addr.into())?;
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
                (h, p_str[1..].parse::<u16>().unwrap_or(19302))
            } else {
                (stun_server.as_str(), 19302)
            };
            resolve_addr(host, port)?
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

// ─── Conduit Duplex Stream ─────────────────────────────────────────────────────

/// A full-duplex QUIC bi-stream: both sides can send and receive simultaneously.
///
/// Obtained via `ConduitClient::open_duplex_stream()` or from a
/// `ConduitEvent::DuplexStreamOpen` delivered to the server event channel.
///
/// Each `send_data` / `recv_data` call transmits one logical message (compressed
/// + protobuf-framed). The stream stays open until `close()` is called or the
/// underlying QUIC connection closes.
#[derive(Debug)]
pub struct ConduitDuplexStream {
    /// Logical channel name (from the opening Packet).
    pub name:    String,
    /// Peer's socket address (client_id on the server side; empty on the client side).
    pub peer_id: String,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
}

impl ConduitDuplexStream {
    /// Send a message to the remote side (does NOT close the stream).
    pub async fn send_data(&mut self, data: Vec<u8>) -> anyhow::Result<()> {
        let (flags, body) = compress_for_packet(data, CompressionPolicy::Auto).await?;
        let pkt = Packet {
            version:      PROTO_VERSION,
            r#type:       PacketType::StreamData as i32,
            flags:        flags | FLAG_DUPLEX,
            channel_name: self.name.clone(),
            timestamp:    now_ms(),
            payload:      body,
            ..Default::default()
        };
        let encoded = pkt.encode_to_vec();
        self.send.write_all(&(encoded.len() as u32).to_be_bytes()).await?;
        self.send.write_all(&encoded).await?;
        METRICS.bytes_sent.fetch_add(4 + encoded.len() as u64, Ordering::Relaxed);
        METRICS.messages_sent.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Receive the next message from the remote side.
    /// Returns `None` when the remote closed their send half.
    pub async fn recv_data(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        match read_framed_packet(&mut self.recv, MAX_STREAM_SIZE).await? {
            None => Ok(None),
            Some(pkt) => {
                METRICS.bytes_received.fetch_add(4 + pkt.payload.len() as u64, Ordering::Relaxed);
                METRICS.messages_received.fetch_add(1, Ordering::Relaxed);
                Ok(Some(decompress_payload(pkt.flags, &pkt.payload)?))
            }
        }
    }

    /// Close the send half gracefully. The remote can still send until they also close.
    pub async fn close(mut self) -> anyhow::Result<()> {
        self.send.finish()?;
        Ok(())
    }
}

// ─── Conduit Event ─────────────────────────────────────────────────────────────

/// Events pushed from Rust core → callers (server/client event channels).
/// Not Clone because `DuplexStreamOpen` carries non-clonable QUIC stream handles.
#[derive(Debug)]
pub enum ConduitEvent {
    Connect    { client_id: String },
    Message    { client_id: String, payload: Vec<u8> },
    /// Named binary stream (ephemeral, already decompressed).
    BinaryStream { client_id: String, name: String, payload: Vec<u8> },
    /// A full-duplex stream was opened. The handle is valid for reads/writes
    /// until `stream.close()` or the underlying QUIC connection closes.
    DuplexStreamOpen { client_id: String, stream: ConduitDuplexStream },
    Disconnect { client_id: String },
}

// ─── Conduit Client (Pure Rust Async) ──────────────────────────────────────────

#[derive(Debug)]
pub struct ConduitClient {
    pub conn: Connection,
    pub(crate) tx_stop: mpsc::Sender<()>,
}

impl ConduitClient {
    /// Connect, accepting any certificate (development / LAN without PKI).
    pub async fn connect(
        host: &str,
        port: u16,
        connect_timeout_secs: u64,
        tx_event: mpsc::Sender<ConduitEvent>,
        local_port: Option<u16>,
    ) -> Result<Self, anyhow::Error> {
        Self::connect_inner(host, port, connect_timeout_secs, tx_event, local_port, None).await
    }

    /// Production variant: pin to cert_der instead of accepting any certificate.
    pub async fn connect_pinned(
        host: &str,
        port: u16,
        connect_timeout_secs: u64,
        tx_event: mpsc::Sender<ConduitEvent>,
        local_port: Option<u16>,
        cert_der: Vec<u8>,
    ) -> Result<Self, anyhow::Error> {
        Self::connect_inner(host, port, connect_timeout_secs, tx_event, local_port, Some(cert_der)).await
    }

    async fn connect_inner(
        host: &str,
        port: u16,
        connect_timeout_secs: u64,
        tx_event: mpsc::Sender<ConduitEvent>,
        local_port: Option<u16>,
        cert_der: Option<Vec<u8>>,
    ) -> Result<Self, anyhow::Error> {
        let peer_addr = resolve_addr(host, port)?;
        let bind_port = local_port.unwrap_or(0);
        let local = if peer_addr.is_ipv6() {
            format!("[::]:{}", bind_port).parse()?
        } else {
            format!("0.0.0.0:{}", bind_port).parse()?
        };

        let client_cfg = match cert_der {
            Some(der) => make_pinned_client_config(&der)?,
            None => {
                let mut crypto = rustls::ClientConfig::builder()
                    .with_root_certificates(rustls::RootCertStore::empty())
                    .with_no_client_auth();
                crypto.dangerous().set_certificate_verifier(Arc::new(DummyVerifier {}));
                crypto.alpn_protocols = vec![b"netconduit".to_vec()];
                static SESSION_STORE: std::sync::OnceLock<
                    Arc<rustls::client::ClientSessionMemoryCache>,
                > = std::sync::OnceLock::new();
                let store = SESSION_STORE
                    .get_or_init(|| Arc::new(rustls::client::ClientSessionMemoryCache::new(256)));
                crypto.resumption = rustls::client::Resumption::store(store.clone());
                crypto.enable_early_data = true;
                let quic_cfg = quinn::crypto::rustls::QuicClientConfig::try_from(crypto)?;
                let mut cfg = ClientConfig::new(Arc::new(quic_cfg));
                cfg.transport_config(make_transport_config(60, 15));
                cfg
            }
        };

        let mut endpoint = Endpoint::client(local)?;
        endpoint.set_default_client_config(client_cfg);

        let conn = tokio::time::timeout(
            Duration::from_secs(connect_timeout_secs),
            endpoint.connect(peer_addr, "localhost")?
        ).await??;

        let (tx_stop, mut rx_stop) = mpsc::channel::<()>(1);
        let conn_clone = conn.clone();

        let pool = ResourcePool::default_for_machine();
        tokio::spawn(async move {
            let max_msg = MAX_MSG_SIZE + PROTO_MAGIC.len() + 4;
            loop {
                tokio::select! {
                    // Uni-stream → MESSAGE
                    uni = conn_clone.accept_uni() => match uni {
                        Ok(mut recv) => {
                            let tx   = tx_event.clone();
                            let permit = match pool.try_acquire() {
                                Some(p) => p,
                                None    => continue, // shed load
                            };
                            tokio::spawn(async move {
                                let _permit = permit;
                                if let Ok(buf) = recv.read_to_end(max_msg).await {
                                    if let Ok(pkt) = decode_stream_frame(&buf) {
                                        let n = buf.len() as u64;
                                        METRICS.bytes_received.fetch_add(n, Ordering::Relaxed);
                                        METRICS.messages_received.fetch_add(1, Ordering::Relaxed);
                                        if let Ok(payload) = decompress_payload(pkt.flags, &pkt.payload) {
                                            let _ = tx.send(ConduitEvent::Message {
                                                client_id: String::new(),
                                                payload,
                                            }).await;
                                        }
                                    }
                                }
                            });
                        }
                        Err(_) => break,
                    },
                    // Bi-stream → STREAM_DATA or DuplexStreamOpen
                    bi = conn_clone.accept_bi() => match bi {
                        Ok((send, mut recv)) => {
                            let tx     = tx_event.clone();
                            let permit = pool.acquire().await; // always accept, don't shed duplex
                            tokio::spawn(async move {
                                let _permit = permit;
                                // Read and validate magic prefix.
                                let mut magic = [0u8; PROTO_MAGIC.len()];
                                if recv.read_exact(&mut magic).await.is_err() { return; }
                                if magic != PROTO_MAGIC { return; }
                                let pkt = match read_framed_packet(&mut recv, MAX_STREAM_SIZE).await {
                                    Ok(Some(p)) => p,
                                    _           => return,
                                };
                                METRICS.bytes_received.fetch_add(
                                    (PROTO_MAGIC.len() + 4 + pkt.payload.len()) as u64,
                                    Ordering::Relaxed,
                                );
                                METRICS.messages_received.fetch_add(1, Ordering::Relaxed);
                                if pkt.flags & FLAG_DUPLEX != 0 {
                                    let stream = ConduitDuplexStream {
                                        name:    pkt.channel_name,
                                        peer_id: String::new(),
                                        send, recv,
                                    };
                                    let _ = tx.send(ConduitEvent::DuplexStreamOpen {
                                        client_id: String::new(),
                                        stream,
                                    }).await;
                                } else {
                                    // Ephemeral bi-stream — first packet IS the entire payload.
                                    if let Ok(payload) = decompress_payload(pkt.flags, &pkt.payload) {
                                        let _ = tx.send(ConduitEvent::BinaryStream {
                                            client_id: String::new(),
                                            name:      pkt.channel_name,
                                            payload,
                                        }).await;
                                    }
                                    drop(send); // close our send half — not used for ephemeral
                                }
                            });
                        }
                        Err(_) => break,
                    },
                    // Datagram → MESSAGE (unreliable, lowest latency)
                    Ok(dgram) = conn_clone.read_datagram() => {
                        let tx     = tx_event.clone();
                        let permit = match pool.try_acquire() {
                            Some(p) => p,
                            None    => continue,
                        };
                        tokio::spawn(async move {
                            let _permit = permit;
                            let buf = dgram.to_vec();
                            if let Ok(pkt) = decode_datagram_frame(&buf) {
                                let n = buf.len() as u64;
                                METRICS.bytes_received.fetch_add(n, Ordering::Relaxed);
                                METRICS.messages_received.fetch_add(1, Ordering::Relaxed);
                                if let Ok(payload) = decompress_payload(pkt.flags, &pkt.payload) {
                                    let _ = tx.send(ConduitEvent::Message {
                                        client_id: String::new(),
                                        payload,
                                    }).await;
                                }
                            }
                        });
                    }
                    _ = rx_stop.recv() => {
                        conn_clone.close(VarInt::from_u32(0), b"disconnect");
                        break;
                    }
                }
            }
            let _ = tx_event.send(ConduitEvent::Disconnect { client_id: String::new() }).await;
        });

        Ok(ConduitClient { conn, tx_stop })
    }

    pub async fn disconnect(&mut self) {
        let _ = self.tx_stop.send(()).await;
    }

    pub async fn send_message(&self, payload: &[u8]) -> Result<(), anyhow::Error> {
        let frame = make_message_frame(payload.to_vec()).await?;
        let n = frame.len() as u64;
        let mut s = self.conn.open_uni().await?;
        s.write_all(&frame).await?;
        s.finish()?;
        METRICS.bytes_sent.fetch_add(n, Ordering::Relaxed);
        METRICS.messages_sent.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub async fn send_binary_stream(&self, stream_name: &str, data: Vec<u8>) -> Result<(), anyhow::Error> {
        let (mut send, _) = self.conn.open_bi().await?;
        write_framed_stream(&mut send, stream_name, data, CompressionPolicy::Auto).await
    }

    /// Open a full-duplex bi-stream to the server.
    ///
    /// Both sides can send and receive simultaneously on the returned stream.
    /// The server receives a `ConduitEvent::DuplexStreamOpen` with a matching handle.
    pub async fn open_duplex_stream(&self, name: &str) -> anyhow::Result<ConduitDuplexStream> {
        let (mut send, recv) = self.conn.open_bi().await?;
        // Write the opening frame: PROTO_MAGIC + length-prefixed Packet with FLAG_DUPLEX.
        send.write_all(&PROTO_MAGIC).await?;
        let pkt = Packet {
            version:      PROTO_VERSION,
            r#type:       PacketType::StreamData as i32,
            flags:        FLAG_DUPLEX,
            channel_name: name.to_string(),
            timestamp:    now_ms(),
            ..Default::default()
        };
        let encoded = pkt.encode_to_vec();
        send.write_all(&(encoded.len() as u32).to_be_bytes()).await?;
        send.write_all(&encoded).await?;
        // Do NOT call finish() — stream stays open for subsequent messages.
        Ok(ConduitDuplexStream {
            name:    name.to_string(),
            peer_id: String::new(),
            send, recv,
        })
    }

    /// Send a raw QUIC datagram — unreliable, ~1200 B max, lowest latency.
    pub fn send_datagram(&self, payload: &[u8]) -> anyhow::Result<()> {
        let codec = select_codec(payload);
        let (flags, body) = match codec {
            CODEC_LZ4 => {
                let c = lz4_flex::compress_prepend_size(payload);
                if c.len() < payload.len() { (FLAG_COMPRESS_LZ4, c) } else { (0u32, payload.to_vec()) }
            }
            // No zstd for datagrams — they're size-limited and zstd is CPU-heavy.
            _ => (0u32, payload.to_vec()),
        };
        let pkt = Packet {
            version: PROTO_VERSION,
            r#type:  PacketType::Message as i32,
            flags:   flags | FLAG_UNRELIABLE,
            timestamp: now_ms(),
            payload: body,
            ..Default::default()
        };
        let mut frame = Vec::with_capacity(PROTO_MAGIC.len() + 64);
        frame.extend_from_slice(&PROTO_MAGIC);
        pkt.encode(&mut frame)?;
        self.conn.send_datagram(bytes::Bytes::from(frame))?;
        Ok(())
    }
}

// ─── Conduit Server (Pure Rust Async) ──────────────────────────────────────────

pub struct ConduitServer {
    tx_stop:     mpsc::Sender<()>,
    tx_send:     mpsc::Sender<(String, Vec<u8>)>,
    connections: Arc<RwLock<HashMap<String, Connection>>>,
    channels:    Arc<RwLock<HashMap<String, ChannelConfig>>>,
    /// Raw DER bytes of the server's self-signed TLS certificate.
    pub cert_der: Vec<u8>,
}

impl ConduitServer {
    /// Start a server. max_connections caps accepted peers; 0 = MAX_CONNECTIONS.
    pub async fn start(
        host: &str,
        port: u16,
        tx_event: mpsc::Sender<ConduitEvent>,
        max_connections: usize,
    ) -> Result<Self, anyhow::Error> {
        let addr = resolve_addr(host, port)?;
        let max_conn = if max_connections == 0 { MAX_CONNECTIONS } else { max_connections };

        let (cert, key, cert_der) = generate_self_signed_cert()?;
        let mut server_crypto = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)?;
        server_crypto.alpn_protocols = vec![b"netconduit".to_vec()];
        let quic_cfg = quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto)?;
        let mut server_cfg = ServerConfig::with_crypto(Arc::new(quic_cfg));
        server_cfg.transport_config(make_transport_config(60, 15));

        let endpoint = Endpoint::server(server_cfg, addr)?;
        let (tx_stop, mut rx_stop) = mpsc::channel::<()>(1);
        let (tx_send, mut rx_send) = mpsc::channel::<(String, Vec<u8>)>(8192);

        let connections: Arc<RwLock<HashMap<String, Connection>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let channels: Arc<RwLock<HashMap<String, ChannelConfig>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let conns_clone = connections.clone();
        let chans_clone = channels.clone();

        // Unicast sender — builds Packet + compresses per-message in spawned tasks.
        let conns_send = connections.clone();
        tokio::spawn(async move {
            while let Some((client_id, raw_payload)) = rx_send.recv().await {
                let conn = conns_send
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .get(&client_id)
                    .cloned();
                if let Some(conn) = conn {
                    tokio::spawn(async move {
                        if let Ok(frame) = make_message_frame(raw_payload).await {
                            let n = frame.len() as u64;
                            if let Ok(mut s) = conn.open_uni().await {
                                let _ = s.write_all(&frame).await;
                                if s.finish().is_ok() {
                                    METRICS.bytes_sent.fetch_add(n, Ordering::Relaxed);
                                    METRICS.messages_sent.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    });
                }
            }
        });

        // Global resource pool for this server — shared across all connections.
        let pool = ResourcePool::default_for_machine();

        // Accept loop — one JoinSet per connection for clean task cancellation.
        let conns_accept = connections.clone();
        let tx_event_accept = tx_event.clone();
        tokio::spawn(async move {
            let max_msg = MAX_MSG_SIZE + PROTO_MAGIC.len() + 4;
            loop {
                tokio::select! {
                    incoming = endpoint.accept() => {
                        let Some(attempt) = incoming else { break };
                        let count = conns_accept.read().unwrap_or_else(|p| p.into_inner()).len();
                        if count >= max_conn {
                            METRICS.connections_rejected.fetch_add(1, Ordering::Relaxed);
                            drop(attempt);
                            continue;
                        }
                        let tx_event  = tx_event_accept.clone();
                        let conns     = conns_accept.clone();
                        let pool      = pool.clone();
                        tokio::spawn(async move {
                            let conn = match attempt.await {
                                Ok(c)  => c,
                                Err(_) => return,
                            };
                            let cid = conn.remote_address().to_string();
                            conns.write().unwrap_or_else(|p| p.into_inner())
                                .insert(cid.clone(), conn.clone());
                            METRICS.connections_active.fetch_add(1, Ordering::Relaxed);
                            METRICS.connections_total.fetch_add(1, Ordering::Relaxed);
                            let _ = tx_event.send(ConduitEvent::Connect { client_id: cid.clone() }).await;

                            let mut tasks: JoinSet<()> = JoinSet::new();
                            loop {
                                tokio::select! {
                                    // Uni-stream → MESSAGE (+ mesh routing)
                                    uni = conn.accept_uni() => match uni {
                                        Ok(mut recv) => {
                                            let tx    = tx_event.clone();
                                            let cid   = cid.clone();
                                            let conns = conns.clone();
                                            let permit = match pool.try_acquire() {
                                                Some(p) => p,
                                                None    => continue,
                                            };
                                            tasks.spawn(async move {
                                                let _permit = permit;
                                                if let Ok(buf) = recv.read_to_end(max_msg).await {
                                                    if let Ok(pkt) = decode_stream_frame(&buf) {
                                                        let n = buf.len() as u64;
                                                        METRICS.bytes_received.fetch_add(n, Ordering::Relaxed);
                                                        METRICS.messages_received.fetch_add(1, Ordering::Relaxed);

                                                        // ── Mesh routing ──────────────────────────
                                                        if !pkt.dst_id.is_empty() && pkt.dst_id != cid {
                                                            if pkt.ttl == 0 { return; } // TTL expired
                                                            let dst_conn = conns
                                                                .read()
                                                                .unwrap_or_else(|p| p.into_inner())
                                                                .get(&pkt.dst_id)
                                                                .cloned();
                                                            if let Some(dst) = dst_conn {
                                                                let mut fwd = pkt;
                                                                fwd.is_mesh = true;
                                                                fwd.ttl = fwd.ttl.saturating_sub(1);
                                                                let frame = frame_packet(&fwd);
                                                                METRICS.mesh_forwards.fetch_add(1, Ordering::Relaxed);
                                                                tokio::spawn(async move {
                                                                    if let Ok(mut s) = dst.open_uni().await {
                                                                        let _ = s.write_all(&frame).await;
                                                                        let _ = s.finish();
                                                                    }
                                                                });
                                                            }
                                                            return; // routed — don't process locally
                                                        }

                                                        if let Ok(payload) = decompress_payload(pkt.flags, &pkt.payload) {
                                                            let _ = tx.send(ConduitEvent::Message { client_id: cid, payload }).await;
                                                        }
                                                    }
                                                }
                                            });
                                        }
                                        Err(_) => break,
                                    },
                                    // Bi-stream → STREAM_DATA or DuplexStreamOpen
                                    bi = conn.accept_bi() => match bi {
                                        Ok((send, mut recv)) => {
                                            let tx     = tx_event.clone();
                                            let cid    = cid.clone();
                                            let permit = pool.acquire().await; // don't shed duplex
                                            tasks.spawn(async move {
                                                let _permit = permit;
                                                let mut magic = [0u8; PROTO_MAGIC.len()];
                                                if recv.read_exact(&mut magic).await.is_err() { return; }
                                                if magic != PROTO_MAGIC { return; }
                                                let pkt = match read_framed_packet(&mut recv, MAX_STREAM_SIZE).await {
                                                    Ok(Some(p)) => p,
                                                    _           => return,
                                                };
                                                METRICS.bytes_received.fetch_add(
                                                    (PROTO_MAGIC.len() + 4 + pkt.payload.len()) as u64,
                                                    Ordering::Relaxed,
                                                );
                                                METRICS.messages_received.fetch_add(1, Ordering::Relaxed);

                                                if pkt.flags & FLAG_DUPLEX != 0 {
                                                    let stream = ConduitDuplexStream {
                                                        name:    pkt.channel_name,
                                                        peer_id: cid.clone(),
                                                        send, recv,
                                                    };
                                                    let _ = tx.send(ConduitEvent::DuplexStreamOpen {
                                                        client_id: cid,
                                                        stream,
                                                    }).await;
                                                } else {
                                                    if let Ok(payload) = decompress_payload(pkt.flags, &pkt.payload) {
                                                        let _ = tx.send(ConduitEvent::BinaryStream {
                                                            client_id: cid,
                                                            name:      pkt.channel_name,
                                                            payload,
                                                        }).await;
                                                    }
                                                    drop(send);
                                                }
                                            });
                                        }
                                        Err(_) => break,
                                    },
                                    // Datagram → MESSAGE (unreliable)
                                    Ok(dgram) = conn.read_datagram() => {
                                        let tx     = tx_event.clone();
                                        let cid    = cid.clone();
                                        let permit = match pool.try_acquire() {
                                            Some(p) => p,
                                            None    => continue,
                                        };
                                        tasks.spawn(async move {
                                            let _permit = permit;
                                            let buf = dgram.to_vec();
                                            if let Ok(pkt) = decode_datagram_frame(&buf) {
                                                let n = buf.len() as u64;
                                                METRICS.bytes_received.fetch_add(n, Ordering::Relaxed);
                                                METRICS.messages_received.fetch_add(1, Ordering::Relaxed);
                                                if let Ok(payload) = decompress_payload(pkt.flags, &pkt.payload) {
                                                    let _ = tx.send(ConduitEvent::Message { client_id: cid, payload }).await;
                                                }
                                            }
                                        });
                                    }
                                }
                            }

                            tasks.abort_all();
                            while tasks.join_next().await.is_some() {}
                            conns.write().unwrap_or_else(|p| p.into_inner()).remove(&cid);
                            METRICS.connections_active.fetch_sub(1, Ordering::Relaxed);
                            let _ = tx_event.send(ConduitEvent::Disconnect { client_id: cid }).await;
                        });
                    }
                    _ = rx_stop.recv() => {
                        endpoint.close(VarInt::from_u32(0), b"shutdown");
                        break;
                    }
                }
            }
        });

        Ok(ConduitServer { tx_stop, tx_send, connections: conns_clone, channels: chans_clone, cert_der })
    }

    pub fn stop(&self) {
        let _ = self.tx_stop.try_send(());
    }

    /// Queue a unicast message. Returns false if send queue is full (backpressure signal).
    pub fn send_message(&self, client_id: String, payload: Vec<u8>) -> bool {
        self.tx_send.try_send((client_id, payload)).is_ok()
    }

    /// Broadcast to all peers concurrently. Compresses once, reuses frame for all peers.
    pub fn broadcast(&self, payload: Vec<u8>) {
        let conns: Vec<Connection> = self
            .connections
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .cloned()
            .collect();
        if conns.is_empty() { return; }
        tokio::spawn(async move {
            let frame = match make_message_frame(payload).await {
                Ok(f)  => Arc::new(f),
                Err(_) => return,
            };
            let n = frame.len() as u64;
            for conn in conns {
                let frame = frame.clone();
                tokio::spawn(async move {
                    if let Ok(mut s) = conn.open_uni().await {
                        let _ = s.write_all(&frame).await;
                        if s.finish().is_ok() {
                            METRICS.bytes_sent.fetch_add(n, Ordering::Relaxed);
                            METRICS.messages_sent.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
    }

    /// Send a named binary stream, honoring the channel's mode and compression policy.
    pub async fn send_binary_stream(
        &self,
        client_id: &str,
        stream_name: &str,
        data: Vec<u8>,
    ) -> Result<(), anyhow::Error> {
        let (conn, mode, policy) = {
            let conns = self.connections.read().unwrap_or_else(|p| p.into_inner());
            let chans = self.channels.read().unwrap_or_else(|p| p.into_inner());
            let conn = conns.get(client_id).cloned();
            let (mode, policy) = chans.get(stream_name)
                .map(|c| (c.mode.clone(), c.compression.clone()))
                .unwrap_or((ChannelMode::ReliableOrdered, CompressionPolicy::Auto));
            (conn, mode, policy)
        };
        if let Some(conn) = conn {
            if mode == ChannelMode::Unreliable {
                // Unreliable channel → datagram path.
                let (flags, body) = compress_for_packet(data, policy).await?;
                let pkt = Packet {
                    version: PROTO_VERSION,
                    r#type:  PacketType::StreamData as i32,
                    flags:   flags | FLAG_UNRELIABLE,
                    channel_name: stream_name.to_string(),
                    timestamp: now_ms(),
                    payload: body,
                    ..Default::default()
                };
                let mut frame = Vec::with_capacity(PROTO_MAGIC.len() + 128);
                frame.extend_from_slice(&PROTO_MAGIC);
                pkt.encode(&mut frame)?;
                conn.send_datagram(bytes::Bytes::from(frame))?;
                return Ok(());
            }
            if let Ok((mut send, _)) = conn.open_bi().await {
                write_framed_stream(&mut send, stream_name, data, policy).await?;
            }
        }
        Ok(())
    }

    /// Register a named logical channel with custom mode and compression policy.
    pub fn register_channel(&self, cfg: ChannelConfig) {
        self.channels
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(cfg.name.clone(), cfg);
    }

    /// Send a QUIC datagram to a single peer (unreliable, lowest latency, ~1200 B max).
    pub fn send_datagram(&self, client_id: &str, payload: Vec<u8>) -> anyhow::Result<()> {
        let conn = self.connections.read().unwrap_or_else(|p| p.into_inner())
            .get(client_id).cloned();
        if let Some(conn) = conn {
            let codec = select_codec(&payload);
            let (flags, body) = match codec {
                CODEC_LZ4 => {
                    let c = lz4_flex::compress_prepend_size(&payload);
                    if c.len() < payload.len() { (FLAG_COMPRESS_LZ4, c) } else { (0u32, payload) }
                }
                _ => (0u32, payload),
            };
            let pkt = Packet {
                version: PROTO_VERSION,
                r#type:  PacketType::Message as i32,
                flags:   flags | FLAG_UNRELIABLE,
                timestamp: now_ms(),
                payload: body,
                ..Default::default()
            };
            let mut frame = Vec::with_capacity(PROTO_MAGIC.len() + 64);
            frame.extend_from_slice(&PROTO_MAGIC);
            pkt.encode(&mut frame)?;
            conn.send_datagram(bytes::Bytes::from(frame))?;
        }
        Ok(())
    }

    pub fn connection_count(&self) -> usize {
        self.connections.read().unwrap_or_else(|p| p.into_inner()).len()
    }

    pub fn is_connected(&self, client_id: &str) -> bool {
        self.connections.read().unwrap_or_else(|p| p.into_inner()).contains_key(client_id)
    }

    pub fn connected_clients(&self) -> Vec<String> {
        self.connections.read().unwrap_or_else(|p| p.into_inner()).keys().cloned().collect()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Protocol constants ───────────────────────────────────────────────────

    #[test]
    fn proto_magic_correct_bytes() {
        assert_eq!(&PROTO_MAGIC, b"NCON\x01");
        assert_eq!(PROTO_MAGIC.len(), 5);
    }

    #[test]
    fn proto_magic_not_all_zeros() {
        assert!(PROTO_MAGIC.iter().any(|&b| b != 0));
    }

    #[test]
    fn proto_version_is_one() {
        assert_eq!(PROTO_VERSION, 1);
    }

    #[test]
    fn flag_constants_non_overlapping() {
        let flags = [FLAG_COMPRESS_LZ4, FLAG_COMPRESS_ZSTD, FLAG_UNRELIABLE, FLAG_HAS_CHECKSUM, FLAG_LITTLE_ENDIAN];
        for i in 0..flags.len() {
            for j in (i+1)..flags.len() {
                assert_eq!(flags[i] & flags[j], 0, "flag overlap at {} and {}", i, j);
            }
        }
    }

    // ── Codec selection ──────────────────────────────────────────────────────

    #[test]
    fn select_codec_none_for_tiny() {
        assert_eq!(select_codec(&[0u8; 10]), CODEC_NONE);
        assert_eq!(select_codec(&[0u8; 63]), CODEC_NONE);
    }

    #[test]
    fn select_codec_lz4_for_medium() {
        assert_eq!(select_codec(&[0u8; 64]),  CODEC_LZ4);
        assert_eq!(select_codec(&[0u8; 128]), CODEC_LZ4);
        assert_eq!(select_codec(&[0u8; 255]), CODEC_LZ4);
    }

    #[test]
    fn select_codec_zstd_for_large() {
        assert_eq!(select_codec(&[0u8; 256]),  CODEC_ZSTD);
        assert_eq!(select_codec(&[0u8; 4096]), CODEC_ZSTD);
    }

    #[test]
    fn select_codec_empty_is_none() {
        assert_eq!(select_codec(&[]), CODEC_NONE);
    }

    // ── Compression round-trips (legacy API) ─────────────────────────────────

    #[test]
    fn compress_roundtrip_codec_none() {
        let input: Vec<u8> = (0u8..20).collect();
        let compressed = compress_bytes(&input).unwrap();
        assert_eq!(compressed[0], CODEC_NONE);
        let decompressed = decompress_bytes(&compressed).unwrap();
        assert_eq!(decompressed, input);
    }

    #[test]
    fn compress_roundtrip_lz4() {
        let input: Vec<u8> = vec![42u8; 128];
        let compressed = compress_bytes(&input).unwrap();
        assert_eq!(compressed[0], CODEC_LZ4);
        let decompressed = decompress_bytes(&compressed).unwrap();
        assert_eq!(decompressed, input);
    }

    #[test]
    fn compress_roundtrip_zstd() {
        let input: Vec<u8> = vec![0xABu8; 1024];
        let compressed = compress_bytes(&input).unwrap();
        assert_eq!(compressed[0], CODEC_ZSTD);
        assert!(compressed.len() < input.len(), "zstd must shrink compressible data");
        let decompressed = decompress_bytes(&compressed).unwrap();
        assert_eq!(decompressed, input);
    }

    #[test]
    fn compress_roundtrip_random_large() {
        let input: Vec<u8> = (0u8..=255).cycle().take(512).collect();
        let compressed = compress_bytes(&input).unwrap();
        let decompressed = decompress_bytes(&compressed).unwrap();
        assert_eq!(decompressed, input);
    }

    #[test]
    fn decompress_unknown_codec_errors() {
        let bad = vec![0xFF, 1, 2, 3];
        assert!(decompress_bytes(&bad).is_err());
    }

    #[test]
    fn decompress_empty_returns_empty() {
        assert_eq!(decompress_bytes(&[]).unwrap(), Vec::<u8>::new());
    }

    // ── Protobuf payload compression round-trips ─────────────────────────────

    #[tokio::test]
    async fn compress_for_packet_roundtrip_none() {
        let input: Vec<u8> = (0u8..20).collect();
        let (flags, body) = compress_for_packet(input.clone(), CompressionPolicy::Auto).await.unwrap();
        assert_eq!(flags, 0);
        let out = decompress_payload(flags, &body).unwrap();
        assert_eq!(out, input);
    }

    #[tokio::test]
    async fn compress_for_packet_roundtrip_lz4() {
        let input = vec![42u8; 128];
        let (flags, body) = compress_for_packet(input.clone(), CompressionPolicy::Auto).await.unwrap();
        assert_eq!(flags & FLAG_COMPRESS_LZ4, FLAG_COMPRESS_LZ4);
        let out = decompress_payload(flags, &body).unwrap();
        assert_eq!(out, input);
    }

    #[tokio::test]
    async fn compress_for_packet_roundtrip_zstd() {
        let input = vec![0xCCu8; 1024];
        let (flags, body) = compress_for_packet(input.clone(), CompressionPolicy::Auto).await.unwrap();
        assert_eq!(flags & FLAG_COMPRESS_ZSTD, FLAG_COMPRESS_ZSTD);
        let out = decompress_payload(flags, &body).unwrap();
        assert_eq!(out, input);
    }

    #[tokio::test]
    async fn compress_for_packet_never_policy() {
        let input = vec![0xABu8; 1024];
        let (flags, body) = compress_for_packet(input.clone(), CompressionPolicy::Never).await.unwrap();
        assert_eq!(flags, 0);
        assert_eq!(body, input);
    }

    // ── Async compression (legacy) ───────────────────────────────────────────

    #[tokio::test]
    async fn compress_bytes_async_small_stays_none() {
        let input: Vec<u8> = (0u8..10).collect();
        let out = compress_bytes_async(input.clone()).await.unwrap();
        assert_eq!(out[0], CODEC_NONE);
        assert_eq!(&out[1..], input.as_slice());
    }

    #[tokio::test]
    async fn compress_bytes_async_large_uses_spawn_blocking() {
        let input = vec![0xCCu8; 8192];
        let out = compress_bytes_async(input.clone()).await.unwrap();
        assert_eq!(out[0], CODEC_ZSTD);
        let back = decompress_bytes(&out).unwrap();
        assert_eq!(back, input);
    }

    // ── Protobuf frame encode/decode ─────────────────────────────────────────

    #[test]
    fn frame_packet_decode_roundtrip() {
        let pkt = Packet {
            version: PROTO_VERSION,
            r#type: PacketType::Message as i32,
            flags: 0,
            timestamp: 12345678,
            payload: b"hello world".to_vec(),
            ..Default::default()
        };
        let frame = frame_packet(&pkt);
        // Magic prefix
        assert_eq!(&frame[..PROTO_MAGIC.len()], &PROTO_MAGIC);
        let decoded = decode_stream_frame(&frame).unwrap();
        assert_eq!(decoded.version, PROTO_VERSION);
        assert_eq!(decoded.payload, b"hello world");
        assert_eq!(decoded.r#type, PacketType::Message as i32);
    }

    #[test]
    fn decode_stream_frame_bad_magic_errors() {
        let mut frame = frame_packet(&Packet {
            version: PROTO_VERSION,
            r#type: PacketType::Message as i32,
            payload: b"x".to_vec(),
            ..Default::default()
        });
        frame[0] = 0xFF; // corrupt magic
        assert!(decode_stream_frame(&frame).is_err());
    }

    #[test]
    fn decode_stream_frame_too_short_errors() {
        assert!(decode_stream_frame(&[0u8; 4]).is_err());
    }

    #[test]
    fn decode_stream_frame_version_mismatch_errors() {
        let pkt = Packet {
            version: 99,
            r#type: PacketType::Message as i32,
            payload: b"x".to_vec(),
            ..Default::default()
        };
        let encoded = pkt.encode_to_vec();
        let mut frame = Vec::new();
        frame.extend_from_slice(&PROTO_MAGIC);
        frame.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
        frame.extend_from_slice(&encoded);
        assert!(decode_stream_frame(&frame).is_err());
    }

    #[test]
    fn stream_data_packet_channel_name_preserved() {
        let pkt = Packet {
            version: PROTO_VERSION,
            r#type: PacketType::StreamData as i32,
            channel_name: "my_channel".to_string(),
            flags: FLAG_COMPRESS_LZ4,
            payload: b"data".to_vec(),
            ..Default::default()
        };
        let frame = frame_packet(&pkt);
        let decoded = decode_stream_frame(&frame).unwrap();
        assert_eq!(decoded.channel_name, "my_channel");
        assert_eq!(decoded.flags & FLAG_COMPRESS_LZ4, FLAG_COMPRESS_LZ4);
    }

    #[test]
    fn datagram_frame_encode_decode() {
        let pkt = Packet {
            version: PROTO_VERSION,
            r#type: PacketType::Message as i32,
            flags: FLAG_UNRELIABLE,
            payload: b"dgram".to_vec(),
            ..Default::default()
        };
        let mut frame = Vec::new();
        frame.extend_from_slice(&PROTO_MAGIC);
        pkt.encode(&mut frame).unwrap();
        let decoded = decode_datagram_frame(&frame).unwrap();
        assert_eq!(decoded.payload, b"dgram");
        assert_eq!(decoded.flags & FLAG_UNRELIABLE, FLAG_UNRELIABLE);
    }

    #[test]
    fn flags_to_codec_mapping() {
        assert_eq!(flags_to_codec(0), CODEC_NONE);
        assert_eq!(flags_to_codec(FLAG_COMPRESS_LZ4), CODEC_LZ4);
        assert_eq!(flags_to_codec(FLAG_COMPRESS_ZSTD), CODEC_ZSTD);
        // ZSTD bit takes priority over LZ4 if somehow both set.
        assert_eq!(flags_to_codec(FLAG_COMPRESS_LZ4 | FLAG_COMPRESS_ZSTD), CODEC_ZSTD);
    }

    // ── Checksums ────────────────────────────────────────────────────────────

    #[test]
    fn checksum_deterministic() {
        let a = compute_checksum(b"hello world");
        let b = compute_checksum(b"hello world");
        assert_eq!(a, b);
    }

    #[test]
    fn checksum_different_inputs_differ() {
        assert_ne!(compute_checksum(b"foo"), compute_checksum(b"bar"));
    }

    #[test]
    fn checksum_is_hex_32_chars() {
        let h = compute_checksum(b"netconduit");
        assert_eq!(h.len(), 32);
        assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn verify_checksum_valid() {
        let data = b"test payload";
        let hash = compute_checksum(data);
        assert!(verify_checksum(data, &hash));
    }

    #[test]
    fn verify_checksum_tampered_data() {
        let data = b"test payload";
        let hash = compute_checksum(data);
        assert!(!verify_checksum(b"tampered!", &hash));
    }

    #[test]
    fn verify_checksum_wrong_hash() {
        assert!(!verify_checksum(b"hello", "0000000000000000000000000000000000"));
    }

    // ── Channel config ───────────────────────────────────────────────────────

    #[test]
    fn channel_config_reliable_defaults() {
        let c = ChannelConfig::reliable("data");
        assert_eq!(c.name, "data");
        assert_eq!(c.mode, ChannelMode::ReliableOrdered);
        assert_eq!(c.compression, CompressionPolicy::Auto);
    }

    #[test]
    fn channel_config_unreliable_defaults() {
        let c = ChannelConfig::unreliable("game_state");
        assert_eq!(c.mode, ChannelMode::Unreliable);
        assert_eq!(c.compression, CompressionPolicy::Never);
    }

    #[test]
    fn channel_config_unordered_defaults() {
        let c = ChannelConfig::unordered("telemetry");
        assert_eq!(c.mode, ChannelMode::ReliableUnordered);
    }

    // ── Metrics ──────────────────────────────────────────────────────────────

    #[test]
    fn metrics_singleton_readable() {
        let (bs, br, ms, mr, ca, ct, cr) = metrics_snapshot();
        let _ = (bs, br, ms, mr, ca, ct, cr);
    }

    #[test]
    fn metrics_increment_and_read() {
        let (bs_before, _, ms_before, _, _, _, _) = metrics_snapshot();
        METRICS.bytes_sent.fetch_add(100, Ordering::Relaxed);
        METRICS.messages_sent.fetch_add(1, Ordering::Relaxed);
        let (bs_after, _, ms_after, _, _, _, _) = metrics_snapshot();
        assert!(bs_after >= bs_before + 100);
        assert!(ms_after >= ms_before + 1);
    }

    // ── Byte-order helpers ───────────────────────────────────────────────────

    #[test]
    fn pack_unpack_u64_be_roundtrip() {
        let val = 0xDEADBEEF_CAFEBABE_u64;
        let packed = pack_u64_bytes(val, BYTE_ORDER_BE);
        let unpacked = unpack_u64_bytes(&packed, BYTE_ORDER_BE).unwrap();
        assert_eq!(unpacked, val);
    }

    #[test]
    fn pack_unpack_u64_le_roundtrip() {
        let val = 0x1234567890ABCDEFu64;
        let packed = pack_u64_bytes(val, BYTE_ORDER_LE);
        let unpacked = unpack_u64_bytes(&packed, BYTE_ORDER_LE).unwrap();
        assert_eq!(unpacked, val);
    }

    #[test]
    fn pack_unpack_u32_roundtrip() {
        let val = 0xCAFEBABEu32;
        let be = pack_u32_bytes(val, BYTE_ORDER_BE);
        assert_eq!(unpack_u32_bytes(&be, BYTE_ORDER_BE).unwrap(), val);
        let le = pack_u32_bytes(val, BYTE_ORDER_LE);
        assert_eq!(unpack_u32_bytes(&le, BYTE_ORDER_LE).unwrap(), val);
    }

    #[test]
    fn unpack_u64_too_short_errors() {
        assert!(unpack_u64_bytes(&[1, 2, 3], BYTE_ORDER_BE).is_err());
    }

    // ── Reorder buffer ───────────────────────────────────────────────────────

    #[test]
    fn reorder_buffer_in_order_delivery() {
        let mut buf = ConduitReorderBuffer::new(10, 64);
        buf.push(1, 0, b"a".to_vec());
        buf.push(1, 1, b"b".to_vec());
        buf.push(1, 2, b"c".to_vec());
        let ready = buf.drain_ready(1);
        assert_eq!(ready.len(), 3);
        assert_eq!(ready[0].1, b"a");
        assert_eq!(ready[1].1, b"b");
        assert_eq!(ready[2].1, b"c");
    }

    #[test]
    fn reorder_buffer_out_of_order_held() {
        let mut buf = ConduitReorderBuffer::new(10, 64);
        buf.push(1, 2, b"third".to_vec());
        buf.push(1, 0, b"first".to_vec());
        let ready = buf.drain_ready(1);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].1, b"first");
        buf.push(1, 1, b"second".to_vec());
        let ready2 = buf.drain_ready(1);
        assert_eq!(ready2.len(), 2);
    }

    #[test]
    fn reorder_buffer_reset_stream() {
        let mut buf = ConduitReorderBuffer::new(10, 64);
        buf.push(1, 0, b"x".to_vec());
        buf.reset_stream(1);
        assert_eq!(buf.next_expected(1), 0);
        assert_eq!(buf.buffered_count(1), 0);
    }

    // ── Sequence counter ─────────────────────────────────────────────────────

    #[test]
    fn sequence_counter_increments() {
        let mut counter = ConduitSequenceCounter::new();
        assert_eq!(counter.next(0), 0);
        assert_eq!(counter.next(0), 1);
        assert_eq!(counter.next(0), 2);
        assert_eq!(counter.next(1), 0);
    }

    #[test]
    fn sequence_counter_peek_no_advance() {
        let mut counter = ConduitSequenceCounter::new();
        counter.next(5);
        counter.next(5);
        assert_eq!(counter.peek(5), 2);
        assert_eq!(counter.peek(5), 2);
    }

    #[test]
    fn sequence_counter_reset() {
        let mut counter = ConduitSequenceCounter::new();
        counter.next(3);
        counter.next(3);
        counter.reset_stream(3);
        assert_eq!(counter.peek(3), 0);
    }
}
