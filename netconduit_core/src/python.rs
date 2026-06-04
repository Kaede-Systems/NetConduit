#![cfg(feature = "python")]

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use std::time::Duration;

use crate::core::{
    // Compression
    compress_bytes, decompress_bytes,
    // Pack/unpack
    pack_u64_bytes, pack_u32_bytes, unpack_u64_bytes, unpack_u32_bytes,
    // Constants
    BYTE_ORDER_BE, BYTE_ORDER_LE,
    // Types
    ConduitClient, ConduitServer, ConduitEvent,
    ConduitReorderBuffer, ConduitSequenceCounter, ConduitRouteCache,
};

// ─── Compression functions ─────────────────────────────────────────────────────

#[pyfunction]
fn compress_payload<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    let compressed = compress_bytes(data)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
    Ok(PyBytes::new_bound(py, &compressed))
}

#[pyfunction]
fn decompress_payload<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    let decompressed = decompress_bytes(data)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
    Ok(PyBytes::new_bound(py, &decompressed))
}

// ─── Integer pack/unpack ───────────────────────────────────────────────────────

#[pyfunction]
fn pack_u64<'py>(py: Python<'py>, value: u64, byte_order: u32) -> Bound<'py, PyBytes> {
    let bytes = pack_u64_bytes(value, byte_order);
    PyBytes::new_bound(py, &bytes)
}

#[pyfunction]
fn pack_u32<'py>(py: Python<'py>, value: u32, byte_order: u32) -> Bound<'py, PyBytes> {
    let bytes = pack_u32_bytes(value, byte_order);
    PyBytes::new_bound(py, &bytes)
}

#[pyfunction]
fn unpack_u64(data: &[u8], byte_order: u32) -> PyResult<u64> {
    unpack_u64_bytes(data, byte_order)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
}

#[pyfunction]
fn unpack_u32(data: &[u8], byte_order: u32) -> PyResult<u32> {
    unpack_u32_bytes(data, byte_order)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
}

#[pyfunction]
fn host_byte_order() -> u32 {
    if cfg!(target_endian = "little") { BYTE_ORDER_LE } else { BYTE_ORDER_BE }
}

// ─── Checksums ─────────────────────────────────────────────────────────────────

#[pyfunction]
fn compute_checksum(data: &[u8]) -> String {
    crate::core::compute_checksum(data)
}

#[pyfunction]
fn verify_checksum(data: &[u8], expected: &str) -> bool {
    crate::core::verify_checksum(data, expected)
}

// ─── Certificate Generation ────────────────────────────────────────────────────

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

// ─── STUN Hole Punching ────────────────────────────────────────────────────────

#[pyfunction]
fn stun_punch_hole(stun_server: String, local_port: u16, peer_addr: String) -> PyResult<String> {
    crate::core::stun_punch_hole(stun_server, local_port, peer_addr)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
}

// ─── Reorder Buffer Wrapper ────────────────────────────────────────────────────

#[pyclass]
struct ReorderBuffer {
    inner: ConduitReorderBuffer,
}

#[pymethods]
impl ReorderBuffer {
    #[new]
    #[pyo3(signature = (max_gap=64, max_buf=1024))]
    fn new(max_gap: u64, max_buf: usize) -> Self {
        ReorderBuffer {
            inner: ConduitReorderBuffer::new(max_gap, max_buf),
        }
    }

    fn push(&mut self, stream_id: u32, sequence_id: u64, payload: Vec<u8>) -> bool {
        self.inner.push(stream_id, sequence_id, payload)
    }

    fn drain_ready<'py>(&mut self, py: Python<'py>, stream_id: u32) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty_bound(py);
        let items = self.inner.drain_ready(stream_id);
        for (seq, payload) in items {
            let item = pyo3::types::PyTuple::new_bound(
                py,
                &[seq.into_py(py).into_bound(py), PyBytes::new_bound(py, &payload).into_any()]
            );
            list.append(item)?;
        }
        Ok(list)
    }

    fn next_expected(&self, stream_id: u32) -> u64 {
        self.inner.next_expected(stream_id)
    }

    fn buffered_count(&self, stream_id: u32) -> usize {
        self.inner.buffered_count(stream_id)
    }

    fn reset_stream(&mut self, stream_id: u32) {
        self.inner.reset_stream(stream_id);
    }

    fn reset_all(&mut self) {
        self.inner.reset_all();
    }

    fn stream_ids<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty_bound(py);
        for id in self.inner.stream_ids() {
            list.append(id)?;
        }
        Ok(list)
    }
}

// ─── Sequence Counter Wrapper ──────────────────────────────────────────────────

#[pyclass]
struct SequenceCounter {
    inner: ConduitSequenceCounter,
}

#[pymethods]
impl SequenceCounter {
    #[new]
    fn new() -> Self {
        SequenceCounter {
            inner: ConduitSequenceCounter::new(),
        }
    }

    fn next(&mut self, stream_id: u32) -> u64 {
        self.inner.next(stream_id)
    }

    fn peek(&self, stream_id: u32) -> u64 {
        self.inner.peek(stream_id)
    }

    fn reset_stream(&mut self, stream_id: u32) {
        self.inner.reset_stream(stream_id);
    }

    fn reset_all(&mut self) {
        self.inner.reset_all();
    }
}

// ─── Route Cache Wrapper ───────────────────────────────────────────────────────

#[pyclass]
struct RouteCache {
    inner: ConduitRouteCache,
}

#[pymethods]
impl RouteCache {
    #[new]
    fn new(path: String) -> PyResult<Self> {
        let inner = ConduitRouteCache::new(path)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(RouteCache { inner })
    }

    fn set_route(&self, destination: String, next_hop: String) -> PyResult<()> {
        self.inner.set_route(&destination, &next_hop)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    fn get_route(&self, destination: &str) -> PyResult<Option<String>> {
        self.inner.get_route(destination)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    fn remove_route(&self, destination: &str) -> PyResult<()> {
        self.inner.remove_route(destination)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    fn list_routes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty_bound(py);
        let routes = self.inner.list_routes()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        for (dest, hop) in routes {
            list.append(pyo3::types::PyTuple::new_bound(py, &[dest, hop]))?;
        }
        Ok(list)
    }

    fn route_count(&self) -> usize {
        self.inner.route_count()
    }

    fn clear(&self) -> PyResult<()> {
        self.inner.clear()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    fn set_meta(&self, key: String, value: Vec<u8>) -> PyResult<()> {
        self.inner.set_meta(&key, &value)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    fn get_meta(&self, key: &str) -> PyResult<Option<Vec<u8>>> {
        self.inner.get_meta(key)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    fn track_connection(&self, client_id: &str) -> PyResult<()> {
        self.inner.track_connection(client_id)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    fn untrack_connection(&self, client_id: &str) -> PyResult<()> {
        self.inner.untrack_connection(client_id)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    fn get_last_seen(&self, client_id: &str) -> PyResult<Option<u64>> {
        self.inner.get_last_seen(client_id)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    fn list_connections<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty_bound(py);
        let conns = self.inner.list_connections()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        for id in conns {
            list.append(id)?;
        }
        Ok(list)
    }

    fn path(&self) -> &str {
        self.inner.path()
    }

    fn flush(&self) -> PyResult<()> {
        self.inner.flush()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }
}

// ─── QUIC Client Wrapper ───────────────────────────────────────────────────────

#[pyclass]
struct RustQUICClient {
    rt:       Option<Runtime>,
    inner:    Option<ConduitClient>,
    _tx_stop: Option<mpsc::Sender<()>>,
}

#[pymethods]
impl RustQUICClient {
    #[new]
    fn new() -> Self {
        RustQUICClient { rt: None, inner: None, _tx_stop: None }
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
        let rt = Runtime::new()?;
        let (tx_event, mut rx_event) = mpsc::channel::<ConduitEvent>(8192);

        let client_res = rt.block_on(async {
            ConduitClient::connect(&host, port, connect_timeout_secs, tx_event, local_port).await
        });

        match client_res {
            Ok(client) => {
                let py_callback = callback.clone_ref(py);
                
                rt.spawn(async move {
                    while let Some(evt) = rx_event.recv().await {
                        let (event_name, client_id, payload) = match evt {
                            ConduitEvent::Connect { client_id } => ("connect".to_string(), client_id, Vec::new()),
                            ConduitEvent::Message { client_id, payload } => ("message".to_string(), client_id, payload),
                            ConduitEvent::BinaryStream { client_id, decompressed_payload } => ("binary_stream".to_string(), client_id, decompressed_payload),
                            ConduitEvent::Disconnect { client_id } => ("disconnect".to_string(), client_id, Vec::new()),
                        };
                        Python::with_gil(|py| {
                            let bytes_payload = PyBytes::new_bound(py, &payload);
                            if let Err(e) = py_callback.call1(py, (event_name, client_id, bytes_payload)) {
                                e.print(py);
                            }
                        });
                    }
                });

                self.rt = Some(rt);
                self.inner = Some(client);
                Ok(true)
            }
            Err(e) => {
                eprintln!("[Rust Client] Connection failed: {}", e);
                Ok(false)
            }
        }
    }

    fn disconnect(&mut self) -> PyResult<()> {
        if let (Some(mut inner), Some(rt)) = (self.inner.take(), &self.rt) {
            rt.block_on(async move {
                inner.disconnect().await;
            });
        }
        if let Some(rt) = self.rt.take() {
            rt.shutdown_timeout(Duration::from_millis(200));
        }
        Ok(())
    }

    fn send_message(&self, py: Python<'_>, payload: Vec<u8>) -> PyResult<()> {
        if let (Some(inner), Some(rt)) = (&self.inner, &self.rt) {
            py.allow_threads(|| {
                rt.block_on(async move {
                    let _ = inner.send_message(&payload).await;
                });
            });
        }
        Ok(())
    }

    fn send_binary_stream(&self, py: Python<'_>, stream_name: String, data: Vec<u8>) -> PyResult<()> {
        if let (Some(inner), Some(rt)) = (&self.inner, &self.rt) {
            py.allow_threads(|| {
                rt.block_on(async move {
                    let _ = inner.send_binary_stream(&stream_name, data).await;
                });
            });
        }
        Ok(())
    }

    fn is_alive(&self) -> bool {
        self.inner.as_ref().map(|c| c.conn.close_reason().is_none()).unwrap_or(false)
    }

    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new_bound(py);
        if let Some(inner) = &self.inner {
            let s = inner.conn.stats();
            d.set_item("udp_tx_datagrams", s.udp_tx.datagrams)?;
            d.set_item("udp_rx_datagrams", s.udp_rx.datagrams)?;
            d.set_item("udp_tx_bytes",     s.udp_tx.bytes)?;
            d.set_item("udp_rx_bytes",     s.udp_rx.bytes)?;
        }
        Ok(d)
    }
}

// ─── QUIC Server Wrapper ───────────────────────────────────────────────────────

#[pyclass]
struct RustQUICServer {
    rt:    Option<Runtime>,
    inner: Option<ConduitServer>,
}

#[pymethods]
impl RustQUICServer {
    #[new]
    fn new() -> Self {
        RustQUICServer { rt: None, inner: None }
    }

    fn start(&mut self, py: Python<'_>, host: String, port: u16, callback: PyObject) -> PyResult<()> {
        let rt = Runtime::new()?;
        let (tx_event, mut rx_event) = mpsc::channel::<ConduitEvent>(8192);

        let server_res = rt.block_on(async {
            ConduitServer::start(&host, port, tx_event, 0).await
        });

        match server_res {
            Ok(server) => {
                let py_callback = callback.clone_ref(py);

                rt.spawn(async move {
                    while let Some(evt) = rx_event.recv().await {
                        let (event_name, client_id, payload) = match evt {
                            ConduitEvent::Connect { client_id } => ("connect".to_string(), client_id, Vec::new()),
                            ConduitEvent::Message { client_id, payload } => ("message".to_string(), client_id, payload),
                            ConduitEvent::BinaryStream { client_id, decompressed_payload } => ("binary_stream".to_string(), client_id, decompressed_payload),
                            ConduitEvent::Disconnect { client_id } => ("disconnect".to_string(), client_id, Vec::new()),
                        };
                        Python::with_gil(|py| {
                            let bytes_payload = PyBytes::new_bound(py, &payload);
                            if let Err(e) = py_callback.call1(py, (event_name, client_id, bytes_payload)) {
                                e.print(py);
                            }
                        });
                    }
                });

                self.rt = Some(rt);
                self.inner = Some(server);
                Ok(())
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())),
        }
    }

    fn stop(&mut self) -> PyResult<()> {
        if let Some(inner) = self.inner.take() {
            inner.stop();
        }
        if let Some(rt) = self.rt.take() {
            rt.shutdown_timeout(Duration::from_millis(500));
        }
        Ok(())
    }

    fn send_message(&self, client_id: String, payload: Vec<u8>) -> PyResult<()> {
        if let Some(inner) = &self.inner {
            inner.send_message(client_id, payload);
        }
        Ok(())
    }

    fn relay_raw(&self, client_id: String, raw_payload: Vec<u8>) -> PyResult<()> {
        if let Some(inner) = &self.inner {
            inner.send_message(client_id, raw_payload);
        }
        Ok(())
    }

    fn send_binary_stream(&self, py: Python<'_>, client_id: String, stream_name: String, data: Vec<u8>) -> PyResult<()> {
        if let (Some(inner), Some(rt)) = (&self.inner, &self.rt) {
            py.allow_threads(|| {
                rt.block_on(async move {
                    let _ = inner.send_binary_stream(&client_id, &stream_name, data).await;
                });
            });
        }
        Ok(())
    }

    fn connection_count(&self) -> usize {
        self.inner.as_ref().map(|s| s.connection_count()).unwrap_or(0)
    }

    fn is_connected(&self, client_id: &str) -> bool {
        self.inner.as_ref().map(|s| s.is_connected(client_id)).unwrap_or(false)
    }

    fn connected_clients<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty_bound(py);
        if let Some(inner) = &self.inner {
            for key in inner.connected_clients() {
                list.append(key)?;
            }
        }
        Ok(list)
    }
}

// ─── Module Registration ───────────────────────────────────────────────────────

#[pymodule]
fn netconduit_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

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
