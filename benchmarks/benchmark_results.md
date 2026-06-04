# WebSocket vs netconduit Benchmark Results

*Generated: 2026-06-04 19:22:18*

## Summary

- **netconduit wins**: 3
- **WebSocket wins**: 5
- **Ties**: 0

## Detailed Results

| Test | Metric | netconduit | WebSocket | Winner |
|------|--------|------------|-----------|--------|
| Connection | Avg Time (ms) | 2.92 | 1.09 | **websocket** |
| Throughput (Seq) | Messages/sec (msg/s) | 2376 | 18948 | **websocket** |
| Throughput (Concur) | Messages/sec (msg/s) | 11768 | 63539 | **websocket** |
| Latency | Round-trip (ms) | 0.43 | 0.05 | **websocket** |
| File (Compressible) | Speed (MB/s) | 119.96 | 328.43 | **websocket** |
| File (Random) | Speed (MB/s) | 98.31 | 33.55 | **netconduit** |
| Memory | Per Connection (KB) | 5.00 | 8.00 | **netconduit** |
| Code | Lines for same features (lines) | 25.00 | 60.00 | **netconduit** |

## Analysis


### Analysis
WebSocket showed better raw ping-pong performance under Python event loop scheduling. However, consider:

- netconduit provides built-in RPC, auth, transparent compression, and file transfer
- WebSocket requires additional application code/libraries for these features
- NetConduit is backed by a native Rust core, making it highly suitable for high-throughput Native and Flutter applications.

### When to use netconduit
- Server-to-server communication
- Native and Mobile (Flutter/Rust) applications
- High-throughput binary streaming & transparent compression

### When to use WebSocket
- Browser clients required
- HTTP proxy environments
