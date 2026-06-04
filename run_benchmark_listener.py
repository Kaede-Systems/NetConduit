import asyncio
import time
import sys
from conduit import Client, ClientDescriptor

async def main():
    broker_host = sys.argv[1] if len(sys.argv) > 1 else "192.168.1.6"
    broker_port = int(sys.argv[2]) if len(sys.argv) > 2 else 36500

    print(f"Connecting listener 'client_b' to broker at {broker_host}:{broker_port}")

    client_b = Client(ClientDescriptor(
        server_host=broker_host,
        server_port=broker_port,
        password="p2p_test_password",
        name="client_b",
        reconnect_enabled=False,
    ))

    # Benchmark state
    bench_active = False
    expected_chunk_size = 0
    expected_chunks = 0
    received_chunks = 0
    received_bytes = 0
    start_time = 0.0
    end_time = 0.0

    @client_b.on_p2p_request
    async def on_p2p_req(source_id: str) -> bool:
        print(f"[Listener] Received P2P request from {source_id}, auto-accepting...")
        return True

    @client_b.on_p2p_established
    async def on_p2p_est(peer_client: Client):
        nonlocal bench_active, expected_chunk_size, expected_chunks, received_chunks, received_bytes, start_time, end_time
        print("[Listener] Direct P2P connection established with initiator!")

        @peer_client.rpc("peer_hello")
        async def peer_hello(name: str) -> str:
            print(f"[Listener] RPC 'peer_hello' invoked by {name}")
            return f"Hello {name} from homelab remote server!"

        @peer_client.rpc("start_benchmark")
        async def start_benchmark(chunk_size_bytes: int, total_chunks: int) -> bool:
            nonlocal bench_active, expected_chunk_size, expected_chunks, received_chunks, received_bytes, start_time, end_time
            expected_chunk_size = chunk_size_bytes
            expected_chunks = total_chunks
            received_chunks = 0
            received_bytes = 0
            start_time = time.perf_counter()
            bench_active = True
            print(f"\n[BENCHMARK START] Expecting {total_chunks} chunks of {chunk_size_bytes / (1024*1024):.1f} MB...")
            return True

        @peer_client.rpc("stop_benchmark")
        async def stop_benchmark() -> dict:
            nonlocal bench_active, received_chunks, received_bytes, start_time, end_time
            # Wait up to 10 seconds for all chunks to arrive
            for _ in range(100):
                if received_chunks >= expected_chunks:
                    break
                await asyncio.sleep(0.1)
                
            end_time = time.perf_counter()
            bench_active = False
            duration = end_time - start_time
            print(f"[BENCHMARK STOP] Received {received_chunks}/{expected_chunks} chunks. Duration: {duration:.3f} s")
            return {
                "received_chunks": received_chunks,
                "received_bytes": received_bytes,
                "duration": duration,
            }

        @peer_client.on_binary_stream
        async def handle_stream(stream_name: str, data: bytes):
            nonlocal received_chunks, received_bytes
            if stream_name == "bench_chunk":
                received_chunks += 1
                received_bytes += len(data)
                if received_chunks % 10 == 0 or received_chunks == expected_chunks:
                    print(f"  Recv progress: {received_chunks}/{expected_chunks} chunks ({received_bytes / (1024*1024):.1f} MB)")

        @peer_client.on("bench_compress")
        async def handle_compress(msg_data: bytes):
            # The msg_data has been decompressed automatically by python decoder
            print(f"[Listener] Received compressed message payload of size: {len(msg_data) / (1024*1024):.2f} MB")

    connected = await client_b.connect()
    if not connected:
        print("Failed to connect to broker server.")
        return

    print("Listener 'client_b' is connected and waiting for incoming P2P requests...")
    try:
        while True:
            await asyncio.sleep(1)
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        await client_b.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
