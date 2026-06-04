import asyncio
import time
import argparse
import sys
from conduit import Server, ServerDescriptor, Client, ClientDescriptor

async def main():
    parser = argparse.ArgumentParser(description="NetConduit P2P Large Data Transfer Benchmark")
    parser.add_argument("--size-gb", type=float, default=5.0, help="Total data size to transfer in GB (default: 5.0)")
    parser.add_argument("--chunk-mb", type=int, default=50, help="Chunk size in MB (default: 50)")
    parser.add_argument("--parallel", action="store_true", help="Send chunks in parallel using thread pool")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel workers/streams to use (default: 4)")
    args = parser.parse_args()

    total_size_bytes = int(args.size_gb * 1024 * 1024 * 1024)
    chunk_size_bytes = args.chunk_mb * 1024 * 1024
    total_chunks = total_size_bytes // chunk_size_bytes
    actual_size_gb = (total_chunks * chunk_size_bytes) / (1024 * 1024 * 1024)

    broker_port = 36500
    print(f"Starting broker server on 0.0.0.0:{broker_port}...")
    broker_server = Server(ServerDescriptor(
        name="broker_server",
        host="0.0.0.0",
        port=broker_port,
        password="p2p_test_password",
    ))
    await broker_server.start()
    
    import socket
    def get_local_ip():
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"

    local_ip = get_local_ip()
    print(f"Detected local IP: {local_ip}")

    print("Connecting initiator client_a to broker...")
    client_a = Client(ClientDescriptor(
        server_host=local_ip,
        server_port=broker_port,
        password="p2p_test_password",
        name="client_a",
        reconnect_enabled=False,
    ))
    
    assert await client_a.connect()
    print("Initiator client_a is connected to broker server.")
    print("Awaiting Client B (listener) to connect to broker server (waiting 20 seconds)...")
    await asyncio.sleep(20)
    
    print("Attempting direct P2P hole punching to client_b...")
    try:
        # Create peer client with compression enabled for message tests
        peer_client = await client_a.establish_p2p("client_b", timeout=15.0)
        print("UDP Hole Punching SUCCESS! Direct connection established.")
        
        # Test RPC hello
        res = await peer_client.rpc.call("peer_hello", name="kaedepc local machine")
        print(f"RPC Handshake: '{res}'")
        
        # Start benchmark on listener
        print(f"\n[BENCHMARK] Starting direct stream transfer of {actual_size_gb:.2f} GB ({total_chunks} chunks of {args.chunk_mb} MB)...")
        await peer_client.rpc.call("start_benchmark", chunk_size_bytes=chunk_size_bytes, total_chunks=total_chunks)
        
        # Generate dummy data block
        print("Generating dummy chunk data...")
        chunk_data = b"NETCONDUIT_BENCHMARK_BLOCK_" * ((chunk_size_bytes // 27) + 1)
        # Ensure exact size
        chunk_data = chunk_data[:chunk_size_bytes]
        
        start_time = time.perf_counter()
        
        # Send chunks
        if args.parallel:
            print(f"Sending {total_chunks} chunks in parallel using {args.workers} concurrent streams...")
            sem = asyncio.Semaphore(args.workers)
            tasks = []
            
            async def send_chunk_async(idx):
                async with sem:
                    t_chunk_start = time.perf_counter()
                    await asyncio.to_thread(peer_client.send_binary_stream, "bench_chunk", chunk_data)
                    t_chunk_end = time.perf_counter()
                    duration_ms = (t_chunk_end - t_chunk_start) * 1000.0
                    speed_mb = (chunk_size_bytes / (1024 * 1024)) / (duration_ms / 1000.0)
                    print(f"  Parallel chunk {idx+1}/{total_chunks} completed in {duration_ms:.1f} ms ({speed_mb:.1f} MB/s)")
                
            for i in range(total_chunks):
                tasks.append(asyncio.create_task(send_chunk_async(i)))
                
            await asyncio.gather(*tasks)
        else:
            print(f"Sending {total_chunks} chunks sequentially...")
            for i in range(total_chunks):
                t_chunk_start = time.perf_counter()
                peer_client.send_binary_stream("bench_chunk", chunk_data)
                t_chunk_end = time.perf_counter()
                duration_ms = (t_chunk_end - t_chunk_start) * 1000.0
                speed_mb = (chunk_size_bytes / (1024 * 1024)) / (duration_ms / 1000.0)
                print(f"  Sent chunk {i+1}/{total_chunks} ({args.chunk_mb} MB) in {duration_ms:.1f} ms ({speed_mb:.1f} MB/s)")
                await asyncio.sleep(0.01) # Short yield to keep event loop responsive
                
        end_time = time.perf_counter()
        local_duration = end_time - start_time
        
        # Stop benchmark and get listener stats
        print("Stopping benchmark and retrieving listener metrics...")
        stats = await peer_client.rpc.call("stop_benchmark")
        
        # Output throughput results
        received_bytes = stats.get("received_bytes", 0)
        listener_duration = stats.get("duration", 0.0)
        
        received_gb = received_bytes / (1024 * 1024 * 1024)
        throughput_mbs = (received_bytes / (1024 * 1024)) / listener_duration if listener_duration > 0 else 0
        throughput_gbps = (throughput_mbs * 8) / 1024
        
        print("\n" + "="*50)
        print("STREAM THROUGHPUT RESULTS")
        print("="*50)
        print(f"Data Transferred:    {received_gb:.3f} GB ({received_bytes} bytes)")
        print(f"Local Send Time:     {local_duration:.3f} seconds")
        print(f"Listener Recv Time:  {listener_duration:.3f} seconds")
        print(f"Throughput (Speed):  {throughput_mbs:.2f} MB/s ({throughput_gbps:.3f} Gbps)")
        print("="*50 + "\n")
        
        # Part 2: Compression Test
        print("[COMPRESSION TEST] Generating compressible payload...")
        # Create a highly compressible 5 MB string
        compressible_payload = ("A" * 1000 + "B" * 1000 + "C" * 1000) * 1700
        compressible_bytes = compressible_payload.encode('utf-8')
        
        print(f"Original Payload Size: {len(compressible_bytes) / (1024 * 1024):.2f} MB")
        
        # Send compressed message
        # Enable compression on the client
        peer_client._encoder.enable_compression = True
        
        # We can serialize and manually compress to see the ratio
        from conduit.protocol.encoder import _compress
        compressed_bytes, flags = _compress(compressible_bytes, 0)
        
        comp_ratio = (1.0 - (len(compressed_bytes) / len(compressible_bytes))) * 100.0
        print(f"Compressed Payload Size: {len(compressed_bytes) / 1024:.2f} KB")
        print(f"Compression Ratio:       {comp_ratio:.2f}%")
        
        # Send via normal message protocol to verify it transmits successfully
        t0 = time.perf_counter()
        await peer_client.send("bench_compress", compressible_bytes)
        t1 = time.perf_counter()
        print(f"Compressed message sent successfully in {(t1 - t0) * 1000.0:.2f} ms")
        
        # Clean up P2P connection client
        await asyncio.sleep(2)
        print("Done testing.")
        
    except Exception as e:
        print(f"Benchmark failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await client_a.disconnect()
        await broker_server.stop()

if __name__ == "__main__":
    asyncio.run(main())
