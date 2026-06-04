import asyncio
import time
from conduit import Server, ServerDescriptor, Client, ClientDescriptor, data

async def main():
    broker_port = 36500
    print(f"Starting broker server on 0.0.0.0:{broker_port}...")
    broker_server = Server(ServerDescriptor(
        name="broker_server",
        host="0.0.0.0",
        port=broker_port,
        password="p2p_test_password",
    ))
    await broker_server.start()
    
    print("Connecting initiator client_a to broker...")
    client_a = Client(ClientDescriptor(
        server_host="127.0.0.1",
        server_port=broker_port,
        password="p2p_test_password",
        name="client_a",
        reconnect_enabled=False,
    ))
    
    assert await client_a.connect()
    print("Initiator client_a is connected to broker server.")
    print("Awaiting Client B to connect to broker server (waiting 20 seconds)...")
    await asyncio.sleep(20)
    
    print("Attempting direct P2P hole punching to client_b...")
    try:
        peer_client = await client_a.establish_p2p("client_b", timeout=10.0)
        print("UDP Hole Punching SUCCESS! Direct connection established.")
        
        # Test direct RPC call and measure latency
        print("Invoking remote RPC 'peer_hello' on homelab peer client...")
        res = await peer_client.rpc.call("peer_hello", name="kaedepc local machine")
        print(f"RPC Response: '{res}'")
        
        print("\nMeasuring RPC latency over 10 iterations...")
        latencies = []
        for i in range(10):
            t0 = time.perf_counter()
            await peer_client.rpc.call("peer_hello", name=f"lat_test_{i}")
            t1 = time.perf_counter()
            latency_ms = (t1 - t0) * 1000.0
            latencies.append(latency_ms)
            print(f"  Iteration {i+1}: {latency_ms:.2f} ms")
            await asyncio.sleep(0.05)
            
        avg_lat = sum(latencies) / len(latencies)
        min_lat = min(latencies)
        max_lat = max(latencies)
        print(f"\nLatency Results (RPC round-trip):")
        print(f"  Min: {min_lat:.2f} ms")
        print(f"  Max: {max_lat:.2f} ms")
        print(f"  Avg: {avg_lat:.2f} ms\n")
        
        # Test direct messaging
        print("Sending direct message 'peer_chat' to homelab peer client...")
        await peer_client.send("peer_chat", {"text": "Hello remote peer from locally punched UDP socket!"})
        
        await asyncio.sleep(1)
        print("Done testing.")
        
    except Exception as e:
        print(f"P2P connection or RPC failed: {e}")
    finally:
        await client_a.disconnect()
        await broker_server.stop()

if __name__ == "__main__":
    asyncio.run(main())
