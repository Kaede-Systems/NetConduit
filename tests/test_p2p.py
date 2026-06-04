import pytest
import asyncio
import socket
from typing import Any

from conduit import (
    Server,
    Client,
    ServerDescriptor,
    ClientDescriptor,
    Message,
)

def get_free_port() -> int:
    """Get a free port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port

@pytest.mark.asyncio
async def test_p2p_direct_connection():
    # 1. Setup Broker Server
    broker_port = get_free_port()
    broker_server = Server(ServerDescriptor(
        name="broker_server",
        host="127.0.0.1",
        port=broker_port,
        password="broker_password",
    ))
    await broker_server.start()
    
    # 2. Setup Client A (Initiator) and Client B (Listener)
    client_a = Client(ClientDescriptor(
        server_host="127.0.0.1",
        server_port=broker_port,
        password="broker_password",
        name="client_a",
        reconnect_enabled=False,
    ))
    
    client_b = Client(ClientDescriptor(
        server_host="127.0.0.1",
        server_port=broker_port,
        password="broker_password",
        name="client_b",
        reconnect_enabled=False,
    ))
    
    assert await client_a.connect()
    assert await client_b.connect()
    
    # Future to hold B's peer client once established
    peer_client_b_future = asyncio.get_running_loop().create_future()
    # Future to verify messages sent from A to B
    msg_from_a_future = asyncio.get_running_loop().create_future()
    
    # Configure Client B to accept incoming P2P requests
    @client_b.on_p2p_request
    async def on_p2p_req(source_id: str) -> bool:
        return True
        
    @client_b.on_p2p_established
    async def on_p2p_est(peer_client: Client):
        # Register a local RPC method on B's peer client
        @peer_client.rpc("peer_add")
        async def peer_add(a: int, b: int) -> int:
            return a + b
            
        @peer_client.on("peer_msg")
        async def handle_peer_msg(data: Any):
            msg_from_a_future.set_result(data)
            
        peer_client_b_future.set_result(peer_client)
        
    try:
        # 3. Establish P2P Connection from A to B
        peer_client_a = await client_a.establish_p2p("client_b", timeout=5.0)
        assert peer_client_a is not None
        assert peer_client_a.is_connected
        
        # Register local RPC method on A's peer client
        @peer_client_a.rpc("peer_multiply")
        async def peer_multiply(x: float, y: float) -> float:
            return x * y
            
        # Wait for Client B to establish its peer client mapping
        peer_client_b = await asyncio.wait_for(peer_client_b_future, timeout=5.0)
        assert peer_client_b is not None
        
        # 4. Perform symmetric RPC and messaging checks
        
        # Test RPC from Client A to Client B
        res_add = await peer_client_a.rpc.call("peer_add", a=40, b=2)
        assert res_add == 42
        
        # Test RPC from Client B to Client A
        res_mult = await peer_client_b.rpc.call("peer_multiply", x=3.5, y=2.0)
        assert res_mult == 7.0
        
        # Test Message from Client A to Client B
        await peer_client_a.send("peer_msg", {"greeting": "hello from initiator"})
        received_msg = await asyncio.wait_for(msg_from_a_future, timeout=5.0)
        assert received_msg == {"greeting": "hello from initiator"}
        
    finally:
        # Clean up connections and servers
        await client_a.disconnect()
        await client_b.disconnect()
        await broker_server.stop()

def test_stun_dns_resolution():
    from netconduit_core import stun_punch_hole
    # Calling stun_punch_hole with a DNS hostname should not raise value error
    try:
        res = stun_punch_hole("stun.l.google.com:19302", 0, "")
        assert isinstance(res, str)
    except Exception as e:
        pytest.fail(f"stun_punch_hole failed with: {e}")

