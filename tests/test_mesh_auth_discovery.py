import pytest
import asyncio
import socket
from pydantic import BaseModel
from typing import Any

from conduit import (
    Server,
    Client,
    ServerDescriptor,
    ClientDescriptor,
    Message,
)
from conduit.auth.credentials import CredentialsManager

def get_free_port() -> int:
    """Get a free port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port

@pytest.mark.asyncio
async def test_credentials_and_gating():
    port = get_free_port()
    
    server = Server(ServerDescriptor(
        name="auth_server",
        host="127.0.0.1",
        port=port,
        password="ignored_due_to_credentials_manager",
    ))
    
    # Set up credentials manager
    cm = CredentialsManager()
    # admin has role admin and write permission
    cm.add_user(username="admin_user", password="admin_password", roles=["admin"], permissions=["write"])
    # guest has role guest and read permission
    cm.add_user(username="guest_user", password="guest_password", roles=["guest"], permissions=["read"])
    
    server.register_credentials_manager(cm)
    
    # Handlers with security gating
    admin_rpc_called = False
    guest_rpc_called = False
    
    @server.rpc(requires_role="admin")
    async def admin_rpc():
        nonlocal admin_rpc_called
        admin_rpc_called = True
        return "admin_ok"
        
    @server.rpc(requires_role="guest")
    async def guest_rpc():
        nonlocal guest_rpc_called
        guest_rpc_called = True
        return "guest_ok"
        
    admin_msg_received = False
    write_msg_received = False
    gate_msg_received = False
    
    @server.on("admin_msg", requires_role="admin")
    async def handle_admin_msg(conn, data):
        nonlocal admin_msg_received
        admin_msg_received = True
        
    @server.on("write_msg", requires_permission="write")
    async def handle_write_msg(conn, data):
        nonlocal write_msg_received
        write_msg_received = True
        
    @server.on("gated_msg", gate=lambda conn: False)
    async def handle_gated_msg(conn, data):
        nonlocal gate_msg_received
        gate_msg_received = True

    await server.start()
    
    # 1. Connect admin client
    admin_client = Client(ClientDescriptor(
        server_host="127.0.0.1",
        server_port=port,
        username="admin_user",
        password="admin_password",
        reconnect_enabled=False,
        name="admin_client",
    ))
    
    # 2. Connect guest client
    guest_client = Client(ClientDescriptor(
        server_host="127.0.0.1",
        server_port=port,
        username="guest_user",
        password="guest_password",
        reconnect_enabled=False,
        name="guest_client",
    ))
    
    assert await admin_client.connect()
    assert await guest_client.connect()
    
    try:
        # Test RPC gating
        res = await admin_client.rpc.call("admin_rpc")
        assert res == "admin_ok"
        assert admin_rpc_called
        
        # Admin should fail on guest_rpc
        with pytest.raises(Exception):
            await admin_client.rpc.call("guest_rpc")
            
        # Guest should fail on admin_rpc
        with pytest.raises(Exception):
            await guest_client.rpc.call("admin_rpc")
            
        # Guest should succeed on guest_rpc
        res2 = await guest_client.rpc.call("guest_rpc")
        assert res2 == "guest_ok"
        assert guest_rpc_called
        
        # Test message gating
        await admin_client.send("admin_msg", {})
        await admin_client.send("write_msg", {})
        await admin_client.send("gated_msg", {})
        
        await asyncio.sleep(0.2)
        assert admin_msg_received
        assert write_msg_received
        assert not gate_msg_received
        
        # Guest attempts to send write_msg (should be rejected/dropped on server side)
        write_msg_received = False
        await guest_client.send("write_msg", {})
        await asyncio.sleep(0.2)
        assert not write_msg_received
        
    finally:
        await admin_client.disconnect()
        await guest_client.disconnect()
        await server.stop()

@pytest.mark.asyncio
async def test_mdns_discovery():
    port = get_free_port()
    server = Server(ServerDescriptor(
        name="mdns_discoverable_server",
        host="127.0.0.1",
        port=port,
        password="test",
    ))
    await server.start()
    await server.start_discovery()
    
    # Create client to perform discovery
    client = Client(ClientDescriptor(
        server_host="127.0.0.1",
        server_port=port,
        password="test",
        reconnect_enabled=False,
    ))
    
    try:
        # Try local discovery
        peers = await client.discover_peers(timeout=1.0)
        # Check if our server is in the list
        found = False
        for p in peers:
            if p["name"] == "mdns_discoverable_server":
                found = True
                break
        
        # Sometimes multicast doesn't route cleanly on all systems (e.g. loopback multicast rules)
        # So we also test remote unicast discovery by specifying the host
        if not found:
            peers = await client.discover_peers(timeout=1.0, remote_hosts=["127.0.0.1"])
            for p in peers:
                if p["name"] == "mdns_discoverable_server":
                    found = True
                    break
        
        assert found, f"Could not discover mdns_discoverable_server in {peers}"
        
    finally:
        await server.stop_discovery()
        await server.stop()

@pytest.mark.asyncio
async def test_mesh_relay_network():
    port = get_free_port()
    
    server_a = Server(ServerDescriptor(
        name="server_a",
        host="127.0.0.1",
        port=port,
        password="test",
    ))
    
    # Client B acts as the intermediate relay
    client_b = Client(ClientDescriptor(
        server_host="127.0.0.1",
        server_port=port,
        password="test",
        name="client_b",
        reconnect_enabled=False,
    ))
    
    # Client C acts as the end client
    client_c = Client(ClientDescriptor(
        server_host="127.0.0.1",
        server_port=port,
        password="test",
        name="client_c",
        reconnect_enabled=False,
    ))
    
    # Setup virtual next-hop links
    # C maps server_a to next-hop B
    client_c._peer_clients["server_a"] = client_b
    # B maps client_c to C
    client_b._peer_clients["client_c"] = client_c
    
    await server_a.start()
    assert await client_b.connect()
    
    # Handler on server_a for encrypted mesh chat
    mesh_chat_received = False
    
    @server_a.on("mesh_chat")
    async def handle_mesh_chat(conn, data):
        nonlocal mesh_chat_received
        mesh_chat_received = True
        return {"reply": "hello_from_server_a"}
        
    # Handler on client_c for server response
    replies = []
    @client_c.on("mesh_chat_response")
    async def handle_reply(data):
        replies.append(data)
        
    try:
        # Establish E2E TLS tunnel from C to A
        await client_c.establish_mesh_tunnel("server_a")
        
        # Give some time for the final finished handshake packet to reach and complete on the server side
        await asyncio.sleep(0.1)
        
        # Verify handshake completed and both sides created tunnels
        assert "server_a" in client_c._mesh_tunnels
        assert "client_c" in server_a._mesh_tunnels
        assert client_c._mesh_tunnels["server_a"].handshake_done
        assert server_a._mesh_tunnels["client_c"].handshake_done
        
        # Send mesh secure message
        await client_c.send_mesh_secure_message("server_a", "mesh_chat", {"text": "hello_mesh"})
        
        await asyncio.sleep(0.5)
        
        assert mesh_chat_received
        assert len(replies) == 1
        assert replies[0]["reply"] == "hello_from_server_a"
        
    finally:
        await client_b.disconnect()
        await server_a.stop()
