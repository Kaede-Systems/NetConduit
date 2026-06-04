import asyncio
import sys
from conduit import Client, ClientDescriptor, data

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

    @client_b.on_p2p_request
    async def on_p2p_req(source_id: str) -> bool:
        print(f"[Listener] Received P2P request from {source_id}, auto-accepting...")
        return True

    @client_b.on_p2p_established
    async def on_p2p_est(peer_client: Client):
        print("[Listener] Direct P2P connection established with initiator!")

        @peer_client.rpc("peer_hello")
        async def peer_hello(name: str) -> str:
            print(f"[Listener] RPC 'peer_hello' invoked by {name}")
            return f"Hello {name} from homelab remote server!"

        @peer_client.on("peer_chat")
        async def handle_chat(msg_data: dict):
            print(f"[Listener] Received chat message: {msg_data}")

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
