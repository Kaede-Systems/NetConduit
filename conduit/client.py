"""
Conduit Client using Rust QUIC transport underneath.
"""

import asyncio
import hashlib
import time
import logging
from typing import Any, Callable, Dict, List, Optional, Awaitable

from netconduit_core import RustQUICClient, stun_punch_hole

from .data.descriptors import ClientDescriptor
from .protocol import ProtocolEncoder, ProtocolDecoder, MessageType, DecodedMessage
from .connection import Connection
from .messages import MessageRouter, Message
from .rpc import RPC, data

logger = logging.getLogger(__name__)


# Callback types
LifecycleHook = Callable[['Client'], Awaitable[None]]
MessageHandler = Callable[[Any], Awaitable[Any]]
BinaryStreamHandler = Callable[[str, bytes], Awaitable[None]]


class Client:
    """
    Conduit Client using Rust QUIC.
    """
    
    def __init__(self, config: ClientDescriptor):
        """
        Initialize client.
        
        Args:
            config: Client configuration
        """
        self._config = config
        self._rust_client = None
        self._connection = None
        self._connect_lock = asyncio.Lock()
        
        # Protocol
        self._encoder = ProtocolEncoder(enable_compression=config.enable_compression)
        self._decoder = ProtocolDecoder()
        
        # Message routing
        self._message_router = MessageRouter()
        
        # RPC interface
        self._rpc = RPC(self, default_timeout=config.rpc_timeout)
        self._pending_rpcs = {}
        from .rpc.registry import RPCRegistry
        from .rpc.dispatcher import RPCDispatcher
        self._rpc_registry = RPCRegistry()
        self._rpc_dispatcher = RPCDispatcher(self._rpc_registry)
        
        # Session info
        self._session_token = None
        self._server_info = {}
        
        # Lifecycle hooks
        self._on_connect = []
        self._on_disconnect = []
        self._on_reconnect = []
        self._on_binary_stream = None
        self._on_p2p_established = None
        
        # Reconnection state
        self._reconnect_attempts = 0
        self._should_reconnect = True
        self._connect_task = None
        self._reconnect_task = None
        self._auth_future = None
        
        # Mesh network and discovery state
        self._mesh_tunnels = {}
        self._peer_clients = {}
        
        # P2P Setup
        self._setup_p2p_handlers()
    
    # === Decorators ===
    
    def on(self, message_type: str) -> Callable:
        """Register a message handler decorator."""
        def decorator(handler: MessageHandler) -> MessageHandler:
            async def wrapper(conn, data):
                return await handler(data)
            
            self._message_router.register(
                message_type=message_type,
                handler=wrapper,
                requires_auth=False,
            )
            return handler
        return decorator
    
    def on_connect(self, handler: LifecycleHook) -> LifecycleHook:
        """Register connect hook."""
        self._on_connect.append(handler)
        return handler
    
    def on_disconnect(self, handler: LifecycleHook) -> LifecycleHook:
        """Register disconnect hook."""
        self._on_disconnect.append(handler)
        return handler
    
    def on_reconnect(self, handler: LifecycleHook) -> LifecycleHook:
        """Register reconnect hook."""
        self._on_reconnect.append(handler)
        return handler
        
    def on_binary_stream(self, handler: BinaryStreamHandler) -> BinaryStreamHandler:
        """Register binary stream callback decorator."""
        self._on_binary_stream = handler
        return handler
    
    # === Connection Lifecycle ===
    
    async def connect(self) -> bool:
        """Connect to the server."""
        self._connect_task = asyncio.current_task()
        try:
            async with self._connect_lock:
                self._should_reconnect = True
                return await self._do_connect()
        finally:
            self._connect_task = None
    
    async def _do_connect(self) -> bool:
        """Perform the actual connection."""
        try:
            logger.info(f"Connecting to {self._config.server_host}:{self._config.server_port}")
            self._rust_client = RustQUICClient()
            self._loop = asyncio.get_running_loop()
            
            # UDP Hole Punching via STUN if server or config provides STUN server address
            def is_loopback(h: str) -> bool:
                hl = h.lower()
                return hl in ("localhost", "127.0.0.1", "::1") or hl.startswith("127.")
                
            if not is_loopback(self._config.server_host):
                stun_server = self._config.stun_server
                local_port = getattr(self._config, "local_port", 0) or 0
                peer_addr = f"{self._config.server_host}:{self._config.server_port}"
                
                try:
                    logger.info(f"STUN hole punching via {stun_server} from port {local_port} to {peer_addr}")
                    mapped = stun_punch_hole(stun_server, local_port, peer_addr)
                    if mapped:
                        logger.info(f"STUN mapped address successfully resolved: {mapped}")
                except Exception as e:
                    logger.warning(f"STUN hole punching resolution failed (normal behind symmetric NAT or local connections): {e}")

            def rust_callback(event_type, client_id, data):
                self._loop.call_soon_threadsafe(self._handle_rust_event, event_type, client_id, bytes(data))

            local_port = getattr(self._config, "local_port", 0) or 0
            connected = self._rust_client.connect(
                self._config.server_host,
                self._config.server_port,
                self._config.connect_timeout,
                rust_callback,
                local_port
            )
            if not connected:
                logger.error("Rust QUIC connection failed to connect.")
                return False
            
            self._connection = Connection(
                rust_transport=self._rust_client,
                encoder=self._encoder,
                decoder=self._decoder
            )
            
            # Authenticate
            self._auth_future = self._loop.create_future()
            
            password_hash = hashlib.sha256(
                self._config.password.encode('utf-8')
            ).hexdigest()
            
            client_info = {
                "name": self._config.name,
                "version": self._config.version,
            }
            if self._config.username:
                client_info["username"] = self._config.username

            auth_msg = self._encoder.encode_auth_request(
                password_hash=password_hash,
                client_info=client_info
            )
            
            self._rust_client.send_message(auth_msg)
            
            auth_ok = await asyncio.wait_for(self._auth_future, timeout=self._config.connect_timeout)
            if not auth_ok:
                logger.error("Authentication failed.")
                await self.disconnect()
                return False
            
            self._connection.mark_authenticated()
            self._reconnect_attempts = 0
            
            for hook in self._on_connect:
                try:
                    await hook(self)
                except Exception as e:
                    logger.error(f"Error in connect hook: {e}")
            
            logger.info("Client connected and authenticated successfully")
            return True
            
        except Exception as e:
            logger.error(f"QUIC connection error: {e}")
            return False
            
    def _handle_rust_event(self, event_type: str, client_id: str, payload: bytes):
        if event_type == "message":
            try:
                decoded = self._decoder.decode_single(payload)
                if decoded.is_mesh and decoded.route_dst != self._config.name:
                    asyncio.create_task(self._route_mesh_message(decoded.route_dst, payload))
                    return
                asyncio.create_task(self._process_message(decoded))
            except Exception as e:
                logger.error(f"Failed to decode message: {e}")
        elif event_type == "binary_stream":
            asyncio.create_task(self._process_binary_stream(payload))
        elif event_type == "disconnect":
            asyncio.create_task(self._handle_disconnect())
            
    async def _route_mesh_message(self, dst: str, raw_payload: bytes):
        """Zero-copy relay for mesh messages."""
        peer = self._peer_clients.get(dst)
        if peer:
            if hasattr(peer, "_handle_rust_event"):
                peer._handle_rust_event("message", self._config.name, raw_payload)
            elif hasattr(peer, "send_raw"):
                await peer.send_raw(raw_payload)
            elif hasattr(peer, "send_message"):
                if asyncio.iscoroutinefunction(peer.send_message):
                    await peer.send_message(raw_payload)
                else:
                    peer.send_message(raw_payload)
        else:
            # If not in peer_clients, route it back to our connected server
            if self._rust_client:
                self._rust_client.send_message(raw_payload)
            
    async def _process_message(self, decoded: DecodedMessage):
        # Check mesh routing targeting this client
        if decoded.is_mesh and decoded.route_dst == self._config.name:
            src = decoded.route_src
            msg_type_str = decoded.get_message_type_str()
            
            if msg_type_str == "mesh_handshake":
                handshake_data = decoded.get_data()
                tunnel = self._mesh_tunnels.get(src)
                if not tunnel:
                    from .mesh.tunnel import MemoryTLSTunnel
                    tunnel = MemoryTLSTunnel(is_server=True)
                    self._mesh_tunnels[src] = tunnel
                
                tunnel.feed_encrypted(handshake_data)
                completed, response_handshake = tunnel.do_handshake()
                if response_handshake:
                    from .protocol.protocol_pb2 import MessagePayload
                    from .protocol.encoder import serialize_data
                    inner_payload = MessagePayload(
                        type="mesh_handshake",
                        data=serialize_data(response_handshake)
                    )
                    response_packet = self._encoder.encode(
                        MessageType.MESSAGE,
                        payload_bytes=inner_payload.SerializeToString(),
                        route_src=self._config.name,
                        route_dst=src,
                        is_mesh=True
                    )
                    await self._send_mesh_raw(src, response_packet)
                return

            elif msg_type_str == "mesh_secure":
                encrypted_data = decoded.get_data()
                tunnel = self._mesh_tunnels.get(src)
                if tunnel:
                    decrypted = tunnel.feed_encrypted(encrypted_data)
                    inner_decoded = self._decoder.decode_single(decrypted)
                    inner_msg_type_str = inner_decoded.get_message_type_str()
                    inner_data = inner_decoded.get_data()
                    
                    logger.info(f"[Mesh Client E2E] Decrypted inner message: {inner_msg_type_str}")
                    msg = Message(type=inner_msg_type_str, data=inner_data)
                    await self._message_router.route(
                        message=msg,
                        context=self,
                        authenticated=True
                    )
                return

        msg_type = decoded.message_type
        
        if msg_type == MessageType.AUTH_SUCCESS:
            self._session_token = decoded.payload.get("session_token")
            self._server_info = dict(decoded.payload.get("server_info", {}))
            if self._connection:
                self._connection.set_session(self._session_token)
            if self._auth_future and not self._auth_future.done():
                self._auth_future.set_result(True)
            return

        if msg_type == MessageType.AUTH_FAILURE:
            if self._auth_future and not self._auth_future.done():
                self._auth_future.set_result(False)
            return

        # Handle RPC requests (for P2P connections)
        if msg_type == MessageType.RPC_REQUEST:
            method = decoded.get_rpc_method()
            params = decoded.get_rpc_params() or {}
            corr_id = decoded.correlation_id
            
            response = await self._rpc_dispatcher.dispatch(
                method=method,
                params=params,
                authenticated=True
            )
            if response.get("success", False):
                res_val = response.get("result") if "result" in response else response.get("data")
                await self._connection.send_rpc_response(res_val, corr_id)
            else:
                await self._connection.send_rpc_error(
                    response.get("error", "Unknown RPC error"),
                    corr_id,
                    code=response.get("code")
                )
            return

        # Direct RPC interception on the connection
        if self._connection:
            intercepted = self._connection.handle_decoded_message(decoded)
            if intercepted:
                return

        # Fallback RPC future resolution
        if msg_type in (MessageType.RPC_RESPONSE, MessageType.RPC_ERROR):
            corr_id = decoded.correlation_id
            future = self._pending_rpcs.get(corr_id)
            if future and not future.done():
                future.set_result(decoded.payload)
            return

        # Route standard messages
        if msg_type == MessageType.MESSAGE:
            msg_type_str = decoded.get_message_type_str()
            data = decoded.get_data()
            
            msg = Message(type=msg_type_str, data=data)
            await self._message_router.route(
                message=msg,
                context=self,
                authenticated=True,
            )
            
    async def _process_binary_stream(self, payload: bytes):
        if len(payload) < 4:
            return
        name_len = int.from_bytes(payload[:4], byteorder='big')
        if len(payload) < 4 + name_len:
            return
        stream_name = payload[4:4+name_len].decode('utf-8')
        data = payload[4+name_len:]
        if self._on_binary_stream:
            try:
                await self._on_binary_stream(stream_name, data)
            except Exception as e:
                logger.error(f"Error in binary stream handler: {e}")
                
    async def _handle_disconnect(self):
        logger.info("Disconnected from server")
        if self._auth_future and not self._auth_future.done():
            self._auth_future.set_result(False)
        was_connected = self._connection is not None
        if self._connection:
            await self._connection.stop()
            self._connection = None
            
        if was_connected:
            for hook in self._on_disconnect:
                try:
                    await hook(self)
                except Exception as e:
                    logger.error(f"Error in disconnect hook: {e}")
                
        if self._should_reconnect and self._config.reconnect_enabled:
            if not self._reconnect_task or self._reconnect_task.done():
                self._reconnect_task = asyncio.create_task(self._reconnect_loop())
                
    async def _reconnect_loop(self):
        delay = self._config.reconnect_delay
        while self._should_reconnect:
            max_attempts = self._config.reconnect_attempts
            if max_attempts > 0 and self._reconnect_attempts >= max_attempts:
                logger.error(f"Max reconnection attempts ({max_attempts}) reached")
                break
                
            self._reconnect_attempts += 1
            logger.info(f"Reconnecting attempt {self._reconnect_attempts}...")
            await asyncio.sleep(delay)
            if await self._do_connect():
                for hook in self._on_reconnect:
                    try:
                        await hook(self)
                    except Exception as e:
                        logger.error(f"Error in reconnect hook: {e}")
                return
            delay = min(delay * self._config.reconnect_delay_multiplier, self._config.reconnect_delay_max)
            
    async def disconnect(self) -> None:
        """Disconnect client."""
        was_connected = self._connection is not None
        self._should_reconnect = False
        if self._connect_task and not self._connect_task.done():
            self._connect_task.cancel()
        if self._reconnect_task:
            self._reconnect_task.cancel()
            
        # Close any local P2P servers
        for request_id, server_info in list(self._p2p_servers.items()):
            try:
                local_server, _ = server_info
                await local_server.stop()
            except Exception:
                pass
        self._p2p_servers.clear()
        
        # Cancel any pending P2P futures
        for fut in list(self._p2p_futures.values()):
            if not fut.done():
                fut.cancel()
        self._p2p_futures.clear()

        if self._rust_client:
            self._rust_client.disconnect()
            self._rust_client = None
        if self._connection:
            await self._connection.stop()
            self._connection = None
            
        logger.info("Disconnected client")
        if was_connected:
            for hook in self._on_disconnect:
                try:
                    await hook(self)
                except Exception as e:
                    logger.error(f"Error in disconnect hook: {e}")
        
    async def send(self, message_type: str, data: Any) -> None:
        """Send message."""
        if not self._connection:
            raise ConnectionError("Not connected")
        await self._connection.send_message(message_type, data)
        
    def send_binary_stream(self, stream_name: str, data: bytes):
        """Stream direct binary data to the server using QUIC stream."""
        if not self._rust_client:
            raise ConnectionError("Not connected")
        self._rust_client.send_binary_stream(stream_name, data)
        
    async def establish_mesh_tunnel(self, target_node_id: str) -> None:
        """Establish an end-to-end encrypted TLS tunnel to a target node (Client or Server)."""
        from .mesh.tunnel import MemoryTLSTunnel
        
        # We are the client side of the E2E TLS tunnel
        tunnel = MemoryTLSTunnel(is_server=False)
        self._mesh_tunnels[target_node_id] = tunnel
        
        completed, handshake_data = tunnel.do_handshake()
        if handshake_data:
            from .protocol.protocol_pb2 import MessagePayload
            from .protocol.encoder import serialize_data
            inner_payload = MessagePayload(
                type="mesh_handshake",
                data=serialize_data(handshake_data)
            )
            packet = self._encoder.encode(
                MessageType.MESSAGE,
                payload_bytes=inner_payload.SerializeToString(),
                route_src=self._config.name,
                route_dst=target_node_id,
                is_mesh=True
            )
            await self._send_mesh_raw(target_node_id, packet)
            
        # Wait until the handshake is complete
        for _ in range(50):
            if tunnel.handshake_done:
                break
            await asyncio.sleep(0.1)
        else:
            raise TimeoutError(f"Mesh TLS handshake with {target_node_id} timed out")

    async def send_mesh_secure_message(self, target_node_id: str, message_type: str, data: Any) -> None:
        """Send an end-to-end encrypted message via mesh network."""
        tunnel = self._mesh_tunnels.get(target_node_id)
        if not tunnel or not tunnel.handshake_done:
            await self.establish_mesh_tunnel(target_node_id)
            tunnel = self._mesh_tunnels.get(target_node_id)
            if not tunnel or not tunnel.handshake_done:
                raise RuntimeError(f"Could not establish mesh tunnel to {target_node_id}")
                
        # Encode the inner message
        inner_encoded = self._encoder.encode_message(message_type, data)
        # Encrypt with TLS tunnel
        encrypted_payload = tunnel.write_plaintext(inner_encoded)
        # Wrap in a secure mesh packet
        from .protocol.protocol_pb2 import MessagePayload
        from .protocol.encoder import serialize_data
        inner_payload = MessagePayload(
            type="mesh_secure",
            data=serialize_data(encrypted_payload)
        )
        packet = self._encoder.encode(
            MessageType.MESSAGE,
            payload_bytes=inner_payload.SerializeToString(),
            route_src=self._config.name,
            route_dst=target_node_id,
            is_mesh=True
        )
        await self._send_mesh_raw(target_node_id, packet)

    async def _send_mesh_raw(self, target_node_id: str, packet: bytes):
        """Send raw packet to next hop towards target_node_id."""
        peer = self._peer_clients.get(target_node_id)
        if peer:
            if hasattr(peer, "_handle_rust_event"):
                peer._handle_rust_event("message", self._config.name, packet)
            elif hasattr(peer, "send_raw"):
                await peer.send_raw(packet)
            elif hasattr(peer, "send_message"):
                if asyncio.iscoroutinefunction(peer.send_message):
                    await peer.send_message(packet)
                else:
                    peer.send_message(packet)
        else:
            # Send to the server
            if not self._connection:
                raise ConnectionError("Not connected to server")
            await self._connection.send_raw(packet)

    async def discover_peers(self, timeout: float = 1.5, remote_hosts: Optional[List[str]] = None) -> List[dict]:
        """Discover active servers on local multicast and optional list of remote hosts."""
        from .discovery.mdns import DiscoveryService
        return await DiscoveryService.discover(timeout=timeout, remote_hosts=remote_hosts)
        
    async def _send_rpc_request(self, method: str, params: dict) -> int:
        if not self._connection:
            raise ConnectionError("Not connected")
        return await self._connection.send_rpc_request(method, params)
        
    async def _wait_for_rpc_response(self, correlation_id: int) -> Any:
        if not self._connection:
            raise ConnectionError("Not connected")
        return await self._connection.wait_for_rpc_response(correlation_id)
        
    @property
    def is_connected(self) -> bool:
        return self._connection is not None
        
    @property
    def is_authenticated(self) -> bool:
        return self._session_token is not None
        
    @property
    def state(self) -> Any:
        from .transport import ConnectionState
        return ConnectionState.ACTIVE if self.is_connected else ConnectionState.DISCONNECTED
        
    @property
    def rpc(self) -> RPC:
        return self._rpc
        
    @property
    def config(self) -> ClientDescriptor:
        return self._config
        
    @property
    def server_info(self) -> Dict[str, Any]:
        return self._server_info
        
    @property
    def session_token(self) -> Optional[str]:
        return self._session_token
        
    def health(self) -> dict:
        return {
            "connected": self.is_connected,
            "state": self.state.name if hasattr(self.state, "name") else str(self.state),
            "authenticated": self.is_authenticated,
            "reconnect_attempts": self._reconnect_attempts,
            "server_info": self._server_info,
        }

    # === P2P Decorators and Helpers ===

    def on_p2p_request(self, handler: Callable[[str], Awaitable[bool]]) -> Callable:
        """Register P2P request authorization handler."""
        self._p2p_handler = handler
        return handler
        
    def on_p2p_established(self, handler: Callable[['Client'], Awaitable[None]]) -> Callable:
        """Register P2P connection established callback."""
        self._on_p2p_established = handler
        return handler

    def _get_free_port(self) -> int:
        """Get a free port on localhost."""
        import socket
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(('127.0.0.1', 0))
            return s.getsockname()[1]

    def _get_local_ip(self) -> str:
        """Get the local IP address of this machine."""
        import socket
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"

    def _is_loopback(self, h: str) -> bool:
        """Check if host is loopback."""
        hl = h.lower()
        return hl in ("localhost", "127.0.0.1", "::1") or hl.startswith("127.")

    async def establish_p2p(self, target_client_id: str, timeout: float = 20.0) -> 'Client':
        """
        Establish a direct peer-to-peer connection to another client.
        
        Args:
            target_client_id: Name/ID of target client
            timeout: Timeout in seconds
            
        Returns:
            Connected Client instance pointing directly to the peer
        """
        import uuid
        if not self.is_connected:
            raise ConnectionError("Must be connected to server to broker P2P connection")
            
        request_id = str(uuid.uuid4())
        future = self._loop.create_future()
        self._p2p_futures[request_id] = future
        
        # 1. Send p2p broker request to server
        await self.send("p2p_request", {
            "target_id": target_client_id,
            "request_id": request_id,
        })
        
        try:
            # 2. Wait for response from server/target
            response = await asyncio.wait_for(future, timeout=timeout)
            if not response.get("accepted"):
                raise ConnectionError(f"P2P connection rejected by {target_client_id}: {response.get('reason', 'Access Denied')}")
                
            target_addr = response.get("public_addr")
            target_lan_addr = response.get("lan_addr")
            
            # 3. Setup local punch socket port
            local_port = self._get_free_port()
            stun_server = self._config.stun_server
            
            # 4. Perform STUN mapping on local_port
            public_addr = ""
            if not self._is_loopback(self._config.server_host) and stun_server:
                try:
                    public_addr = stun_punch_hole(stun_server, local_port, "")
                except Exception:
                    public_addr = ""
            if not public_addr:
                if self._is_loopback(self._config.server_host):
                    public_addr = f"127.0.0.1:{local_port}"
                else:
                    public_addr = f"{self._get_local_ip()}:{local_port}"
                
            # 5. Tell the target to punch towards our public address and LAN address
            await self.send("p2p_punch_source", {
                "request_id": request_id,
                "source_addr": public_addr,
                "lan_addr": f"{self._get_local_ip()}:{local_port}",
                "target_id": target_client_id,
            })
            
            # Small delay to let message route and target perform its punch
            await asyncio.sleep(0.2)
            
            # 6. Perform our punch towards target's addresses
            try:
                logger.info(f"P2P Punching from initiator port {local_port} to {target_addr}")
                stun_punch_hole(stun_server, local_port, target_addr)
                if target_lan_addr:
                    logger.info(f"P2P Punching from initiator port {local_port} to LAN {target_lan_addr}")
                    stun_punch_hole(stun_server, local_port, target_lan_addr)
            except Exception as e:
                logger.warning(f"P2P Punching from initiator failed: {e}")
                
            await asyncio.sleep(0.1)
            
            # 7. Connect directly to target! Try LAN first, then WAN
            peer_client = None
            if target_lan_addr:
                try:
                    host, port_str = target_lan_addr.split(":")
                    port = int(port_str)
                    from conduit import ClientDescriptor
                    peer_client = Client(ClientDescriptor(
                        server_host=host,
                        server_port=port,
                        password=self._config.password,
                        local_port=local_port,
                        reconnect_enabled=False,
                        name=f"{self._config.name}_to_{target_client_id}_lan",
                        connect_timeout=2,  # Quick timeout for local LAN try
                    ))
                    connected = await peer_client.connect()
                    if connected:
                        logger.info(f"Connected to peer via LAN address {target_lan_addr}")
                    else:
                        peer_client = None
                except Exception as e:
                    logger.warning(f"Failed to connect to LAN address {target_lan_addr}: {e}")
                    peer_client = None
                    
            if not peer_client:
                host, port_str = target_addr.split(":")
                port = int(port_str)
                from conduit import ClientDescriptor
                peer_client = Client(ClientDescriptor(
                    server_host=host,
                    server_port=port,
                    password=self._config.password,
                    local_port=local_port,
                    reconnect_enabled=False,
                    name=f"{self._config.name}_to_{target_client_id}",
                ))
                connected = await peer_client.connect()
                if not connected:
                    raise ConnectionError(f"Failed to connect to peer at {target_addr}")
                logger.info(f"Connected to peer via WAN address {target_addr}")
                
            return peer_client
            
        finally:
            self._p2p_futures.pop(request_id, None)

    def _setup_p2p_handlers(self) -> None:
        """Set up built-in P2P message handlers."""
        self._p2p_handler = None
        self._p2p_servers = {}
        self._p2p_futures = {}
        
        async def handle_p2p_incoming(connection, data):
            import inspect
            source_id = data.get("source_id")
            source_addr = data.get("source_addr")
            request_id = data.get("request_id")
            
            accepted = True
            if self._p2p_handler:
                try:
                    accepted = self._p2p_handler(source_id)
                    if inspect.isawaitable(accepted):
                        accepted = await accepted
                except Exception as e:
                    logger.error(f"Error in p2p handler: {e}")
                    accepted = False
            
            if not accepted:
                await self.send("p2p_accept", {
                    "request_id": request_id,
                    "accepted": False,
                    "reason": "Connection request rejected by peer",
                })
                return
            
            try:
                local_port = self._get_free_port()
                from conduit import Server, ServerDescriptor
                
                local_server = Server(ServerDescriptor(
                    name=f"{self._config.name}_p2p_{request_id[:8]}",
                    host="0.0.0.0",
                    port=local_port,
                    password=self._config.password,
                    enable_compression=self._config.enable_compression,
                ))
                
                connection_future = asyncio.get_running_loop().create_future()
                
                @local_server.on_client_connect
                async def handle_p2p_client_connect(conn):
                    if not connection_future.done():
                        connection_future.set_result(conn)
                
                self._p2p_servers[request_id] = (local_server, connection_future)
                
                stun_server = self._config.stun_server
                public_addr = ""
                if not self._is_loopback(self._config.server_host) and stun_server:
                    try:
                        public_addr = stun_punch_hole(stun_server, local_port, "")
                    except Exception:
                        public_addr = ""
                        
                if not public_addr:
                    if self._is_loopback(self._config.server_host):
                        public_addr = f"127.0.0.1:{local_port}"
                    else:
                        public_addr = f"{self._get_local_ip()}:{local_port}"
                
                await self.send("p2p_accept", {
                    "request_id": request_id,
                    "accepted": True,
                    "public_addr": public_addr,
                    "lan_addr": f"{self._get_local_ip()}:{local_port}",
                })
                
                # Wait for connection in background
                async def wait_for_peer_conn():
                    try:
                        conn = await connection_future
                        # Wrap as Client
                        peer_client = Client(ClientDescriptor(
                            server_host=conn.remote_address,
                            server_port=local_port,
                            password=self._config.password,
                        ))
                        peer_client._connection = conn
                        peer_client._session_token = "p2p"
                        
                        # Redirect routing
                        local_server._message_router = peer_client._message_router
                        local_server._rpc_registry = peer_client._rpc_registry
                        local_server._rpc_dispatcher = peer_client._rpc_dispatcher
                        
                        @local_server.on_binary_stream
                        async def forward_binary_stream(client_id, stream_name, data):
                            if peer_client._on_binary_stream:
                                await peer_client._on_binary_stream(stream_name, data)
                        
                        if self._on_p2p_established:
                            res = self._on_p2p_established(peer_client)
                            if inspect.isawaitable(res):
                                await res
                    except Exception as e:
                        logger.error(f"Error establishing peer connection: {e}")
                        
                asyncio.create_task(wait_for_peer_conn())
                
            except Exception as e:
                logger.error(f"Failed to setup local P2P server: {e}")
                await self.send("p2p_accept", {
                    "request_id": request_id,
                    "accepted": False,
                    "reason": str(e),
                })

        async def handle_p2p_punch_cmd(connection, data):
            request_id = data.get("request_id")
            source_addr = data.get("source_addr")
            source_lan_addr = data.get("lan_addr")
            
            server_info = self._p2p_servers.get(request_id)
            if server_info:
                local_server, _ = server_info
                local_port = local_server._config.port
                stun_server = self._config.stun_server
                try:
                    logger.info(f"P2P Punching from listener port {local_port} to {source_addr}")
                    stun_punch_hole(stun_server, local_port, source_addr)
                    if source_lan_addr:
                        logger.info(f"P2P Punching from listener port {local_port} to LAN {source_lan_addr}")
                        stun_punch_hole(stun_server, local_port, source_lan_addr)
                except Exception as e:
                    logger.warning(f"P2P Punching from listener failed: {e}")
                
                try:
                    await local_server.start()
                except Exception as e:
                    logger.error(f"Failed to start local P2P server after punch: {e}")

        async def handle_p2p_request_response(connection, data):
            request_id = data.get("request_id")
            future = self._p2p_futures.get(request_id)
            if future and not future.done():
                future.set_result(data)

        self._message_router.register("p2p_incoming", handle_p2p_incoming, requires_auth=False)
        self._message_router.register("p2p_punch_cmd", handle_p2p_punch_cmd, requires_auth=False)
        self._message_router.register("p2p_request_response", handle_p2p_request_response, requires_auth=False)
