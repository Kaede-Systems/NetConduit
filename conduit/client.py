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
        self._cached_session_token = None
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
            
            client_info = {
                "name": self._config.name,
                "version": self._config.version,
            }
            if self._config.username:
                client_info["username"] = self._config.username

            if self._cached_session_token:
                client_info["session_token"] = self._cached_session_token
                password_hash = ""
            else:
                password_hash = hashlib.sha256(
                    self._config.password.encode('utf-8')
                ).hexdigest()

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
                import inspect
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
                
                # Check if this mesh handshake completes a fallback for a pending P2P request from 'src'
                if tunnel.handshake_done:
                    pending_request_id = None
                    for req_id, server_info in list(self._p2p_servers.items()):
                        if len(server_info) >= 3 and server_info[2] == src:
                            pending_request_id = req_id
                            local_server, connection_future, _ = server_info
                            break
                            
                    if pending_request_id:
                        logger.info(f"P2P direct connection failed. Fallback to Mesh P2P Client for {src} (like WebRTC TURN)")
                        self._p2p_servers.pop(pending_request_id, None)
                        try:
                            asyncio.create_task(local_server.stop())
                        except Exception as e:
                            logger.warning(f"Error stopping local server during mesh fallback: {e}")
                        if not connection_future.done():
                            connection_future.cancel()
                            
                        peer_client = MeshFallbackClient(self, src, tunnel)
                        self._peer_clients[src] = peer_client
                        if self._on_p2p_established:
                            res = self._on_p2p_established(peer_client)
                            if inspect.isawaitable(res):
                                asyncio.create_task(res)
                return

            elif msg_type_str == "mesh_secure":
                encrypted_data = decoded.get_data()
                tunnel = self._mesh_tunnels.get(src)
                if tunnel:
                    decrypted = tunnel.feed_encrypted(encrypted_data)
                    # If we have a mesh fallback client for this src, pass the decrypted payload to it!
                    peer_client = self._peer_clients.get(src)
                    if peer_client and hasattr(peer_client, "_mesh_connection"):
                        peer_client._mesh_connection.handle_decrypted_payload(decrypted)
                        return
                        
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
            self._cached_session_token = self._session_token
            self._server_info = dict(decoded.payload.get("server_info", {}))
            if self._connection:
                self._connection.set_session(self._session_token)
            if self._auth_future and not self._auth_future.done():
                self._auth_future.set_result(True)
            return

        if msg_type == MessageType.AUTH_FAILURE:
            self._cached_session_token = None
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
        if peer and not hasattr(peer, "_mesh_connection"):
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

    def _is_private_ip(self, host: str) -> bool:
        """Check if a host is a private/local IP address."""
        import ipaddress
        try:
            ip = ipaddress.ip_address(host)
            return ip.is_private or ip.is_loopback
        except ValueError:
            hl = host.lower()
            if hl in ("localhost", "localhost.localdomain") or hl.endswith(".local"):
                return True
            return False

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
            
            # 3. Setup local punch socket port (get it early so it can be captured by the connection helpers)
            local_port = self._get_free_port()

            # 4. Connect directly to target! Try LAN and WAN in parallel (like WebRTC ICE racing)
            peer_client = None
            
            async def try_connect_lan():
                if not target_lan_addr:
                    return None
                try:
                    host, port_str = target_lan_addr.split(":")
                    port = int(port_str)
                    from conduit import ClientDescriptor
                    # Since LAN is direct, bind to a random local port to avoid conflict with the punched local_port
                    lan_port = self._get_free_port()
                    cli = Client(ClientDescriptor(
                        server_host=host,
                        server_port=port,
                        password=self._config.password,
                        local_port=lan_port,
                        reconnect_enabled=False,
                        name=f"{self._config.name}_to_{target_client_id}_lan",
                        connect_timeout=3,
                    ))
                    connected = await cli.connect()
                    if connected:
                        logger.info(f"Connected to peer via LAN address {target_lan_addr}")
                        return cli
                except Exception as e:
                    logger.warning(f"Failed to connect to LAN address {target_lan_addr}: {e}")
                return None

            async def try_connect_wan():
                try:
                    host, port_str = target_addr.split(":")
                    port = int(port_str)
                    from conduit import ClientDescriptor
                    # WAN must bind to the punched local_port to traverse NAT
                    cli = Client(ClientDescriptor(
                        server_host=host,
                        server_port=port,
                        password=self._config.password,
                        local_port=local_port,
                        reconnect_enabled=False,
                        name=f"{self._config.name}_to_{target_client_id}",
                        connect_timeout=5,
                    ))
                    connected = await cli.connect()
                    if connected:
                        logger.info(f"Connected to peer via WAN address {target_addr}")
                        return cli
                except Exception as e:
                    logger.warning(f"Failed to connect to WAN address {target_addr}: {e}")
                return None

            # Helper to run STUN and hole punching in the background (concurrent with connection attempts)
            async def do_punching():
                stun_server = self._config.stun_server
                
                # Perform STUN mapping on local_port (skip if local network)
                public_addr = ""
                is_local = self._is_loopback(self._config.server_host) or self._is_private_ip(self._config.server_host)
                if not is_local and stun_server:
                    try:
                        public_addr = await asyncio.to_thread(stun_punch_hole, stun_server, local_port, "")
                    except Exception:
                        public_addr = ""
                if not public_addr:
                    if self._is_loopback(self._config.server_host):
                        public_addr = f"127.0.0.1:{local_port}"
                    else:
                        public_addr = f"{self._get_local_ip()}:{local_port}"
                    
                # Tell the target to punch towards our public address and LAN address
                await self.send("p2p_punch_source", {
                    "request_id": request_id,
                    "source_addr": public_addr,
                    "lan_addr": f"{self._get_local_ip()}:{local_port}",
                    "target_id": target_client_id,
                })
                
                # Small delay to let message route and target perform its punch
                await asyncio.sleep(0.2)
                
                # Perform our punch towards target's addresses
                try:
                    target_ip = target_addr.split(":")[0]
                    is_target_local = self._is_loopback(target_ip) or self._is_private_ip(target_ip)
                    punch_stun = "" if (is_local or is_target_local) else stun_server
                    
                    logger.info(f"P2P Punching from initiator port {local_port} to {target_addr} (STUN: {punch_stun or 'None/LAN'})")
                    await asyncio.to_thread(stun_punch_hole, punch_stun, local_port, target_addr)
                    if target_lan_addr:
                        logger.info(f"P2P Punching from initiator port {local_port} to LAN {target_lan_addr} (STUN: {punch_stun or 'None/LAN'})")
                        await asyncio.to_thread(stun_punch_hole, punch_stun, local_port, target_lan_addr)
                except Exception as e:
                    logger.warning(f"P2P Punching from initiator failed: {e}")

            # Start punching in the background
            punch_task = asyncio.create_task(do_punching())

            # Race the connection attempts concurrently
            tasks = []
            if target_lan_addr:
                tasks.append(asyncio.create_task(try_connect_lan()))
            tasks.append(asyncio.create_task(try_connect_wan()))
            
            try:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    res = task.result()
                    if res:
                        peer_client = res
                        break
                        
                if not peer_client and pending:
                    for task in asyncio.as_completed(pending):
                        res = await task
                        if res:
                            peer_client = res
                            break
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                if not punch_task.done():
                    punch_task.cancel()
                    
            # 8. Fallback to E2E encrypted Mesh network if both connection attempts fail (like WebRTC TURN fallback)
            if not peer_client:
                logger.info(f"Direct connection to {target_client_id} failed. Falling back to Mesh tunnel (like WebRTC TURN)...")
                try:
                    await self.establish_mesh_tunnel(target_client_id)
                    tunnel = self._mesh_tunnels.get(target_client_id)
                    if tunnel and tunnel.handshake_done:
                        peer_client = MeshFallbackClient(self, target_client_id, tunnel)
                        self._peer_clients[target_client_id] = peer_client
                        logger.info(f"Mesh fallback P2P connection established to {target_client_id}!")
                except Exception as e:
                    logger.error(f"Mesh fallback connection failed: {e}")
                    
            if not peer_client:
                raise ConnectionError(f"Failed to connect to peer at {target_addr} (direct and fallback failed)")
                
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
                
                self._p2p_servers[request_id] = (local_server, connection_future, source_id)
                
                stun_server = self._config.stun_server
                public_addr = ""
                is_local = self._is_loopback(self._config.server_host) or self._is_private_ip(self._config.server_host)
                if not is_local and stun_server:
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
                local_server = server_info[0]
                local_port = local_server._config.port
                stun_server = self._config.stun_server
                
                is_local = self._is_loopback(self._config.server_host) or self._is_private_ip(self._config.server_host)
                source_ip = source_addr.split(":")[0]
                is_source_local = self._is_loopback(source_ip) or self._is_private_ip(source_ip)
                
                try:
                    punch_stun = "" if (is_local or is_source_local) else stun_server
                    logger.info(f"P2P Punching from listener port {local_port} to {source_addr} (STUN: {punch_stun or 'None/LAN'})")
                    stun_punch_hole(punch_stun, local_port, source_addr)
                    if source_lan_addr:
                        logger.info(f"P2P Punching from listener port {local_port} to LAN {source_lan_addr} (STUN: {punch_stun or 'None/LAN'})")
                        stun_punch_hole(punch_stun, local_port, source_lan_addr)
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


class MeshConnection:
    """
    A connection wrapper that implements the Conduit connection interface,
    but tunnels all messages over an in-memory MemoryTLSTunnel mesh routing system.
    """
    def __init__(self, main_client: Any, peer_id: str, tunnel: Any, encoder=None, decoder=None):
        self._main_client = main_client
        self._peer_id = peer_id
        self._tunnel = tunnel
        self._encoder = encoder or ProtocolEncoder()
        self._decoder = decoder or ProtocolDecoder()
        self._pending_rpcs = {}
        from conduit.connection.connection import ConnectionStats
        self._stats = ConnectionStats()
        self._on_message = None
        self._on_disconnect = None
        self._authenticated = True
        
    @property
    def id(self) -> str:
        return f"mesh_{self._peer_id}"
        
    @property
    def is_connected(self) -> bool:
        return self._tunnel.handshake_done
        
    @property
    def is_authenticated(self) -> bool:
        return True
        
    @property
    def remote_address(self) -> str:
        return self._peer_id
        
    @property
    def stats(self):
        return self._stats
        
    async def send_message(self, message_type: str, data: Any) -> None:
        encoded = self._encoder.encode_message(message_type, data)
        await self._send_encrypted(encoded)
        
    async def send_rpc_request(self, method: str, params: dict) -> int:
        encoded, corr_id = self._encoder.encode_rpc_request(method, params)
        future = asyncio.get_running_loop().create_future()
        self._pending_rpcs[corr_id] = future
        await self._send_encrypted(encoded)
        return corr_id
        
    async def wait_for_rpc_response(self, correlation_id: int) -> Any:
        future = self._pending_rpcs.get(correlation_id)
        if future is None:
            raise ValueError(f"No pending RPC for correlation ID {correlation_id}")
        try:
            return await future
        finally:
            self._pending_rpcs.pop(correlation_id, None)
            
    async def send_rpc_response(self, result: Any, correlation_id: int) -> None:
        encoded = self._encoder.encode_rpc_response(result, correlation_id)
        await self._send_encrypted(encoded)
        
    async def send_rpc_error(self, error: str, correlation_id: int, code: int = None) -> None:
        encoded = self._encoder.encode_rpc_error(error, correlation_id, code)
        await self._send_encrypted(encoded)
        
    async def send_raw(self, encoded: bytes) -> None:
        await self._send_encrypted(encoded)
        
    async def _send_encrypted(self, payload: bytes) -> None:
        encrypted = self._tunnel.write_plaintext(payload)
        from .protocol.protocol_pb2 import MessagePayload
        from .protocol.encoder import serialize_data
        inner_payload = MessagePayload(
            type="mesh_secure",
            data=serialize_data(encrypted)
        )
        packet = self._encoder.encode(
            MessageType.MESSAGE,
            payload_bytes=inner_payload.SerializeToString(),
            route_src=self._main_client._config.name,
            route_dst=self._peer_id,
            is_mesh=True
        )
        await self._main_client._send_mesh_raw(self._peer_id, packet)
        self._stats.bytes_sent += len(packet)
        self._stats.messages_sent += 1

    def handle_decrypted_payload(self, decrypted: bytes) -> None:
        decoded = self._decoder.decode_single(decrypted)
        
        # Intercept connection-specific RPCs (like responses/errors)
        intercepted = self.handle_decoded_message(decoded)
        if intercepted:
            return
                
        if self._on_message:
            asyncio.create_task(self._on_message(self, decoded))
            
    def handle_decoded_message(self, message: Any) -> bool:
        self._stats.messages_received += 1
        self._stats.bytes_received += len(message.raw_payload)
        
        msg_type = message.message_type
        if msg_type in (MessageType.RPC_RESPONSE, MessageType.RPC_ERROR):
            corr_id = message.correlation_id
            future = self._pending_rpcs.get(corr_id)
            if future and not future.done():
                future.set_result(message.payload)
                return True
        return False
            
    def set_message_handler(self, handler: Callable) -> None:
        self._on_message = handler
        
    def set_disconnect_handler(self, handler: Callable) -> None:
        self._on_disconnect = handler
        
    async def stop(self) -> None:
        if self._on_disconnect:
            try:
                handler = self._on_disconnect
                self._on_disconnect = None
                await handler(self)
            except Exception as e:
                logger.error(f"Error in disconnect callback: {e}")


class MeshFallbackClient(Client):
    """
    A Client subclass that routes all messages and RPCs over an
    established E2E encrypted mesh tunnel rather than a direct QUIC link.
    """
    def __init__(self, main_client: Client, peer_id: str, tunnel: Any):
        from conduit import ClientDescriptor
        dummy_desc = ClientDescriptor(
            server_host="mesh-fallback",
            server_port=1,
            password=main_client._config.password,
            name=f"{main_client._config.name}_to_{peer_id}_mesh",
        )
        super().__init__(dummy_desc)
        self._main_client = main_client
        self._peer_id = peer_id
        self._tunnel = tunnel
        
        self._mesh_connection = MeshConnection(main_client, peer_id, tunnel, self._encoder, self._decoder)
        self._connection = self._mesh_connection
        self._session_token = "mesh"
        
        async def on_connection_message(conn, decoded_msg):
            msg_type = decoded_msg.get_message_type_str()
            if msg_type == "mesh_stream_data":
                data = decoded_msg.get_data()
                stream_name = data.get("stream_name")
                stream_bytes = data.get("data")
                if self._on_binary_stream:
                    await self._on_binary_stream(stream_name, stream_bytes)
                return
                
            await self._process_message(decoded_msg)
            
        self._mesh_connection.set_message_handler(on_connection_message)

    async def connect(self) -> bool:
        return True

    async def disconnect(self) -> None:
        await self._mesh_connection.stop()

    def send_binary_stream(self, stream_name: str, data: bytes):
        """Route binary stream data over the mesh tunnel."""
        asyncio.create_task(self.send("mesh_stream_data", {
            "stream_name": stream_name,
            "data": data
        }))
