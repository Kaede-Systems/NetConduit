"""
Conduit Server using Rust QUIC transport underneath.
"""

import asyncio
import hashlib
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Awaitable

from netconduit_core import RustQUICServer, stun_punch_hole

from .data.descriptors import ServerDescriptor
from .transport import AuthHandler
from .protocol import ProtocolEncoder, ProtocolDecoder, MessageType, DecodedMessage
from .connection import Connection, ConnectionPool
from .messages import MessageRouter, Message
from .rpc import RPCRegistry, RPCDispatcher
from .response import Response, Error
from .ratelimit import RateLimiter, RateLimitConfig
from .auth.credentials import CredentialsManager, UserProfile
from .heartbeat import HeartbeatManager

import logging
logger = logging.getLogger(__name__)


class ServerState(Enum):
    """Server lifecycle states."""
    CREATED = auto()
    INITIALIZING = auto()
    RUNNING = auto()
    STOPPING = auto()
    CLOSED = auto()


# Callback types
LifecycleHook = Callable[['Server'], Awaitable[None]]
ConnectionHook = Callable[[Connection], Awaitable[None]]
MessageHandler = Callable[[Connection, Any], Awaitable[Any]]
BinaryStreamHandler = Callable[[str, str, bytes], Awaitable[None]]


class Server:
    """
    Conduit Server using Rust QUIC.
    """
    
    def __init__(self, config: ServerDescriptor):
        """
        Initialize server.
        
        Args:
            config: Server configuration
        """
        self._config = config
        self._rust_server = None
        self._stop_event = asyncio.Event()
        
        # Authentication
        self._auth_handler = AuthHandler(
            password=config.password,
            session_timeout=config.connection_timeout,
        )
        self._valid_sessions = {}
        
        # Connection pool
        self._pool = ConnectionPool(max_connections=config.max_connections)
        
        # Message routing
        self._message_router = MessageRouter()
        
        # RPC
        self._rpc_registry = RPCRegistry()
        self._rpc_dispatcher = RPCDispatcher(self._rpc_registry)
        
        # Protocol
        self._encoder = ProtocolEncoder(enable_compression=config.enable_compression)
        self._decoder = ProtocolDecoder()
        
        # Response helpers
        self._response = Response()
        self._error = Error()
        
        # Lifecycle hooks
        self._on_startup = []
        self._on_shutdown = []
        self._on_connect = []
        self._on_disconnect = []
        self._on_binary_stream = None
        
        # Rate limiting config
        self._rate_limit_config = RateLimitConfig(
            enabled=config.rate_limit_enabled,
            messages_per_second=config.rate_limit_messages_per_second,
            bytes_per_second=config.rate_limit_bytes_per_second,
        )
        
        # Connection rate limiters (per connection)
        self._connection_limiters = {}
        
        # Credentials manager
        self._credentials_manager = None
        
        # Mesh network tunnels
        self._mesh_tunnels = {}
        self._mesh_routing_table = {}
        self._discovery_service = None
        
        # Active task tracking for clean shutdown
        self._active_tasks: set = set()
        
        # Application-level heartbeat (independent from QUIC transport keepalive)
        self._heartbeat = HeartbeatManager(
            interval=getattr(config, 'heartbeat_interval', 30.0),
            timeout=getattr(config, 'heartbeat_timeout', 90.0),
            max_missed=3,
            on_stale=self._handle_stale_connection,
        )
        
        # State
        self._server_state = ServerState.CREATED
        self._running = False
        
        # P2P Setup
        self._setup_p2p_handlers()
        
        logger.info(f"Server '{config.name}' created [state: CREATED]")
        
    def register_credentials_manager(self, manager: CredentialsManager) -> None:
        """Register a credentials manager for authenticated user verification."""
        self._credentials_manager = manager
    
    # === Decorators ===
    
    def on(
        self,
        message_type: str,
        requires_auth: bool = True,
        requires_role: Optional[str] = None,
        requires_permission: Optional[str] = None,
        gate: Optional[Callable[[Connection], bool]] = None
    ) -> Callable:
        """Register a message handler with optional security gating."""
        def decorator(handler: MessageHandler) -> MessageHandler:
            async def wrapped_handler(connection, data):
                # 1. Gate Callback
                if gate:
                    if not gate(connection):
                        logger.warning(f"Message {message_type} blocked by custom gate")
                        return {"success": False, "error": "Access denied by gating rules"}
                # 2. Role Gating
                if requires_role:
                    profile = getattr(connection.session, "profile", None)
                    if not profile or requires_role not in getattr(profile, "roles", set()):
                        logger.warning(f"Message {message_type} blocked: missing role '{requires_role}'")
                        return {"success": False, "error": f"Required role '{requires_role}' is missing"}
                # 3. Permission Gating
                if requires_permission:
                    profile = getattr(connection.session, "profile", None)
                    if not profile or requires_permission not in getattr(profile, "permissions", set()):
                        logger.warning(f"Message {message_type} blocked: missing permission '{requires_permission}'")
                        return {"success": False, "error": f"Required permission '{requires_permission}' is missing"}
                        
                response = handler(connection, data)
                if asyncio.iscoroutine(response):
                    return await response
                return response

            self._message_router.register(
                message_type=message_type,
                handler=wrapped_handler,
                requires_auth=requires_auth,
            )
            return handler
        return decorator
    
    def rpc(
        self,
        name: Optional[str] = None,
        requires_auth: bool = True,
        requires_role: Optional[str] = None,
        requires_permission: Optional[str] = None,
        gate: Optional[Callable[[Connection], bool]] = None
    ) -> Callable:
        """Register an RPC method with optional security gating."""
        if callable(name):
            handler = name
            self._rpc_registry.register(handler)
            rpc_method = self._rpc_registry.get(handler.__name__)
            if rpc_method:
                rpc_method.requires_role = None
                rpc_method.requires_permission = None
                rpc_method.gate = None
            return handler
        
        def decorator(handler: Callable) -> Callable:
            self._rpc_registry.register(handler, name=name, requires_auth=requires_auth)
            rpc_method = self._rpc_registry.get(name or handler.__name__)
            if rpc_method:
                rpc_method.requires_role = requires_role
                rpc_method.requires_permission = requires_permission
                rpc_method.gate = gate
            return handler
        return decorator
    
    def on_startup(self, handler: LifecycleHook) -> LifecycleHook:
        """Register startup hook."""
        self._on_startup.append(handler)
        return handler
    
    def on_shutdown(self, handler: LifecycleHook) -> LifecycleHook:
        """Register shutdown hook."""
        self._on_shutdown.append(handler)
        return handler
    
    def on_client_connect(self, handler: ConnectionHook) -> ConnectionHook:
        """Register client connect hook."""
        self._on_connect.append(handler)
        return handler
    
    def on_client_disconnect(self, handler: ConnectionHook) -> ConnectionHook:
        """Register client disconnect hook."""
        self._on_disconnect.append(handler)
        return handler

    def on_binary_stream(self, handler: BinaryStreamHandler) -> BinaryStreamHandler:
        """Register binary stream callback decorator."""
        self._on_binary_stream = handler
        return handler
    
    @property
    def state(self) -> ServerState:
        return self._server_state
    
    # === Server Lifecycle ===
    
    async def run(self) -> None:
        """Start the server and run until stopped."""
        await self.start()
        try:
            await self._stop_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()
    
    async def start(self) -> None:
        """Start the server (without blocking)."""
        if self._running:
            return
        
        self._server_state = ServerState.INITIALIZING
        logger.info(f"Server '{self._config.name}' [state: INITIALIZING]")
        
        # Run startup hooks
        for hook in self._on_startup:
            await hook(self)
        
        # Setup connection pool callbacks
        self._pool.set_callbacks(
            on_connect=self._handle_client_connect_hook,
            on_disconnect=self._handle_client_disconnect_hook,
        )
        
        self._rust_server = RustQUICServer()
        self._loop = asyncio.get_running_loop()
        self._stop_event.clear()

        # UDP Hole Punching via STUN if configured
        def is_loopback(h: str) -> bool:
            hl = h.lower()
            return hl in ("localhost", "127.0.0.1", "::1") or hl.startswith("127.")
            
        if not is_loopback(self._config.host):
            stun_server = self._config.stun_server
            local_port = self._config.port
            try:
                logger.info(f"STUN hole punching from server side via {stun_server} on port {local_port}")
                mapped = stun_punch_hole(stun_server, local_port, "")
                if mapped:
                    logger.info(f"STUN mapped server address successfully resolved: {mapped}")
            except Exception as e:
                logger.warning(f"STUN hole punching resolution failed: {e}")

        def rust_callback(event_type, client_id, data):
            self._loop.call_soon_threadsafe(self._handle_rust_event, event_type, client_id, bytes(data))

        # Start Rust QUIC Server
        self._rust_server.start(self._config.host, self._config.port, rust_callback)
        
        self._running = True
        self._server_state = ServerState.RUNNING
        logger.info(f"Server '{self._config.name}' listening on QUIC {self._config.host}:{self._config.port} [state: RUNNING]")
        
        # Start application-level heartbeat (runs in background, never blocks)
        self._heartbeat.start(self._encoder.encode_heartbeat_ping)
    
    async def stop(self) -> None:
        """Stop the server, cancelling all active tasks."""
        if not self._running:
            return
        
        self._server_state = ServerState.STOPPING
        logger.info(f"Server '{self._config.name}' [state: STOPPING]")
        
        self._running = False
        self._stop_event.set()
        
        # Stop heartbeat manager first (it's a background task)
        await self._heartbeat.stop()
        
        # Cancel all outstanding RPC/message handler tasks
        tasks_to_cancel = list(self._active_tasks)
        if tasks_to_cancel:
            logger.debug(f"Cancelling {len(tasks_to_cancel)} active tasks")
            for task in tasks_to_cancel:
                task.cancel()
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
        self._active_tasks.clear()
        
        # Stop Rust Server
        if self._rust_server:
            self._rust_server.stop()
            self._rust_server = None
        
        # Close all connections
        await self._pool.close_all()
        self._connection_limiters.clear()
        
        # Cancel any pending P2P requests
        for fut in list(self._p2p_requests.values()):
            if not fut.done():
                fut.cancel()
        self._p2p_requests.clear()
        
        # Stop discovery service if running
        await self.stop_discovery()
        
        # Run shutdown hooks
        for hook in self._on_shutdown:
            await hook(self)
        
        self._server_state = ServerState.CLOSED
        logger.info(f"Server '{self._config.name}' [state: CLOSED]")

    async def _handle_stale_connection(self, client_id: str) -> None:
        """Called by HeartbeatManager when a client stops responding to pings."""
        logger.warning(f"[Heartbeat] Client {client_id} is stale — disconnecting")
        self._heartbeat.unregister(client_id)
        if self._rust_server and self._rust_server.is_connected(client_id):
            # Send a CLOSE frame before forcibly removing
            try:
                close_frame = self._encoder.encode_close("heartbeat timeout")
                self._rust_server.send_message(client_id, close_frame)
            except Exception:
                pass
        await self._process_disconnect(client_id)

    async def start_discovery(self) -> None:
        """Start advertising this server via mDNS."""
        from .discovery.mdns import DiscoveryService
        if not self._discovery_service:
            self._discovery_service = DiscoveryService()
        await self._discovery_service.start_advertiser(
            name=self._config.name,
            host=self._config.host,
            port=self._config.port,
            metadata={"version": self._config.version}
        )

    async def stop_discovery(self) -> None:
        """Stop advertising this server via mDNS."""
        if self._discovery_service:
            await self._discovery_service.stop_advertiser()
            self._discovery_service = None

    # === Rust Events ===
    
    def _track_task(self, coro) -> asyncio.Task:
        """Create and track an asyncio task, auto-removing it when done."""
        task = asyncio.create_task(coro)
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)
        return task

    def _handle_rust_event(self, event_type: str, client_id: str, payload: bytes):
        if event_type == "connect":
            self._track_task(self._process_connect(client_id))
        elif event_type == "disconnect":
            self._track_task(self._process_disconnect(client_id))
        elif event_type == "message":
            try:
                decoded = self._decoder.decode_single(payload)
                self._track_task(self._process_message(client_id, decoded, payload))
            except Exception as e:
                logger.error(f"Failed to decode message from {client_id}: {e}")
        elif event_type == "binary_stream":
            self._track_task(self._process_binary_stream(client_id, payload))

    async def _process_connect(self, client_id: str):
        logger.debug(f"Client QUIC connection established from {client_id}")
        connection = Connection(
            rust_transport=self._rust_server,
            client_id=client_id,
            encoder=self._encoder,
            decoder=self._decoder
        )
        await self._pool.add(connection)

    async def _process_disconnect(self, client_id: str):
        logger.debug(f"Client QUIC disconnected: {client_id}")
        self._heartbeat.unregister(client_id)
        await self._pool.remove(client_id)

    async def _process_message(self, client_id: str, decoded: DecodedMessage, raw_payload: bytes):
        connection = self._pool.get(client_id)
        if not connection:
            return
            
        msg_type = decoded.message_type
        
        if msg_type == MessageType.AUTH_REQUEST:
            payload = decoded.payload
            password_hash = payload.get("password_hash", "")
            client_info = dict(payload.get("client_info", {}))
            username = client_info.get("username")
            session_token = client_info.get("session_token")
            
            profile = None
            is_token_auth = False
            
            # Check if token-based session resumption is attempted and valid
            if session_token and session_token in self._valid_sessions:
                username, profile = self._valid_sessions[session_token]
                is_token_auth = True
                logger.info(f"Client {client_id} successfully authenticated via token-based resumption (0-RTT style)")
            
            if not is_token_auth:
                # Check credentials manager if configured
                if self._credentials_manager and username:
                    if not self._credentials_manager.verify(username, password_hash):
                        fail_msg = self._encoder.encode_auth_failure("Invalid credentials")
                        self._rust_server.send_message(client_id, fail_msg)
                        await self._pool.remove(client_id)
                        return
                    profile = self._credentials_manager.get_profile(username)
                else:
                    if not self._auth_handler.verify_simple(password_hash):
                        # Send failure
                        fail_msg = self._encoder.encode_auth_failure("Invalid password")
                        self._rust_server.send_message(client_id, fail_msg)
                        await self._pool.remove(client_id)
                        return
                    # Grant admin profile by default for simple auth password
                    from .auth.credentials import UserProfile
                    profile = UserProfile(username="admin", roles={"admin"}, permissions={"read", "write"})
            
            # Create session
            if not is_token_auth:
                import uuid
                session_token = str(uuid.uuid4())
                self._valid_sessions[session_token] = (username, profile)
                
            from .transport.auth import Session
            session = Session(token=session_token, client_id=client_id, client_info=client_info)
            session.profile = profile  # Attach user profile to session
            connection.set_session(session)
            
            # Send success
            success_msg = self._encoder.encode_auth_success(
                session_token=session_token,
                server_info={
                    "name": self._config.name,
                    "version": self._config.version,
                }
            )
            self._rust_server.send_message(client_id, success_msg)
            # Register with heartbeat manager
            def _make_sender(cid):
                def _send(data): self._rust_server.send_message(cid, data)
                return _send
            self._heartbeat.register(client_id, _make_sender(client_id))
            
            # Trigger lifecycle hooks for connect
            if self._pool._on_connect:
                await self._pool._on_connect(connection)
            return

        # Check mesh routing relay!
        if decoded.is_mesh:
            if decoded.route_src and decoded.route_src != client_id:
                self._mesh_routing_table[decoded.route_src] = client_id
                
            if decoded.route_dst == self._config.name:
                # We are the final destination of this mesh message!
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
                        # Send handshake response back
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
                        target_conn_id = client_id
                        if src in self._mesh_routing_table:
                            target_conn_id = self._mesh_routing_table[src]
                        self._rust_server.send_message(target_conn_id, response_packet)
                    return
                    
                elif msg_type_str == "mesh_secure":
                    encrypted_data = decoded.get_data()
                    tunnel = self._mesh_tunnels.get(src)
                    if tunnel:
                        decrypted = tunnel.feed_encrypted(encrypted_data)
                        inner_decoded = self._decoder.decode_single(decrypted)
                        inner_msg_type_str = inner_decoded.get_message_type_str()
                        inner_data = inner_decoded.get_data()
                        
                        logger.info(f"[Mesh Server E2E] Decrypted inner message: {inner_msg_type_str}")
                        msg = Message(type=inner_msg_type_str, data=inner_data)
                        response = await self._message_router.route(
                            message=msg,
                            context=connection,
                            authenticated=True
                        )
                        if response is not None:
                            # Send response back encrypted
                            encoded_res = self._encoder.encode_message(inner_msg_type_str + "_response", response)
                            encrypted_res = tunnel.write_plaintext(encoded_res)
                            from .protocol.protocol_pb2 import MessagePayload
                            from .protocol.encoder import serialize_data
                            inner_payload = MessagePayload(
                                type="mesh_secure",
                                data=serialize_data(encrypted_res)
                            )
                            response_packet = self._encoder.encode(
                                MessageType.MESSAGE,
                                payload_bytes=inner_payload.SerializeToString(),
                                route_src=self._config.name,
                                route_dst=src,
                                is_mesh=True
                            )
                            target_conn_id = client_id
                            if src in self._mesh_routing_table:
                                target_conn_id = self._mesh_routing_table[src]
                            self._rust_server.send_message(target_conn_id, response_packet)
                    return
            else:
                # Relay packet to destination client!
                dst = decoded.route_dst
                conn = self._pool.get(dst)
                if not conn:
                    # Fallback to name-based lookup in pool
                    for c in self._pool.get_all():
                        if c.session and c.session.client_info and c.session.client_info.get("name") == dst:
                            conn = c
                            break
                if not conn:
                    next_hop = self._mesh_routing_table.get(dst)
                    if next_hop:
                        conn = self._pool.get(next_hop)
                if conn:
                    # ZEROCOPY Relay: send original raw payload directly
                    self._rust_server.send_message(conn.id, raw_payload)
                    logger.info(f"[Mesh Relay Server] Relayed packet from {decoded.route_src} to {dst} via {conn.id}")
                return

        # Drop unauthenticated messages
        if not connection.is_authenticated:
            logger.warning(f"Dropping unauthenticated packet of type {msg_type} from {client_id}")
            return

        # Rate limiting enforcement
        if self._rate_limit_config.enabled:
            if connection.id not in self._connection_limiters:
                self._connection_limiters[connection.id] = self._rate_limit_config.create_limiter()
            limiter = self._connection_limiters[connection.id]
            msg_size = len(decoded.raw_payload)
            if not limiter.try_acquire(msg_size):
                logger.warning(f"Rate limit exceeded for connection {connection.id[:8]}")
                return

        # Intercept connection-specific RPCs (like responses)
        intercepted = connection.handle_decoded_message(decoded)
        if intercepted:
            return

        # Handle RPC requests
        if msg_type == MessageType.RPC_REQUEST:
            method = decoded.get_rpc_method()
            params = decoded.get_rpc_params() or {}
            corr_id = decoded.correlation_id
            
            # Gating checks
            rpc_method = self._rpc_registry.get(method)
            if rpc_method:
                # Custom gate
                gate_cb = getattr(rpc_method, "gate", None)
                if gate_cb and not gate_cb(connection):
                    await connection.send_rpc_error("Access denied by gating rules", corr_id, code=403)
                    return
                # Role gating
                req_role = getattr(rpc_method, "requires_role", None)
                if req_role:
                    profile = getattr(connection.session, "profile", None)
                    if not profile or req_role not in getattr(profile, "roles", set()):
                        await connection.send_rpc_error(f"Required role '{req_role}' is missing", corr_id, code=403)
                        return
                # Permission gating
                req_perm = getattr(rpc_method, "requires_permission", None)
                if req_perm:
                    profile = getattr(connection.session, "profile", None)
                    if not profile or req_perm not in getattr(profile, "permissions", set()):
                        await connection.send_rpc_error(f"Required permission '{req_perm}' is missing", corr_id, code=403)
                        return

            # Dispatch RPC
            response = await self._rpc_dispatcher.dispatch(
                method=method,
                params=params,
                authenticated=connection.is_authenticated
            )
            
            if response.get("success", False):
                res_val = response.get("result") if "result" in response else response.get("data")
                await connection.send_rpc_response(res_val, corr_id)
            else:
                await connection.send_rpc_error(
                    response.get("error", "Unknown RPC error"),
                    corr_id,
                    code=response.get("code")
                )
            return

        # Handle heartbeat pong — record liveness, never propagate
        if msg_type == MessageType.HEARTBEAT_PONG:
            self._heartbeat.record_pong(client_id)
            return
        
        # Respond to heartbeat pings from client
        if msg_type == MessageType.HEARTBEAT_PING:
            pong = self._encoder.encode_heartbeat_pong()
            self._rust_server.send_message(client_id, pong)
            return

        # Route standard messages
        if msg_type == MessageType.MESSAGE:
            msg_type_str = decoded.get_message_type_str()
            data = decoded.get_data()
            msg = Message(type=msg_type_str, data=data)
            
            response = await self._message_router.route(
                message=msg,
                context=connection,
                authenticated=connection.is_authenticated,
            )
            if response is not None:
                await connection.send_message(msg_type_str + "_response", response)
            return

    async def _process_binary_stream(self, client_id: str, payload: bytes):
        if len(payload) < 4:
            return
        name_len = int.from_bytes(payload[:4], byteorder='big')
        if len(payload) < 4 + name_len:
            return
        stream_name = payload[4:4+name_len].decode('utf-8')
        data = payload[4+name_len:]
        
        if self._on_binary_stream:
            try:
                await self._on_binary_stream(client_id, stream_name, data)
            except Exception as e:
                logger.error(f"Error in binary stream handler: {e}")

    async def _handle_client_connect_hook(self, connection: Connection) -> None:
        for hook in self._on_connect:
            try:
                await hook(connection)
            except Exception as e:
                logger.error(f"Error in connect hook: {e}")
                
    async def _handle_client_disconnect_hook(self, connection: Connection) -> None:
        if connection.id in self._connection_limiters:
            del self._connection_limiters[connection.id]
        for hook in self._on_disconnect:
            try:
                await hook(connection)
            except Exception as e:
                logger.error(f"Error in disconnect hook: {e}")
                
    # === Broadcasting ===
    
    async def broadcast(self, message_type: str, data: Any, exclude: Optional[set] = None) -> int:
        """Broadcast message to all connected clients."""
        return await self._pool.broadcast(message_type, data, exclude)
        
    def send_binary_stream(self, client_id: str, stream_name: str, data: bytes):
        """Stream direct binary data to a specific client using QUIC stream."""
        if not self._rust_server:
            raise RuntimeError("Server not started")
        self._rust_server.send_binary_stream(client_id, stream_name, data)
        
    # === Properties ===
    
    @property
    def is_running(self) -> bool:
        return self._running
        
    @property
    def connection_count(self) -> int:
        return self._pool.count
        
    @property
    def connections(self) -> List[Connection]:
        return self._pool.get_all()
        
    @property
    def config(self) -> ServerDescriptor:
        return self._config
        
    @property
    def address(self) -> tuple:
        return (self._config.host, self._config.port)
        
    @property
    def response(self) -> Response:
        return self._response
        
    @property
    def error(self) -> Error:
        return self._error

    def _setup_p2p_handlers(self) -> None:
        """Set up built-in P2P brokering handlers."""
        self._p2p_requests = {}
        
        async def handle_p2p_request(connection, data):
            import uuid
            target_id = data.get("target_id")
            request_id = data.get("request_id") or str(uuid.uuid4())
            
            target_conn = self._pool.get(target_id)
            if not target_conn:
                for c in self._pool.get_all():
                    if c.session and c.session.client_info and c.session.client_info.get("name") == target_id:
                        target_conn = c
                        break
                        
            if not target_conn:
                return {
                    "request_id": request_id,
                    "accepted": False,
                    "reason": "Target client not connected",
                }
                
            source_name = connection.session.client_info.get("name") if connection.session and connection.session.client_info else connection.id
            await target_conn.send_message("p2p_incoming", {
                "source_id": source_name,
                "source_addr": connection.remote_address,
                "request_id": request_id,
            })
            
            future = asyncio.get_running_loop().create_future()
            self._p2p_requests[request_id] = future
            
            try:
                res = await asyncio.wait_for(future, timeout=15.0)
                return {
                    "request_id": request_id,
                    **res
                }
            except asyncio.TimeoutError:
                return {
                    "request_id": request_id,
                    "accepted": False,
                    "reason": "Target client response timed out",
                }
            finally:
                self._p2p_requests.pop(request_id, None)

        async def handle_p2p_accept(connection, data):
            request_id = data.get("request_id")
            future = self._p2p_requests.get(request_id)
            if future and not future.done():
                future.set_result({
                    "accepted": data.get("accepted"),
                    "public_addr": data.get("public_addr"),
                    "lan_addr": data.get("lan_addr"),
                    "reason": data.get("reason"),
                })

        async def handle_p2p_punch_source(connection, data):
            target_id = data.get("target_id")
            request_id = data.get("request_id")
            source_addr = data.get("source_addr")
            lan_addr = data.get("lan_addr")
            
            target_conn = self._pool.get(target_id)
            if not target_conn:
                for c in self._pool.get_all():
                    if c.session and c.session.client_info and c.session.client_info.get("name") == target_id:
                        target_conn = c
                        break
                        
            if target_conn:
                await target_conn.send_message("p2p_punch_cmd", {
                    "request_id": request_id,
                    "source_addr": source_addr,
                    "lan_addr": lan_addr,
                })

        self._message_router.register("p2p_request", handle_p2p_request, requires_auth=True)
        self._message_router.register("p2p_accept", handle_p2p_accept, requires_auth=True)
        self._message_router.register("p2p_punch_source", handle_p2p_punch_source, requires_auth=True)
