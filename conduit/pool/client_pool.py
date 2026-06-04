"""
Client Connection Pool Module

Provides pooled connections to servers with load balancing.
"""

import asyncio
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass, field
import time
import random
import logging

logger = logging.getLogger(__name__)


@dataclass
class PoolStats:
    """Statistics for a connection pool."""
    total_connections: int = 0
    active_connections: int = 0
    requests_sent: int = 0
    requests_failed: int = 0
    avg_latency_ms: float = 0.0


class ClientPool:
    """
    Connection pool for multiple client connections.
    
    Supports load balancing across multiple connections or servers.
    
    Usage:
        pool = ClientPool(
            servers=[
                ("server1.example.com", 8080),
                ("server2.example.com", 8080),
            ],
            password="secret",
            pool_size=3,
        )
        
        await pool.connect_all()
        
        # Make RPC call (auto load-balanced)
        result = await pool.rpc("add", a=10, b=20)
        
        # Broadcast to all
        await pool.broadcast_rpc("notify", message="hello")
    """
    
    def __init__(
        self,
        servers: List[tuple],
        password: str,
        pool_size: int = 1,
        strategy: str = "round_robin",  # round_robin, random, least_latency
        **client_kwargs,
    ):
        """
        Initialize connection pool.
        
        Args:
            servers: List of (host, port) tuples
            password: Password for authentication
            pool_size: Number of connections per server
            strategy: Load balancing strategy
            **client_kwargs: Additional ClientDescriptor arguments
        """
        self.servers = servers
        self.password = password
        self.pool_size = pool_size
        self.strategy = strategy
        self.client_kwargs = client_kwargs
        
        self._clients: List = []
        self._current_index = 0
        self._latencies: Dict[int, float] = {}
        self._stats = PoolStats()
        self._lock = asyncio.Lock()
        self._registered_handlers: Dict[str, Callable] = {}
    
    @property
    def stats(self) -> PoolStats:
        return self._stats
    
    @property
    def connected_count(self) -> int:
        return sum(1 for c in self._clients if c.is_connected)
    
    async def connect_all(self) -> int:
        """
        Connect to all servers.
        
        Returns:
            Number of successful connections
        """
        from conduit import Client, ClientDescriptor
        
        connected = 0
        
        for host, port in self.servers:
            for i in range(self.pool_size):
                client = Client(ClientDescriptor(
                    server_host=host,
                    server_port=port,
                    password=self.password,
                    reconnect_enabled=True,
                    **self.client_kwargs,
                ))
                
                # Apply registered handlers to this client
                for msg_type, handler in self._registered_handlers.items():
                    client._message_router.register(
                        message_type=msg_type,
                        handler=lambda conn, data, h=handler: h(data),
                        requires_auth=False,
                    )
                
                try:
                    if await client.connect():
                        self._clients.append(client)
                        connected += 1
                        logger.info(f"Pool: Connected to {host}:{port} (#{i+1})")
                except Exception as e:
                    logger.error(f"Pool: Failed to connect to {host}:{port}: {e}")
        
        self._stats.total_connections = len(self._clients)
        self._stats.active_connections = connected
        
        return connected
    
    async def disconnect_all(self) -> None:
        """Disconnect all clients."""
        for client in self._clients:
            try:
                await client.disconnect()
            except Exception:
                pass
        
        self._clients.clear()
        self._stats.active_connections = 0
    
    def _get_next_client(self):
        """Get the next client based on strategy."""
        active = [c for c in self._clients if c.is_connected]
        if not active:
            return None
        
        if self.strategy == "random":
            return random.choice(active)
        
        elif self.strategy == "least_latency":
            # Sort by latency, pick lowest
            idx_clients = [(i, c) for i, c in enumerate(active)]
            idx_clients.sort(key=lambda x: self._latencies.get(x[0], 0))
            return idx_clients[0][1]
        
        else:  # round_robin (default)
            client = active[self._current_index % len(active)]
            self._current_index = (self._current_index + 1) % len(active)
            return client
    
    async def rpc(
        self,
        method: str,
        timeout: Optional[float] = None,
        **params,
    ) -> Any:
        """
        Make an RPC call using a pooled connection.
        
        Args:
            method: RPC method name
            timeout: Optional timeout
            **params: Method parameters
            
        Returns:
            RPC result
        """
        from conduit import data
        
        client = self._get_next_client()
        if not client:
            raise ConnectionError("No active connections in pool")
        
        start = time.time()
        
        try:
            kwargs = {"args": data(**params)}
            if timeout:
                kwargs["timeout"] = timeout
            
            result = await client.rpc.call(method, **kwargs)
            
            # Track latency
            latency = (time.time() - start) * 1000
            client_idx = self._clients.index(client)
            self._latencies[client_idx] = latency
            
            self._stats.requests_sent += 1
            
            return result
            
        except Exception as e:
            self._stats.requests_failed += 1
            raise
    
    async def broadcast_rpc(
        self,
        method: str,
        **params,
    ) -> List[Any]:
        """
        Send RPC call to all connected clients.
        
        Returns:
            List of results from all clients
        """
        from conduit import data
        
        results = []
        
        for client in self._clients:
            if client.is_connected:
                try:
                    result = await client.rpc.call(method, args=data(**params))
                    results.append(result)
                except Exception as e:
                    results.append({"error": str(e)})
        
        return results
    
    async def send(self, message_type: str, data_dict: dict) -> int:
        """
        Send message using a pooled connection.
        
        Returns:
            1 if sent, 0 if no connection
        """
        client = self._get_next_client()
        if not client:
            return 0
        
        await client.send(message_type, data_dict)
        return 1
    
    async def broadcast(self, message_type: str, data_dict: dict) -> int:
        """
        Send message to all connected clients.
        
        Returns:
            Number of clients message was sent to
        """
        sent = 0
        for client in self._clients:
            if client.is_connected:
                try:
                    await client.send(message_type, data_dict)
                    sent += 1
                except Exception:
                    pass
        return sent
    
    def on(self, message_type: str) -> Callable:
        """
        Register a message handler on all pool clients.
        
        Usage:
            @pool.on("notification")
            async def handle_notification(msg):
                print(f"Got: {msg}")
        """
        def decorator(handler):
            self._registered_handlers[message_type] = handler
            for client in self._clients:
                client._message_router.register(
                    message_type=message_type,
                    handler=lambda conn, data, h=handler: h(data),
                    requires_auth=False,
                )
            return handler
        return decorator
