"""
Connection Pool

Manages multiple client connections on the server side.
"""

import asyncio
from typing import Dict, List, Optional, Set, Callable, Any
import logging

from .connection import Connection

logger = logging.getLogger(__name__)


class ConnectionPool:
    """
    Pool of client connections.
    
    Manages multiple connections, provides broadcast, and cleanup.
    """
    
    def __init__(self, max_connections: int = 100):
        """
        Initialize connection pool.
        
        Args:
            max_connections: Maximum number of connections
        """
        self._max_connections = max_connections
        self._connections: Dict[str, Connection] = {}
        self._lock = asyncio.Lock()
        
        # Callbacks
        self._on_connect: Optional[Callable] = None
        self._on_disconnect: Optional[Callable] = None
    
    def set_callbacks(
        self,
        on_connect: Optional[Callable] = None,
        on_disconnect: Optional[Callable] = None
    ) -> None:
        """Set connection lifecycle callbacks."""
        self._on_connect = on_connect
        self._on_disconnect = on_disconnect
    
    async def add(self, connection: Connection) -> bool:
        """
        Add a connection to the pool.
        
        Args:
            connection: Connection to add
            
        Returns:
            True if added, False if pool is full
        """
        async with self._lock:
            if len(self._connections) >= self._max_connections:
                logger.warning("Connection pool full, rejecting connection")
                return False
            
            self._connections[connection.id] = connection
            
            # Set disconnect callback to auto-remove
            original_handler = connection._on_disconnect
            
            async def on_disconnect(conn: Connection):
                await self.remove(conn.id)
                if original_handler:
                    await original_handler(conn)
                if self._on_disconnect:
                    await self._on_disconnect(conn)
            
            connection.set_disconnect_handler(on_disconnect)
            
            logger.debug(f"Added connection {connection.id} to pool ({self.count}/{self._max_connections})")
            
            if self._on_connect:
                await self._on_connect(connection)
            
            return True
    
    async def remove(self, connection_id: str) -> bool:
        """
        Remove a connection from the pool.
        
        Args:
            connection_id: ID of connection to remove
            
        Returns:
            True if removed, False if not found
        """
        async with self._lock:
            if connection_id in self._connections:
                del self._connections[connection_id]
                logger.debug(f"Removed connection {connection_id} from pool ({self.count}/{self._max_connections})")
                return True
            return False
    
    def get(self, connection_id: str) -> Optional[Connection]:
        """
        Get a connection by ID.
        
        Args:
            connection_id: Connection ID
            
        Returns:
            Connection if found
        """
        return self._connections.get(connection_id)
    
    def get_all(self) -> List[Connection]:
        """Get all connections."""
        return list(self._connections.values())
    
    def get_authenticated(self) -> List[Connection]:
        """Get all authenticated connections."""
        return [c for c in self._connections.values() if c.is_authenticated]
    
    async def broadcast(
        self,
        message_type: str,
        data: Any,
        exclude: Optional[Set[str]] = None
    ) -> int:
        """
        Broadcast a message to all connections.
        
        Args:
            message_type: Message type
            data: Message data
            exclude: Set of connection IDs to exclude
            
        Returns:
            Number of connections message was sent to
        """
        exclude = exclude or set()
        count = 0
        
        tasks = []
        for conn_id, conn in self._connections.items():
            if conn_id not in exclude and conn.is_authenticated:
                tasks.append(self._send_to_connection(conn, message_type, data))
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            count = sum(1 for r in results if r is True)
        
        return count
    
    async def _send_to_connection(
        self,
        connection: Connection,
        message_type: str,
        data: Any
    ) -> bool:
        """Send to a single connection."""
        try:
            await connection.send_message(message_type, data)
            return True
        except Exception as e:
            logger.error(f"Failed to send to {connection.id}: {e}")
            return False
    
    async def close_all(self) -> None:
        """Close all connections."""
        async with self._lock:
            conns = list(self._connections.values())
            self._connections.clear()
            
        if conns:
            tasks = [conn.stop() for conn in conns]
            await asyncio.gather(*tasks, return_exceptions=True)
    
    @property
    def count(self) -> int:
        """Get connection count."""
        return len(self._connections)
    
    @property
    def is_full(self) -> bool:
        """Check if pool is full."""
        return len(self._connections) >= self._max_connections
    
    @property
    def available_slots(self) -> int:
        """Get number of available slots."""
        return self._max_connections - len(self._connections)
    
    def __len__(self) -> int:
        return len(self._connections)
    
    def __contains__(self, connection_id: str) -> bool:
        return connection_id in self._connections
