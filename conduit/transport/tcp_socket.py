"""
TCP Socket Wrapper

Async TCP socket implementation for client and server.
"""

import asyncio
import socket
from typing import Optional, Tuple, Callable, Awaitable
import logging

logger = logging.getLogger(__name__)


class TCPSocket:
    """
    Async TCP socket wrapper.
    
    Provides async read/write operations on top of asyncio streams.
    """
    
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        buffer_size: int = 65536
    ):
        """
        Initialize TCP socket wrapper.
        
        Args:
            reader: Asyncio stream reader
            writer: Asyncio stream writer
            buffer_size: Read buffer size
        """
        self._reader = reader
        self._writer = writer
        self._buffer_size = buffer_size
        self._closed = False
        self._remote_address: Optional[Tuple[str, int]] = None
        self._local_address: Optional[Tuple[str, int]] = None
        
        # Extract addresses
        try:
            peername = writer.get_extra_info('peername')
            if peername:
                self._remote_address = (peername[0], peername[1])
            sockname = writer.get_extra_info('sockname')
            if sockname:
                self._local_address = (sockname[0], sockname[1])
        except Exception:
            pass
    
    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        timeout: float = 10.0,
        use_ipv6: bool = False,
        buffer_size: int = 65536,
        ssl_context=None,  # Optional SSL context for TLS
    ) -> 'TCPSocket':
        """
        Connect to a remote server.
        
        Args:
            host: Server hostname or IP
            port: Server port
            timeout: Connection timeout
            use_ipv6: Use IPv6 family
            buffer_size: Socket buffer size
            ssl_context: Optional SSL context for TLS encryption
            
        Returns:
            Connected TCPSocket
        """
        family = socket.AF_INET6 if use_ipv6 else socket.AF_INET
        
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(
                host=host,
                port=port,
                family=family,
                ssl=ssl_context,
                server_hostname=host if ssl_context else None,
            ),
            timeout=timeout
        )
        
        return cls(reader, writer, buffer_size)
    
    async def read(self, n: int = -1) -> bytes:
        """
        Read data from socket.
        
        Args:
            n: Number of bytes to read (-1 for any available)
            
        Returns:
            Received bytes
        """
        if self._closed:
            return b''
        
        try:
            if n <= 0:
                return await self._reader.read(self._buffer_size)
            else:
                return await self._reader.read(n)
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            self._closed = True
            return b''
    
    async def read_exactly(self, n: int) -> bytes:
        """
        Read exactly n bytes.
        
        Args:
            n: Number of bytes to read
            
        Returns:
            Exactly n bytes
            
        Raises:
            asyncio.IncompleteReadError: If connection closed before n bytes received
        """
        if self._closed:
            raise asyncio.IncompleteReadError(b'', n)
        
        return await self._reader.readexactly(n)
    
    async def readline(self) -> bytes:
        """Read until newline."""
        if self._closed:
            return b''
        return await self._reader.readline()
    
    async def write(self, data: bytes) -> None:
        """
        Write data to socket.
        
        Args:
            data: Bytes to send
        """
        if self._closed:
            raise ConnectionResetError("Connection closed")
        
        try:
            self._writer.write(data)
            await self._writer.drain()
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError) as e:
            self._closed = True
            raise
    
    async def close(self) -> None:
        """Close the socket."""
        if self._closed:
            return
        
        self._closed = True
        
        try:
            self._writer.close()
            await self._writer.wait_closed()
        except Exception:
            pass
    
    @property
    def is_closed(self) -> bool:
        """Check if socket is closed."""
        return self._closed or self._writer.is_closing()
    
    @property
    def remote_address(self) -> Optional[Tuple[str, int]]:
        """Get remote address."""
        return self._remote_address
    
    @property
    def local_address(self) -> Optional[Tuple[str, int]]:
        """Get local address."""
        return self._local_address
    
    def at_eof(self) -> bool:
        """Check if reader is at EOF."""
        return self._reader.at_eof()


# Connection handler type
ConnectionHandler = Callable[['TCPSocket'], Awaitable[None]]


class TCPServer:
    """
    Async TCP server.
    
    Accepts incoming connections and handles them with the provided handler.
    """
    
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8080,
        handler: Optional[ConnectionHandler] = None,
        ipv6: bool = False,
        buffer_size: int = 65536,
        max_connections: int = 100,
        ssl_context=None,  # Optional SSL context for TLS
    ):
        """
        Initialize TCP server.
        
        Args:
            host: Bind address
            port: Bind port
            handler: Connection handler function
            ipv6: Enable IPv6
            buffer_size: Socket buffer size
            max_connections: Maximum concurrent connections
            ssl_context: Optional SSL context for TLS encryption
        """
        self._host = host
        self._port = port
        self._handler = handler
        self._ipv6 = ipv6
        self._buffer_size = buffer_size
        self._max_connections = max_connections
        self._ssl_context = ssl_context
        
        self._server: Optional[asyncio.Server] = None
        self._running = False
        self._connections: set[asyncio.Task] = set()
        self._connection_semaphore = asyncio.Semaphore(max_connections)
    
    def set_handler(self, handler: ConnectionHandler) -> None:
        """Set the connection handler."""
        self._handler = handler
    
    def set_ssl_context(self, ssl_context) -> None:
        """Set the SSL context for TLS."""
        self._ssl_context = ssl_context
    
    async def start(self) -> None:
        """Start the server."""
        if self._running:
            return
        
        if self._handler is None:
            raise ValueError("Connection handler not set")
        
        family = socket.AF_INET6 if self._ipv6 else socket.AF_INET
        
        self._server = await asyncio.start_server(
            self._handle_connection,
            host=self._host,
            port=self._port,
            family=family,
            reuse_address=True,
            start_serving=True,
            ssl=self._ssl_context,  # TLS support
        )
        
        self._running = True
        tls_status = " (TLS)" if self._ssl_context else ""
        logger.info(f"Server listening on {self._host}:{self._port}{tls_status}")
    
    async def stop(self) -> None:
        """Stop the server."""
        if not self._running:
            return
        
        self._running = False
        
        # Stop accepting new connections
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        
        # Cancel all active connections
        for task in self._connections:
            task.cancel()
        
        if self._connections:
            await asyncio.gather(*self._connections, return_exceptions=True)
        
        self._connections.clear()
        
        logger.info("Server stopped")
    
    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter
    ) -> None:
        """Handle an incoming connection."""
        async with self._connection_semaphore:
            socket_wrapper = TCPSocket(reader, writer, self._buffer_size)
            
            remote = socket_wrapper.remote_address
            logger.debug(f"New connection from {remote}")
            
            task = asyncio.current_task()
            if task:
                self._connections.add(task)
            
            try:
                await self._handler(socket_wrapper)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Error handling connection from {remote}: {e}")
            finally:
                try:
                    await asyncio.shield(socket_wrapper.close())
                except Exception:
                    pass
                finally:
                    if task:
                        self._connections.discard(task)
                    logger.debug(f"Connection closed from {remote}")
    
    async def wait_until_stopped(self) -> None:
        """Wait until server is stopped."""
        if self._server:
            await self._server.serve_forever()
    
    @property
    def is_running(self) -> bool:
        """Check if server is running."""
        return self._running
    
    @property
    def connection_count(self) -> int:
        """Get current connection count."""
        return len(self._connections)
    
    @property
    def address(self) -> Tuple[str, int]:
        """Get server address."""
        return (self._host, self._port)
