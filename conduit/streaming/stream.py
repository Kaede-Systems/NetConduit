"""
Bidirectional Streaming API Module

Provides continuous data streaming between server and client.
Both Server and Client can create streams and consume streams.
"""

import asyncio
from typing import Optional, Callable, Any, AsyncIterator, Dict, Set, TYPE_CHECKING
from dataclasses import dataclass, field
import time
import logging
import uuid

if TYPE_CHECKING:
    from conduit import Server, Client

logger = logging.getLogger(__name__)


@dataclass
class StreamInfo:
    """Information about a stream."""
    stream_id: str
    name: str
    owner: str  # "server" or client_id
    direction: str  # "push" (owner sends) or "pull" (owner receives)
    created_at: float = field(default_factory=time.time)
    message_count: int = 0
    bytes_sent: int = 0


class BidirectionalStream:
    """
    A bidirectional data stream that can be used by both Server and Client.
    
    Server creating a stream (Server pushes, Clients consume):
        stream = Stream("sensor_data", owner="server")
        await stream.push({"temperature": 22.5})
    
    Client creating a stream (Client pushes, Server consumes):
        stream = Stream("video_frames", owner=client_id)
        await stream.push(frame_data)
    """
    
    def __init__(
        self,
        name: str,
        owner: str = "server",
        buffer_size: int = 100,
    ):
        self.name = name
        self.owner = owner
        self.stream_id = str(uuid.uuid4())
        self.buffer_size = buffer_size
        
        self._subscribers: Dict[str, asyncio.Queue] = {}
        self._data_queue: asyncio.Queue = asyncio.Queue(maxsize=buffer_size)
        self._running = True
        self._info = StreamInfo(
            stream_id=self.stream_id,
            name=name,
            owner=owner,
            direction="push",
        )
        
        # Callbacks
        self._on_subscribe: Optional[Callable] = None
        self._on_unsubscribe: Optional[Callable] = None
        self._on_data: Optional[Callable] = None
    
    @property
    def info(self) -> StreamInfo:
        return self._info
    
    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)
    
    @property
    def is_active(self) -> bool:
        return self._running
    
    def on_subscribe(self, handler: Callable) -> Callable:
        """Decorator to register subscribe callback."""
        self._on_subscribe = handler
        return handler
    
    def on_unsubscribe(self, handler: Callable) -> Callable:
        """Decorator to register unsubscribe callback."""
        self._on_unsubscribe = handler
        return handler
    
    def on_data(self, handler: Callable) -> Callable:
        """Decorator to register data received callback."""
        self._on_data = handler
        return handler
    
    async def subscribe(self, subscriber_id: str) -> asyncio.Queue:
        """Subscribe to this stream to receive data."""
        if subscriber_id in self._subscribers:
            return self._subscribers[subscriber_id]
        
        queue = asyncio.Queue(maxsize=self.buffer_size)
        self._subscribers[subscriber_id] = queue
        
        logger.debug(f"Stream {self.name}: {subscriber_id[:8]} subscribed")
        
        if self._on_subscribe:
            try:
                await self._on_subscribe(subscriber_id)
            except Exception as e:
                logger.error(f"Subscribe callback error: {e}")
        
        return queue
    
    async def unsubscribe(self, subscriber_id: str) -> None:
        """Unsubscribe from this stream."""
        if subscriber_id in self._subscribers:
            del self._subscribers[subscriber_id]
            
            logger.debug(f"Stream {self.name}: {subscriber_id[:8]} unsubscribed")
            
            if self._on_unsubscribe:
                try:
                    await self._on_unsubscribe(subscriber_id)
                except Exception as e:
                    logger.error(f"Unsubscribe callback error: {e}")
    
    async def push(self, data: Any) -> int:
        """
        Push data to all subscribers.
        
        Returns:
            Number of subscribers who received the data
        """
        if not self._running:
            return 0
        
        if not self._subscribers:
            return 0
        
        message = {
            "stream": self.name,
            "stream_id": self.stream_id,
            "data": data,
            "timestamp": time.time(),
            "sequence": self._info.message_count,
        }
        
        sent = 0
        for subscriber_id, queue in list(self._subscribers.items()):
            try:
                queue.put_nowait(message)
                sent += 1
            except asyncio.QueueFull:
                logger.warning(f"Stream {self.name}: queue full for {subscriber_id[:8]}")
        
        self._info.message_count += 1
        
        return sent
    
    async def receive(self, timeout: Optional[float] = None) -> Optional[Any]:
        """
        Receive data posted to this stream (for stream consumers).
        
        Args:
            timeout: Optional timeout in seconds
            
        Returns:
            Data item or None if timeout/closed
        """
        try:
            if timeout:
                data = await asyncio.wait_for(self._data_queue.get(), timeout=timeout)
            else:
                data = await self._data_queue.get()
            return data
        except asyncio.TimeoutError:
            return None
    
    async def post(self, data: Any) -> bool:
        """
        Post data to this stream (for external producers).
        
        Args:
            data: Data to post
            
        Returns:
            True if posted, False if queue full
        """
        if not self._running:
            return False
        
        message = {
            "stream": self.name,
            "data": data,
            "timestamp": time.time(),
        }
        
        try:
            self._data_queue.put_nowait(message)
            
            if self._on_data:
                try:
                    await self._on_data(data)
                except Exception as e:
                    logger.error(f"Data callback error: {e}")
            
            return True
        except asyncio.QueueFull:
            return False
    
    async def close(self) -> None:
        """Close the stream and notify all subscribers."""
        self._running = False
        
        # Notify subscribers
        close_msg = {"stream": self.name, "closed": True}
        for subscriber_id, queue in list(self._subscribers.items()):
            try:
                queue.put_nowait(close_msg)
            except asyncio.QueueFull:
                pass
        
        # Notify local queue/iterator
        try:
            self._data_queue.put_nowait(close_msg)
        except asyncio.QueueFull:
            pass
        
        self._subscribers.clear()
        logger.info(f"Stream {self.name} closed")
    
    async def __aiter__(self) -> AsyncIterator[Any]:
        """Async iterator for consuming stream data."""
        while True:
            msg = await self._data_queue.get()
            if msg is None or msg.get("closed"):
                break
            yield msg.get("data")


class StreamManager:
    """
    Manages multiple bidirectional streams.
    
    Can be used by both Server and Client.
    
    Server usage:
        manager = StreamManager()
        manager.register_server_handlers(server)
        
        # Create a stream for pushing to clients
        sensor_stream = manager.create("sensors")
        await sensor_stream.push({"temp": 22.5})
        
        # Handle client-created streams
        @manager.on_client_stream("video_frames")
        async def handle_video(client_id, data):
            process_frame(data)
    
    Client usage:
        manager = StreamManager()
        manager.register_client_handlers(client)
        
        # Subscribe to server stream
        async for data in manager.subscribe("sensors"):
            print(data)
        
        # Create a stream to push to server
        video_stream = manager.create("video_frames")
        await video_stream.push(frame_data)
    """
    
    def __init__(self, owner: str = "server"):
        self.owner = owner
        self._streams: Dict[str, BidirectionalStream] = {}
        self._client_stream_handlers: Dict[str, Callable] = {}
        self._server: Optional["Server"] = None
        self._client: Optional["Client"] = None
    
    def create(self, name: str, buffer_size: int = 100) -> BidirectionalStream:
        """Create a new stream."""
        if name in self._streams:
            return self._streams[name]
        
        stream = BidirectionalStream(name, owner=self.owner, buffer_size=buffer_size)
        self._streams[name] = stream
        logger.info(f"Created stream: {name}")
        return stream
    
    def get(self, name: str) -> Optional[BidirectionalStream]:
        """Get a stream by name."""
        return self._streams.get(name)
    
    def list_streams(self) -> list:
        """List all streams."""
        return [
            {
                "name": s.name,
                "owner": s.owner,
                "subscribers": s.subscriber_count,
                "active": s.is_active,
            }
            for s in self._streams.values()
        ]
    
    def on_client_stream(self, stream_name: str) -> Callable:
        """Decorator to handle data from client-created streams."""
        def decorator(handler: Callable) -> Callable:
            self._client_stream_handlers[stream_name] = handler
            return handler
        return decorator
    
    # === Server Integration ===
    
    def register_server_handlers(self, server: "Server") -> None:
        """Register stream RPC handlers on a server."""
        self._server = server
        
        @server.rpc
        async def stream_list() -> dict:
            """List available streams."""
            return {"streams": self.list_streams()}
        
        @server.rpc
        async def stream_subscribe(stream_name: str) -> dict:
            """Subscribe to a stream."""
            stream = self._streams.get(stream_name)
            if not stream:
                return {"error": f"Stream not found: {stream_name}"}
            
            # Get client ID from context (simplified)
            client_id = str(uuid.uuid4())  # Would be from connection context
            await stream.subscribe(client_id)
            return {"subscribed": True, "stream": stream_name}
        
        @server.rpc
        async def stream_unsubscribe(stream_name: str) -> dict:
            """Unsubscribe from a stream."""
            stream = self._streams.get(stream_name)
            if stream:
                client_id = str(uuid.uuid4())  # Would be from connection context
                await stream.unsubscribe(client_id)
            return {"unsubscribed": True}
        
        @server.rpc
        async def stream_create(stream_name: str) -> dict:
            """Client requests to create a stream."""
            if stream_name in self._streams:
                return {"error": "Stream already exists"}
            
            client_id = str(uuid.uuid4())  # Would be from connection context
            stream = BidirectionalStream(stream_name, owner=client_id)
            self._streams[stream_name] = stream
            return {"created": True, "stream": stream_name, "stream_id": stream.stream_id}
        
        # Handle client stream data
        @server.on("stream_data")
        async def handle_stream_data(client, data):
            """Receive stream data from client."""
            stream_name = data.get("stream")
            payload = data.get("data")
            
            # Check if we have a handler
            handler = self._client_stream_handlers.get(stream_name)
            if handler:
                await handler(client.id, payload)
            
            # Also post to stream if it exists
            stream = self._streams.get(stream_name)
            if stream:
                await stream.post(payload)
            
            return {"received": True}
        
        logger.info("Stream handlers registered on server")
    
    def register_client_handlers(self, client: "Client") -> None:
        """Register stream handlers on a client."""
        self._client = client
        
        @client.on("stream_data")
        async def handle_stream_data(msg):
            """Receive stream data from server."""
            stream_name = msg.get("stream")
            stream = self._streams.get(stream_name)
            if stream:
                await stream.post(msg)
        
        @client.on("stream_closed")
        async def handle_stream_closed(msg):
            """Stream was closed by server."""
            stream_name = msg.get("stream")
            stream = self._streams.get(stream_name)
            if stream:
                await stream.close()
        
        logger.info("Stream handlers registered on client")
    
    async def subscribe(
        self,
        stream_name: str,
        on_data: Optional[Callable] = None,
    ) -> AsyncIterator[Any]:
        """
        Subscribe to a stream and iterate over data.
        
        Args:
            stream_name: Name of stream to subscribe to
            on_data: Optional callback for each data item
            
        Yields:
            Stream data items
        """
        if not self._client:
            raise RuntimeError("Client not registered. Call register_client_handlers first.")
        
        from conduit import data
        
        # Subscribe via RPC
        result = await self._client.rpc.call("stream_subscribe", args=data(
            stream_name=stream_name,
        ))
        
        if not result.get("success"):
            raise Exception(f"Failed to subscribe: {result}")
        
        # Create local stream to receive data
        stream = self.create(stream_name)
        
        try:
            async for item in stream:
                if on_data:
                    await on_data(item)
                yield item
        finally:
            await self._client.rpc.call("stream_unsubscribe", args=data(
                stream_name=stream_name,
            ))
    
    async def push_to_server(self, stream_name: str, data_item: Any) -> bool:
        """Push data to a stream on the server (client-side)."""
        if not self._client:
            raise RuntimeError("Client not registered")
        
        await self._client.send("stream_data", {
            "stream": stream_name,
            "data": data_item,
            "timestamp": time.time(),
        })
        return True
    
    async def push_to_clients(
        self,
        stream_name: str,
        data_item: Any,
        client_ids: Optional[Set[str]] = None,
    ) -> int:
        """Push data to stream subscribers (server-side)."""
        if not self._server:
            raise RuntimeError("Server not registered")
        
        stream = self._streams.get(stream_name)
        if not stream:
            return 0
        
        # Push locally
        count = await stream.push(data_item)
        
        # Also broadcast to clients
        msg = {
            "stream": stream_name,
            "data": data_item,
            "timestamp": time.time(),
        }
        
        if client_ids:
            for cid in client_ids:
                conn = self._server._pool.get(cid)
                if conn:
                    await conn.send_message("stream_data", msg)
        else:
            await self._server.broadcast("stream_data", msg)
        
        return count
    
    async def close_all(self) -> None:
        """Close all streams."""
        for stream in self._streams.values():
            await stream.close()
        self._streams.clear()


# Backwards compatibility
Stream = BidirectionalStream
