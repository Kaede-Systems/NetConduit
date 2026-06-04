"""
Bidirectional File Transfer Module

Provides chunked file upload/download with progress tracking and resume support.
Both Server and Client can send and receive files.
"""

import asyncio
import hashlib
import os
import time
import base64
import uuid
from typing import Optional, Callable, Any, Dict, TYPE_CHECKING
from dataclasses import dataclass, field
import logging

if TYPE_CHECKING:
    from conduit import Server, Client

logger = logging.getLogger(__name__)

# Default chunk size (64KB)
DEFAULT_CHUNK_SIZE = 64 * 1024


@dataclass
class TransferProgress:
    """Progress tracking for file transfers."""
    filename: str
    total_size: int
    transferred: int = 0
    chunks_sent: int = 0
    direction: str = "upload"  # "upload" or "download"
    start_time: float = field(default_factory=time.time)
    
    @property
    def percent(self) -> float:
        if self.total_size == 0:
            return 100.0
        return (self.transferred / self.total_size) * 100
    
    @property
    def elapsed(self) -> float:
        return time.time() - self.start_time
    
    @property
    def speed(self) -> float:
        """Bytes per second."""
        if self.elapsed == 0:
            return 0
        return self.transferred / self.elapsed
    
    @property
    def eta(self) -> float:
        """Estimated time remaining in seconds."""
        if self.speed == 0:
            return float('inf')
        remaining = self.total_size - self.transferred
        return remaining / self.speed
    
    def to_dict(self) -> dict:
        return {
            "filename": self.filename,
            "total_size": self.total_size,
            "transferred": self.transferred,
            "percent": round(self.percent, 2),
            "speed_mbps": round(self.speed / 1024 / 1024, 2),
            "eta_seconds": round(self.eta, 1) if self.eta != float('inf') else None,
            "direction": self.direction,
        }


@dataclass
class TransferMetadata:
    """Metadata for a file transfer."""
    filename: str
    size: int
    checksum: str
    chunk_size: int
    total_chunks: int
    transfer_id: str


class FileTransferHandler:
    """
    Bidirectional file transfer handler.
    
    Can be used by both Server and Client for sending and receiving files.
    
    Usage (Server - receiving from client):
        transfer = FileTransferHandler(storage_dir="./uploads")
        transfer.register_server_handlers(server)
        
        @server.on_file_received
        async def handle_file(client, filepath, metadata):
            print(f"Got {filepath} from {client.id}")
    
    Usage (Server - sending to client):
        await transfer.send_to_client(server, client_id, "report.pdf")
    
    Usage (Client - sending to server):
        await transfer.send_to_server(client, "upload.zip", on_progress=callback)
    
    Usage (Client - receiving from server):
        await transfer.receive_from_server(client, "report.pdf", "./downloads/")
    """
    
    def __init__(
        self,
        storage_dir: str = "./transfers",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ):
        self.storage_dir = storage_dir
        self.chunk_size = chunk_size
        self._active_transfers: Dict[str, dict] = {}
        self._on_file_received: Optional[Callable] = None
        self._on_transfer_progress: Optional[Callable] = None
        
        os.makedirs(storage_dir, exist_ok=True)
    
    def on_file_received(self, handler: Callable) -> Callable:
        """Decorator to register file received callback."""
        self._on_file_received = handler
        return handler
    
    def on_progress(self, handler: Callable) -> Callable:
        """Decorator to register progress callback."""
        self._on_transfer_progress = handler
        return handler
    
    # === Server Integration ===
    
    def register_server_handlers(self, server: "Server") -> None:
        """Register RPC handlers on a server for file transfer."""
        
        @server.rpc
        async def file_upload_start(filename: str, size: int, checksum: str) -> dict:
            """Start receiving a file upload from client."""
            return await self.start_receive(filename, size, checksum)
        
        @server.rpc
        async def file_upload_chunk(transfer_id: str, chunk_index: int, chunk_data: Any = None, data_b64: Optional[str] = None) -> dict:
            """Receive a file chunk from client."""
            data_to_use = chunk_data if chunk_data is not None else data_b64
            return await self.receive_chunk(transfer_id, chunk_index, data_to_use)
        
        @server.rpc
        async def file_upload_complete(transfer_id: str) -> dict:
            """Complete a file upload from client."""
            result = await self.complete_receive(transfer_id)
            if result.get("success") and self._on_file_received:
                # Get connection from context if available
                await self._on_file_received(None, result.get("path"), result)
            return result
        
        @server.rpc
        async def file_download_start(filename: str) -> dict:
            """Start a file download to client."""
            return await self.start_download(filename)
        
        @server.rpc
        async def file_download_chunk(transfer_id: str, chunk_index: int) -> dict:
            """Get a chunk for download to client."""
            return await self.get_download_chunk(transfer_id, chunk_index)
        
        @server.rpc
        async def file_list() -> dict:
            """List available files for download."""
            files = []
            for f in os.listdir(self.storage_dir):
                if not f.startswith('.'):
                    path = os.path.join(self.storage_dir, f)
                    if os.path.isfile(path):
                        files.append({
                            "name": f,
                            "size": os.path.getsize(path),
                        })
            return {"files": files}
        
        logger.info("File transfer handlers registered on server")
    
    def register_client_handlers(self, client: "Client") -> None:
        """Register message handlers on a client for receiving files from server."""
        
        @client.on("file_incoming")
        async def handle_file_incoming(msg):
            """Server is sending a file to us."""
            transfer_id = msg.get("transfer_id")
            filename = msg.get("filename")
            size = msg.get("size")
            checksum = msg.get("checksum")
            
            # Start receiving
            result = await self.start_receive(filename, size, checksum, transfer_id)
            
            # Acknowledge
            await client.send("file_incoming_ack", {
                "transfer_id": transfer_id,
                "ready": True,
            })
        
        @client.on("file_chunk")
        async def handle_file_chunk(msg):
            """Receive a chunk from server."""
            transfer_id = msg.get("transfer_id")
            chunk_index = msg.get("chunk_index")
            chunk_data = msg.get("chunk_data")
            if chunk_data is None:
                chunk_data = msg.get("data_b64")
            
            result = await self.receive_chunk(transfer_id, chunk_index, chunk_data)
            
            # Progress callback
            if self._on_transfer_progress:
                transfer = self._active_transfers.get(transfer_id)
                if transfer:
                    progress = TransferProgress(
                        filename=transfer["filename"],
                        total_size=transfer["size"],
                        transferred=transfer["received_chunks"] * self.chunk_size,
                        chunks_sent=transfer["received_chunks"],
                        direction="download",
                    )
                    await self._on_transfer_progress(progress)
        
        @client.on("file_complete")
        async def handle_file_complete(msg):
            """File transfer from server complete."""
            transfer_id = msg.get("transfer_id")
            result = await self.complete_receive(transfer_id)
            
            if result.get("success") and self._on_file_received:
                await self._on_file_received(result.get("path"), result)
        
        logger.info("File transfer handlers registered on client")
    
    # === Sending Files ===
    
    async def send_to_server(
        self,
        client: "Client",
        filepath: str,
        on_progress: Optional[Callable[[TransferProgress], None]] = None,
    ) -> dict:
        """
        Send a file from client to server.
        
        Args:
            client: Connected Client instance
            filepath: Path to file to send
            on_progress: Progress callback
            
        Returns:
            Transfer result
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")
        
        filename = os.path.basename(filepath)
        size = os.path.getsize(filepath)
        checksum = self._compute_checksum(filepath)
        total_chunks = (size + self.chunk_size - 1) // self.chunk_size
        
        logger.info(f"Sending to server: {filename} ({size} bytes, {total_chunks} chunks)")
        
        from conduit import data
        
        # Start transfer
        result = await client.rpc.call("file_upload_start", args=data(
            filename=filename,
            size=size,
            checksum=checksum,
        ))
        
        if not result.get("success"):
            return result
        
        transfer_id = result.get("data", {}).get("transfer_id")
        
        # Send chunks
        progress = TransferProgress(filename=filename, total_size=size, direction="upload")
        
        with open(filepath, "rb") as f:
            chunk_index = 0
            while True:
                chunk = f.read(self.chunk_size)
                if not chunk:
                    break
                
                result = await client.rpc.call("file_upload_chunk", args=data(
                    transfer_id=transfer_id,
                    chunk_index=chunk_index,
                    chunk_data=chunk,
                ))
                
                if not result.get("success"):
                    logger.error(f"Chunk {chunk_index} failed: {result}")
                    return result
                
                progress.transferred += len(chunk)
                progress.chunks_sent = chunk_index + 1
                
                if on_progress:
                    on_progress(progress)
                
                chunk_index += 1
        
        # Complete transfer
        result = await client.rpc.call("file_upload_complete", args=data(
            transfer_id=transfer_id,
        ))
        
        logger.info(f"Upload complete: {filename} in {progress.elapsed:.2f}s")
        return result
    
    async def send_to_client(
        self,
        server: "Server",
        client_id: str,
        filepath: str,
        on_progress: Optional[Callable[[TransferProgress], None]] = None,
    ) -> dict:
        """
        Send a file from server to a specific client.
        
        Args:
            server: Server instance
            client_id: ID of client to send to
            filepath: Path to file to send
            on_progress: Progress callback
            
        Returns:
            Transfer result
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")
        
        filename = os.path.basename(filepath)
        size = os.path.getsize(filepath)
        checksum = self._compute_checksum(filepath)
        total_chunks = (size + self.chunk_size - 1) // self.chunk_size
        transfer_id = str(uuid.uuid4())
        
        logger.info(f"Sending to client {client_id[:8]}: {filename} ({size} bytes)")
        
        # Notify client
        connection = server._pool.get(client_id)
        if not connection:
            return {"error": "Client not found"}
        
        await connection.send_message("file_incoming", {
            "transfer_id": transfer_id,
            "filename": filename,
            "size": size,
            "checksum": checksum,
            "total_chunks": total_chunks,
        })
        
        # Wait for ack (with timeout)
        # For simplicity, we'll just proceed and send chunks
        await asyncio.sleep(0.1)
        
        # Send chunks
        progress = TransferProgress(filename=filename, total_size=size, direction="upload")
        
        with open(filepath, "rb") as f:
            chunk_index = 0
            while True:
                chunk = f.read(self.chunk_size)
                if not chunk:
                    break
                
                await connection.send_message("file_chunk", {
                    "transfer_id": transfer_id,
                    "chunk_index": chunk_index,
                    "chunk_data": chunk,
                })
                
                progress.transferred += len(chunk)
                progress.chunks_sent = chunk_index + 1
                
                if on_progress:
                    on_progress(progress)
                
                chunk_index += 1
                
                # Small delay to prevent overwhelming
                if chunk_index % 10 == 0:
                    await asyncio.sleep(0.01)
        
        # Complete
        await connection.send_message("file_complete", {
            "transfer_id": transfer_id,
            "checksum": checksum,
        })
        
        logger.info(f"Sent to client: {filename} in {progress.elapsed:.2f}s")
        return {"success": True, "filename": filename, "size": size}
    
    # === Receiving Files ===
    
    async def receive_from_server(
        self,
        client: "Client",
        remote_filename: str,
        local_dir: str = "./downloads",
        on_progress: Optional[Callable[[TransferProgress], None]] = None,
    ) -> dict:
        """
        Download a file from server.
        
        Args:
            client: Connected Client instance
            remote_filename: Filename on server
            local_dir: Local directory to save to
            on_progress: Progress callback
            
        Returns:
            Download result
        """
        from conduit import data
        
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, remote_filename)
        
        # Get file info
        result = await client.rpc.call("file_download_start", args=data(
            filename=remote_filename,
        ))
        
        if not result.get("success"):
            return result
        
        info = result.get("data", {})
        transfer_id = info.get("transfer_id")
        size = info.get("size", 0)
        total_chunks = info.get("total_chunks", 0)
        
        logger.info(f"Downloading: {remote_filename} ({size} bytes, {total_chunks} chunks)")
        
        progress = TransferProgress(filename=remote_filename, total_size=size, direction="download")
        
        with open(local_path, "wb") as f:
            for chunk_index in range(total_chunks):
                result = await client.rpc.call("file_download_chunk", args=data(
                    transfer_id=transfer_id,
                    chunk_index=chunk_index,
                ))
                
                if not result.get("success"):
                    return result
                
                chunk_data = result.get("data", {}).get("chunk_data")
                if chunk_data is None:
                    chunk_b64 = result.get("data", {}).get("data_b64", "")
                    chunk = base64.b64decode(chunk_b64)
                else:
                    chunk = chunk_data
                f.write(chunk)
                
                progress.transferred += len(chunk)
                progress.chunks_sent = chunk_index + 1
                
                if on_progress:
                    on_progress(progress)
        
        logger.info(f"Download complete: {remote_filename} in {progress.elapsed:.2f}s")
        return {"success": True, "path": local_path, "size": size}
    
    # === Internal Methods ===
    
    async def start_receive(
        self,
        filename: str,
        size: int,
        checksum: str,
        transfer_id: Optional[str] = None,
    ) -> dict:
        """Start receiving a file."""
        if transfer_id is None:
            transfer_id = str(uuid.uuid4())
        
        total_chunks = (size + self.chunk_size - 1) // self.chunk_size
        temp_path = os.path.join(self.storage_dir, f".{transfer_id}.tmp")
        
        self._active_transfers[transfer_id] = {
            "filename": filename,
            "size": size,
            "checksum": checksum,
            "total_chunks": total_chunks,
            "received_chunks": 0,
            "temp_path": temp_path,
            "final_path": os.path.join(self.storage_dir, filename),
            "file": open(temp_path, "wb"),
            "start_time": time.time(),
        }
        
        logger.debug(f"Started receive: {filename} ({size} bytes, id={transfer_id[:8]})")
        
        return {
            "transfer_id": transfer_id,
            "chunk_size": self.chunk_size,
            "total_chunks": total_chunks,
        }
    
    async def receive_chunk(
        self,
        transfer_id: str,
        chunk_index: int,
        chunk_data: Any,
    ) -> dict:
        """Receive a file chunk."""
        transfer = self._active_transfers.get(transfer_id)
        if not transfer:
            return {"error": "Transfer not found"}
        
        if isinstance(chunk_data, str):
            chunk = base64.b64decode(chunk_data)
        else:
            chunk = chunk_data
            
        transfer["file"].write(chunk)
        transfer["received_chunks"] += 1
        
        return {
            "received": True,
            "chunk_index": chunk_index,
            "chunks_received": transfer["received_chunks"],
            "total_chunks": transfer["total_chunks"],
        }
    
    async def complete_receive(self, transfer_id: str) -> dict:
        """Complete a file receive."""
        transfer = self._active_transfers.get(transfer_id)
        if not transfer:
            return {"error": "Transfer not found"}
        
        transfer["file"].close()
        
        # Verify checksum
        computed = self._compute_checksum(transfer["temp_path"])
        if computed != transfer["checksum"]:
            os.remove(transfer["temp_path"])
            del self._active_transfers[transfer_id]
            return {"error": "Checksum mismatch", "expected": transfer["checksum"], "got": computed}
        
        # Move to final location
        final_path = transfer["final_path"]
        if os.path.exists(final_path):
            # Add timestamp to avoid overwrite
            base, ext = os.path.splitext(final_path)
            final_path = f"{base}_{int(time.time())}{ext}"
        
        os.rename(transfer["temp_path"], final_path)
        
        elapsed = time.time() - transfer["start_time"]
        del self._active_transfers[transfer_id]
        
        logger.info(f"Completed receive: {transfer['filename']} in {elapsed:.2f}s")
        
        return {
            "success": True,
            "filename": transfer["filename"],
            "size": transfer["size"],
            "path": final_path,
            "elapsed": elapsed,
        }
    
    async def start_download(self, filename: str) -> dict:
        """Start a file download."""
        filepath = os.path.join(self.storage_dir, filename)
        if not os.path.exists(filepath):
            return {"error": "File not found"}
        
        size = os.path.getsize(filepath)
        total_chunks = (size + self.chunk_size - 1) // self.chunk_size
        transfer_id = str(uuid.uuid4())
        
        self._active_transfers[transfer_id] = {
            "filepath": filepath,
            "size": size,
            "total_chunks": total_chunks,
            "file": open(filepath, "rb"),
        }
        
        return {
            "transfer_id": transfer_id,
            "size": size,
            "total_chunks": total_chunks,
            "chunk_size": self.chunk_size,
        }
    
    async def get_download_chunk(self, transfer_id: str, chunk_index: int) -> dict:
        """Get a chunk for download."""
        transfer = self._active_transfers.get(transfer_id)
        if not transfer:
            return {"error": "Transfer not found"}
        
        transfer["file"].seek(chunk_index * self.chunk_size)
        chunk = transfer["file"].read(self.chunk_size)
        
        return {
            "chunk_index": chunk_index,
            "chunk_data": chunk,
            "size": len(chunk),
        }
    
    def _compute_checksum(self, filepath: str) -> str:
        """Compute SHA256 checksum of a file."""
        sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()


# Backwards compatibility alias
FileTransfer = FileTransferHandler
