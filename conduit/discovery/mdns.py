"""
Local Multicast and Remote Unicast Service Discovery (mDNS-like).
"""

import socket
import struct
import json
import asyncio
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

MULTICAST_GRP = "224.0.0.251"
MULTICAST_PORT = 53535  # Custom port to prevent system daemon binding conflicts


class DiscoveryService:
    """
    Handles local service advertisement over multicast UDP,
    and discovery of local/remote servers.
    """
    
    def __init__(self, service_type: str = "conduit"):
        self.service_type = service_type
        self._advertiser_task: Optional[asyncio.Task] = None
        self._running = False
        
    async def start_advertiser(self, name: str, host: str, port: int, metadata: Optional[dict] = None) -> None:
        """Start advertising this server on the local network."""
        self._running = True
        self._advertiser_task = asyncio.create_task(
            self._run_advertiser(name, host, port, metadata or {})
        )
        
    async def stop_advertiser(self) -> None:
        """Stop advertising."""
        self._running = False
        if self._advertiser_task:
            self._advertiser_task.cancel()
            try:
                await self._advertiser_task
            except asyncio.CancelledError:
                pass
            self._advertiser_task = None

    async def _run_advertiser(self, name: str, host: str, port: int, metadata: dict) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            pass
            
        # Join multicast group
        mreq = struct.pack("4sl", socket.inet_aton(MULTICAST_GRP), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        
        # Bind to multicast port
        try:
            sock.bind(("", MULTICAST_PORT))
        except Exception as e:
            logger.warning(f"Advertiser failed to bind to wildcard port {MULTICAST_PORT}: {e}")
            # Try binding to specific multicast address or a different local port
            try:
                sock.bind((MULTICAST_GRP, MULTICAST_PORT))
            except Exception:
                sock.bind(("", 0))
                
        sock.setblocking(False)
        loop = asyncio.get_running_loop()
        logger.info(f"mDNS Advertiser started for '{name}' on multicast {MULTICAST_GRP}:{MULTICAST_PORT}")
        
        packet_data = {
            "type": "announce",
            "service": self.service_type,
            "name": name,
            "host": host,
            "port": port,
            "metadata": metadata
        }
        encoded = json.dumps(packet_data).encode("utf-8")
        
        while self._running:
            try:
                # Announce on the multicast group
                sock.sendto(encoded, (MULTICAST_GRP, MULTICAST_PORT))
                
                # Listen for queries and respond
                for _ in range(10):
                    if not self._running:
                        break
                    try:
                        data, addr = await loop.sock_recvfrom(sock, 1024)
                        msg = json.loads(data.decode("utf-8"))
                        if msg.get("type") == "query" and msg.get("service") == self.service_type:
                            response = {
                                "type": "response",
                                "service": self.service_type,
                                "name": name,
                                "host": host,
                                "port": port,
                                "metadata": metadata
                            }
                            sock.sendto(json.dumps(response).encode("utf-8"), addr)
                    except (BlockingIOError, InterruptedError):
                        await asyncio.sleep(0.1)
                    except Exception:
                        await asyncio.sleep(0.1)
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Advertiser error: {e}")
                await asyncio.sleep(1.0)
        sock.close()

    @classmethod
    async def discover(cls, timeout: float = 1.5, remote_hosts: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Discover active servers on local multicast and optional list of remote hosts.
        """
        discovered = {}
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            pass
            
        sock.setblocking(False)
        loop = asyncio.get_running_loop()
        
        sock.bind(("", 0))
        
        # Try joining multicast
        try:
            mreq = struct.pack("4sl", socket.inet_aton(MULTICAST_GRP), socket.INADDR_ANY)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        except Exception as e:
            logger.warning(f"Could not join multicast membership in discovery: {e}")
            
        query_msg = {
            "type": "query",
            "service": "conduit"
        }
        encoded_query = json.dumps(query_msg).encode("utf-8")
        
        # Send local multicast
        try:
            sock.sendto(encoded_query, (MULTICAST_GRP, MULTICAST_PORT))
        except Exception:
            pass
            
        # Send remote unicast
        if remote_hosts:
            for r_host in remote_hosts:
                try:
                    if ":" in r_host:
                        h, p = r_host.split(":")
                        p = int(p)
                    else:
                        h, p = r_host, MULTICAST_PORT
                    sock.sendto(encoded_query, (h, p))
                except Exception as e:
                    logger.debug(f"Unicast query to {r_host} failed: {e}")
                    
        # Collect responses
        start_time = loop.time()
        while loop.time() - start_time < timeout:
            time_left = timeout - (loop.time() - start_time)
            if time_left <= 0:
                break
            try:
                data, addr = await asyncio.wait_for(
                    loop.sock_recvfrom(sock, 1024),
                    timeout=min(time_left, 0.2)
                )
                msg = json.loads(data.decode("utf-8"))
                if msg.get("type") in ("response", "announce") and msg.get("service") == "conduit":
                    name = msg.get("name")
                    host = msg.get("host")
                    port = msg.get("port")
                    if host in ("0.0.0.0", "::"):
                        host = addr[0]
                    key = f"{host}:{port}"
                    discovered[key] = {
                        "name": name,
                        "host": host,
                        "port": port,
                        "metadata": msg.get("metadata", {})
                    }
            except asyncio.TimeoutError:
                continue
            except Exception:
                continue
                
        sock.close()
        return list(discovered.values())
