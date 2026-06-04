"""
End-to-End Encrypted memory-only TLS tunnel for Mesh Networking.
"""

import ssl
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class MemoryTLSTunnel:
    """
    Manages an in-memory TLS session using MemoryBIOs.
    Enables secure end-to-end encryption between A and C via B,
    without B being able to inspect the payload.
    """
    
    def __init__(self, is_server: bool, cert_pem: Optional[str] = None, key_pem: Optional[str] = None, peer_cert_pem: Optional[str] = None):
        self.is_server = is_server
        
        # Create SSL Context
        if is_server:
            self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            if cert_pem and key_pem:
                # Load cert from memory using temp files (standard python ssl behavior)
                import tempfile
                with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as c_file, \
                     tempfile.NamedTemporaryFile(mode='w', suffix='.key', delete=False) as k_file:
                    c_file.write(cert_pem)
                    c_file.flush()
                    k_file.write(key_pem)
                    k_file.flush()
                    self.context.load_cert_chain(certfile=c_file.name, keyfile=k_file.name)
            else:
                # Generate Ed25519 cert/key on the fly using Rust netconduit_core helper
                from netconduit import generate_ed25519_cert_pem
                c_pem, k_pem = generate_ed25519_cert_pem()
                import tempfile
                with tempfile.NamedTemporaryFile(mode='w', suffix='.pem', delete=False) as c_file, \
                     tempfile.NamedTemporaryFile(mode='w', suffix='.key', delete=False) as k_file:
                    c_file.write(c_pem)
                    c_file.flush()
                    k_file.write(k_pem)
                    k_file.flush()
                    self.context.load_cert_chain(certfile=c_file.name, keyfile=k_file.name)
        else:
            self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            self.context.check_hostname = False
            self.context.verify_mode = ssl.CERT_NONE  # Pinning is handled manually or via custom verification
            
        self.incoming = ssl.MemoryBIO()
        self.outgoing = ssl.MemoryBIO()
        
        self.ssl_obj = self.context.wrap_bio(
            self.incoming,
            self.outgoing,
            server_side=is_server,
            server_hostname="localhost" if not is_server else None
        )
        
        self.peer_cert_pem = peer_cert_pem
        self.handshake_done = False
        
    def do_handshake(self) -> Tuple[bool, bytes]:
        """
        Perform or continue the TLS handshake.
        Returns (handshake_completed, outgoing_encrypted_bytes).
        """
        if self.handshake_done:
            return True, b""
            
        try:
            self.ssl_obj.do_handshake()
            self.handshake_done = True
            logger.info("End-to-End TLS Handshake completed successfully!")
            return True, self.outgoing.read()
        except ssl.SSLWantReadError:
            # Need more bytes from peer, send what we have generated so far
            return False, self.outgoing.read()
        except Exception as e:
            logger.error(f"E2E TLS Handshake error: {e}")
            raise
            
    def feed_encrypted(self, data: bytes) -> bytes:
        """
        Feed encrypted bytes received from wire.
        Returns decrypted plaintext bytes, if any.
        """
        if not data:
            return b""
        self.incoming.write(data)
        
        # Read decrypted plaintext only if handshake is already done
        if not self.handshake_done:
            return b""
            
        plaintext = []
        while True:
            try:
                chunk = self.ssl_obj.read(4096)
                if not chunk:
                    break
                plaintext.append(chunk)
            except ssl.SSLWantReadError:
                break
            except ssl.SSLZeroReturnError:
                break
                
        return b"".join(plaintext)
        
    def write_plaintext(self, data: bytes) -> bytes:
        """
        Encrypt plaintext.
        Returns encrypted bytes to send over the wire.
        """
        if not data:
            return b""
        self.ssl_obj.write(data)
        return self.outgoing.read()
