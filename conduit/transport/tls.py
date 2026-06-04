"""
TLS/SSL Support for Conduit

Provides SSL context creation and socket wrapping for encrypted connections.
"""

import ssl
from typing import Optional
from dataclasses import dataclass


@dataclass
class TLSConfig:
    """TLS configuration for server or client."""
    
    enabled: bool = False
    cert_file: Optional[str] = None
    key_file: Optional[str] = None
    ca_file: Optional[str] = None
    verify: bool = True
    verify_client: bool = False  # Server-side: require client certs


def create_server_ssl_context(config: TLSConfig) -> Optional[ssl.SSLContext]:
    """
    Create SSL context for server.
    
    Args:
        config: TLS configuration
        
    Returns:
        SSLContext if TLS enabled, None otherwise
    """
    if not config.enabled:
        return None
    
    if not config.cert_file or not config.key_file:
        raise ValueError("SSL requires cert_file and key_file for server")
    
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(config.cert_file, config.key_file)
    
    # Client certificate verification
    if config.verify_client:
        context.verify_mode = ssl.CERT_REQUIRED
        if config.ca_file:
            context.load_verify_locations(config.ca_file)
    else:
        context.verify_mode = ssl.CERT_NONE
    
    # Security settings
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.set_ciphers('ECDHE+AESGCM:DHE+AESGCM:ECDHE+CHACHA20:DHE+CHACHA20')
    
    return context


def create_client_ssl_context(config: TLSConfig) -> Optional[ssl.SSLContext]:
    """
    Create SSL context for client.
    
    Args:
        config: TLS configuration
        
    Returns:
        SSLContext if TLS enabled, None otherwise
    """
    if not config.enabled:
        return None
    
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    
    # Server certificate verification
    if config.verify:
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = True
        if config.ca_file:
            context.load_verify_locations(config.ca_file)
        else:
            context.load_default_certs()
    else:
        # Must set check_hostname=False BEFORE verify_mode=CERT_NONE
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    
    # Client certificate (mutual TLS)
    if config.cert_file and config.key_file:
        context.load_cert_chain(config.cert_file, config.key_file)
    
    # Security settings
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    
    return context


