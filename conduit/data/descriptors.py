"""
Server and Client Descriptor Data Models

Configuration objects for Server and Client instances.
All configuration is validated using Pydantic.
"""

from typing import Optional
from pydantic import BaseModel, Field, field_validator


class ServerDescriptor(BaseModel):
    """
    Configuration for a Conduit Server.
    
    Contains all settings needed to initialize and run a server.
    """
    
    # Server identity
    name: str = Field(default="conduit_server", description="Server name for identification")
    version: str = Field(default="1.0.0", description="Server version")
    description: str = Field(default="", description="Server description")
    
    # Network configuration
    host: str = Field(default="0.0.0.0", description="Host address to bind to")
    port: int = Field(default=8080, ge=1, le=65535, description="Port to listen on")
    ipv6: bool = Field(default=False, description="Enable IPv6 support")
    stun_server: str = Field(default="stun.l.google.com:19302", description="Public STUN server address")
    
    # Authentication
    password: str = Field(..., min_length=1, description="Password for client authentication")
    
    # Server limits
    max_connections: int = Field(default=100, ge=1, description="Maximum concurrent connections")
    max_message_size: int = Field(
        default=10 * 1024 * 1024,  # 10MB
        ge=1024,
        description="Maximum message size in bytes"
    )
    buffer_size: int = Field(
        default=64 * 1024,  # 64KB
        ge=1024,
        description="Socket buffer size in bytes"
    )
    
    # Timeouts
    connection_timeout: int = Field(
        default=120,
        ge=1,
        description="Idle connection timeout in seconds"
    )
    auth_timeout: int = Field(
        default=10,
        ge=1,
        description="Authentication timeout in seconds"
    )
    
    # Heartbeat configuration
    heartbeat_interval: int = Field(
        default=30,
        ge=1,
        description="Heartbeat interval in seconds"
    )
    heartbeat_timeout: int = Field(
        default=90,
        ge=1,
        description="Heartbeat timeout in seconds (should be > interval)"
    )
    
    # Backpressure configuration
    send_queue_size: int = Field(
        default=1000,
        ge=1,
        description="Maximum messages in send queue"
    )
    receive_queue_size: int = Field(
        default=1000,
        ge=1,
        description="Maximum messages in receive queue"
    )
    enable_backpressure: bool = Field(
        default=True,
        description="Enable flow control"
    )
    
    # Advanced options
    enable_compression: bool = Field(
        default=False,
        description="Enable compression for large messages"
    )
    
    # TLS/SSL Configuration
    ssl_enabled: bool = Field(
        default=False,
        description="Enable TLS/SSL encryption"
    )
    ssl_cert_file: Optional[str] = Field(
        default=None,
        description="Path to SSL certificate file (.pem)"
    )
    ssl_key_file: Optional[str] = Field(
        default=None,
        description="Path to SSL private key file (.pem)"
    )
    ssl_ca_file: Optional[str] = Field(
        default=None,
        description="Path to CA certificate file for client verification"
    )
    ssl_verify_client: bool = Field(
        default=False,
        description="Require client certificate verification"
    )
    
    # Rate Limiting (optional)
    rate_limit_enabled: bool = Field(
        default=False,
        description="Enable per-connection rate limiting"
    )
    rate_limit_messages_per_second: int = Field(
        default=100,
        ge=1,
        description="Maximum messages per second per connection"
    )
    rate_limit_bytes_per_second: int = Field(
        default=10 * 1024 * 1024,  # 10MB/s
        ge=1024,
        description="Maximum bytes per second per connection"
    )
    
    protocol_version: str = Field(
        default="1.0",
        description="Protocol version"
    )
    
    @field_validator('heartbeat_timeout')
    @classmethod
    def validate_heartbeat_timeout(cls, v, info):
        """Ensure heartbeat timeout is greater than interval."""
        if 'heartbeat_interval' in info.data:
            if v <= info.data['heartbeat_interval']:
                raise ValueError('heartbeat_timeout must be greater than heartbeat_interval')
        return v
    
    model_config = {
        "extra": "forbid",  # Disallow extra fields
    }


class ClientDescriptor(BaseModel):
    """
    Configuration for a Conduit Client.
    
    Contains all settings needed to connect to a server.
    Only server_host, server_port, and password are required.
    """
    
    # === REQUIRED ARGUMENTS ===
    server_host: str = Field(..., description="Server hostname or IP address")
    server_port: int = Field(..., ge=1, le=65535, description="Server port")
    password: str = Field(..., min_length=1, description="Password for authentication")
    
    # === OPTIONAL ARGUMENTS ===
    
    # Client identity
    name: str = Field(default="conduit_client", description="Client name for identification")
    version: str = Field(default="1.0.0", description="Client version")
    description: str = Field(default="", description="Client description")
    username: Optional[str] = Field(default=None, description="Username for authentication")
    
    # Network
    use_ipv6: bool = Field(default=False, description="Use IPv6 for connection")
    local_port: Optional[int] = Field(
        default=None,
        ge=0,
        le=65535,
        description="Local port to bind to (optional)"
    )
    stun_server: str = Field(default="stun.l.google.com:19302", description="Public STUN server address")
    
    # Connection behavior
    connect_timeout: int = Field(
        default=10,
        ge=1,
        description="Connection timeout in seconds"
    )
    reconnect_enabled: bool = Field(
        default=True,
        description="Enable automatic reconnection"
    )
    reconnect_attempts: int = Field(
        default=5,
        ge=0,
        description="Maximum reconnection attempts (0 = unlimited)"
    )
    reconnect_delay: float = Field(
        default=2.0,
        ge=0.1,
        description="Initial delay between reconnection attempts in seconds"
    )
    reconnect_delay_max: float = Field(
        default=60.0,
        ge=1.0,
        description="Maximum delay between reconnection attempts"
    )
    reconnect_delay_multiplier: float = Field(
        default=2.0,
        ge=1.0,
        description="Multiplier for exponential backoff"
    )
    
    # Message configuration
    max_message_size: int = Field(
        default=10 * 1024 * 1024,  # 10MB
        ge=1024,
        description="Maximum message size in bytes"
    )
    buffer_size: int = Field(
        default=64 * 1024,  # 64KB
        ge=1024,
        description="Socket buffer size in bytes"
    )
    
    # Timeouts
    rpc_timeout: float = Field(
        default=30.0,
        ge=0.1,
        description="Default RPC call timeout in seconds"
    )
    send_timeout: float = Field(
        default=10.0,
        ge=0.1,
        description="Send operation timeout in seconds"
    )
    
    # Heartbeat
    heartbeat_interval: int = Field(
        default=30,
        ge=1,
        description="Heartbeat interval in seconds"
    )
    heartbeat_timeout: int = Field(
        default=90,
        ge=1,
        description="Heartbeat timeout in seconds"
    )
    
    # Backpressure
    send_queue_size: int = Field(
        default=1000,
        ge=1,
        description="Maximum messages in send queue"
    )
    receive_queue_size: int = Field(
        default=1000,
        ge=1,
        description="Maximum messages in receive queue"
    )
    
    # Advanced
    enable_compression: bool = Field(
        default=False,
        description="Enable compression for large messages"
    )
    
    # TLS/SSL Configuration
    ssl_enabled: bool = Field(
        default=False,
        description="Enable TLS/SSL encryption"
    )
    ssl_verify: bool = Field(
        default=True,
        description="Verify server certificate"
    )
    ssl_ca_file: Optional[str] = Field(
        default=None,
        description="Path to CA certificate file for server verification"
    )
    ssl_cert_file: Optional[str] = Field(
        default=None,
        description="Path to client certificate for mutual TLS"
    )
    ssl_key_file: Optional[str] = Field(
        default=None,
        description="Path to client private key for mutual TLS"
    )
    
    protocol_version: str = Field(
        default="1.0",
        description="Protocol version"
    )
    
    @field_validator('heartbeat_timeout')
    @classmethod
    def validate_heartbeat_timeout(cls, v, info):
        """Ensure heartbeat timeout is greater than interval."""
        if 'heartbeat_interval' in info.data:
            if v <= info.data['heartbeat_interval']:
                raise ValueError('heartbeat_timeout must be greater than heartbeat_interval')
        return v
    
    @field_validator('reconnect_delay_max')
    @classmethod
    def validate_reconnect_delay_max(cls, v, info):
        """Ensure max delay is greater than initial delay."""
        if 'reconnect_delay' in info.data:
            if v < info.data['reconnect_delay']:
                raise ValueError('reconnect_delay_max must be >= reconnect_delay')
        return v
    
    model_config = {
        "extra": "forbid",  # Disallow extra fields
    }
