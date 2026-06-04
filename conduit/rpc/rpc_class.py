"""
RPC Client Class

Client-side RPC interface for making remote procedure calls.
"""

import asyncio
import time
from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..client import Client


from ..exceptions import RPCError


class RPCTimeout(RPCError):
    """Exception raised when an RPC call times out."""
    
    def __init__(self, method: str, timeout: float):
        super().__init__(f"RPC call to '{method}' timed out after {timeout}s")
        self.method = method
        self.timeout = timeout


class RPC:
    """
    RPC client interface.
    
    Provides a clean API for making RPC calls to the server.
    
    Usage:
        rpc = RPC(client)
        
        # Discover available methods
        methods = await rpc.call("listall")
        
        # Call a method
        result = await rpc.call("calculate", args=data(a=10, b=20))
        
        # With explicit timeout
        result = await rpc.call("slow_method", timeout=60.0)
    """
    
    def __init__(self, client: 'Client', default_timeout: float = 30.0):
        """
        Initialize RPC interface.
        
        Args:
            client: Connected Conduit client
            default_timeout: Default timeout for RPC calls in seconds
        """
        self._client = client
        self._default_timeout = default_timeout
        self._pending_calls: Dict[int, asyncio.Future] = {}
    
    async def call(
        self,
        method: str,
        args: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
        **kwargs: Any
    ) -> Any:
        """
        Call an RPC method on the server.
        
        Args:
            method: Method name to call
            args: Arguments as dictionary (use data() helper)
            timeout: Timeout in seconds (uses default if not specified)
            **kwargs: Additional arguments (merged with args)
            
        Returns:
            The result from the RPC method
            
        Raises:
            RPCError: If the call fails
            RPCTimeout: If the call times out
        """
        # Merge args and kwargs
        params = {}
        if args:
            params.update(args)
        if kwargs:
            params.update(kwargs)
        
        # Use default timeout if not specified
        if timeout is None:
            timeout = self._default_timeout
        
        start_time = time.time()
        
        try:
            # Send RPC request and get correlation ID
            correlation_id = await self._client._send_rpc_request(method, params)
            
            # Wait for response with timeout
            response = await asyncio.wait_for(
                self._client._wait_for_rpc_response(correlation_id),
                timeout=timeout
            )
            
            elapsed_ms = (time.time() - start_time) * 1000
            
            # Check if response indicates success
            if isinstance(response, dict):
                success = response.get("success", True)
                
                if not success:
                    error_msg = response.get("error", "Unknown error")
                    error_code = response.get("code")
                    error_details = response.get("details")
                    raise RPCError(error_msg, code=error_code, details=error_details)
                
                # Return the data/result
                if "data" in response:
                    return response["data"]
                if "result" in response:
                    return response["result"]
                
                return response
            
            return response
            
        except asyncio.TimeoutError:
            raise RPCTimeout(method, timeout)
        except RPCError:
            raise
        except Exception as e:
            raise RPCError(f"RPC call failed: {str(e)}")
    
    async def call_no_wait(
        self,
        method: str,
        args: Optional[Dict[str, Any]] = None,
        **kwargs: Any
    ) -> int:
        """
        Call an RPC method without waiting for response.
        
        Useful for fire-and-forget calls.
        
        Args:
            method: Method name to call
            args: Arguments as dictionary
            **kwargs: Additional arguments
            
        Returns:
            Correlation ID for the call
        """
        params = {}
        if args:
            params.update(args)
        if kwargs:
            params.update(kwargs)
        
        return await self._client._send_rpc_request(method, params)
    
    async def discover(self) -> list[Dict[str, Any]]:
        """
        Discover available RPC methods on the server.
        
        Returns:
            List of method info dictionaries
        """
        result = await self.call("listall")
        
        if isinstance(result, dict) and "methods" in result:
            return result["methods"]
        
        return result
    
    @property
    def default_timeout(self) -> float:
        """Get the default timeout."""
        return self._default_timeout
    
    @default_timeout.setter
    def default_timeout(self, value: float) -> None:
        """Set the default timeout."""
        if value <= 0:
            raise ValueError("Timeout must be positive")
        self._default_timeout = value

    def __call__(self, name: Optional[Any] = None) -> Any:
        """Register an RPC method on the client (for P2P connection use)."""
        if callable(name):
            handler = name
            self._client._rpc_registry.register(handler)
            return handler
        
        def decorator(handler: Any) -> Any:
            self._client._rpc_registry.register(handler, name=name)
            return handler
        return decorator

