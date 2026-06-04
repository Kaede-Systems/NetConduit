"""
RPC Dispatcher

Dispatches RPC requests to handlers and handles responses.
"""

import asyncio
import traceback
from typing import Any, Dict, Optional
from pydantic import BaseModel, ValidationError

from .registry import RPCRegistry, RPCMethod
from ..response import Response, Error


class RPCDispatcher:
    """
    Dispatches RPC requests to registered handlers.
    
    Handles parameter validation, execution, and response wrapping.
    """
    
    def __init__(self, registry: RPCRegistry):
        """
        Initialize dispatcher.
        
        Args:
            registry: RPC registry containing registered methods
        """
        self.registry = registry
        self._response = Response()
        self._error = Error()
    
    async def dispatch(
        self,
        method: str,
        params: Dict[str, Any],
        authenticated: bool = False
    ) -> Dict[str, Any]:
        """
        Dispatch an RPC request to the appropriate handler.
        
        Args:
            method: RPC method name
            params: Method parameters
            authenticated: Whether the caller is authenticated
            
        Returns:
            Response dictionary (success or error)
        """
        # Handle built-in listall method
        if method == "listall":
            return self._handle_listall()
        
        # Find the method
        rpc_method = self.registry.get(method)
        
        if rpc_method is None:
            return self._error.not_found("RPC method", method)
        
        # Check authentication requirement
        if rpc_method.requires_auth and not authenticated:
            return self._error.permission_denied(f"Method '{method}' requires authentication")
        
        try:
            # Validate and convert parameters using Pydantic model if available
            validated_params = self._validate_params(rpc_method, params)
            
            # Execute the handler
            result = await self._execute_handler(rpc_method, validated_params)
            
            # If handler returns a dict with 'success' key, it's already formatted
            if isinstance(result, dict) and 'success' in result:
                return result
            
            # Otherwise wrap it
            return self._response(result)
            
        except ValidationError as e:
            # Pydantic validation error
            error_details = []
            for err in e.errors():
                error_details.append({
                    "field": ".".join(str(loc) for loc in err["loc"]),
                    "message": err["msg"],
                    "type": err["type"],
                })
            return self._error(
                f"Validation error in {method}",
                code=2000,  # VALIDATION
                details={"errors": error_details}
            )
            
        except TypeError as e:
            # Parameter type error
            return self._error.validation(str(e))
            
        except asyncio.CancelledError:
            raise  # Re-raise cancellation
            
        except Exception as e:
            # Unexpected error
            tb = traceback.format_exc()
            return self._error.internal(f"Error in {method}: {str(e)}")
    
    def _validate_params(
        self,
        rpc_method: RPCMethod,
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Validate parameters against the method's expected types.
        
        Args:
            rpc_method: The RPC method
            params: Raw parameters
            
        Returns:
            Validated parameters dictionary
        """
        import inspect
        handler = rpc_method.handler
        sig = inspect.signature(handler)
        
        validated = {}
        
        # Check if the handler signature has a single parameter of type BaseModel
        # (This is the standard backwards-compatible single-model pattern)
        has_single_model = False
        if len(sig.parameters) == 1:
            param = list(sig.parameters.values())[0]
            param_type = param.annotation
            if isinstance(param_type, type) and issubclass(param_type, BaseModel):
                has_single_model = True
                # Validate the entire params dict as the model
                validated[param.name] = param_type(**params)
                
        if not has_single_model:
            # Bind parameters individually
            for param_name, param in sig.parameters.items():
                param_type = param.annotation
                
                # Check if this parameter is a Pydantic model
                if isinstance(param_type, type) and issubclass(param_type, BaseModel):
                    # Extract fields belonging to the model
                    model_fields = {}
                    for field_name in param_type.model_fields:
                        if field_name in params:
                            model_fields[field_name] = params[field_name]
                    # Validate and construct the model
                    validated[param_name] = param_type(**model_fields)
                elif param_name in params:
                    validated[param_name] = params[param_name]
                elif param.default is not inspect.Parameter.empty:
                    pass
                    
        return validated
    
    async def _execute_handler(
        self,
        rpc_method: RPCMethod,
        params: Any
    ) -> Any:
        """
        Execute the RPC handler as a non-blocking task.
        
        Handlers run as independent asyncio Tasks so they:
        - Don't block the event loop for other connections
        - Can be awaited with a server-side timeout
        - Are automatically cleaned up when cancelled
        
        Args:
            rpc_method: The RPC method
            params: Validated parameters
            
        Returns:
            Handler result
        """
        import inspect
        handler = rpc_method.handler
        
        if isinstance(params, dict):
            coro_or_result = handler(**params)
        else:
            coro_or_result = handler(params)
        
        # Await if awaitable — run as a task for proper cancellation support
        if inspect.isawaitable(coro_or_result):
            # Create a task so it's independently scheduled
            task = asyncio.ensure_future(coro_or_result)
            try:
                result = await task
            except asyncio.CancelledError:
                task.cancel()
                raise
        else:
            result = coro_or_result
        
        return result

    
    def _handle_listall(self) -> Dict[str, Any]:
        """
        Handle the built-in listall method.
        
        Returns:
            List of available RPC methods
        """
        method_info = self.registry.get_method_info()
        return self._response({
            "methods": method_info,
            "count": len(method_info),
        })
    
    async def dispatch_batch(
        self,
        requests: list[Dict[str, Any]],
        authenticated: bool = False
    ) -> list[Dict[str, Any]]:
        """
        Dispatch multiple RPC requests.
        
        Args:
            requests: List of request dicts with 'method' and 'params'
            authenticated: Whether caller is authenticated
            
        Returns:
            List of response dicts
        """
        results = []
        for request in requests:
            method = request.get("method", "")
            params = request.get("params", {})
            result = await self.dispatch(method, params, authenticated)
            results.append(result)
        return results
