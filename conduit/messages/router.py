"""
Message Router

Routes messages to registered handlers based on message type.
"""

import asyncio
from typing import Any, Callable, Dict, List, Optional, Awaitable
from dataclasses import dataclass, field
import logging

from .message import Message

logger = logging.getLogger(__name__)


# Handler type
MessageHandler = Callable[[Any, Any], Awaitable[Any]]  # (client/connection, data) -> response


@dataclass
class RegisteredHandler:
    """A registered message handler."""
    
    message_type: str
    handler: MessageHandler
    requires_auth: bool = True
    priority: int = 0


class MessageRouter:
    """
    Routes incoming messages to registered handlers.
    
    Usage:
        router = MessageRouter()
        
        @router.on("hello")
        async def handle_hello(client, data):
            return {"response": "Hi!"}
    """
    
    def __init__(self):
        """Initialize router."""
        self._handlers: Dict[str, List[RegisteredHandler]] = {}
        self._default_handler: Optional[MessageHandler] = None
        self._error_handler: Optional[Callable] = None
    
    def on(
        self,
        message_type: str,
        requires_auth: bool = True,
        priority: int = 0
    ) -> Callable:
        """
        Decorator to register a message handler.
        
        Args:
            message_type: Type of message to handle
            requires_auth: Whether handler requires authentication
            priority: Handler priority (higher = called first)
            
        Returns:
            Decorator function
        """
        def decorator(handler: MessageHandler) -> MessageHandler:
            self.register(message_type, handler, requires_auth, priority)
            return handler
        return decorator
    
    def register(
        self,
        message_type: str,
        handler: MessageHandler,
        requires_auth: bool = True,
        priority: int = 0
    ) -> None:
        """
        Register a message handler.
        
        Args:
            message_type: Type of message to handle
            handler: Handler function
            requires_auth: Whether handler requires authentication
            priority: Handler priority
        """
        registered = RegisteredHandler(
            message_type=message_type,
            handler=handler,
            requires_auth=requires_auth,
            priority=priority,
        )
        
        if message_type not in self._handlers:
            self._handlers[message_type] = []
        
        self._handlers[message_type].append(registered)
        
        # Sort by priority (descending)
        self._handlers[message_type].sort(key=lambda h: -h.priority)
    
    def unregister(self, message_type: str) -> bool:
        """
        Unregister all handlers for a message type.
        
        Args:
            message_type: Type to unregister
            
        Returns:
            True if handlers were removed
        """
        if message_type in self._handlers:
            del self._handlers[message_type]
            return True
        return False
    
    def set_default_handler(self, handler: MessageHandler) -> None:
        """
        Set the default handler for unregistered message types.
        
        Args:
            handler: Default handler function
        """
        self._default_handler = handler
    
    def set_error_handler(self, handler: Callable) -> None:
        """
        Set the error handler for handler exceptions.
        
        Args:
            handler: Error handler function
        """
        self._error_handler = handler
    
    async def route(
        self,
        message: Message,
        context: Any,
        authenticated: bool = False
    ) -> Optional[Any]:
        """
        Route a message to its handlers.
        
        Args:
            message: Message to route
            context: Context object (client/connection) passed to handlers
            authenticated: Whether the sender is authenticated
            
        Returns:
            Response from handler, or None
        """
        message_type = message.type
        
        handlers = self._handlers.get(message_type, [])
        
        # Use default handler if no specific handlers
        if not handlers and self._default_handler:
            handlers = [RegisteredHandler(
                message_type=message_type,
                handler=self._default_handler,
                requires_auth=False,
            )]
        
        if not handlers:
            logger.warning(f"No handler for message type: {message_type}")
            return None
        
        result = None
        
        for registered in handlers:
            # Check authentication requirement
            if registered.requires_auth and not authenticated:
                logger.warning(f"Handler for {message_type} requires authentication")
                continue
            
            try:
                # Call handler
                response = registered.handler(context, message.data)
                
                # Await if awaitable
                import inspect
                if inspect.isawaitable(response):
                    response = await response
                
                result = response
                
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in handler for {message_type}: {e}")
                
                if self._error_handler:
                    try:
                        self._error_handler(e, message, context)
                    except Exception:
                        pass
        
        return result
    
    async def route_dict(
        self,
        message_type: str,
        data: Any,
        context: Any,
        authenticated: bool = False
    ) -> Optional[Any]:
        """
        Route a message from type and data.
        
        Convenience method when you don't have a Message object.
        
        Args:
            message_type: Type of message
            data: Message data
            context: Context object
            authenticated: Whether authenticated
            
        Returns:
            Handler response
        """
        message = Message(type=message_type, data=data)
        return await self.route(message, context, authenticated)
    
    def has_handler(self, message_type: str) -> bool:
        """Check if a handler exists for message type."""
        return message_type in self._handlers and len(self._handlers[message_type]) > 0
    
    def list_types(self) -> List[str]:
        """List all registered message types."""
        return list(self._handlers.keys())
    
    def clear(self) -> None:
        """Clear all handlers."""
        self._handlers.clear()
        self._default_handler = None
