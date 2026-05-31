"""Event registration and dispatch helpers for Ormdantic."""

from __future__ import annotations

import inspect
from collections import defaultdict
from typing import Any, Awaitable, Callable

EventHandler = Callable[..., Any | Awaitable[Any]]


class EventRegistry:
    """Store and dispatch sync or async event handlers."""

    def __init__(self) -> None:
        """Create an empty event registry."""
        self._handlers: dict[str, list[EventHandler]] = defaultdict(list)

    def on(self, event: str, handler: EventHandler) -> EventHandler:
        """Register a handler for an event and return the handler."""
        self._handlers[event].append(handler)
        return handler

    async def dispatch(self, event: str, **payload: Any) -> None:
        """Dispatch an event payload to all registered handlers."""
        for handler in self._handlers.get(event, []):
            result = handler(**payload)
            if inspect.isawaitable(result):
                await result
