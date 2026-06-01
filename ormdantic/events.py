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

    def off(self, event: str, handler: EventHandler) -> None:
        """Remove a previously registered event handler."""
        if handler in self._handlers.get(event, []):
            self._handlers[event].remove(handler)

    def clear(self, event: str | None = None) -> None:
        """Clear handlers for one event or for all events."""
        if event is None:
            self._handlers.clear()
        else:
            self._handlers.pop(event, None)

    async def dispatch(self, event: str, **payload: Any) -> None:
        """Dispatch an event payload to all registered handlers."""
        for handler in self._handlers.get(event, []):
            result = handler(**payload)
            if inspect.isawaitable(result):
                await result
