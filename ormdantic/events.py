from __future__ import annotations

import inspect
from collections import defaultdict
from typing import Any, Awaitable, Callable

EventHandler = Callable[..., Any | Awaitable[Any]]


class EventRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, list[EventHandler]] = defaultdict(list)

    def on(self, event: str, handler: EventHandler) -> EventHandler:
        self._handlers[event].append(handler)
        return handler

    async def dispatch(self, event: str, **payload: Any) -> None:
        for handler in self._handlers.get(event, []):
            result = handler(**payload)
            if inspect.isawaitable(result):
                await result
