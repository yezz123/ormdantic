"""asynchronous ORM that uses pydantic models to represent database tables ✨"""

__version__ = "1.7.0"

from ormdantic.engine import runtime_capabilities
from ormdantic.events import EventRegistry
from ormdantic.loaders import joined, lazy, selectin
from ormdantic.orm import Ormdantic

__all__ = [
    "Ormdantic",
    "EventRegistry",
    "joined",
    "selectin",
    "lazy",
    "runtime_capabilities",
]
