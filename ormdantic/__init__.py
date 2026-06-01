"""asynchronous ORM that uses pydantic models to represent database tables ✨"""

__version__ = "2.0.0"

from ormdantic.association import association_proxy, hybrid_property
from ormdantic.engine import runtime_capabilities
from ormdantic.errors import (
    ConfigurationError,
    MismatchingBackReferenceError,
    MustUnionForeignKeyError,
    TypeConversionError,
    UndefinedBackReferenceError,
)
from ormdantic.events import EventRegistry
from ormdantic.expressions import QueryExpression, column
from ormdantic.loaders import joined, lazy, selectin
from ormdantic.orm import Ormdantic
from ormdantic.table import Order, Table

__all__ = [
    "Ormdantic",
    "Table",
    "Order",
    "ConfigurationError",
    "UndefinedBackReferenceError",
    "MismatchingBackReferenceError",
    "MustUnionForeignKeyError",
    "TypeConversionError",
    "EventRegistry",
    "QueryExpression",
    "column",
    "association_proxy",
    "hybrid_property",
    "joined",
    "selectin",
    "lazy",
    "runtime_capabilities",
]
