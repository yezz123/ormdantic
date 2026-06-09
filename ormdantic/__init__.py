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
from ormdantic.expressions import (
    QueryExpression,
    assignment,
    avg,
    case,
    cast,
    column,
    count,
    group,
    literal,
    max,
    min,
    not_,
    projection,
    raw_sql_safe,
    select_query,
    sum,
    tuple_,
    update_query,
)
from ormdantic.loaders import (
    joined,
    joinedload,
    lazy,
    lazyload,
    load,
    noload,
    selectin,
    selectinload,
)
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
    "projection",
    "assignment",
    "select_query",
    "update_query",
    "case",
    "cast",
    "tuple_",
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "not_",
    "group",
    "literal",
    "raw_sql_safe",
    "association_proxy",
    "hybrid_property",
    "joined",
    "joinedload",
    "selectin",
    "selectinload",
    "lazy",
    "lazyload",
    "noload",
    "load",
    "runtime_capabilities",
]
