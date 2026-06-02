"""Composable query expression helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True)
class QueryExpression:
    """Compatibility expression facade consumed by the Rust filter normalizer."""

    connector: Literal["and", "or", "leaf"]
    filters: dict[str, Any] | None = None
    children: tuple["QueryExpression", ...] = ()

    def __and__(self, other: "QueryExpression") -> "QueryExpression":
        return QueryExpression("and", children=(self, other))

    def __or__(self, other: "QueryExpression") -> "QueryExpression":
        return QueryExpression("or", children=(self, other))

    def to_where(self) -> dict[str, Any]:
        """Lower simple AND expressions into the current Rust filter contract."""
        if self.connector == "leaf":
            return dict(self.filters or {})
        if self.connector == "and":
            merged: dict[str, Any] = {}
            for child in self.children:
                merged.update(child.to_where())
            return merged
        raise ValueError("OR expressions require native disjunction support")

    def to_filter_tree(self) -> dict[str, Any]:
        """Return the recursive payload consumed by the Rust filter normalizer."""
        if self.connector == "leaf":
            return {"connector": "leaf", "filters": dict(self.filters or {})}
        return {
            "connector": self.connector,
            "children": [child.to_filter_tree() for child in self.children],
        }


@dataclass(frozen=True, eq=False)
class ColumnExpression:
    """Column helper for building query expressions."""

    name: str

    def eq(self, value: Any) -> QueryExpression:
        return QueryExpression("leaf", {self.name: value})

    def __eq__(self, value: Any) -> QueryExpression:  # type: ignore[override]
        return self.eq(value)

    def ne(self, value: Any) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__ne": value})

    def __ne__(self, value: Any) -> QueryExpression:  # type: ignore[override]
        return self.ne(value)

    def lt(self, value: Any) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__lt": value})

    def __lt__(self, value: Any) -> QueryExpression:
        return self.lt(value)

    def le(self, value: Any) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__le": value})

    def __le__(self, value: Any) -> QueryExpression:
        return self.le(value)

    def gt(self, value: Any) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__gt": value})

    def __gt__(self, value: Any) -> QueryExpression:
        return self.gt(value)

    def ge(self, value: Any) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__ge": value})

    def __ge__(self, value: Any) -> QueryExpression:
        return self.ge(value)

    def like(self, value: str) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__like": value})

    def ilike(self, value: str) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__ilike": value})

    def in_(self, values: list[Any]) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__in": values})

    def not_in(self, values: list[Any]) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__not_in": values})

    def is_null(self) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__is_null": True})

    def is_not_null(self) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__is_not_null": True})

    def is_(self, value: Any) -> QueryExpression:
        if value is not None:
            raise ValueError("only None is supported by is_()")
        return self.is_null()

    def is_not(self, value: Any) -> QueryExpression:
        if value is not None:
            raise ValueError("only None is supported by is_not()")
        return self.is_not_null()


def column(name: str) -> ColumnExpression:
    """Create a query expression column reference."""
    return ColumnExpression(name)
