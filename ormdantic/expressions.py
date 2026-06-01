"""Composable query expression helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True)
class QueryExpression:
    """A small expression tree that lowers into table filter dictionaries."""

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


@dataclass(frozen=True)
class ColumnExpression:
    """Column helper for building query expressions."""

    name: str

    def eq(self, value: Any) -> QueryExpression:
        return QueryExpression("leaf", {self.name: value})

    def ne(self, value: Any) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__ne": value})

    def lt(self, value: Any) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__lt": value})

    def le(self, value: Any) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__le": value})

    def gt(self, value: Any) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__gt": value})

    def ge(self, value: Any) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__ge": value})

    def like(self, value: str) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__like": value})

    def ilike(self, value: str) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__ilike": value})

    def in_(self, values: list[Any]) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__in": values})

    def is_null(self) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__is_null": True})

    def is_not_null(self) -> QueryExpression:
        return QueryExpression("leaf", {f"{self.name}__is_not_null": True})


def column(name: str) -> ColumnExpression:
    """Create a query expression column reference."""
    return ColumnExpression(name)
