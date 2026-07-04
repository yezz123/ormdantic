"""Composable, typed query expression helpers."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from itertools import count as counter
from typing import Any, Literal, Protocol, Sequence


class SerializableExpression(Protocol):
    """Expression node that can be serialized for the Rust SQL AST."""

    def to_expression_payload(
        self, ctx: SerializationContext | None = None
    ) -> dict[str, Any]:
        """Serialize this expression into a stable Python-to-Rust payload."""


@dataclass
class SerializationContext:
    """Holds deterministic bind names and values while serializing expressions."""

    values: dict[str, Any] = field(default_factory=dict)
    _counter: Any = field(default_factory=counter)

    def bind(self, value: Any) -> str:
        name = f"expr_param_{next(self._counter)}"
        self.values[name] = value
        return name


@dataclass(frozen=True)
class SqlExpression:
    """Typed SQL expression node rendered by the Rust compiler."""

    kind: str
    data: dict[str, Any] = field(default_factory=dict)

    def to_expression_payload(
        self, ctx: SerializationContext | None = None
    ) -> dict[str, Any]:
        ctx = ctx or SerializationContext()
        kind = self.kind
        if kind in {"column", "param", "literal", "raw_safe"}:
            return {"kind": kind, **self.data}
        if kind == "binary":
            return {
                "kind": "binary",
                "op": self.data["op"],
                "left": expression_payload(self.data["left"], ctx),
                "right": expression_payload(self.data["right"], ctx),
            }
        if kind == "unary":
            return {
                "kind": "unary",
                "op": self.data["op"],
                "expr": expression_payload(self.data["expr"], ctx),
            }
        if kind == "function":
            return {
                "kind": "function",
                "name": self.data["name"],
                "args": [
                    expression_payload(arg, ctx) for arg in self.data.get("args", ())
                ],
            }
        if kind == "between":
            return {
                "kind": "between",
                "expr": expression_payload(self.data["expr"], ctx),
                "low": expression_payload(self.data["low"], ctx),
                "high": expression_payload(self.data["high"], ctx),
            }
        if kind == "in_list":
            return {
                "kind": "in_list",
                "expr": expression_payload(self.data["expr"], ctx),
                "values": [
                    expression_payload(value, ctx)
                    for value in self.data.get("values", ())
                ],
                "negated": self.data.get("negated", False),
            }
        if kind == "case":
            return {
                "kind": "case",
                "whens": [
                    {
                        "when": expression_payload(condition, ctx),
                        "then": expression_payload(value, ctx),
                    }
                    for condition, value in self.data.get("whens", ())
                ],
                "else": (
                    expression_payload(self.data["else"], ctx)
                    if "else" in self.data
                    else None
                ),
            }
        if kind == "cast":
            return {
                "kind": "cast",
                "expr": expression_payload(self.data["expr"], ctx),
                "type": self.data["type"],
            }
        if kind == "tuple":
            return {
                "kind": "tuple",
                "values": [
                    expression_payload(value, ctx)
                    for value in self.data.get("values", ())
                ],
            }
        if kind == "subquery":
            return {
                "kind": "subquery",
                "query": self.data["query"].to_query_payload(ctx),
            }
        if kind == "exists":
            return {
                "kind": "exists",
                "query": self.data["query"].to_query_payload(ctx),
                "negated": self.data.get("negated", False),
            }
        if kind == "in_subquery":
            return {
                "kind": "in_subquery",
                "expr": expression_payload(self.data["expr"], ctx),
                "query": self.data["query"].to_query_payload(ctx),
                "negated": self.data.get("negated", False),
            }
        if kind == "window":
            return {
                "kind": "window",
                "expr": expression_payload(self.data["expr"], ctx),
                "partition_by": [
                    expression_payload(expr, ctx)
                    for expr in self.data.get("partition_by", ())
                ],
                "order_by": [
                    window_order_payload(order, ctx)
                    for order in self.data.get("order_by", ())
                ],
            }
        raise ValueError(f"unsupported expression kind '{kind}'")

    def as_(self, alias: str) -> "ProjectionExpression":
        """Alias this expression when used as a projection."""
        return ProjectionExpression(self, alias)

    def asc(
        self, *, nulls: Literal["first", "last"] | None = None
    ) -> "OrderExpression":
        """Order by this expression ascending."""
        return OrderExpression(self, "asc", nulls)

    def desc(
        self, *, nulls: Literal["first", "last"] | None = None
    ) -> "OrderExpression":
        """Order by this expression descending."""
        return OrderExpression(self, "desc", nulls)

    def over(
        self,
        *,
        partition_by: Sequence[SerializableExpression] = (),
        order_by: Sequence["OrderExpression | SerializableExpression"] = (),
    ) -> "SqlExpression":
        """Use this expression as a SQL window expression."""
        return over(self, partition_by=partition_by, order_by=order_by)

    def eq(self, value: Any) -> "QueryExpression":
        if value is None:
            return self.is_null()
        return predicate("eq", self, value)

    def ne(self, value: Any) -> "QueryExpression":
        if value is None:
            return self.is_not_null()
        return predicate("ne", self, value)

    def lt(self, value: Any) -> "QueryExpression":
        return predicate("lt", self, value)

    def le(self, value: Any) -> "QueryExpression":
        return predicate("le", self, value)

    def gt(self, value: Any) -> "QueryExpression":
        return predicate("gt", self, value)

    def ge(self, value: Any) -> "QueryExpression":
        return predicate("ge", self, value)

    def __eq__(self, value: object) -> "QueryExpression":  # ty: ignore[invalid-method-override]
        return self.eq(value)

    def __ne__(self, value: object) -> "QueryExpression":  # ty: ignore[invalid-method-override]
        return self.ne(value)

    def __lt__(self, value: Any) -> "QueryExpression":
        return self.lt(value)

    def __le__(self, value: Any) -> "QueryExpression":
        return self.le(value)

    def __gt__(self, value: Any) -> "QueryExpression":
        return self.gt(value)

    def __ge__(self, value: Any) -> "QueryExpression":
        return self.ge(value)

    def like(self, value: str) -> "QueryExpression":
        return predicate("like", self, value)

    def ilike(self, value: str) -> "QueryExpression":
        return predicate("ilike", self, value)

    def contains(self, value: str) -> "QueryExpression":
        return self.like(f"%{value}%")

    def icontains(self, value: str) -> "QueryExpression":
        return self.ilike(f"%{value}%")

    def startswith(self, value: str) -> "QueryExpression":
        return self.like(f"{value}%")

    def istartswith(self, value: str) -> "QueryExpression":
        return self.ilike(f"{value}%")

    def endswith(self, value: str) -> "QueryExpression":
        return self.like(f"%{value}")

    def iendswith(self, value: str) -> "QueryExpression":
        return self.ilike(f"%{value}")

    def between(self, low: Any, high: Any) -> "QueryExpression":
        return QueryExpression(
            "leaf",
            legacy_filters=self._legacy_range("ge", low)
            | self._legacy_range("le", high),
            expr=SqlExpression(
                "between",
                {"expr": self, "low": bind_value(low), "high": bind_value(high)},
            ),
        )

    def not_between(self, low: Any, high: Any) -> "QueryExpression":
        return not_(self.between(low, high))

    def in_(self, values: Sequence[Any]) -> "QueryExpression":
        return QueryExpression(
            "leaf",
            legacy_filters=self._legacy_filter("in", list(values)),
            expr=SqlExpression(
                "in_list",
                {
                    "expr": self,
                    "values": [bind_value(value) for value in values],
                    "negated": False,
                },
            ),
        )

    def in_query(
        self, query: "SelectExpressionQuery", *, negated: bool = False
    ) -> "QueryExpression":
        """Compare this expression against the result of a typed subquery."""
        return QueryExpression(
            "leaf",
            expr=SqlExpression(
                "in_subquery",
                {
                    "expr": self,
                    "query": query,
                    "negated": negated,
                },
            ),
        )

    def not_in(self, values: Sequence[Any]) -> "QueryExpression":
        return QueryExpression(
            "leaf",
            legacy_filters=self._legacy_filter("not_in", list(values)),
            expr=SqlExpression(
                "in_list",
                {
                    "expr": self,
                    "values": [bind_value(value) for value in values],
                    "negated": True,
                },
            ),
        )

    def not_in_query(self, query: "SelectExpressionQuery") -> "QueryExpression":
        """Compare this expression against rows absent from a typed subquery."""
        return self.in_query(query, negated=True)

    def is_null(self) -> "QueryExpression":
        return QueryExpression(
            "leaf",
            legacy_filters=self._legacy_filter("is_null", True),
            expr=SqlExpression("unary", {"op": "is_null", "expr": self}),
        )

    def is_not_null(self) -> "QueryExpression":
        return QueryExpression(
            "leaf",
            legacy_filters=self._legacy_filter("is_not_null", True),
            expr=SqlExpression("unary", {"op": "is_not_null", "expr": self}),
        )

    def is_(self, value: Any) -> "QueryExpression":
        if value is not None:
            raise ValueError("only None is supported by is_()")
        return self.is_null()

    def is_not(self, value: Any) -> "QueryExpression":
        if value is not None:
            raise ValueError("only None is supported by is_not()")
        return self.is_not_null()

    def __add__(self, value: Any) -> "SqlExpression":
        return binary("add", self, value)

    def __sub__(self, value: Any) -> "SqlExpression":
        return binary("sub", self, value)

    def __mul__(self, value: Any) -> "SqlExpression":
        return binary("mul", self, value)

    def __truediv__(self, value: Any) -> "SqlExpression":
        return binary("div", self, value)

    def __radd__(self, value: Any) -> "SqlExpression":
        return binary("add", bind_value(value), self)

    def __rsub__(self, value: Any) -> "SqlExpression":
        return binary("sub", bind_value(value), self)

    def __rmul__(self, value: Any) -> "SqlExpression":
        return binary("mul", bind_value(value), self)

    def __rtruediv__(self, value: Any) -> "SqlExpression":
        return binary("div", bind_value(value), self)

    def cast(self, type_name: str) -> "SqlExpression":
        """Cast this expression to a SQL type."""
        return SqlExpression("cast", {"expr": self, "type": type_name})

    def set(self, value: Any) -> "AssignmentExpression":
        """Assign this column to a typed expression or bound value."""
        if self.kind != "column":
            raise ValueError("only column expressions can be assigned")
        return AssignmentExpression(self.data["name"], value)

    def _legacy_filter(self, operator: str, value: Any) -> dict[str, Any]:
        if self.kind != "column":
            return {}
        suffix = "" if operator == "eq" else f"__{operator}"
        return {f"{self.data['name']}{suffix}": value}

    def _legacy_range(self, operator: str, value: Any) -> dict[str, Any]:
        return self._legacy_filter(operator, value)


class ColumnExpression(SqlExpression):
    """Column helper for building query expressions."""

    name: str
    table: str | None = None

    def __init__(self, name: str, table: str | None = None) -> None:
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "table", table)
        object.__setattr__(self, "kind", "column")
        data: dict[str, Any] = {"name": name}
        if table is not None:
            data["table"] = table
        object.__setattr__(self, "data", data)


@dataclass(frozen=True)
class BoundValue:
    """Python value that should become a deterministic SQL bind parameter."""

    value: Any

    def to_expression_payload(
        self, ctx: SerializationContext | None = None
    ) -> dict[str, Any]:
        ctx = ctx or SerializationContext()
        return {"kind": "param", "name": ctx.bind(self.value)}


@dataclass(frozen=True)
class QueryExpression:
    """Boolean query expression used by filters and typed SQL compilation."""

    connector: Literal["and", "or", "leaf"]
    legacy_filters: dict[str, Any] | None = None
    children: tuple["QueryExpression", ...] = ()
    expr: SqlExpression | None = None

    def __and__(self, other: "QueryExpression") -> "QueryExpression":
        return QueryExpression(
            "and",
            children=(self, other),
            expr=binary("and", self, other),
        )

    def __or__(self, other: "QueryExpression") -> "QueryExpression":
        return QueryExpression(
            "or",
            children=(self, other),
            expr=binary("or", self, other),
        )

    def to_where(self) -> dict[str, Any]:
        """Lower simple AND expressions into the current Rust filter contract."""
        if self.connector == "leaf":
            return self._legacy_filter_payload()
        if self.connector == "and":
            merged: dict[str, Any] = {}
            for child in self.children:
                merged.update(child.to_where())
            return merged
        raise ValueError("OR expressions require native disjunction support")

    def to_filter_tree(self) -> dict[str, Any]:
        """Return the recursive payload consumed by the Rust filter normalizer."""
        if self.connector == "leaf":
            return {"connector": "leaf", "filters": self._legacy_filter_payload()}
        return {
            "connector": self.connector,
            "children": [child.to_filter_tree() for child in self.children],
        }

    def supports_legacy_filters(self) -> bool:
        """Return whether this expression can lower into legacy filter specs."""
        if self.connector == "leaf":
            return self.legacy_filters is not None
        return all(child.supports_legacy_filters() for child in self.children)

    def to_expression_payload(
        self, ctx: SerializationContext | None = None
    ) -> dict[str, Any]:
        if self.expr is None:
            raise ValueError("query expression does not have a typed SQL expression")
        return self.expr.to_expression_payload(ctx)

    def _legacy_filter_payload(self) -> dict[str, Any]:
        if self.legacy_filters is None:
            raise ValueError(
                "expression-only predicates require typed SQL compilation; "
                "use select_query(), Table.select(), or update_query()"
            )
        return dict(self.legacy_filters)


@dataclass(frozen=True)
class ProjectionExpression:
    """Expression projection with an optional alias."""

    expr: SerializableExpression
    alias: str | None = None

    def to_projection_payload(self, ctx: SerializationContext) -> dict[str, Any]:
        payload: dict[str, Any] = {"expr": expression_payload(self.expr, ctx)}
        if self.alias is not None:
            payload["alias"] = self.alias
        return payload


@dataclass(frozen=True)
class OrderExpression:
    """Stable order-by expression with optional NULLS FIRST/LAST."""

    expr: SerializableExpression
    direction: Literal["asc", "desc"]
    nulls: Literal["first", "last"] | None = None

    def to_order_payload(self, ctx: SerializationContext) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "expr": expression_payload(self.expr, ctx),
            "direction": self.direction,
        }
        if self.nulls is not None:
            payload["nulls"] = self.nulls
        return payload


@dataclass(frozen=True)
class SelectExpressionQuery:
    """Serializable SELECT statement for the Rust SQL AST compiler."""

    table: str
    projections: tuple[ProjectionExpression, ...]
    table_alias: str | None = None
    ctes: tuple["CommonTableExpression", ...] = ()
    where: QueryExpression | None = None
    group_by: tuple[SerializableExpression, ...] = ()
    having: QueryExpression | None = None
    order_by: tuple[OrderExpression, ...] = ()
    limit: int | None = None
    offset: int | None = None
    distinct: bool = False

    def to_query_payload(
        self, ctx: SerializationContext | None = None
    ) -> dict[str, Any]:
        ctx = ctx or SerializationContext()
        payload: dict[str, Any] = {
            "table": self.table,
            "values": ctx.values,
        }
        if self.table_alias is not None:
            payload["table_alias"] = self.table_alias
        if self.ctes:
            payload["ctes"] = [cte.to_cte_payload(ctx) for cte in self.ctes]
        payload["projections"] = [
            projection.to_projection_payload(ctx) for projection in self.projections
        ]
        if self.where is not None:
            payload["where"] = self.where.to_expression_payload(ctx)
        if self.group_by:
            payload["group_by"] = [
                expression_payload(expr, ctx) for expr in self.group_by
            ]
        if self.having is not None:
            payload["having"] = self.having.to_expression_payload(ctx)
        if self.order_by:
            payload["order_by"] = [
                order.to_order_payload(ctx) for order in self.order_by
            ]
        if self.limit is not None:
            payload["limit"] = self.limit
        if self.offset is not None:
            payload["offset"] = self.offset
        if self.distinct:
            payload["distinct"] = True
        payload["values"] = ctx.values
        return payload

    def to_expression_payload(
        self, ctx: SerializationContext | None = None
    ) -> dict[str, Any]:
        """Serialize this SELECT as a scalar subquery expression."""
        return subquery(self).to_expression_payload(ctx)

    def with_cte(self, *ctes: "CommonTableExpression") -> "SelectExpressionQuery":
        """Return a copy with additional common table expressions."""
        return replace(self, ctes=self.ctes + tuple(ctes))


@dataclass(frozen=True)
class CommonTableExpression:
    """Serializable common table expression used by typed SELECT queries."""

    name: str
    query: SelectExpressionQuery
    columns: tuple[str, ...] = ()
    recursive: bool = False

    def to_cte_payload(self, ctx: SerializationContext) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "query": self.query.to_query_payload(ctx),
        }
        if self.columns:
            payload["columns"] = list(self.columns)
        if self.recursive:
            payload["recursive"] = True
        return payload


@dataclass(frozen=True)
class AssignmentExpression:
    """Column assignment used by typed UPDATE queries."""

    column: str
    expr: SerializableExpression | Any

    def to_assignment_payload(self, ctx: SerializationContext) -> dict[str, Any]:
        return {"column": self.column, "expr": expression_payload(self.expr, ctx)}


@dataclass(frozen=True)
class RelationExpression:
    """Registered relationship helper for typed predicates and aggregate ordering."""

    source_table: str
    source_pk: str
    relationship: str
    target_table: str
    target_pk: str
    target_alias: str
    correlation_source_column: str
    correlation_target_column: str
    kind: Literal["collection", "scalar"]
    outer_alias: str | None = None

    def column(self, name: str) -> ColumnExpression:
        """Return a target-table column qualified to this relation subquery."""
        return column(name, table=self.target_alias)

    def any(self, where: QueryExpression | None = None) -> QueryExpression:
        """Match rows where a collection relationship has at least one row."""
        self._require_kind("collection", "any")
        return exists(self._select(literal(1), where=where))

    def none(self, where: QueryExpression | None = None) -> QueryExpression:
        """Match rows where no related row satisfies the optional predicate."""
        return not_exists(self._select(literal(1), where=where))

    def every(self, where: QueryExpression) -> QueryExpression:
        """Match collection rows where every related row satisfies a predicate."""
        self._require_kind("collection", "every")
        return not_exists(self._select(literal(1), where=not_(where)))

    def has(self, where: QueryExpression | None = None) -> QueryExpression:
        """Match rows where a scalar relationship target satisfies a predicate."""
        self._require_kind("scalar", "has")
        return exists(self._select(literal(1), where=where))

    def count(self, where: QueryExpression | None = None) -> SqlExpression:
        """Return a scalar subquery counting rows for this relationship."""
        return subquery(self._select(func("COUNT", raw_sql_safe("*")), where=where))

    def _select(
        self,
        projection: ProjectionExpression | SerializableExpression,
        *,
        where: QueryExpression | None = None,
    ) -> SelectExpressionQuery:
        return select_query(
            self.target_table,
            projection,
            table_alias=self.target_alias,
            where=self._correlation() if where is None else self._correlation() & where,
        )

    def _correlation(self) -> QueryExpression:
        return column(
            self.correlation_target_column, table=self.target_alias
        ) == column(
            self.correlation_source_column,
            table=self.outer_alias or self.source_table,
        )

    def _require_kind(
        self, expected: Literal["collection", "scalar"], operation: str
    ) -> None:
        if self.kind != expected:
            label = "collection" if expected == "collection" else "scalar"
            raise ValueError(
                f"relation '{self.source_table}.{self.relationship}' is not a "
                f"{label} relationship; cannot use {operation}()"
            )


@dataclass(frozen=True)
class UpdateExpressionQuery:
    """Serializable UPDATE statement for the Rust SQL AST compiler."""

    table: str
    assignments: tuple[AssignmentExpression, ...]
    where: QueryExpression | None = None

    def to_query_payload(self) -> dict[str, Any]:
        ctx = SerializationContext()
        payload: dict[str, Any] = {
            "table": self.table,
            "assignments": [
                assignment.to_assignment_payload(ctx) for assignment in self.assignments
            ],
            "values": ctx.values,
        }
        if self.where is not None:
            payload["where"] = self.where.to_expression_payload(ctx)
        payload["values"] = ctx.values
        return payload


def expression_payload(
    expr: SerializableExpression | Any, ctx: SerializationContext
) -> dict[str, Any]:
    if hasattr(expr, "to_expression_payload"):
        return expr.to_expression_payload(ctx)
    return bind_value(expr).to_expression_payload(ctx)


def bind_value(value: Any) -> BoundValue:
    return BoundValue(value)


def literal(value: None | bool | int | str) -> SqlExpression:
    """Create an inline SQL literal."""
    return SqlExpression("literal", {"value": value})


def raw_sql_safe(sql: str) -> SqlExpression:
    """Opt into a raw SQL fragment that is trusted by the caller."""
    return SqlExpression("raw_safe", {"sql": sql})


def cast(expr: SerializableExpression | Any, type_name: str) -> SqlExpression:
    """Cast an expression to a SQL type."""
    return SqlExpression("cast", {"expr": expr, "type": type_name})


def case(
    *whens: tuple[QueryExpression, SerializableExpression | Any],
    else_: SerializableExpression | Any | None = None,
) -> SqlExpression:
    """Create a SQL CASE expression."""
    data: dict[str, Any] = {"whens": whens}
    if else_ is not None:
        data["else"] = else_
    return SqlExpression("case", data)


def tuple_(*values: SerializableExpression | Any) -> SqlExpression:
    """Create a SQL tuple expression."""
    return SqlExpression("tuple", {"values": values})


def window_order_payload(
    order: "OrderExpression | SerializableExpression", ctx: SerializationContext
) -> dict[str, Any]:
    if isinstance(order, OrderExpression):
        return order.to_order_payload(ctx)
    return OrderExpression(order, "asc").to_order_payload(ctx)


def binary(op: str, left: Any, right: Any) -> SqlExpression:
    return SqlExpression("binary", {"op": op, "left": left, "right": right})


def predicate(op: str, left: SqlExpression, right: Any) -> QueryExpression:
    legacy_filters = (
        None
        if hasattr(right, "to_expression_payload")
        else left._legacy_filter(op, right)
    )
    return QueryExpression(
        "leaf",
        legacy_filters=legacy_filters,
        expr=binary(op, left, right),
    )


def not_(expr: QueryExpression) -> QueryExpression:
    """Negate a boolean expression."""
    return QueryExpression(
        "leaf", expr=SqlExpression("unary", {"op": "not", "expr": expr})
    )


def exists(query: "SelectExpressionQuery", *, negated: bool = False) -> QueryExpression:
    """Build an `EXISTS (SELECT ...)` predicate."""
    return QueryExpression(
        "leaf",
        expr=SqlExpression("exists", {"query": query, "negated": negated}),
    )


def not_exists(query: "SelectExpressionQuery") -> QueryExpression:
    """Build a `NOT EXISTS (SELECT ...)` predicate."""
    return exists(query, negated=True)


def subquery(query: "SelectExpressionQuery") -> SqlExpression:
    """Use a typed SELECT as a scalar subquery expression."""
    return SqlExpression("subquery", {"query": query})


def over(
    expr: SerializableExpression,
    *,
    partition_by: Sequence[SerializableExpression] = (),
    order_by: Sequence["OrderExpression | SerializableExpression"] = (),
) -> SqlExpression:
    """Create a SQL window expression."""
    return SqlExpression(
        "window",
        {
            "expr": expr,
            "partition_by": tuple(partition_by),
            "order_by": tuple(order_by),
        },
    )


def group(expr: QueryExpression) -> QueryExpression:
    """Explicit grouping helper for readability."""
    return QueryExpression("leaf", expr=expr.expr)


def column(name: str, *, table: str | None = None) -> ColumnExpression:
    """Create a query expression column reference."""
    return ColumnExpression(name, table)


def projection(
    expr: SerializableExpression, alias: str | None = None
) -> ProjectionExpression:
    """Create a selectable projection."""
    return ProjectionExpression(expr, alias)


def assignment(
    column_name: str, expr: SerializableExpression | Any
) -> AssignmentExpression:
    """Create a typed UPDATE assignment."""
    return AssignmentExpression(column_name, expr)


def select_query(
    table: str,
    *projections: ProjectionExpression | SerializableExpression,
    table_alias: str | None = None,
    with_: Sequence[CommonTableExpression] = (),
    where: QueryExpression | None = None,
    group_by: Sequence[SerializableExpression] = (),
    having: QueryExpression | None = None,
    order_by: Sequence[OrderExpression] = (),
    limit: int | None = None,
    offset: int | None = None,
    distinct: bool = False,
) -> SelectExpressionQuery:
    """Build a serializable SELECT expression query."""
    normalized = tuple(
        item if isinstance(item, ProjectionExpression) else ProjectionExpression(item)
        for item in projections
    )
    return SelectExpressionQuery(
        table=table,
        projections=normalized,
        table_alias=table_alias,
        ctes=tuple(with_),
        where=where,
        group_by=tuple(group_by),
        having=having,
        order_by=tuple(order_by),
        limit=limit,
        offset=offset,
        distinct=distinct,
    )


def relation(
    table_map: Any,
    model: type[Any],
    relationship: str,
    *,
    outer_alias: str | None = None,
    target_alias: str | None = None,
) -> RelationExpression:
    """Create a typed relationship helper from registered ORM metadata."""
    table = table_map.model_to_data.get(model)
    if table is None:
        raise ValueError(f"model '{model.__name__}' is not registered as a table")
    relation_info = table.relationships.get(relationship)
    if relation_info is None:
        available = ", ".join(sorted(table.relationships)) or "none"
        raise ValueError(
            f"'{table.model.__name__}.{relationship}' is not a relationship; "
            f"available relationships: {available}"
        )
    target = table_map.name_to_data[relation_info.foreign_table]
    alias = target_alias or f"ormdantic_rel_{relationship}"
    if relation_info.back_references is not None:
        return RelationExpression(
            source_table=table.tablename,
            source_pk=table.pk,
            relationship=relationship,
            target_table=target.tablename,
            target_pk=target.pk,
            target_alias=alias,
            correlation_source_column=table.pk,
            correlation_target_column=relation_info.back_references,
            kind="collection",
            outer_alias=outer_alias,
        )
    return RelationExpression(
        source_table=table.tablename,
        source_pk=table.pk,
        relationship=relationship,
        target_table=target.tablename,
        target_pk=target.pk,
        target_alias=alias,
        correlation_source_column=relationship,
        correlation_target_column=target.pk,
        kind="scalar",
        outer_alias=outer_alias,
    )


def update_query(
    table: str,
    *assignments: AssignmentExpression,
    where: QueryExpression | None = None,
) -> UpdateExpressionQuery:
    """Build a serializable UPDATE expression query."""
    return UpdateExpressionQuery(table=table, assignments=assignments, where=where)


def func(name: str, *args: SerializableExpression | Any) -> SqlExpression:
    """Create a SQL function expression."""
    return SqlExpression("function", {"name": name.upper(), "args": args})


def cte(
    name: str,
    query: SelectExpressionQuery,
    *,
    columns: Sequence[str] = (),
    recursive: bool = False,
) -> CommonTableExpression:
    """Create a common table expression for a typed SELECT query."""
    return CommonTableExpression(
        name=name,
        query=query,
        columns=tuple(columns),
        recursive=recursive,
    )


def count(expr: SerializableExpression | None = None) -> SqlExpression:
    return func("COUNT", raw_sql_safe("*") if expr is None else expr)


def sum(expr: SerializableExpression) -> SqlExpression:  # noqa: A001
    return func("SUM", expr)


def avg(expr: SerializableExpression) -> SqlExpression:
    return func("AVG", expr)


def min(expr: SerializableExpression) -> SqlExpression:  # noqa: A001
    return func("MIN", expr)


def max(expr: SerializableExpression) -> SqlExpression:  # noqa: A001
    return func("MAX", expr)
