from __future__ import annotations

import pytest
from ormdantic._ormdantic import (
    compile_typed_expression_query,
    compile_typed_update_query,
)

from ormdantic import (
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


def test_expression_helpers_preserve_legacy_filter_payloads() -> None:
    assert column("strength").eq(5).to_where() == {"strength": 5}
    assert column("strength").ne(5).to_where() == {"strength__ne": 5}
    assert (column("strength") < 5).to_where() == {"strength__lt": 5}
    assert (column("strength") <= 5).to_where() == {"strength__le": 5}
    assert (column("strength") > 5).to_where() == {"strength__gt": 5}
    assert (column("strength") >= 5).to_where() == {"strength__ge": 5}
    assert column("name").contains("mo").to_where() == {"name__like": "%mo%"}
    assert column("name").startswith("mo").to_where() == {"name__like": "mo%"}
    assert column("name").endswith("cha").to_where() == {"name__like": "%cha"}
    assert column("name").icontains("MO").to_where() == {"name__ilike": "%MO%"}
    assert column("name").istartswith("MO").to_where() == {"name__ilike": "MO%"}
    assert column("name").iendswith("CHA").to_where() == {"name__ilike": "%CHA"}
    assert column("strength").between(2, 5).to_where() == {
        "strength__ge": 2,
        "strength__le": 5,
    }
    assert column("id").not_in(["1", "2"]).to_filter_tree() == {
        "connector": "leaf",
        "filters": {"id__not_in": ["1", "2"]},
    }
    assert column("deleted_at").is_not_null().to_where() == {
        "deleted_at__is_not_null": True
    }
    assert (column("deleted_at") == None).to_where() == {  # noqa: E711
        "deleted_at__is_null": True
    }
    assert (column("deleted_at") != None).to_where() == {  # noqa: E711
        "deleted_at__is_not_null": True
    }


def test_typed_expression_query_serializes_and_compiles_with_stable_params() -> None:
    total = sum(column("total"))
    query = select_query(
        "orders",
        column("customer_id"),
        total.as_("total_sum"),
        count().as_("row_count"),
        avg(column("total") + 2).as_("adjusted_avg"),
        where=column("status").in_(["paid", "refunded"])
        & column("deleted_at").is_null(),
        group_by=[column("customer_id")],
        having=total > 100,
        order_by=[total.desc(nulls="last"), column("customer_id").asc()],
        limit=10,
    )

    payload = query.to_query_payload()
    compiled = compile_typed_expression_query("postgresql", payload)

    assert compiled == {
        "sql": 'SELECT "customer_id", SUM("total") AS "total_sum", COUNT(*) AS "row_count", AVG(("total" + $1)) AS "adjusted_avg" FROM "orders" WHERE (("status" IN ($2, $3)) AND ("deleted_at" IS NULL)) GROUP BY "customer_id" HAVING (SUM("total") > $4) ORDER BY SUM("total") DESC NULLS LAST, "customer_id" ASC LIMIT 10',
        "params": [
            "expr_param_0",
            "expr_param_1",
            "expr_param_2",
            "expr_param_3",
        ],
        "operation": "select",
        "values": {
            "expr_param_0": 2,
            "expr_param_1": "paid",
            "expr_param_2": "refunded",
            "expr_param_3": 100,
        },
    }


def test_raw_sql_safe_and_not_between_compile_through_typed_bridge() -> None:
    query = select_query(
        "flavors",
        raw_sql_safe("LOWER(name)").as_("normalized_name"),
        where=not_(column("strength").between(1, 3)),
        order_by=[raw_sql_safe("LOWER(name)").asc(nulls="first")],
    )

    compiled = compile_typed_expression_query("sqlite", query.to_query_payload())

    assert compiled["sql"] == (
        'SELECT LOWER(name) AS "normalized_name" FROM "flavors" '
        'WHERE (NOT ("strength" BETWEEN ? AND ?)) '
        "ORDER BY LOWER(name) ASC NULLS FIRST"
    )
    assert compiled["params"] == ["expr_param_0", "expr_param_1"]
    assert compiled["values"] == {"expr_param_0": 1, "expr_param_1": 3}


def test_none_comparison_operators_compile_as_null_checks() -> None:
    query = select_query(
        "orders",
        column("id"),
        where=(
            (column("deleted_at") == None)  # noqa: E711
            | (column("archived_at") != None)  # noqa: E711
        ),
    )

    compiled = compile_typed_expression_query("postgresql", query.to_query_payload())

    assert compiled == {
        "sql": 'SELECT "id" FROM "orders" WHERE (("deleted_at" IS NULL) OR ("archived_at" IS NOT NULL))',
        "params": [],
        "operation": "select",
        "values": {},
    }


def test_empty_typed_in_predicates_compile_to_constants() -> None:
    query = select_query(
        "orders",
        column("id"),
        where=column("status").in_([]) & column("kind").not_in([]),
    )

    compiled = compile_typed_expression_query("sqlite", query.to_query_payload())

    assert compiled == {
        "sql": 'SELECT "id" FROM "orders" WHERE ((1 = 0) AND (1 = 1))',
        "params": [],
        "operation": "select",
        "values": {},
    }


def test_projection_group_literal_and_min_max_helpers_compile() -> None:
    query = select_query(
        "orders",
        projection(min(column("total")), "minimum_total"),
        max(column("total")).as_("maximum_total"),
        literal("orders").as_("source"),
        where=group(
            column("status").istartswith("pa") | column("status").iendswith("ed")
        ),
        order_by=[max(column("total")).asc(nulls="first")],
        distinct=True,
    )

    compiled = compile_typed_expression_query("postgresql", query.to_query_payload())

    assert compiled == {
        "sql": 'SELECT DISTINCT MIN("total") AS "minimum_total", MAX("total") AS "maximum_total", \'orders\' AS "source" FROM "orders" WHERE (("status" ILIKE $1) OR ("status" ILIKE $2)) ORDER BY MAX("total") ASC NULLS FIRST',
        "params": ["expr_param_0", "expr_param_1"],
        "operation": "select",
        "values": {"expr_param_0": "pa%", "expr_param_1": "%ed"},
    }


def test_typed_update_query_compiles_with_stable_assignment_then_where_params() -> None:
    query = update_query(
        "orders",
        assignment("total", 5 + column("total")),
        column("status").set("archived"),
        where=column("customer_id") == "alice",
    )

    compiled = compile_typed_update_query("postgresql", query.to_query_payload())

    assert compiled == {
        "sql": 'UPDATE "orders" SET "total" = ($1 + "total"), "status" = $2 WHERE ("customer_id" = $3)',
        "params": ["expr_param_0", "expr_param_1", "expr_param_2"],
        "operation": "update",
        "values": {
            "expr_param_0": 5,
            "expr_param_1": "archived",
            "expr_param_2": "alice",
        },
    }


def test_case_cast_tuple_and_null_predicates_compile() -> None:
    query = select_query(
        "customers",
        column("id"),
        cast(column("created_at"), "TEXT").as_("created_at_text"),
        case(
            (column("tier") == "gold", literal("priority")),
            else_=literal("standard"),
        ).as_("service_level"),
        tuple_(column("country"), column("city")).as_("location_key"),
        where=(column("tier") == "gold") & (column("deleted_at") != None),  # noqa: E711
    )

    compiled = compile_typed_expression_query("postgresql", query.to_query_payload())

    assert compiled == {
        "sql": 'SELECT "id", CAST("created_at" AS TEXT) AS "created_at_text", CASE WHEN ("tier" = $1) THEN \'priority\' ELSE \'standard\' END AS "service_level", ("country", "city") AS "location_key" FROM "customers" WHERE (("tier" = $2) AND ("deleted_at" IS NOT NULL))',
        "params": [
            "expr_param_0",
            "expr_param_1",
        ],
        "operation": "select",
        "values": {
            "expr_param_0": "gold",
            "expr_param_1": "gold",
        },
    }


def test_subquery_payloads_remain_unsupported_in_python_facade() -> None:
    subquery_payload = {
        "table": "orders",
        "projections": [{"expr": {"kind": "column", "name": "customer_id"}}],
        "values": {},
    }
    query = {
        "table": "customers",
        "projections": [{"expr": {"kind": "column", "name": "id"}}],
        "where": {"kind": "exists", "subquery": subquery_payload},
        "values": {},
    }

    with pytest.raises(ValueError, match="unsupported expression kind 'exists'"):
        compile_typed_expression_query("postgresql", query)
