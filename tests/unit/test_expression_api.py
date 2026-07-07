from __future__ import annotations

import pytest
from ormdantic._ormdantic import (
    compile_typed_expression_query,
    compile_typed_update_query,
)

from ormdantic import (
    assignment,
    association_proxy,
    avg,
    case,
    cast,
    column,
    count,
    cte,
    exists,
    group,
    hybrid_property,
    literal,
    max,
    min,
    not_,
    not_exists,
    over,
    projection,
    raw_sql_safe,
    select_query,
    subquery,
    sum,
    tuple_,
    update_query,
)
from ormdantic.expressions import (
    SerializationContext,
    SqlExpression,
    expression_payload,
    relation,
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
    assert (column("kind").eq("bean") & column("size").ge(2)).to_where() == {
        "kind": "bean",
        "size__ge": 2,
    }
    assert (column("deleted_at") == None).to_where() == {  # noqa: E711
        "deleted_at__is_null": True
    }
    assert (column("deleted_at") != None).to_where() == {  # noqa: E711
        "deleted_at__is_not_null": True
    }
    with pytest.raises(ValueError, match="expression-only predicates"):
        column("left").eq(column("right")).to_filter_tree()


def test_expression_error_paths_and_operator_helpers() -> None:
    strength = column("strength")

    assert strength.not_between(1, 3).to_expression_payload()["op"] == "not"
    assert (strength - 2).to_expression_payload()["op"] == "sub"
    assert (strength * 2).to_expression_payload()["op"] == "mul"
    assert (strength / 2).to_expression_payload()["op"] == "div"
    assert (10 - strength).to_expression_payload()["op"] == "sub"
    assert (10 * strength).to_expression_payload()["op"] == "mul"
    assert (10 / strength).to_expression_payload()["op"] == "div"
    assert strength.cast("TEXT").to_expression_payload()["kind"] == "cast"

    assert strength.is_(None).to_where() == {"strength__is_null": True}
    assert strength.is_not(None).to_where() == {"strength__is_not_null": True}
    with pytest.raises(ValueError, match="only None"):
        strength.is_("value")
    with pytest.raises(ValueError, match="only None"):
        strength.is_not("value")
    with pytest.raises(ValueError, match="only column expressions"):
        (strength + 1).set(2)
    with pytest.raises(ValueError, match="OR expressions"):
        (strength.eq(1) | strength.eq(2)).to_where()
    with pytest.raises(ValueError, match="typed SQL expression"):
        group(QueryExpressionForTest()).to_expression_payload()
    with pytest.raises(ValueError, match="unsupported expression kind"):
        SqlExpression("unknown", {}).to_expression_payload()


class QueryExpressionForTest:
    expr = None


def test_expression_query_serialization_edges_and_relation_errors() -> None:
    base = select_query("numbers", column("id"))
    named_cte = cte("ids", base, columns=["id"], recursive=True)
    query = select_query(
        "ids",
        projection(select_query("other", column("value")), "scalar_value"),
        over(count(), partition_by=[column("id")], order_by=[column("id")]).as_(
            "partition_count"
        ),
        with_=[named_cte],
        offset=5,
    )

    payload = query.to_query_payload()

    assert query.to_expression_payload()["kind"] == "subquery"
    assert query.with_cte(named_cte).ctes == (named_cte, named_cte)
    assert payload["ctes"][0]["columns"] == ["id"]
    assert payload["ctes"][0]["recursive"] is True
    assert payload["projections"][0]["alias"] == "scalar_value"
    assert payload["projections"][0]["expr"]["kind"] == "subquery"
    assert payload["projections"][1]["expr"]["order_by"][0]["direction"] == "asc"
    assert payload["offset"] == 5

    class Model:
        pass

    class RegisteredModel:
        pass

    class TableData:
        def __init__(self) -> None:
            self.model = RegisteredModel
            self.tablename = "registered"
            self.pk = "id"
            self.relationships: dict[str, object] = {}

    class TableMap:
        model_to_data = {RegisteredModel: TableData()}
        name_to_data: dict[str, object] = {}

    with pytest.raises(ValueError, match="is not registered"):
        relation(TableMap, Model, "missing")
    with pytest.raises(ValueError, match="available relationships: none"):
        relation(TableMap, RegisteredModel, "missing")


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


def test_association_and_hybrid_expressions_compile_through_typed_bridge() -> None:
    class Customer:
        account_name = association_proxy("account", "name")

        @account_name.expression
        def account_name(cls):
            return column("account_name")

        @hybrid_property
        def status_label(self) -> str:
            return "active"

        @status_label.expression
        def status_label(cls):
            return column("status")

    query = select_query(
        "customers",
        Customer.account_name.as_("account"),
        where=Customer.account_name.startswith("acme")
        & (Customer.status_label == "active"),
    )

    compiled = compile_typed_expression_query("postgresql", query.to_query_payload())

    assert compiled == {
        "sql": 'SELECT "account_name" AS "account" FROM "customers" WHERE (("account_name" LIKE $1) AND ("status" = $2))',
        "params": ["expr_param_0", "expr_param_1"],
        "operation": "select",
        "values": {"expr_param_0": "acme%", "expr_param_1": "active"},
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


def test_subquery_predicates_compile_through_typed_bridge() -> None:
    order_count = select_query(
        "orders",
        count(),
        where=column("customer_id", table="orders") == column("id", table="customers"),
    )
    paid_customer_ids = select_query(
        "orders",
        column("customer_id"),
        where=column("status") == "paid",
    )
    banned_customers = select_query(
        "bans",
        literal(1),
        where=column("customer_id", table="bans") == column("id", table="customers"),
    )
    query = select_query(
        "customers",
        column("id"),
        subquery(order_count).as_("order_count"),
        where=exists(paid_customer_ids)
        & column("id").in_query(paid_customer_ids)
        & not_exists(banned_customers),
    )

    compiled = compile_typed_expression_query("postgresql", query.to_query_payload())

    assert compiled == {
        "sql": 'SELECT "id", (SELECT COUNT(*) FROM "orders" WHERE ("orders"."customer_id" = "customers"."id")) AS "order_count" FROM "customers" WHERE (((EXISTS (SELECT "customer_id" FROM "orders" WHERE ("status" = $1))) AND ("id" IN (SELECT "customer_id" FROM "orders" WHERE ("status" = $2)))) AND (NOT EXISTS (SELECT 1 FROM "bans" WHERE ("bans"."customer_id" = "customers"."id"))))',
        "params": ["expr_param_0", "expr_param_1"],
        "operation": "select",
        "values": {"expr_param_0": "paid", "expr_param_1": "paid"},
    }


def test_ctes_and_window_expressions_compile_through_typed_bridge() -> None:
    paid_orders = select_query(
        "orders",
        column("customer_id"),
        sum(column("total")).as_("paid_total"),
        where=column("status") == "paid",
        group_by=[column("customer_id")],
    )
    query = select_query(
        "paid_orders",
        column("customer_id"),
        column("paid_total"),
        over(
            sum(column("paid_total")),
            order_by=[column("paid_total").desc()],
        ).as_("running_total"),
        with_=[cte("paid_orders", paid_orders)],
        where=column("paid_total") > 100,
        order_by=[column("paid_total").desc()],
    )

    compiled = compile_typed_expression_query("postgresql", query.to_query_payload())

    assert compiled == {
        "sql": 'WITH "paid_orders" AS (SELECT "customer_id", SUM("total") AS "paid_total" FROM "orders" WHERE ("status" = $1) GROUP BY "customer_id") SELECT "customer_id", "paid_total", SUM("paid_total") OVER (ORDER BY "paid_total" DESC) AS "running_total" FROM "paid_orders" WHERE ("paid_total" > $2) ORDER BY "paid_total" DESC',
        "params": ["expr_param_0", "expr_param_1"],
        "operation": "select",
        "values": {"expr_param_0": "paid", "expr_param_1": 100},
    }


def test_select_query_table_alias_compiles_through_typed_bridge() -> None:
    query = select_query(
        "employees",
        column("name", table="manager"),
        table_alias="manager",
        where=column("id", table="manager") == column("manager_id", table="employees"),
    )

    compiled = compile_typed_expression_query("postgresql", query.to_query_payload())

    assert compiled == {
        "sql": 'SELECT "manager"."name" FROM "employees" AS "manager" WHERE ("manager"."id" = "employees"."manager_id")',
        "params": [],
        "operation": "select",
        "values": {},
    }


def test_expression_payload_remaining_helper_edges() -> None:
    sub = select_query("orders", column("id"))
    window_payload = (
        count()
        .over(
            partition_by=[column("customer_id")],
            order_by=[column("id").desc()],
        )
        .to_expression_payload()
    )
    assert window_payload["kind"] == "window"
    assert window_payload["partition_by"][0]["name"] == "customer_id"
    assert window_payload["order_by"][0]["direction"] == "desc"

    assert column("id").not_in_query(sub).to_expression_payload()["negated"] is True
    assert (
        column("status").eq("paid") | column("status").eq("refunded")
    ).to_filter_tree() == {
        "connector": "or",
        "children": [
            {"connector": "leaf", "filters": {"status": "paid"}},
            {"connector": "leaf", "filters": {"status": "refunded"}},
        ],
    }

    update_payload = update_query(
        "orders",
        assignment("status", "archived"),
    ).to_query_payload()
    assert "where" not in update_payload
    assert update_payload["values"] == {"expr_param_0": "archived"}

    case_payload = case(
        (column("status") == "paid", literal("ok"))
    ).to_expression_payload()
    assert case_payload["else"] is None

    ctx = SerializationContext()
    assert expression_payload("plain", ctx) == {"kind": "param", "name": "expr_param_0"}
    assert ctx.values == {"expr_param_0": "plain"}
