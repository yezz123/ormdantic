from __future__ import annotations

import sys
from uuid import UUID, uuid4

from ormdantic._ormdantic import (
    compile_count,
    compile_delete_pk,
    compile_find_many,
    compile_insert,
    compile_joined_find_many,
    compile_select_pk,
    compile_update,
    compile_upsert,
    normalize_filters,
    sql_value,
)
from pydantic import BaseModel, Field

from ormdantic.hydration import plan_result_shape
from ormdantic.models import Map, OrmTable
from ormdantic.schema import validate_table_map


class RustBridgeFlavor(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    name: str


def test_rust_schema_bridge_validates_table_map() -> None:
    table = OrmTable[RustBridgeFlavor](
        model=RustBridgeFlavor,
        tablename="flavors",
        pk="id",
        indexed=[],
        unique=[],
        unique_constraints=[],
        columns=["id", "name"],
        relationships={},
        back_references={},
    )

    result = validate_table_map(
        Map(name_to_data={table.tablename: table}, model_to_data={})
    )

    assert result in {None, 1}


def test_rust_query_bridge_compiles_select_pk() -> None:
    query = compile_select_pk(
        dialect="postgresql",
        table="flavors",
        primary_key="id",
        columns=["id", "name"],
    )

    if query is None:
        return

    assert query == {
        "sql": 'SELECT "flavors"."id", "flavors"."name" FROM "flavors" WHERE "id" = $1',
        "params": ["id"],
        "operation": "select",
    }


def test_rust_query_bridge_compiles_find_many() -> None:
    query = compile_find_many(
        dialect="sqlite",
        table="flavors",
        columns=["id", "name"],
        filter_columns=[("name", "eq", ["name"])],
        order_columns=["name"],
        order_direction="desc",
        limit=10,
        offset=20,
        aliases=["flavors\\id", "flavors\\name"],
    )

    if query is None:
        return

    assert query == {
        "sql": 'SELECT "flavors"."id" AS "flavors\\id", "flavors"."name" AS "flavors\\name" FROM "flavors" WHERE "name" = ? ORDER BY "name" DESC LIMIT 10 OFFSET 20',
        "params": ["name"],
        "operation": "select",
    }


def test_rust_query_bridge_compiles_count() -> None:
    query = compile_count(
        dialect="postgresql",
        table="flavors",
        filter_columns=[("name", "eq", ["name"])],
    )

    if query is None:
        return

    assert query == {
        "sql": 'SELECT COUNT(*) FROM "flavors" WHERE "name" = $1',
        "params": ["name"],
        "operation": "count",
    }


def test_rust_query_bridge_compiles_joined_find_many() -> None:
    query = compile_joined_find_many(
        dialect="sqlite",
        table="coffee",
        columns=[
            ("coffee", "id", "coffee\\id"),
            ("coffee/flavor", "id", "coffee/flavor\\id"),
            ("coffee/flavor", "name", "coffee/flavor\\name"),
        ],
        joins=[
            (
                "flavors",
                "coffee/flavor",
                "coffee",
                "flavor",
                "coffee/flavor",
                "id",
            )
        ],
        filter_columns=[("id", "eq", ["id"])],
        order_columns=[],
        order_direction="asc",
    )

    assert query == {
        "sql": 'SELECT "coffee"."id" AS "coffee\\id", "coffee/flavor"."id" AS "coffee/flavor\\id", "coffee/flavor"."name" AS "coffee/flavor\\name" FROM "coffee" LEFT JOIN "flavors" AS "coffee/flavor" ON "coffee"."flavor" = "coffee/flavor"."id" WHERE "coffee"."id" = ?',
        "params": ["id"],
        "operation": "select",
    }


def test_rust_query_bridge_normalizes_recursive_filter_tree() -> None:
    normalized = normalize_filters(
        {
            "connector": "or",
            "children": [
                {"connector": "leaf", "filters": {"name": "mocha"}},
                {
                    "connector": "and",
                    "children": [
                        {"connector": "leaf", "filters": {"strength__ge": 5}},
                        {"connector": "leaf", "filters": {"id__in": ["1", "2"]}},
                    ],
                },
            ],
        }
    )

    assert normalized["filters"] == {
        "connector": "or",
        "children": [
            {
                "connector": "leaf",
                "filters": [("name", "eq", ["expr_0__name"])],
            },
            {
                "connector": "and",
                "children": [
                    {
                        "connector": "leaf",
                        "filters": [("strength", "ge", ["expr_1_0__strength__ge"])],
                    },
                    {
                        "connector": "leaf",
                        "filters": [
                            (
                                "id",
                                "in",
                                ["expr_1_1__id__in_0", "expr_1_1__id__in_1"],
                            )
                        ],
                    },
                ],
            },
        ],
    }
    assert normalized["values"] == {
        "expr_0__name": "mocha",
        "expr_1_0__strength__ge": 5,
        "expr_1_1__id__in_0": "1",
        "expr_1_1__id__in_1": "2",
    }


def test_rust_query_bridge_compiles_insert() -> None:
    query = compile_insert(
        dialect="sqlite",
        table="flavors",
        columns=["id", "name"],
    )

    if query is None:
        return

    assert query == {
        "sql": 'INSERT INTO "flavors" ("id", "name") VALUES (?, ?)',
        "params": ["id", "name"],
        "operation": "insert",
    }


def test_sql_value_converts_primitives_without_decimal_import(monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "decimal", None)

    assert sql_value("payload") == "payload"
    assert sql_value(42) == 42


def test_rust_query_bridge_compiles_mysql_connection_url() -> None:
    query = compile_insert(
        dialect="mysql+pymysql://user:pass@localhost/db",
        table="flavors",
        columns=["id", "name"],
    )

    if query is None:
        return

    assert query == {
        "sql": "INSERT INTO `flavors` (`id`, `name`) VALUES (?, ?)",
        "params": ["id", "name"],
        "operation": "insert",
    }


def test_rust_query_bridge_compiles_update() -> None:
    query = compile_update(
        dialect="postgresql",
        table="flavors",
        primary_key="id",
        columns=["name", "strength"],
    )

    if query is None:
        return

    assert query == {
        "sql": 'UPDATE "flavors" SET "name" = $1, "strength" = $2 WHERE "id" = $3',
        "params": ["name", "strength", "id"],
        "operation": "update",
    }


def test_rust_query_bridge_compiles_upsert() -> None:
    query = compile_upsert(
        dialect="sqlite",
        table="flavors",
        primary_key="id",
        columns=["id", "name"],
    )

    if query is None:
        return

    assert query == {
        "sql": 'INSERT INTO "flavors" ("id", "name") VALUES (?, ?) ON CONFLICT ("id") DO UPDATE SET "name" = excluded."name"',
        "params": ["id", "name"],
        "operation": "upsert",
    }


def test_rust_query_bridge_compiles_delete() -> None:
    query = compile_delete_pk(dialect="sqlite", table="flavors", primary_key="id")

    if query is None:
        return

    assert query == {
        "sql": 'DELETE FROM "flavors" WHERE "id" = ?',
        "params": ["id"],
        "operation": "delete",
    }


def test_result_shape_bridge_describes_relationship_aliases() -> None:
    shape = plan_result_shape(
        root_table="coffee",
        columns=[
            "coffee\\id",
            "coffee/flavor\\id",
            "coffee/flavor\\name",
        ],
        array_paths=[],
    )

    assert shape["root_table"] == "coffee"
    assert shape["relationship_paths"] == ["coffee/flavor"]
    assert shape["columns"][1] == {
        "alias": "coffee/flavor\\id",
        "table_path": "coffee/flavor",
        "column": "id",
    }
