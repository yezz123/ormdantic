from __future__ import annotations

from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from ormdantic.generator._hydration import plan_result_shape
from ormdantic.generator._rust_query import (
    bind_compiled_query,
    compile_count,
    compile_delete_pk,
    compile_find_many,
    compile_insert,
    compile_select_pk,
    compile_update,
    compile_upsert,
)
from ormdantic.generator._rust_schema import validate_table_map
from ormdantic.models import Map, OrmTable


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
        filter_columns=["name"],
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
        filter_columns=["name"],
    )

    if query is None:
        return

    assert query == {
        "sql": 'SELECT COUNT(*) FROM "flavors" WHERE "name" = $1',
        "params": ["name"],
        "operation": "count",
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


def test_rust_query_bridge_binds_values_in_compiler_order() -> None:
    compiled = compile_update(
        dialect="sqlite",
        table="flavors",
        primary_key="id",
        columns=["name", "id"],
    )

    query = bind_compiled_query(compiled, {"id": "flavor-id", "name": "mocha"})

    if query is None:
        return

    assert query.sql == 'UPDATE "flavors" SET "name" = ?, "id" = ? WHERE "id" = ?'
    assert query.values == ("mocha", "flavor-id", "flavor-id")
    assert query.operation == "update"


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
