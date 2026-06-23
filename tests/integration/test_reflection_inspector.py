from __future__ import annotations

import importlib

import pytest
from pydantic import BaseModel

from ormdantic import Ormdantic


@pytest.mark.asyncio
async def test_sqlite_inspector_reflects_existing_schema_filters_cache_and_scaffold(
    tmp_path,
) -> None:
    runtime = importlib.import_module("ormdantic._ormdantic")
    url = f"sqlite:///{tmp_path / 'existing.sqlite3'}"
    runtime.execute_native(
        url,
        (
            'CREATE TABLE "supplier" ('
            '"id" TEXT PRIMARY KEY, '
            '"name" TEXT NOT NULL UNIQUE)'
        ),
        [],
    )
    runtime.execute_native(
        url,
        (
            'CREATE TABLE "flavor" ('
            '"id" INTEGER PRIMARY KEY AUTOINCREMENT, '
            '"supplier_id" TEXT NOT NULL REFERENCES "supplier"("id") '
            "ON DELETE CASCADE ON UPDATE NO ACTION, "
            '"name" TEXT NOT NULL, '
            '"sku" TEXT NOT NULL UNIQUE, '
            '"rating" INTEGER NOT NULL, '
            'CONSTRAINT "flavor_name_supplier_unique" UNIQUE ("name", "supplier_id"), '
            'CONSTRAINT "flavor_rating_check" CHECK ("rating" >= 0 AND "rating" <= 5))'
        ),
        [],
    )
    runtime.execute_native(
        url,
        'CREATE INDEX "flavor_name_idx" ON "flavor" ("name")',
        [],
    )

    db = Ormdantic(url)

    @db.table("flavor", pk="id")
    class Flavor(BaseModel):
        id: int
        supplier_id: str
        name: str
        sku: str
        rating: int
        notes: str

    inspector = db.inspect()

    assert await inspector.table_names(name_patterns=["flav*"]) == ["flavor"]
    assert await inspector.table_names(exclude_tables=["sup*"]) == ["flavor"]
    assert await inspector.table_names() == ["flavor", "supplier"]

    columns = await inspector.columns("flavor")
    assert {column["name"] for column in columns} == {
        "id",
        "supplier_id",
        "name",
        "sku",
        "rating",
    }
    id_column = next(column for column in columns if column["name"] == "id")
    assert id_column["primary_key"] is True
    assert id_column["autoincrement"] is True
    assert id_column["type"] == "int"

    indexes = await inspector.indexes("flavor")
    assert any(index["name"] == "flavor_name_idx" for index in indexes)

    foreign_keys = await inspector.foreign_keys("flavor")
    assert foreign_keys == [
        {
            "name": None,
            "from": "supplier_id",
            "to": "id",
            "table": "supplier",
            "columns": ["supplier_id"],
            "foreign_table": "supplier",
            "foreign_columns": ["id"],
            "on_delete": "cascade",
            "on_update": None,
            "deferrable": None,
            "initially_deferred": False,
            "validated": True,
        }
    ]

    constraints = await inspector.constraints("flavor")
    assert any(
        constraint["type"] == "primary_key" and constraint["columns"] == ["id"]
        for constraint in constraints
    )
    assert any(
        constraint["type"] == "unique" and constraint["columns"] == ["sku"]
        for constraint in constraints
    )
    assert any(
        constraint["type"] == "check"
        and constraint.get("name") == "flavor_rating_check"
        for constraint in constraints
    )

    diff = await inspector.compare_to_models(include_tables=["flavor"])
    assert any(
        change.action == "add"
        and change.object_type == "column"
        and change.name == "notes"
        for change in diff.changes
    )

    source = await inspector.scaffold_models(include_tables=["flavor"])
    assert "@db.table('flavor', pk='id')" in source
    assert "class Flavor(BaseModel):" in source
    assert "supplier_id: str" in source

    runtime.execute_native(
        url,
        'CREATE TABLE "cached_later" ("id" TEXT PRIMARY KEY)',
        [],
    )
    assert "cached_later" not in await inspector.table_names()
    inspector.invalidate_cache()
    assert "cached_later" in await inspector.table_names()
