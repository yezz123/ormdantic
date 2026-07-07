from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

import pytest

from ormdantic import reflection
from ormdantic._migrations.models import (
    ColumnSnapshot,
    ExclusionConstraintSnapshot,
    ForeignKeyConstraintSnapshot,
    IndexSnapshot,
    MigrationChange,
    NamespaceSnapshot,
    SchemaDiff,
    SchemaSnapshot,
    TableCheckSnapshot,
    TableSnapshot,
    UniqueConstraintSnapshot,
)
from ormdantic.errors import ReflectionError


class FakeEvents:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def dispatch(self, name: str, **payload: Any) -> None:
        self.calls.append((name, payload))


class FakeMigrations:
    def __init__(self, snapshot: SchemaSnapshot) -> None:
        self._snapshot = snapshot
        self.live_calls: list[dict[str, Any]] = []
        self.diff_calls: list[tuple[SchemaSnapshot, SchemaSnapshot]] = []

    def live_snapshot(
        self,
        *,
        include_tables: Sequence[str] | None,
        exclude_tables: Sequence[str] | None,
        schema: str | None,
    ) -> SchemaSnapshot:
        self.live_calls.append(
            {
                "include_tables": include_tables,
                "exclude_tables": exclude_tables,
                "schema": schema,
            }
        )
        return self._snapshot

    def snapshot(self) -> SchemaSnapshot:
        return self._snapshot

    def diff(self, before: SchemaSnapshot, after: SchemaSnapshot) -> SchemaDiff:
        self.diff_calls.append((before, after))
        return SchemaDiff(
            changes=[
                MigrationChange(
                    "add",
                    "table",
                    "flavor",
                    "flavor",
                    "add table flavor",
                )
            ]
        )


class FakeDatabase:
    def __init__(self, snapshot: SchemaSnapshot) -> None:
        self._events = FakeEvents()
        self.migrations = FakeMigrations(snapshot)

    def _context(self, operation: str, **payload: Any) -> dict[str, Any]:
        return {"operation": operation, "backend": "sqlite", **payload}


def snapshot() -> SchemaSnapshot:
    flavor = TableSnapshot(
        "Flavor",
        "flavor",
        "id",
        columns=[
            ColumnSnapshot("id", "int", False, True),
            ColumnSnapshot(
                "supplier_id",
                "int",
                True,
                False,
                foreign_table="supplier",
                foreign_column="id",
                foreign_key_name="fk_supplier",
                on_delete="CASCADE",
                unique=True,
                checks=[("positive_supplier", "supplier_id > 0", "CHECK")],
            ),
        ],
        indexes=[IndexSnapshot("ix_flavor_supplier", ["supplier_id"])],
        unique_constraints=[["supplier_id"]],
        named_unique_constraints=[
            UniqueConstraintSnapshot("uq_supplier", ["supplier_id"])
        ],
        check_constraints=[
            TableCheckSnapshot("ck_supplier", "supplier_id > 0", validated=False)
        ],
        foreign_key_constraints=[
            ForeignKeyConstraintSnapshot(
                "fk_supplier_table",
                ["supplier_id"],
                "supplier",
                ["id"],
                on_update="RESTRICT",
            )
        ],
        exclusion_constraints=[
            ExclusionConstraintSnapshot(
                "ex_supplier",
                columns=[("supplier_id", "=")],
                expressions=[("tsrange(created_at, updated_at)", "&&")],
            )
        ],
    )
    weird = TableSnapshot(
        "Weird",
        "123 weird",
        "class",
        columns=[
            ColumnSnapshot("class", "str", False, True),
            ColumnSnapshot("9lives", "uuid", True, False, autoincrement=True),
            ColumnSnapshot("", "json", False, False),
        ],
    )
    empty = TableSnapshot(
        "Empty",
        "empty",
        "id",
        columns=[],
    )
    return SchemaSnapshot(
        tables=[flavor, weird, empty],
        namespaces=[NamespaceSnapshot("inventory", "warehouse")],
    )


def test_inspector_reflection_wrappers_cache_events_and_metadata() -> None:
    async def exercise() -> None:
        database = FakeDatabase(snapshot())
        inspector = reflection.Inspector(database)

        assert await inspector.table_names(
            include_tables=["flavor"], name_patterns=["fl*"]
        ) == ["flavor", "123 weird", "empty"]
        assert database.migrations.live_calls[-1]["include_tables"] == (
            "flavor",
            "fl*",
        )

        schema = await inspector.schema(
            include_tables=["flavor"], name_patterns=["fl*"]
        )
        assert schema.tables[0].name == "flavor"
        assert len(database.migrations.live_calls) == 1

        inspector.invalidate_cache(include_tables=["flavor"], name_patterns=["fl*"])
        assert (await inspector.schema_dict(include_tables=["flavor"]))["tables"]

        assert await inspector.namespaces() == [
            {"name": "inventory", "comment": "warehouse"}
        ]
        assert await inspector.schema_names() == ["inventory"]
        assert await inspector.tables()
        assert (await inspector.columns("flavor"))[0]["type"] == "int"
        assert await inspector.indexes("flavor") == [
            {"name": "ix_flavor_supplier", "columns": ["supplier_id"], "unique": False}
        ]
        assert (await inspector.foreign_keys("flavor"))[0]["name"] == "fk_supplier"
        assert (await inspector.unique_constraints("flavor"))[0]["type"] == "unique"
        assert (await inspector.check_constraints("flavor"))[0]["type"] == "check"
        constraints = await inspector.constraints("flavor")
        assert {constraint["type"] for constraint in constraints} == {
            "primary_key",
            "unique",
            "check",
            "foreign_key",
            "exclusion",
        }
        assert (await inspector.constraints("empty"))[0]["columns"] == ["id"]

        diff = await inspector.compare_to_models(schema="inventory")
        assert diff.summary() == ["add table flavor"]
        assert database.migrations.diff_calls[-1][1].tables[1].schema == "inventory"

        scaffold = await inspector.scaffold_models(database_variable="123 db")
        assert "_123_db = Ormdantic" in scaffold
        assert "class Reflected123" in scaffold
        assert "class_: str" in scaffold
        assert "_9lives: UUID | None = Field(default=None, alias='9lives')" in scaffold
        assert "value: dict[str, Any] = Field(alias='')" in scaffold
        assert "class Empty(BaseModel):\n    pass" in scaffold

        with pytest.raises(ReflectionError):
            await inspector.columns("missing", schema="inventory")

        after_events = [
            payload
            for name, payload in database._events.calls
            if name == "after_reflection"
        ]
        assert any(payload.get("row_count") == 3 for payload in after_events)
        assert any(
            isinstance(payload.get("error"), ReflectionError)
            for payload in after_events
        )

    asyncio.run(exercise())


def test_reflection_row_count_and_identifier_helpers_cover_edges() -> None:
    assert reflection._row_count({"tables": [1, 2]}) == 2
    assert reflection._row_count({"tables": "not-a-list"}) is None
    assert reflection._row_count(object()) is None
    assert reflection._include_patterns(None, None) is None
    assert reflection._tuple_or_none(None) is None
    assert reflection._class_name("") == "ReflectedTable"
    assert reflection._field_name("for") == "for_"
    assert reflection._field_name("9lives") == "_9lives"
    assert reflection._python_type("unknown") == "Any"
    used = {"Flavor"}
    assert reflection._unique_name("Flavor", used) == "Flavor_2"
    assert "Flavor_2" in used
    scaffold = reflection._scaffold_models(
        SchemaSnapshot(
            tables=[
                TableSnapshot(
                    "Scoped",
                    "scoped",
                    "id",
                    schema="inventory",
                    columns=[ColumnSnapshot("id", "int", False, True)],
                )
            ]
        ),
        database_variable="db",
    )
    assert "@db.table('scoped', pk='id', schema='inventory')" in scaffold
