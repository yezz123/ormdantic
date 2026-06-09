from __future__ import annotations

import pytest

from ormdantic import migrations
from ormdantic._migrations import planning
from ormdantic._migrations.models import (
    ColumnSnapshot,
    IndexSnapshot,
    MigrationOperation,
    SchemaSnapshot,
    TableSnapshot,
)


def table(
    name: str,
    columns: list[ColumnSnapshot],
    *,
    indexes: list[IndexSnapshot] | None = None,
    unique_constraints: list[list[str]] | None = None,
) -> TableSnapshot:
    return TableSnapshot(
        model_key=name.title(),
        name=name,
        primary_key="id",
        columns=columns,
        indexes=indexes or [],
        unique_constraints=unique_constraints or [],
        relationships=[],
    )


def column(
    name: str,
    kind: str = "str",
    *,
    nullable: bool = False,
    primary_key: bool = False,
    unique: bool = False,
    checks: list[tuple[str, str, str]] | None = None,
    foreign_table: str | None = None,
    foreign_column: str | None = None,
) -> ColumnSnapshot:
    return ColumnSnapshot(
        name=name,
        kind=kind,
        nullable=nullable,
        primary_key=primary_key,
        unique=unique,
        checks=checks or [],
        foreign_table=foreign_table,
        foreign_column=foreign_column,
    )


def test_public_migration_facade_re_exports_planning_helpers() -> None:
    assert migrations.diff_snapshots is planning.diff_snapshots
    assert migrations.create_migration_artifact is planning.create_migration_artifact
    assert migrations.squash_migrations is planning.squash_migrations
    assert migrations._build_plan is planning._build_plan
    assert migrations._classify_sql_operation is planning._classify_sql_operation


def test_diff_snapshots_reports_columns_indexes_and_constraints() -> None:
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [
                    column("id", primary_key=True),
                    column("name", checks=[("length", ">=", "2")]),
                    column("supplier_id", foreign_table="supplier", foreign_column="id"),
                ],
                indexes=[IndexSnapshot("flavor_name_idx", ["name"], unique=False)],
                unique_constraints=[["name"]],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [
                    column("id", primary_key=True),
                    column("name", nullable=True, checks=[("length", ">=", "3")]),
                    column("rating", "int", nullable=True),
                ],
                indexes=[IndexSnapshot("flavor_rating_idx", ["rating"], unique=True)],
                unique_constraints=[["name", "rating"]],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    summary = diff.summary()

    assert "Changed column flavor.name: nullable, checks" in summary
    assert "Added column flavor.rating" in summary
    assert "Removed column flavor.supplier_id" in summary
    assert "Added index flavor_rating_idx on flavor" in summary
    assert "Removed index flavor_name_idx from flavor" in summary
    assert any(change.object_type == "constraint" for change in diff.changes)
    assert diff.has_unsafe_operations
    assert diff.has_destructive_operations


def test_sql_operation_classification_extracts_metadata() -> None:
    assert planning._classify_sql_operation('CREATE TABLE IF NOT EXISTS "flavor" (id TEXT)') == {
        "kind": "create_table",
        "table": "flavor",
        "object_name": None,
        "requires_rebuild": False,
        "reversible": True,
        "destructive": False,
        "unsafe": False,
    }
    drop = planning._classify_sql_operation('ALTER TABLE "flavor" DROP COLUMN "code"')
    assert drop["kind"] == "alter_table"
    assert drop["table"] == "flavor"
    assert drop["requires_rebuild"]
    assert drop["destructive"]
    assert planning._classify_sql_operation("DELETE FROM flavor")["reversible"] is False
    assert planning._classify_sql_operation(
        "CREATE UNIQUE INDEX flavor_name_idx ON flavor (name)"
    )["unsafe"]


def test_check_constraint_helpers_render_supported_checks() -> None:
    assert planning._check_expression("name", ("length", ">=", "2")) == (
        "LENGTH(name) >= 2"
    )
    assert planning._check_suffix(("comparison", "<=", "10")) == "le"
    with pytest.raises(ValueError, match="unsupported check constraint kind"):
        planning._check_expression("name", ("regex", "~", "^[a]"))
    with pytest.raises(ValueError, match="unsupported check constraint operator"):
        planning._check_suffix(("comparison", "!=", "10"))


def test_snapshot_coercion_accepts_objects_and_mappings() -> None:
    snapshot = SchemaSnapshot(tables=[table("flavor", [column("id", primary_key=True)])])

    assert planning._coerce_snapshot(snapshot) is snapshot
    assert planning._coerce_snapshot(snapshot.to_dict()).to_dict() == snapshot.to_dict()


def test_sqlite_rebuild_rewrites_rebuild_operations(monkeypatch) -> None:
    before_table = table(
        "flavor",
        [
            column("id", primary_key=True),
            column("name"),
            column("code"),
        ],
    )
    after_table = table(
        "flavor",
        [
            column("id", primary_key=True),
            column("name"),
        ],
        indexes=[IndexSnapshot("flavor_name_idx", ["name"], unique=False)],
    )

    def fake_compile_schema_diff(
        dialect: str,
        from_snapshot: SchemaSnapshot,
        to_snapshot: SchemaSnapshot,
    ) -> list[dict[str, str]]:
        del dialect, from_snapshot
        target = to_snapshot.tables[0]
        return [
            {"sql": f'CREATE TABLE "{target.name}" ("id" TEXT, "name" TEXT)'},
            {"sql": f'CREATE INDEX "{target.name}_name_idx" ON "{target.name}" ("name")'},
        ]

    monkeypatch.setattr(
        planning,
        "_compile_schema_diff",
        fake_compile_schema_diff,
    )
    operation = MigrationOperation(
        'ALTER TABLE "flavor" DROP COLUMN "code"',
        table="flavor",
        requires_rebuild=True,
    )

    rewritten = planning._rewrite_sqlite_rebuild_operations(
        [operation],
        SchemaSnapshot(tables=[before_table]),
        SchemaSnapshot(tables=[after_table]),
        destructive=True,
        unsafe=True,
    )

    phases = [item.metadata.get("phase") for item in rewritten]
    assert phases == [
        "drop_temp",
        "create_temp",
        "copy_rows",
        "drop_old",
        "rename",
        "create_index",
    ]
    assert rewritten[2].sql == (
        'INSERT INTO "__ormdantic_rebuild_flavor" ("id", "name") '
        'SELECT "id", "name" FROM "flavor"'
    )
    assert rewritten[3].destructive


def test_sqlite_unresolved_rebuild_operations_raise() -> None:
    with pytest.raises(ValueError, match="require a table rebuild"):
        planning._raise_if_unsupported_sqlite_plan(
            "sqlite",
            [
                MigrationOperation(
                    'ALTER TABLE "flavor" DROP COLUMN "code"',
                    requires_rebuild=True,
                )
            ],
        )
