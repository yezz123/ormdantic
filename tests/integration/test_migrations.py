from __future__ import annotations

import importlib
import sys
from enum import Enum

import pytest
from pydantic import BaseModel, Field

from ormdantic import Ormdantic, TableColumn
from ormdantic.cli import main
from ormdantic.migrations import (
    EnumTypeSnapshot,
    ForeignKeyConstraintSnapshot,
    MigrationArtifact,
    MigrationOperation,
    MigrationPlan,
    NamespaceSnapshot,
    SchemaSnapshot,
    SequenceSnapshot,
    ViewSnapshot,
    create_migration_artifact,
    squash_migrations,
)


def test_schema_snapshot_roundtrip_diff_and_dry_run() -> None:
    old_db = Ormdantic("sqlite:///:memory:")

    @old_db.table("flavor", pk="id", indexed=["name"])
    class OldFlavor(BaseModel):
        id: str
        name: str

    old_snapshot = old_db.migrations.snapshot()

    new_db = Ormdantic("sqlite:///:memory:")

    @new_db.table(
        "flavor", pk="id", indexed=["name"], unique_constraints=[["name", "code"]]
    )
    class NewFlavor(BaseModel):
        id: str
        name: str = Field(min_length=2)
        code: str
        strength: int | None = None

    new_snapshot = new_db.migrations.snapshot()
    roundtrip = SchemaSnapshot.from_json(new_snapshot.to_json())

    assert roundtrip.to_dict() == new_snapshot.to_dict()

    diff = new_db.migrations.diff(old_snapshot, roundtrip)
    assert "Added column flavor.code" in diff.summary()
    assert "Added column flavor.strength" in diff.summary()
    assert any(
        change.object_type == "constraint"
        and change.action == "add"
        and change.name == "flavor_unique_0"
        for change in diff.changes
    )
    assert diff.has_unsafe_operations
    assert not diff.has_destructive_operations

    plan = new_db.migrations.generate_plan(old_snapshot, roundtrip, dialect="sqlite")
    assert plan.safety["requires_rebuild"] is True
    assert any(
        operation.metadata.get("sqlite_rebuild") for operation in plan.operations
    )


@pytest.mark.asyncio
async def test_generated_plan_apply_records_once_and_rolls_back(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'generated.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    plan = db.migrations.generate_plan(dialect="sqlite")

    assert await db.migrations.apply("001_create_flavor", plan) is True
    assert await db.migrations.apply("001_create_flavor", plan) is False
    assert await db.migrations.applied_revisions() == ["001_create_flavor"]

    assert await db.migrations.rollback("001_create_flavor", plan) is True
    assert await db.migrations.rollback("001_create_flavor", plan) is False
    assert await db.migrations.applied_revisions() == []


@pytest.mark.asyncio
async def test_destructive_migration_requires_explicit_opt_in(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'destructive.sqlite3'}")

    safe_plan = MigrationPlan(
        [MigrationOperation("CREATE TABLE migration_extra (id TEXT)")]
    )
    assert await db.migrations.apply("001", safe_plan) is True

    destructive_plan = MigrationPlan([MigrationOperation("DROP TABLE migration_extra")])
    with pytest.raises(ValueError, match="allow_destructive=True"):
        await db.migrations.apply("002", destructive_plan)

    assert await db.migrations.applied_revisions() == ["001"]
    assert (
        await db.migrations.apply("002", destructive_plan, allow_destructive=True)
        is True
    )
    assert await db.migrations.applied_revisions() == ["001", "002"]


def test_generated_diff_sql_renders_supported_dialects() -> None:
    old_db = Ormdantic("sqlite:///:memory:")

    @old_db.table("flavor", pk="id")
    class OldFlavor(BaseModel):
        id: str
        name: str

    new_db = Ormdantic("sqlite:///:memory:")

    @new_db.table("flavor", pk="id")
    class NewFlavor(BaseModel):
        id: str
        name: str
        rating: int | None = None

    old_snapshot = old_db.migrations.snapshot()
    new_snapshot = new_db.migrations.snapshot()

    expected = {
        "sqlite": ['ALTER TABLE "flavor" ADD COLUMN "rating" INTEGER'],
        "postgresql": ['ALTER TABLE "flavor" ADD COLUMN "rating" INTEGER'],
        "mysql": ["ALTER TABLE `flavor` ADD COLUMN `rating` INTEGER"],
        "mariadb": ["ALTER TABLE `flavor` ADD COLUMN `rating` INTEGER"],
        "mssql": ["ALTER TABLE [flavor] ADD [rating] INTEGER"],
        "oracle": ['ALTER TABLE "flavor" ADD ("rating" INTEGER)'],
    }
    for dialect, sql in expected.items():
        assert (
            new_db.migrations.dry_run(old_snapshot, new_snapshot, dialect=dialect)
            == sql
        )


@pytest.mark.asyncio
async def test_sqlite_rebuild_plan_preserves_common_columns(tmp_path) -> None:
    old_db = Ormdantic("sqlite:///:memory:")

    @old_db.table("flavor", pk="id")
    class OldFlavor(BaseModel):
        id: str
        name: str
        code: str

    new_db = Ormdantic("sqlite:///:memory:")

    @new_db.table("flavor", pk="id")
    class NewFlavor(BaseModel):
        id: str
        name: str

    old_snapshot = old_db.migrations.snapshot()
    new_snapshot = new_db.migrations.snapshot()
    plan = new_db.migrations.generate_plan(old_snapshot, new_snapshot, dialect="sqlite")

    assert plan.safety["requires_rebuild"] is True
    assert any('DROP TABLE "flavor"' == operation.sql for operation in plan.operations)
    assert any(
        'INSERT INTO "__ormdantic_rebuild_flavor" ("id", "name") '
        'SELECT "id", "name" FROM "flavor"' == operation.sql
        for operation in plan.operations
    )

    url = f"sqlite:///{tmp_path / 'rebuild.sqlite3'}"
    runtime = importlib.import_module("ormdantic._ormdantic")
    runtime.execute_native(
        url,
        (
            'CREATE TABLE "flavor" ('
            '"id" TEXT PRIMARY KEY NOT NULL, '
            '"name" TEXT NOT NULL, '
            '"code" TEXT NOT NULL)'
        ),
        [],
    )
    runtime.execute_native(
        url,
        "INSERT INTO flavor (id, name, code) VALUES ('f1', 'vanilla', 'v')",
        [],
    )

    db = Ormdantic(url)
    assert (
        await db.migrations.apply("001_rebuild_flavor", plan, allow_destructive=True)
        is True
    )
    columns = runtime.execute_native(
        url,
        "SELECT name FROM pragma_table_info('flavor') ORDER BY cid",
        [],
    )["rows"]
    assert columns == [["id"], ["name"]]
    rows = runtime.execute_native(url, "SELECT id, name FROM flavor", [])["rows"]
    assert rows == [["f1", "vanilla"]]


@pytest.mark.asyncio
async def test_migration_artifacts_apply_directory_and_squash(tmp_path) -> None:
    base = SchemaSnapshot.empty()
    first_db = Ormdantic("sqlite:///:memory:")

    @first_db.table("flavor", pk="id")
    class InitialFlavor(BaseModel):
        id: str
        name: str

    first = first_db.migrations.snapshot()

    second_db = Ormdantic("sqlite:///:memory:")

    @second_db.table("flavor", pk="id")
    class ExpandedFlavor(BaseModel):
        id: str
        name: str
        rating: int | None = None

    second = second_db.migrations.snapshot()
    first_artifact = create_migration_artifact(
        "001_initial", base, first, dialect="sqlite"
    )
    second_artifact = create_migration_artifact(
        "002_rating", first, second, dialect="sqlite"
    )

    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    first_path = migrations_dir / "001_initial.json"
    second_path = migrations_dir / "002_rating.toml"
    first_artifact.write(first_path)
    second_artifact.write(second_path)

    roundtrip = MigrationArtifact.read(second_path)
    assert roundtrip.revision == second_artifact.revision
    assert [operation.sql for operation in roundtrip.operations] == [
        operation.sql for operation in second_artifact.operations
    ]
    assert roundtrip.checksum == second_artifact.checksum

    db = Ormdantic(f"sqlite:///{tmp_path / 'artifact.sqlite3'}")
    assert await db.migrations.apply_directory(migrations_dir) == [
        "001_initial",
        "002_rating",
    ]
    assert await db.migrations.apply_directory(migrations_dir) == []
    assert await db.migrations.applied_revisions() == ["001_initial", "002_rating"]

    squashed = squash_migrations(
        "001_squashed",
        [first_path, second_path],
        dialect="sqlite",
    )
    assert squashed.revision == "001_squashed"
    assert squashed.from_snapshot.to_dict() == base.to_dict()
    assert squashed.to_snapshot.to_dict() == second.to_dict()
    assert any("CREATE TABLE" in sql for sql in squashed.to_plan().dry_run())
    assert not any("ALTER TABLE" in sql for sql in squashed.to_plan().dry_run())


def test_migration_cli_create_preview_and_apply(tmp_path, capsys) -> None:
    old_snapshot = SchemaSnapshot.empty()
    db = Ormdantic("sqlite:///:memory:")

    @db.table("flavor", pk="id")
    class CliFlavor(BaseModel):
        id: str
        name: str

    new_snapshot = db.migrations.snapshot()
    old_path = tmp_path / "old.json"
    new_path = tmp_path / "new.toml"
    artifact_path = tmp_path / "001_cli.toml"
    old_snapshot.write(old_path)
    new_snapshot.write(new_path)
    assert SchemaSnapshot.read(new_path).to_dict() == new_snapshot.to_dict()

    assert (
        main(
            [
                "migrations",
                "create",
                "001_cli",
                "--from",
                str(old_path),
                "--to",
                str(new_path),
                "--dialect",
                "sqlite",
                "--out",
                str(artifact_path),
                "--format",
                "toml",
            ]
        )
        == 0
    )
    assert artifact_path.exists()
    assert MigrationArtifact.read(artifact_path).revision == "001_cli"

    assert main(["migrations", "preview", str(artifact_path)]) == 0
    assert "CREATE TABLE" in capsys.readouterr().out

    url = f"sqlite:///{tmp_path / 'cli.sqlite3'}"
    assert main(["migrations", "apply", url, str(artifact_path)]) == 0
    assert "Applied migration: 001_cli" in capsys.readouterr().out
    assert main(["migrations", "status", url]) == 0
    assert "Dirty: no" in capsys.readouterr().out
    assert main(["migrations", "history", url]) == 0
    history_output = capsys.readouterr().out
    assert "001_cli" in history_output
    assert "applied" in history_output
    assert main(["migrations", "current", url]) == 0
    assert "001_cli" in capsys.readouterr().out
    assert (
        main(["migrations", "check", str(tmp_path), "--pattern", "*001_cli*.toml"]) == 0
    )
    assert "ok" in capsys.readouterr().out
    assert main(["migrations", "rollback", url, str(artifact_path)]) == 0
    assert "Rolled back migration: 001_cli" in capsys.readouterr().out
    assert main(["migrations", "repair", url, "--clear-dirty"]) == 0
    assert "Repaired migration rows:" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_rollback_requires_explicit_down_sql(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'rollback_guard.sqlite3'}")
    plan = MigrationPlan(
        operations=[MigrationOperation("CREATE TABLE rollback_guard (id TEXT)")]
    )
    assert await db.migrations.apply("001", plan) is True
    with pytest.raises(ValueError, match="rollback SQL is unavailable"):
        await db.migrations.rollback("001", plan)


@pytest.mark.asyncio
async def test_migration_history_checksum_dirty_and_repair(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'history.sqlite3'}")
    plan = MigrationPlan(
        operations=[MigrationOperation("CREATE TABLE history_table (id TEXT)")],
        rollback_operations=[MigrationOperation("DROP TABLE history_table")],
    )
    assert await db.migrations.apply("001", plan, checksum="checksum-a") is True
    history = await db.migrations.history()
    assert history[-1].checksum == "checksum-a"
    assert history[-1].status == "applied"
    assert history[-1].dirty is False
    with pytest.raises(ValueError, match="already applied with checksum"):
        await db.migrations.apply("001", plan, checksum="checksum-b")

    failing = MigrationPlan([MigrationOperation("CREATE TABLE broken (")])
    with pytest.raises(ValueError):
        await db.migrations.apply("002", failing, checksum="checksum-c")
    assert await db.migrations.is_dirty() is True
    with pytest.raises(ValueError, match="run repair"):
        await db.migrations.apply(
            "003",
            MigrationPlan([MigrationOperation("CREATE TABLE blocked (id TEXT)")]),
        )
    repaired = await db.migrations.repair(clear_dirty=True)
    assert repaired >= 1
    assert await db.migrations.is_dirty() is False


def test_live_autogenerate_from_sqlite(tmp_path) -> None:
    url = f"sqlite:///{tmp_path / 'autogen.sqlite3'}"
    db = Ormdantic(url)

    @db.table("flavor", pk="id")
    class Flavor(BaseModel):
        id: str
        name: str
        rating: int | None = None

    runtime = importlib.import_module("ormdantic._ormdantic")
    runtime.execute_native(
        url, "CREATE TABLE flavor (id TEXT PRIMARY KEY, name TEXT NOT NULL)", []
    )
    artifact = db.migrations.autogenerate("001_auto", description="autogen")
    assert artifact is not None
    assert any("ADD COLUMN" in operation.sql for operation in artifact.operations)
    assert artifact.description == "autogen"
    assert artifact.checksum


def test_autogenerate_applies_table_filters_to_target_snapshot(tmp_path) -> None:
    url = f"sqlite:///{tmp_path / 'scoped_autogen.sqlite3'}"
    db = Ormdantic(url)

    @db.table("flavor", pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    @db.table("supplier", pk="id")
    class Supplier(BaseModel):
        id: str
        name: str

    runtime = importlib.import_module("ormdantic._ormdantic")
    runtime.execute_native(
        url, "CREATE TABLE flavor (id TEXT PRIMARY KEY, name TEXT NOT NULL)", []
    )

    artifact = db.migrations.autogenerate(
        "001_scoped",
        include_tables=["flavor"],
        skip_noop=False,
    )

    assert artifact is not None
    assert [table.name for table in artifact.from_snapshot.tables] == ["flavor"]
    assert [table.name for table in artifact.to_snapshot.tables] == ["flavor"]
    assert artifact.operations == []
    assert not any(
        "supplier" in operation.sql.lower() for operation in artifact.operations
    )


def test_autogenerate_filters_target_enum_types_to_included_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ScopedFlavorKind(str, Enum):
        mocha = "mocha"

    class ScopedSupplierKind(str, Enum):
        roaster = "roaster"

    db = Ormdantic("postgresql://localhost/db")

    @db.table("flavor", pk="id")
    class Flavor(BaseModel):
        id: str
        kind: ScopedFlavorKind

    @db.table("supplier", pk="id")
    class Supplier(BaseModel):
        id: str
        kind: ScopedSupplierKind

    manager = db.migrations
    monkeypatch.setattr(
        manager, "live_snapshot", lambda **kwargs: SchemaSnapshot.empty()
    )

    artifact = manager.autogenerate(
        "001_scoped_enum",
        include_tables=["flavor"],
        dialect="postgresql",
        skip_noop=False,
    )

    assert artifact is not None
    assert [table.name for table in artifact.to_snapshot.tables] == ["flavor"]
    assert artifact.to_snapshot.enum_types == [
        EnumTypeSnapshot("scoped_flavor_kind", ["mocha"])
    ]
    assert not any(
        enum_type.name == "scoped_supplier_kind"
        for enum_type in artifact.to_snapshot.enum_types
    )


def test_autogenerate_filters_target_sequences_to_included_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("postgresql://localhost/db")
    db.sequence("flavor_id_seq")
    db.sequence("supplier_id_seq")

    @db.table(
        "flavor",
        pk="id",
        column_options={"id": TableColumn(server_default="nextval('flavor_id_seq')")},
    )
    class Flavor(BaseModel):
        id: int | None = None
        name: str

    @db.table(
        "supplier",
        pk="id",
        column_options={"id": TableColumn(server_default="nextval('supplier_id_seq')")},
    )
    class Supplier(BaseModel):
        id: int | None = None
        name: str

    manager = db.migrations
    monkeypatch.setattr(
        manager, "live_snapshot", lambda **kwargs: SchemaSnapshot.empty()
    )

    artifact = manager.autogenerate(
        "001_scoped_sequence",
        include_tables=["flavor"],
        dialect="postgresql",
        skip_noop=False,
    )

    assert artifact is not None
    assert [table.name for table in artifact.to_snapshot.tables] == ["flavor"]
    assert artifact.to_snapshot.sequences == [SequenceSnapshot("flavor_id_seq")]
    assert not any(
        sequence.name == "supplier_id_seq"
        for sequence in artifact.to_snapshot.sequences
    )


def test_autogenerate_filters_target_namespaces_to_included_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("postgresql://localhost/db")
    db.namespace("inventory")
    db.namespace("warehouse")

    @db.table("flavor", pk="id", schema="inventory")
    class Flavor(BaseModel):
        id: str
        name: str

    @db.table("supplier", pk="id", schema="warehouse")
    class Supplier(BaseModel):
        id: str
        name: str

    manager = db.migrations
    monkeypatch.setattr(
        manager, "live_snapshot", lambda **kwargs: SchemaSnapshot.empty()
    )

    artifact = manager.autogenerate(
        "001_scoped_namespace",
        include_tables=["flavor"],
        dialect="postgresql",
        skip_noop=False,
    )

    assert artifact is not None
    assert [table.name for table in artifact.to_snapshot.tables] == ["flavor"]
    assert artifact.to_snapshot.namespaces == [NamespaceSnapshot("inventory")]
    assert not any(
        namespace.name == "warehouse" for namespace in artifact.to_snapshot.namespaces
    )


def test_autogenerate_preserves_reflected_enum_types_when_target_has_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("sqlite:///:memory:")
    manager = db.migrations
    enum_type = EnumTypeSnapshot("flavor_kind", ["mocha", "latte"], schema="public")
    live_snapshot = SchemaSnapshot(tables=[], enum_types=[enum_type])

    monkeypatch.setattr(
        manager,
        "live_snapshot",
        lambda **kwargs: live_snapshot,
    )
    monkeypatch.setattr(manager, "snapshot", SchemaSnapshot.empty)

    artifact = manager.autogenerate(
        "001_preserve_enums",
        dialect="postgresql",
        skip_noop=False,
    )

    assert artifact is not None
    assert artifact.to_snapshot.enum_types == [enum_type]
    assert not any("DROP TYPE" in operation.sql for operation in artifact.operations)


def test_autogenerate_preserves_reflected_namespaces_when_target_has_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("postgresql://localhost/db")
    manager = db.migrations
    namespace = NamespaceSnapshot("inventory")
    live_snapshot = SchemaSnapshot(tables=[], namespaces=[namespace])

    monkeypatch.setattr(
        manager,
        "live_snapshot",
        lambda **kwargs: live_snapshot,
    )
    monkeypatch.setattr(manager, "snapshot", SchemaSnapshot.empty)

    artifact = manager.autogenerate(
        "001_preserve_namespaces",
        dialect="postgresql",
        skip_noop=False,
    )

    assert artifact is not None
    assert artifact.to_snapshot.namespaces == [namespace]
    assert not any("DROP SCHEMA" in operation.sql for operation in artifact.operations)


def test_autogenerate_preserves_reflected_sequences_when_target_has_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("postgresql://localhost/db")
    manager = db.migrations
    sequence = SequenceSnapshot("flavor_id_seq", schema="public", cache=20)
    live_snapshot = SchemaSnapshot(tables=[], sequences=[sequence])

    monkeypatch.setattr(
        manager,
        "live_snapshot",
        lambda **kwargs: live_snapshot,
    )
    monkeypatch.setattr(manager, "snapshot", SchemaSnapshot.empty)

    artifact = manager.autogenerate(
        "001_preserve_sequences",
        dialect="postgresql",
        skip_noop=False,
    )

    assert artifact is not None
    assert artifact.to_snapshot.sequences == [sequence]
    assert not any(
        "DROP SEQUENCE" in operation.sql for operation in artifact.operations
    )


def test_autogenerate_preserves_reflected_views_when_target_has_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("postgresql://localhost/db")
    manager = db.migrations
    view = ViewSnapshot("active_flavors", "SELECT id FROM flavor", schema="public")
    live_snapshot = SchemaSnapshot(tables=[], views=[view])

    monkeypatch.setattr(
        manager,
        "live_snapshot",
        lambda **kwargs: live_snapshot,
    )
    monkeypatch.setattr(manager, "snapshot", SchemaSnapshot.empty)

    artifact = manager.autogenerate(
        "001_preserve_views",
        dialect="postgresql",
        skip_noop=False,
    )

    assert artifact is not None
    assert artifact.to_snapshot.views == [view]
    assert not any("DROP VIEW" in operation.sql for operation in artifact.operations)


def test_autogenerate_preserves_reflected_materialized_views_when_target_has_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("postgresql://localhost/db")
    manager = db.migrations
    view = ViewSnapshot(
        "active_flavor_counts",
        "SELECT supplier_id, count(*) AS flavor_count FROM flavor GROUP BY supplier_id",
        schema="public",
        materialized=True,
    )
    live_snapshot = SchemaSnapshot(tables=[], views=[view])

    monkeypatch.setattr(
        manager,
        "live_snapshot",
        lambda **kwargs: live_snapshot,
    )
    monkeypatch.setattr(manager, "snapshot", SchemaSnapshot.empty)

    artifact = manager.autogenerate(
        "001_preserve_materialized_views",
        dialect="postgresql",
        skip_noop=False,
    )

    assert artifact is not None
    assert artifact.to_snapshot.views == [view]
    assert not any(
        "DROP MATERIALIZED VIEW" in operation.sql for operation in artifact.operations
    )


def test_live_snapshot_reflects_sqlite_unique_constraints_and_index_columns(
    tmp_path,
) -> None:
    url = f"sqlite:///{tmp_path / 'sqlite_reflect_unique.sqlite3'}"
    runtime = importlib.import_module("ormdantic._ormdantic")
    runtime.execute_native(
        url,
        "CREATE TABLE supplier (id TEXT PRIMARY KEY)",
        [],
    )
    runtime.execute_native(
        url,
        "CREATE TABLE origin (id TEXT, code TEXT, PRIMARY KEY(id, code))",
        [],
    )
    runtime.execute_native(
        url,
        (
            "CREATE TABLE flavor ("
            "id TEXT PRIMARY KEY, "
            "code TEXT UNIQUE, "
            "name TEXT NOT NULL, "
            "supplier_id TEXT, "
            "origin_id TEXT, "
            "origin_code TEXT, "
            "CONSTRAINT flavor_name_supplier_unique UNIQUE(name, supplier_id), "
            "CONSTRAINT flavor_name_check CHECK (LENGTH(name) >= 2), "
            "CONSTRAINT flavor_origin_fk "
            "FOREIGN KEY(origin_id, origin_code) REFERENCES origin(id, code) "
            "ON DELETE CASCADE NOT DEFERRABLE, "
            "CONSTRAINT flavor_supplier_fk "
            "FOREIGN KEY(supplier_id) REFERENCES supplier(id) "
            "ON DELETE SET NULL ON UPDATE CASCADE "
            "DEFERRABLE INITIALLY DEFERRED)"
        ),
        [],
    )
    runtime.execute_native(
        url,
        "CREATE INDEX flavor_name_idx ON flavor (name)",
        [],
    )
    runtime.execute_native(
        url,
        (
            "CREATE INDEX flavor_name_lower_active_idx "
            "ON flavor (name, LOWER(name)) WHERE supplier_id IS NOT NULL"
        ),
        [],
    )
    runtime.execute_native(
        url,
        (
            "CREATE INDEX flavor_lower_active_idx "
            "ON flavor (LOWER(name)) WHERE supplier_id IS NOT NULL"
        ),
        [],
    )

    db = Ormdantic(url)
    snapshot = db.migrations.live_snapshot(include_tables=["flavor"])
    table = snapshot.tables[0]

    assert any(column.name == "code" and column.unique for column in table.columns)
    assert table.unique_constraints == []
    assert any(
        constraint.name == "flavor_name_supplier_unique"
        and constraint.columns == ["name", "supplier_id"]
        for constraint in table.named_unique_constraints
    )
    supplier_id = next(
        column for column in table.columns if column.name == "supplier_id"
    )
    assert supplier_id.foreign_table == "supplier"
    assert supplier_id.foreign_column == "id"
    assert supplier_id.foreign_key_name == "flavor_supplier_fk"
    assert supplier_id.on_delete == "set_null"
    assert supplier_id.on_update == "cascade"
    assert supplier_id.deferrable is True
    assert supplier_id.initially_deferred is True
    origin_id = next(column for column in table.columns if column.name == "origin_id")
    assert origin_id.foreign_table is None
    assert table.foreign_key_constraints == [
        ForeignKeyConstraintSnapshot(
            "flavor_origin_fk",
            ["origin_id", "origin_code"],
            "origin",
            ["id", "code"],
            on_delete="cascade",
            deferrable=False,
        )
    ]
    indexes = {index.name: index for index in table.indexes}
    assert indexes["flavor_name_idx"].columns == ["name"]
    assert indexes["flavor_name_lower_active_idx"].columns == ["name"]
    assert indexes["flavor_name_lower_active_idx"].expressions == ["LOWER(name)"]
    assert indexes["flavor_name_lower_active_idx"].where == "supplier_id IS NOT NULL"
    assert indexes["flavor_lower_active_idx"].columns == []
    assert indexes["flavor_lower_active_idx"].expressions == ["LOWER(name)"]
    assert indexes["flavor_lower_active_idx"].where == "supplier_id IS NOT NULL"
    assert any(
        check.name == "flavor_name_check" and check.expression == "LENGTH(name) >= 2"
        for check in table.check_constraints
    )


def test_migration_cli_autogenerate_command(tmp_path, capsys) -> None:
    url = f"sqlite:///{tmp_path / 'cli_autogen.sqlite3'}"
    runtime = importlib.import_module("ormdantic._ormdantic")
    runtime.execute_native(
        url, "CREATE TABLE flavor (id TEXT PRIMARY KEY, name TEXT NOT NULL)", []
    )

    module_name = "migrations_cli_fixture"
    module_path = tmp_path / f"{module_name}.py"
    module_path.write_text(
        "\n".join(
            [
                "from pydantic import BaseModel",
                "from ormdantic import Ormdantic",
                f"db = Ormdantic({url!r})",
                "@db.table('flavor', pk='id')",
                "class Flavor(BaseModel):",
                "    id: str",
                "    name: str",
                "    rating: int | None = None",
                "",
            ]
        )
    )
    sys.path.insert(0, str(tmp_path))
    try:
        artifact_path = tmp_path / "001_auto.json"
        assert (
            main(
                [
                    "migrations",
                    "autogenerate",
                    f"{module_name}:db",
                    "001_auto",
                    "--out",
                    str(artifact_path),
                ]
            )
            == 0
        )
        assert artifact_path.exists()
        assert MigrationArtifact.read(artifact_path).revision == "001_auto"
    finally:
        sys.path.remove(str(tmp_path))
        if module_name in sys.modules:
            del sys.modules[module_name]
    assert str(artifact_path) in capsys.readouterr().out
