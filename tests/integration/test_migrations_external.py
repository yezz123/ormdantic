from __future__ import annotations

import os
from dataclasses import dataclass, replace

import pytest
from pydantic import BaseModel, Field

from ormdantic import (
    Ormdantic,
    TableCheck,
    TableColumn,
    TableForeignKey,
    TableIndex,
    TableUnique,
)
from ormdantic.migrations import (
    EnumTypeSnapshot,
    MigrationArtifact,
    MigrationOperation,
    MigrationPlan,
    SchemaSnapshot,
)


@dataclass(frozen=True)
class MigrationSmokeDatabase:
    dialect: str
    env_var: str
    create_sql: str
    drop_sql: str
    cleanup_sql: tuple[str, ...]
    autogen_create_sql: str
    autogen_cleanup_sql: tuple[str, ...]
    reflect_create_sql: tuple[str, ...]
    reflect_cleanup_sql: tuple[str, ...]

    @property
    def url(self) -> str | None:
        return os.getenv(self.env_var) or os.getenv(
            self.env_var.replace("ORMDANTIC_TEST_", "ORMDANTIC_")
        )


SMOKE_DATABASES = [
    MigrationSmokeDatabase(
        "postgresql",
        "ORMDANTIC_TEST_POSTGRES_URL",
        "CREATE TABLE {table} (id VARCHAR(255) PRIMARY KEY)",
        "DROP TABLE {table}",
        ("DROP TABLE IF EXISTS {table}",),
        "CREATE TABLE {table} (id VARCHAR(255) PRIMARY KEY, name TEXT NOT NULL)",
        ("DROP TABLE IF EXISTS {table}",),
        (
            "CREATE TABLE {parent} (id VARCHAR(255) PRIMARY KEY, code VARCHAR(255) UNIQUE, name VARCHAR(255) NOT NULL)",
            "COMMENT ON TABLE {parent} IS '{parent} metadata'",
            "COMMENT ON COLUMN {parent}.name IS '{parent} name metadata'",
            "CREATE INDEX {index} ON {parent} (name)",
            "CREATE TABLE {child} (id VARCHAR(255) PRIMARY KEY, parent_id VARCHAR(255) NOT NULL REFERENCES {parent}(id))",
        ),
        ("DROP TABLE IF EXISTS {child}", "DROP TABLE IF EXISTS {parent}"),
    ),
    MigrationSmokeDatabase(
        "mysql",
        "ORMDANTIC_TEST_MYSQL_URL",
        "CREATE TABLE {table} (id VARCHAR(255) PRIMARY KEY)",
        "DROP TABLE {table}",
        ("DROP TABLE IF EXISTS {table}",),
        "CREATE TABLE {table} (id VARCHAR(255) PRIMARY KEY, name TEXT NOT NULL)",
        ("DROP TABLE IF EXISTS {table}",),
        (
            "CREATE TABLE {parent} (id VARCHAR(255) PRIMARY KEY, code VARCHAR(255) UNIQUE, name VARCHAR(255) NOT NULL COMMENT '{parent} name metadata')",
            "ALTER TABLE {parent} COMMENT = '{parent} metadata'",
            "CREATE INDEX {index} ON {parent} (name)",
            "CREATE TABLE {child} (id VARCHAR(255) PRIMARY KEY, parent_id VARCHAR(255) NOT NULL, FOREIGN KEY (parent_id) REFERENCES {parent}(id))",
        ),
        ("DROP TABLE IF EXISTS {child}", "DROP TABLE IF EXISTS {parent}"),
    ),
    MigrationSmokeDatabase(
        "mariadb",
        "ORMDANTIC_TEST_MARIADB_URL",
        "CREATE TABLE {table} (id VARCHAR(255) PRIMARY KEY)",
        "DROP TABLE {table}",
        ("DROP TABLE IF EXISTS {table}",),
        "CREATE TABLE {table} (id VARCHAR(255) PRIMARY KEY, name TEXT NOT NULL)",
        ("DROP TABLE IF EXISTS {table}",),
        (
            "CREATE TABLE {parent} (id VARCHAR(255) PRIMARY KEY, code VARCHAR(255) UNIQUE, name VARCHAR(255) NOT NULL COMMENT '{parent} name metadata')",
            "ALTER TABLE {parent} COMMENT = '{parent} metadata'",
            "CREATE INDEX {index} ON {parent} (name)",
            "CREATE TABLE {child} (id VARCHAR(255) PRIMARY KEY, parent_id VARCHAR(255) NOT NULL, FOREIGN KEY (parent_id) REFERENCES {parent}(id))",
        ),
        ("DROP TABLE IF EXISTS {child}", "DROP TABLE IF EXISTS {parent}"),
    ),
    MigrationSmokeDatabase(
        "mssql",
        "ORMDANTIC_TEST_MSSQL_URL",
        "CREATE TABLE {table} (id NVARCHAR(255) PRIMARY KEY)",
        "DROP TABLE {table}",
        ("IF OBJECT_ID(N'{table}', N'U') IS NOT NULL DROP TABLE {table}",),
        "CREATE TABLE {table} (id NVARCHAR(255) PRIMARY KEY, name NVARCHAR(255) NOT NULL)",
        ("IF OBJECT_ID(N'{table}', N'U') IS NOT NULL DROP TABLE {table}",),
        (
            "CREATE TABLE {parent} (id NVARCHAR(255) PRIMARY KEY, code NVARCHAR(255) UNIQUE, name NVARCHAR(255) NOT NULL)",
            "DECLARE @schema sysname = SCHEMA_NAME(); "
            "EXEC sys.sp_addextendedproperty @name = N'MS_Description', "
            "@value = N'{parent} metadata', "
            "@level0type = N'SCHEMA', @level0name = @schema, "
            "@level1type = N'TABLE', @level1name = N'{parent}'",
            "DECLARE @schema sysname = SCHEMA_NAME(); "
            "EXEC sys.sp_addextendedproperty @name = N'MS_Description', "
            "@value = N'{parent} name metadata', "
            "@level0type = N'SCHEMA', @level0name = @schema, "
            "@level1type = N'TABLE', @level1name = N'{parent}', "
            "@level2type = N'COLUMN', @level2name = N'name'",
            "CREATE INDEX {index} ON {parent} (name)",
            "CREATE TABLE {child} (id NVARCHAR(255) PRIMARY KEY, parent_id NVARCHAR(255) NOT NULL, FOREIGN KEY (parent_id) REFERENCES {parent}(id))",
        ),
        (
            "IF OBJECT_ID(N'{child}', N'U') IS NOT NULL DROP TABLE {child}",
            "IF OBJECT_ID(N'{parent}', N'U') IS NOT NULL DROP TABLE {parent}",
        ),
    ),
    MigrationSmokeDatabase(
        "oracle",
        "ORMDANTIC_TEST_ORACLE_URL",
        "CREATE TABLE {table} (id VARCHAR2(255) PRIMARY KEY)",
        "DROP TABLE {table}",
        ("DROP TABLE {table}",),
        'CREATE TABLE "{table}" ("id" VARCHAR2(255) PRIMARY KEY, "name" VARCHAR2(255) NOT NULL)',
        ('DROP TABLE "{table}"',),
        (
            'CREATE TABLE "{parent}" ("id" VARCHAR2(255) PRIMARY KEY, "code" VARCHAR2(255) UNIQUE, "name" VARCHAR2(255) NOT NULL)',
            "COMMENT ON TABLE \"{parent}\" IS '{parent} metadata'",
            'COMMENT ON COLUMN "{parent}"."name" IS \'{parent} name metadata\'',
            'CREATE INDEX "{index}" ON "{parent}" ("name")',
            'CREATE TABLE "{child}" ("id" VARCHAR2(255) PRIMARY KEY, "parent_id" VARCHAR2(255) NOT NULL, FOREIGN KEY ("parent_id") REFERENCES "{parent}"("id"))',
        ),
        ('DROP TABLE "{child}"', 'DROP TABLE "{parent}"'),
    ),
]

NO_BOUND_SEQUENCE_DATABASES = [
    database
    for database in SMOKE_DATABASES
    if database.dialect in {"postgresql", "mariadb", "mssql", "oracle"}
]

SEQUENCE_COMMENT_DATABASES = [
    database
    for database in SMOKE_DATABASES
    if database.dialect in {"postgresql", "mssql"}
]

NAMESPACE_COMMENT_DATABASES = [
    database
    for database in SMOKE_DATABASES
    if database.dialect in {"postgresql", "mssql"}
]

INDEX_COMMENT_DATABASES = [
    database
    for database in SMOKE_DATABASES
    if database.dialect in {"postgresql", "mysql", "mariadb", "mssql"}
]

CONSTRAINT_COMMENT_DATABASES = [
    database
    for database in SMOKE_DATABASES
    if database.dialect in {"postgresql", "mssql"}
]

NO_BOUND_IDENTITY_DATABASES = [
    database
    for database in SMOKE_DATABASES
    if database.dialect in {"postgresql", "oracle"}
]

AUTO_INCREMENT_COUNTER_DATABASES = [
    database for database in SMOKE_DATABASES if database.dialect in {"mysql", "mariadb"}
]

PUBLIC_INSPECTOR_DATABASES = [
    database
    for database in SMOKE_DATABASES
    if database.dialect in {"postgresql", "mysql", "mariadb"}
]

VIEW_DEFINITION_DATABASES = [
    database
    for database in SMOKE_DATABASES
    if database.dialect in {"postgresql", "mysql", "mariadb", "mssql", "oracle"}
]

MATERIALIZED_VIEW_DEFINITION_DATABASES = [
    database
    for database in SMOKE_DATABASES
    if database.dialect in {"postgresql", "oracle"}
]

ENUM_COMMENT_DATABASES = [
    database for database in SMOKE_DATABASES if database.dialect == "postgresql"
]


@pytest.mark.parametrize(
    "database",
    SMOKE_DATABASES,
    ids=[database.dialect for database in SMOKE_DATABASES],
)
@pytest.mark.asyncio
async def test_external_migration_execution_smoke(
    database: MigrationSmokeDatabase,
) -> None:
    url = database.url
    if not url:
        pytest.skip(f"{database.env_var} not configured")

    db = Ormdantic(url)
    await db.migrations.repair(clear_dirty=True)
    run_id = os.getpid()
    table_name = f"orm_mig_{database.dialect}_{run_id}"
    plan = MigrationPlan(
        operations=[MigrationOperation(database.create_sql.format(table=table_name))],
        rollback_operations=[
            MigrationOperation(
                database.drop_sql.format(table=table_name),
                destructive=True,
                unsafe=True,
            )
        ],
    )
    revision = f"smoke_{database.dialect}_{run_id}"
    checksum = f"checksum-{database.dialect}"
    try:
        assert (
            await db.migrations.apply(
                revision,
                plan,
                allow_destructive=True,
                checksum=checksum,
                description=f"{database.dialect} migration smoke",
            )
            is True
        )
        assert (
            await db.migrations.apply(
                revision,
                plan,
                allow_destructive=True,
                checksum=checksum,
            )
            is False
        )
        status = await db.migrations.status()
        assert status["dirty"] is False
        assert revision in status["applied"]
        history_entry = next(
            entry
            for entry in await db.migrations.history()
            if entry.revision == revision
        )
        assert history_entry.checksum == checksum
        assert history_entry.status == "applied"
        assert history_entry.dirty is False
        await db.migrations.rollback(revision, plan, allow_destructive=True)
        current = await db.migrations.current()
        assert current is None or current.revision != revision
        assert revision not in await db.migrations.applied_revisions()
    finally:
        try:
            import importlib

            runtime = importlib.import_module("ormdantic._ormdantic")
            for statement in database.cleanup_sql:
                try:
                    runtime.execute_native(
                        url,
                        statement.format(table=table_name),
                        [],
                    )
                except Exception:
                    pass
        except Exception:
            pass


@pytest.mark.parametrize(
    "database",
    SMOKE_DATABASES,
    ids=[database.dialect for database in SMOKE_DATABASES],
)
@pytest.mark.asyncio
async def test_external_live_autogenerate_smoke(
    database: MigrationSmokeDatabase,
    tmp_path,
) -> None:
    url = database.url
    if not url:
        pytest.skip(f"{database.env_var} not configured")

    runtime = pytest.importorskip("ormdantic._ormdantic")
    run_id = os.getpid()
    table_name = f"orm_autogen_{database.dialect}_{run_id}"
    revision = f"autogen_{database.dialect}_{run_id}"
    artifact_path = tmp_path / f"{revision}.toml"
    db = Ormdantic(url)
    await db.migrations.repair(clear_dirty=True)

    @db.table(table_name, pk="id")
    class AutogenFlavor(BaseModel):
        id: str = Field(max_length=255)
        name: str = Field(max_length=255)
        rating: int | None = None

    try:
        for statement in database.autogen_cleanup_sql:
            try:
                runtime.execute_native(url, statement.format(table=table_name), [])
            except Exception:
                pass
        runtime.execute_native(
            url,
            database.autogen_create_sql.format(table=table_name),
            [],
        )

        artifact = db.migrations.autogenerate(
            revision,
            include_tables=[table_name],
            description=f"{database.dialect} autogenerate smoke",
            path=artifact_path,
        )
        assert artifact is not None
        assert artifact_path.exists()
        assert artifact.from_snapshot.tables[0].name == table_name
        assert any(
            "rating" in operation.sql.lower() and "add" in operation.sql.lower()
            for operation in artifact.operations
        )

        loaded = MigrationArtifact.read(artifact_path)
        loaded.validate_checksum()
        assert loaded.revision == revision
        assert loaded.checksum == artifact.checksum
        assert loaded.metadata["autogenerated"] is True
        assert loaded.safety["rollback_available"] is True
        assert loaded.to_plan().rollback_available is True

        assert await db.migrations.apply_artifact(loaded) is True
        applied = db.migrations.live_snapshot(include_tables=[table_name])
        applied_columns = {
            column.name
            for table in applied.tables
            if table.name == table_name
            for column in table.columns
        }
        assert "rating" in applied_columns

        assert (
            await db.migrations.rollback_artifact(
                loaded,
                allow_destructive=True,
            )
            is True
        )
        rolled_back = db.migrations.live_snapshot(include_tables=[table_name])
        rolled_back_columns = {
            column.name
            for table in rolled_back.tables
            if table.name == table_name
            for column in table.columns
        }
        assert "rating" not in rolled_back_columns
    finally:
        for statement in database.autogen_cleanup_sql:
            try:
                runtime.execute_native(url, statement.format(table=table_name), [])
            except Exception:
                pass


@pytest.mark.parametrize(
    "database",
    SMOKE_DATABASES,
    ids=[database.dialect for database in SMOKE_DATABASES],
)
def test_external_live_snapshot_reflects_core_metadata(
    database: MigrationSmokeDatabase,
) -> None:
    url = database.url
    if not url:
        pytest.skip(f"{database.env_var} not configured")

    runtime = pytest.importorskip("ormdantic._ormdantic")
    run_id = os.getpid()
    parent = f"orm_ref_parent_{database.dialect}_{run_id}"
    child = f"orm_ref_child_{database.dialect}_{run_id}"
    index = f"orm_ref_idx_{database.dialect}_{run_id}"
    db = Ormdantic(url)
    values = {"parent": parent, "child": child, "index": index}

    try:
        for statement in database.reflect_cleanup_sql:
            try:
                runtime.execute_native(url, statement.format(**values), [])
            except Exception:
                pass
        for statement in database.reflect_create_sql:
            runtime.execute_native(url, statement.format(**values), [])

        snapshot = db.migrations.live_snapshot(include_tables=[parent, child])
        tables = {table.name: table for table in snapshot.tables}
        assert set(tables) == {parent, child}
        assert tables[parent].primary_key == "id"
        assert tables[parent].comment == f"{parent} metadata"
        assert (
            next(
                column
                for column in tables[parent].columns
                if column.name.lower() == "name"
            ).comment
            == f"{parent} name metadata"
        )
        assert any(
            column.name == "code" and column.unique for column in tables[parent].columns
        )
        assert any(
            item.name == index and item.columns == ["name"]
            for item in tables[parent].indexes
        )
        child_parent_id = next(
            column for column in tables[child].columns if column.name == "parent_id"
        )
        assert child_parent_id.foreign_table == parent
        assert child_parent_id.foreign_column == "id"
    finally:
        for statement in database.reflect_cleanup_sql:
            try:
                runtime.execute_native(url, statement.format(**values), [])
            except Exception:
                pass


@pytest.mark.parametrize(
    "database",
    PUBLIC_INSPECTOR_DATABASES,
    ids=[database.dialect for database in PUBLIC_INSPECTOR_DATABASES],
)
@pytest.mark.asyncio
async def test_external_public_inspector_reflects_core_metadata(
    database: MigrationSmokeDatabase,
) -> None:
    url = database.url
    if not url:
        pytest.skip(f"{database.env_var} not configured")

    runtime = pytest.importorskip("ormdantic._ormdantic")
    run_id = os.getpid()
    parent = f"orm_insp_parent_{database.dialect}_{run_id}"
    child = f"orm_insp_child_{database.dialect}_{run_id}"
    index = f"orm_insp_idx_{database.dialect}_{run_id}"
    db = Ormdantic(url)
    inspector = db.inspect()
    values = {"parent": parent, "child": child, "index": index}

    try:
        for statement in database.reflect_cleanup_sql:
            try:
                runtime.execute_native(url, statement.format(**values), [])
            except Exception:
                pass
        for statement in database.reflect_create_sql:
            runtime.execute_native(url, statement.format(**values), [])

        assert await inspector.table_names(include_tables=[parent]) == [parent]
        columns = await inspector.columns(parent)
        assert {column["name"] for column in columns} >= {"id", "code", "name"}
        indexes = await inspector.indexes(parent)
        assert any(item["name"] == index for item in indexes)
        constraints = await inspector.constraints(parent)
        assert any(
            item["type"] == "primary_key" and item["columns"] == ["id"]
            for item in constraints
        )
        assert any(
            item["type"] == "unique" and item["columns"] == ["code"]
            for item in constraints
        )
        foreign_keys = await inspector.foreign_keys(child)
        assert any(
            item["foreign_table"] == parent and item["foreign_columns"] == ["id"]
            for item in foreign_keys
        )
        source = await inspector.scaffold_models(include_tables=[parent])
        assert f"@db.table({parent!r}, pk='id')" in source
    finally:
        for statement in database.reflect_cleanup_sql:
            try:
                runtime.execute_native(url, statement.format(**values), [])
            except Exception:
                pass


@pytest.mark.parametrize(
    "database",
    SMOKE_DATABASES,
    ids=[database.dialect for database in SMOKE_DATABASES],
)
@pytest.mark.asyncio
async def test_external_supported_schema_metadata_does_not_autogenerate_drift(
    database: MigrationSmokeDatabase,
) -> None:
    url = database.url
    if not url:
        pytest.skip(f"{database.env_var} not configured")

    runtime = pytest.importorskip("ormdantic._ormdantic")
    run_id = os.getpid()
    parent = f"orm_drift_parent_{database.dialect}_{run_id}"
    child = f"orm_drift_child_{database.dialect}_{run_id}"
    revision = f"schema_drift_{database.dialect}_{run_id}"
    db = Ormdantic(url)
    await db.migrations.repair(clear_dirty=True)

    @db.table(
        parent,
        pk="id",
        indexed=["name"],
        unique_constraints=[
            TableUnique(
                name=f"{parent}_code_name_unique",
                columns=["code", "name"],
            )
        ],
        column_options={
            "name": TableColumn(comment=f"{parent} display name"),
            "rating": TableColumn(server_default="0"),
        },
        comment=f"{parent} metadata",
    )
    class DriftParent(BaseModel):
        id: str = Field(max_length=255)
        name: str = Field(max_length=255, min_length=2)
        code: str = Field(max_length=255)
        rating: int = Field(ge=0, le=100)

    @db.table(
        child,
        pk="id",
        indexes=[TableIndex(name=f"{child}_parent_idx", columns=["parent_id"])],
        column_options={
            "parent_id": TableColumn(
                foreign_key_name=f"{child}_parent_fk",
                on_delete="cascade",
            )
        },
        comment=f"{child} metadata",
    )
    class DriftChild(BaseModel):
        id: str = Field(max_length=255)
        parent_id: DriftParent | str
        label: str = Field(max_length=255)

    target = db.migrations.snapshot()
    plan = db.migrations.generate_plan(SchemaSnapshot.empty(), target)
    applied = False

    try:
        for statement in reversed(plan.rollback_sql()):
            try:
                runtime.execute_native(url, statement, [])
            except Exception:
                pass
        assert (
            await db.migrations.apply(
                revision,
                plan,
                allow_destructive=True,
                checksum=f"checksum-{revision}",
                description=f"{database.dialect} represented metadata drift",
            )
            is True
        )
        applied = True

        live = db.migrations.live_snapshot(include_tables=[parent, child])
        drift_plan = db.migrations.generate_plan(live, target)

        assert drift_plan.dry_run() == []
        assert db.migrations.diff(live, target).summary() == []
    finally:
        if applied:
            try:
                await db.migrations.rollback(
                    revision,
                    plan,
                    allow_destructive=True,
                )
            except Exception:
                pass
        for statement in plan.rollback_sql():
            try:
                runtime.execute_native(url, statement, [])
            except Exception:
                pass


@pytest.mark.parametrize(
    "database",
    AUTO_INCREMENT_COUNTER_DATABASES,
    ids=[database.dialect for database in AUTO_INCREMENT_COUNTER_DATABASES],
)
@pytest.mark.asyncio
async def test_external_mysql_auto_increment_counters_do_not_autogenerate_drift(
    database: MigrationSmokeDatabase,
) -> None:
    url = database.url
    if not url:
        pytest.skip(f"{database.env_var} not configured")

    runtime = pytest.importorskip("ormdantic._ormdantic")
    run_id = os.getpid()
    table_name = f"orm_ai_counter_{database.dialect}_{run_id}"
    revision = f"ai_counter_{database.dialect}_{run_id}"
    db = Ormdantic(url)
    await db.migrations.repair(clear_dirty=True)

    @db.table(
        table_name,
        pk="id",
        column_options={"id": TableColumn(autoincrement=True)},
    )
    class AutoIncrementFlavor(BaseModel):
        id: int | None = None
        name: str = Field(max_length=255)

    target = db.migrations.snapshot()
    plan = db.migrations.generate_plan(SchemaSnapshot.empty(), target)
    applied = False

    try:
        for statement in reversed(plan.rollback_sql()):
            try:
                runtime.execute_native(url, statement, [])
            except Exception:
                pass
        assert (
            await db.migrations.apply(
                revision,
                plan,
                allow_destructive=True,
                checksum=f"checksum-{revision}",
                description=f"{database.dialect} auto-increment counter drift",
            )
            is True
        )
        applied = True
        runtime.execute_native(
            url,
            f"INSERT INTO {table_name} (name) VALUES ('Mocha')",
            [],
        )

        live = db.migrations.live_snapshot(include_tables=[table_name])
        live_table = next(table for table in live.tables if table.name == table_name)
        assert live_table.mysql_auto_increment is not None
        assert db.migrations.generate_plan(live, target).dry_run() == []
        assert db.migrations.diff(live, target).summary() == []
    finally:
        if applied:
            try:
                await db.migrations.rollback(
                    revision,
                    plan,
                    allow_destructive=True,
                )
            except Exception:
                pass
        for statement in plan.rollback_sql():
            try:
                runtime.execute_native(url, statement, [])
            except Exception:
                pass


@pytest.mark.parametrize(
    "database",
    VIEW_DEFINITION_DATABASES,
    ids=[database.dialect for database in VIEW_DEFINITION_DATABASES],
)
@pytest.mark.asyncio
async def test_external_regular_view_definitions_do_not_autogenerate_drift(
    database: MigrationSmokeDatabase,
) -> None:
    url = database.url
    if not url:
        pytest.skip(f"{database.env_var} not configured")

    runtime = pytest.importorskip("ormdantic._ormdantic")
    run_id = os.getpid()
    table_name = f"orm_view_flavor_{database.dialect}_{run_id}"
    view_name = f"orm_view_active_{database.dialect}_{run_id}"
    revision = f"view_rewrite_{database.dialect}_{run_id}"
    db = Ormdantic(url)
    await db.migrations.repair(clear_dirty=True)

    @db.table(table_name, pk="id")
    class ViewFlavor(BaseModel):
        id: str = Field(max_length=255)
        name: str = Field(max_length=255)

    view_definition = (
        f'SELECT "id", "name" FROM "{table_name}"'
        if database.dialect == "oracle"
        else f"SELECT id, name FROM {table_name}"
    )
    db.view(view_name, view_definition)
    target = db.migrations.snapshot()
    plan = db.migrations.generate_plan(SchemaSnapshot.empty(), target)
    applied = False

    try:
        for statement in reversed(plan.rollback_sql()):
            try:
                runtime.execute_native(url, statement, [])
            except Exception:
                pass
        assert (
            await db.migrations.apply(
                revision,
                plan,
                allow_destructive=True,
                checksum=f"checksum-{revision}",
                description=f"{database.dialect} simple view rewrite drift",
            )
            is True
        )
        applied = True

        live = db.migrations.live_snapshot(include_tables=[table_name, view_name])
        live_view = next(view for view in live.views if view.name == view_name)
        if database.dialect != "oracle":
            assert live_view.definition != target.views[0].definition
        assert db.migrations.generate_plan(live, target).dry_run() == []
        assert db.migrations.diff(live, target).summary() == []
    finally:
        if applied:
            try:
                await db.migrations.rollback(
                    revision,
                    plan,
                    allow_destructive=True,
                )
            except Exception:
                pass
        for statement in plan.rollback_sql():
            try:
                runtime.execute_native(url, statement, [])
            except Exception:
                pass


@pytest.mark.parametrize(
    "database",
    MATERIALIZED_VIEW_DEFINITION_DATABASES,
    ids=[database.dialect for database in MATERIALIZED_VIEW_DEFINITION_DATABASES],
)
@pytest.mark.asyncio
async def test_external_materialized_view_definitions_do_not_autogenerate_drift(
    database: MigrationSmokeDatabase,
) -> None:
    url = database.url
    if not url:
        pytest.skip(f"{database.env_var} not configured")

    runtime = pytest.importorskip("ormdantic._ormdantic")
    run_id = os.getpid()
    table_name = f"orm_mview_flavor_{database.dialect}_{run_id}"
    view_name = f"orm_mview_active_{database.dialect}_{run_id}"
    revision = f"mview_definition_{database.dialect}_{run_id}"
    db = Ormdantic(url)
    await db.migrations.repair(clear_dirty=True)

    @db.table(table_name, pk="id")
    class MaterializedViewFlavor(BaseModel):
        id: str = Field(max_length=255)
        name: str = Field(max_length=255)

    view_definition = (
        f'SELECT "id", "name" FROM "{table_name}"'
        if database.dialect == "oracle"
        else f"SELECT id, name FROM {table_name}"
    )
    db.view(view_name, view_definition, materialized=True)
    target = db.migrations.snapshot()
    plan = db.migrations.generate_plan(SchemaSnapshot.empty(), target)
    applied = False

    try:
        for statement in reversed(plan.rollback_sql()):
            try:
                runtime.execute_native(url, statement, [])
            except Exception:
                pass
        assert (
            await db.migrations.apply(
                revision,
                plan,
                allow_destructive=True,
                checksum=f"checksum-{revision}",
                description=f"{database.dialect} materialized view definition drift",
            )
            is True
        )
        applied = True

        live = db.migrations.live_snapshot(include_tables=[table_name, view_name])
        live_view = next(view for view in live.views if view.name == view_name)
        assert live_view.materialized is True
        if database.dialect == "postgresql":
            assert live_view.definition != target.views[0].definition
        assert db.migrations.generate_plan(live, target).dry_run() == []
        assert db.migrations.diff(live, target).summary() == []
    finally:
        if applied:
            try:
                await db.migrations.rollback(
                    revision,
                    plan,
                    allow_destructive=True,
                )
            except Exception:
                pass
        for statement in plan.rollback_sql():
            try:
                runtime.execute_native(url, statement, [])
            except Exception:
                pass


@pytest.mark.parametrize(
    "database",
    CONSTRAINT_COMMENT_DATABASES,
    ids=[database.dialect for database in CONSTRAINT_COMMENT_DATABASES],
)
@pytest.mark.asyncio
async def test_external_constraint_comments_do_not_autogenerate_drift(
    database: MigrationSmokeDatabase,
) -> None:
    url = database.url
    if not url:
        pytest.skip(f"{database.env_var} not configured")

    runtime = pytest.importorskip("ormdantic._ormdantic")
    run_id = os.getpid()
    parent_name = f"occp_{database.dialect}_{run_id}"
    child_name = f"occc_{database.dialect}_{run_id}"
    unique_name = f"{parent_name}_code_region_uq"
    check_name = f"{parent_name}_rating_ck"
    fk_name = f"{child_name}_parent_fk"
    revision = f"constraint_comment_{database.dialect}_{run_id}"
    unique_comment = f"{database.dialect} unique metadata"
    check_comment = f"{database.dialect} check metadata"
    fk_comment = f"{database.dialect} fk metadata"
    db = Ormdantic(url)
    await db.migrations.repair(clear_dirty=True)

    @db.table(
        parent_name,
        pk="id",
        unique_constraints=[
            TableUnique(
                name=unique_name,
                columns=["code", "region"],
                comment=unique_comment,
            )
        ],
        check_constraints=[
            TableCheck(name=check_name, expression="rating >= 0", comment=check_comment)
        ],
    )
    class ConstraintCommentParent(BaseModel):
        id: str = Field(max_length=255)
        code: str = Field(max_length=255)
        region: str = Field(max_length=255)
        rating: int

    @db.table(
        child_name,
        pk="id",
        foreign_key_constraints=[
            TableForeignKey(
                name=fk_name,
                columns=["parent_code", "parent_region"],
                foreign_table=parent_name,
                foreign_columns=["code", "region"],
                on_delete="cascade",
                comment=fk_comment,
            )
        ],
    )
    class ConstraintCommentChild(BaseModel):
        id: str = Field(max_length=255)
        parent_code: str = Field(max_length=255)
        parent_region: str = Field(max_length=255)

    target = db.migrations.snapshot()
    plan = db.migrations.generate_plan(SchemaSnapshot.empty(), target)
    applied = False

    try:
        for statement in reversed(plan.rollback_sql()):
            try:
                runtime.execute_native(url, statement, [])
            except Exception:
                pass
        assert (
            await db.migrations.apply(
                revision,
                plan,
                allow_destructive=True,
                checksum=f"checksum-{revision}",
                description=f"{database.dialect} constraint comment drift",
            )
            is True
        )
        applied = True

        live = db.migrations.live_snapshot(include_tables=[parent_name, child_name])
        live_tables = {table.name: table for table in live.tables}
        live_parent = live_tables[parent_name]
        live_child = live_tables[child_name]
        reflected_unique = next(
            constraint
            for constraint in live_parent.named_unique_constraints
            if constraint.name == unique_name
        )
        reflected_check = next(
            constraint
            for constraint in live_parent.check_constraints
            if constraint.name == check_name
        )
        reflected_fk = next(
            constraint
            for constraint in live_child.foreign_key_constraints
            if constraint.name == fk_name
        )
        assert reflected_unique.comment == unique_comment
        assert reflected_check.comment == check_comment
        assert reflected_fk.comment == fk_comment

        assert db.migrations.generate_plan(live, target).dry_run() == []
        assert db.migrations.diff(live, target).summary() == []
    finally:
        if applied:
            try:
                await db.migrations.rollback(
                    revision,
                    plan,
                    allow_destructive=True,
                )
            except Exception:
                pass
        for statement in plan.rollback_sql():
            try:
                runtime.execute_native(url, statement, [])
            except Exception:
                pass


@pytest.mark.parametrize(
    "database",
    INDEX_COMMENT_DATABASES,
    ids=[database.dialect for database in INDEX_COMMENT_DATABASES],
)
@pytest.mark.asyncio
async def test_external_index_comments_do_not_autogenerate_drift(
    database: MigrationSmokeDatabase,
) -> None:
    url = database.url
    if not url:
        pytest.skip(f"{database.env_var} not configured")

    runtime = pytest.importorskip("ormdantic._ormdantic")
    run_id = os.getpid()
    table_name = f"orm_idx_comment_{database.dialect}_{run_id}"
    index_name = f"{table_name}_name_idx"
    revision = f"idx_comment_{database.dialect}_{run_id}"
    comment = f"{database.dialect} index metadata"
    db = Ormdantic(url)
    await db.migrations.repair(clear_dirty=True)

    @db.table(
        table_name,
        pk="id",
        indexes=[TableIndex(name=index_name, columns=["name"], comment=comment)],
    )
    class IndexedFlavor(BaseModel):
        id: str = Field(max_length=255)
        name: str = Field(max_length=255)

    target = db.migrations.snapshot()
    plan = db.migrations.generate_plan(SchemaSnapshot.empty(), target)
    applied = False

    try:
        for statement in reversed(plan.rollback_sql()):
            try:
                runtime.execute_native(url, statement, [])
            except Exception:
                pass
        assert (
            await db.migrations.apply(
                revision,
                plan,
                allow_destructive=True,
                checksum=f"checksum-{revision}",
                description=f"{database.dialect} index comment drift",
            )
            is True
        )
        applied = True

        live = db.migrations.live_snapshot(include_tables=[table_name])
        live_table = next(table for table in live.tables if table.name == table_name)
        reflected = next(
            index for index in live_table.indexes if index.name == index_name
        )
        assert reflected.comment == comment

        assert db.migrations.generate_plan(live, target).dry_run() == []
        assert db.migrations.diff(live, target).summary() == []
    finally:
        if applied:
            try:
                await db.migrations.rollback(
                    revision,
                    plan,
                    allow_destructive=True,
                )
            except Exception:
                pass
        for statement in plan.rollback_sql():
            try:
                runtime.execute_native(url, statement, [])
            except Exception:
                pass


@pytest.mark.parametrize(
    "database",
    ENUM_COMMENT_DATABASES,
    ids=[database.dialect for database in ENUM_COMMENT_DATABASES],
)
@pytest.mark.asyncio
async def test_external_enum_type_comments_do_not_autogenerate_drift(
    database: MigrationSmokeDatabase,
) -> None:
    url = database.url
    if not url:
        pytest.skip(f"{database.env_var} not configured")

    runtime = pytest.importorskip("ormdantic._ormdantic")
    run_id = os.getpid()
    enum_name = f"orm_enum_comment_{run_id}"
    revision = f"enum_comment_{database.dialect}_{run_id}"
    comment = "postgresql enum metadata"
    db = Ormdantic(url)
    await db.migrations.repair(clear_dirty=True)
    target = SchemaSnapshot(
        enum_types=[
            EnumTypeSnapshot(
                enum_name,
                ["mocha", "latte"],
                comment=comment,
            )
        ]
    )
    plan = db.migrations.generate_plan(SchemaSnapshot.empty(), target)
    applied = False

    try:
        for statement in reversed(plan.rollback_sql()):
            try:
                runtime.execute_native(url, statement, [])
            except Exception:
                pass
        assert (
            await db.migrations.apply(
                revision,
                plan,
                allow_destructive=True,
                checksum=f"checksum-{revision}",
                description="postgresql enum type comment drift",
            )
            is True
        )
        applied = True

        live = db.migrations.live_snapshot()
        matches = [
            enum_type for enum_type in live.enum_types if enum_type.name == enum_name
        ]
        assert len(matches) == 1
        reflected = matches[0]
        assert reflected.comment == comment

        before = SchemaSnapshot(enum_types=[reflected])
        assert db.migrations.generate_plan(before, target).dry_run() == []
        assert db.migrations.diff(before, target).summary() == []
    finally:
        if applied:
            try:
                await db.migrations.rollback(
                    revision,
                    plan,
                    allow_destructive=True,
                )
            except Exception:
                pass
        for statement in plan.rollback_sql():
            try:
                runtime.execute_native(url, statement, [])
            except Exception:
                pass


@pytest.mark.parametrize(
    "database",
    NAMESPACE_COMMENT_DATABASES,
    ids=[database.dialect for database in NAMESPACE_COMMENT_DATABASES],
)
@pytest.mark.asyncio
async def test_external_namespace_comments_do_not_autogenerate_drift(
    database: MigrationSmokeDatabase,
) -> None:
    url = database.url
    if not url:
        pytest.skip(f"{database.env_var} not configured")

    runtime = pytest.importorskip("ormdantic._ormdantic")
    run_id = os.getpid()
    namespace_name = f"orm_ns_comment_{database.dialect}_{run_id}"
    revision = f"ns_comment_{database.dialect}_{run_id}"
    comment = f"{database.dialect} namespace metadata"
    db = Ormdantic(url)
    await db.migrations.repair(clear_dirty=True)
    db.namespace(namespace_name, comment=comment)
    target = db.migrations.snapshot()
    plan = db.migrations.generate_plan(SchemaSnapshot.empty(), target)
    applied = False

    try:
        for statement in reversed(plan.rollback_sql()):
            try:
                runtime.execute_native(url, statement, [])
            except Exception:
                pass
        assert (
            await db.migrations.apply(
                revision,
                plan,
                allow_destructive=True,
                checksum=f"checksum-{revision}",
                description=f"{database.dialect} namespace comment drift",
            )
            is True
        )
        applied = True

        live = db.migrations.live_snapshot(schema=namespace_name)
        matches = [
            namespace
            for namespace in live.namespaces
            if namespace.name == namespace_name
        ]
        assert len(matches) == 1
        reflected = matches[0]
        assert reflected.comment == comment

        before = SchemaSnapshot(namespaces=[reflected])
        after = SchemaSnapshot(namespaces=target.namespaces)
        assert db.migrations.generate_plan(before, after).dry_run() == []
        assert db.migrations.diff(before, after).summary() == []
    finally:
        if applied:
            try:
                await db.migrations.rollback(
                    revision,
                    plan,
                    allow_destructive=True,
                )
            except Exception:
                pass
        for statement in plan.rollback_sql():
            try:
                runtime.execute_native(url, statement, [])
            except Exception:
                pass


@pytest.mark.parametrize(
    "database",
    SEQUENCE_COMMENT_DATABASES,
    ids=[database.dialect for database in SEQUENCE_COMMENT_DATABASES],
)
@pytest.mark.asyncio
async def test_external_sequence_comments_do_not_autogenerate_drift(
    database: MigrationSmokeDatabase,
) -> None:
    url = database.url
    if not url:
        pytest.skip(f"{database.env_var} not configured")

    runtime = pytest.importorskip("ormdantic._ormdantic")
    run_id = os.getpid()
    sequence_name = f"orm_seq_comment_{database.dialect}_{run_id}"
    revision = f"seq_comment_{database.dialect}_{run_id}"
    comment = f"{database.dialect} sequence metadata"
    db = Ormdantic(url)
    await db.migrations.repair(clear_dirty=True)
    db.sequence(
        sequence_name,
        data_type="bigint",
        start=1,
        increment=1,
        min_value=1,
        max_value=9223372036854775807,
        cache=2,
        comment=comment,
    )
    target = db.migrations.snapshot()
    plan = db.migrations.generate_plan(SchemaSnapshot.empty(), target)
    applied = False

    try:
        for statement in reversed(plan.rollback_sql()):
            try:
                runtime.execute_native(url, statement, [])
            except Exception:
                pass
        assert (
            await db.migrations.apply(
                revision,
                plan,
                allow_destructive=True,
                checksum=f"checksum-{revision}",
                description=f"{database.dialect} sequence comment drift",
            )
            is True
        )
        applied = True

        live = db.migrations.live_snapshot()
        matches = [
            sequence for sequence in live.sequences if sequence.name == sequence_name
        ]
        assert len(matches) == 1
        reflected = matches[0]
        assert reflected.comment == comment

        before = SchemaSnapshot(sequences=[reflected])
        after = SchemaSnapshot(sequences=target.sequences)
        assert db.migrations.generate_plan(before, after).dry_run() == []
        assert db.migrations.diff(before, after).summary() == []
    finally:
        if applied:
            try:
                await db.migrations.rollback(
                    revision,
                    plan,
                    allow_destructive=True,
                )
            except Exception:
                pass
        for statement in plan.rollback_sql():
            try:
                runtime.execute_native(url, statement, [])
            except Exception:
                pass


@pytest.mark.parametrize(
    "database",
    NO_BOUND_SEQUENCE_DATABASES,
    ids=[database.dialect for database in NO_BOUND_SEQUENCE_DATABASES],
)
@pytest.mark.asyncio
async def test_external_sequence_no_bound_defaults_do_not_autogenerate_drift(
    database: MigrationSmokeDatabase,
) -> None:
    url = database.url
    if not url:
        pytest.skip(f"{database.env_var} not configured")

    runtime = pytest.importorskip("ormdantic._ormdantic")
    run_id = os.getpid()
    sequence_name = f"orm_nb_seq_{database.dialect}_{run_id}"
    revision = f"no_bound_seq_{database.dialect}_{run_id}"
    db = Ormdantic(url)
    await db.migrations.repair(clear_dirty=True)
    db.sequence(
        sequence_name,
        data_type=None if database.dialect == "oracle" else "bigint",
        no_min_value=True,
        no_max_value=True,
    )
    target = db.migrations.snapshot()
    plan = db.migrations.generate_plan(SchemaSnapshot.empty(), target)
    applied = False

    try:
        assert (
            await db.migrations.apply(
                revision,
                plan,
                allow_destructive=True,
                checksum=f"checksum-{revision}",
                description=f"{database.dialect} no-bound sequence",
            )
            is True
        )
        applied = True

        live = db.migrations.live_snapshot(
            include_tables=(
                None if database.dialect == "postgresql" else [f"{sequence_name}_scope"]
            )
        )
        matches = [
            sequence for sequence in live.sequences if sequence.name == sequence_name
        ]
        assert len(matches) == 1
        reflected = matches[0]
        assert reflected.min_value is not None
        assert reflected.max_value is not None

        no_bound_target = replace(
            reflected,
            min_value=None,
            max_value=None,
            no_min_value=True,
            no_max_value=True,
        )
        before = SchemaSnapshot(sequences=[reflected])
        after = SchemaSnapshot(sequences=[no_bound_target])

        assert db.migrations.diff(before, after).summary() == []
        assert db.migrations.generate_plan(before, after).dry_run() == []
    finally:
        if applied:
            try:
                await db.migrations.rollback(
                    revision,
                    plan,
                    allow_destructive=True,
                )
            except Exception:
                pass
        for statement in plan.rollback_sql():
            try:
                runtime.execute_native(url, statement, [])
            except Exception:
                pass


@pytest.mark.parametrize(
    "database",
    NO_BOUND_IDENTITY_DATABASES,
    ids=[database.dialect for database in NO_BOUND_IDENTITY_DATABASES],
)
@pytest.mark.asyncio
async def test_external_identity_no_bound_defaults_do_not_autogenerate_drift(
    database: MigrationSmokeDatabase,
) -> None:
    url = database.url
    if not url:
        pytest.skip(f"{database.env_var} not configured")

    runtime = pytest.importorskip("ormdantic._ormdantic")
    run_id = os.getpid()
    table_name = f"orm_nb_ident_{database.dialect}_{run_id}"
    revision = f"no_bound_ident_{database.dialect}_{run_id}"
    db = Ormdantic(url)
    await db.migrations.repair(clear_dirty=True)
    plan: MigrationPlan | None = None
    applied = False

    try:
        if database.dialect == "oracle":
            try:
                runtime.execute_native(url, f'DROP TABLE "{table_name}"', [])
            except Exception:
                pass
            runtime.execute_native(
                url,
                (
                    f'CREATE TABLE "{table_name}" ('
                    '"id" NUMBER GENERATED BY DEFAULT AS IDENTITY '
                    "(NOMINVALUE NOMAXVALUE) PRIMARY KEY, "
                    '"name" VARCHAR2(255) NOT NULL)'
                ),
                [],
            )
            applied = True
        else:

            @db.table(
                table_name,
                pk="id",
                column_options={
                    "id": TableColumn(
                        identity=True,
                        identity_no_min_value=True,
                        identity_no_max_value=True,
                    )
                },
            )
            class NoBoundIdentityFlavor(BaseModel):
                id: int
                name: str

            target = db.migrations.snapshot()
            plan = db.migrations.generate_plan(SchemaSnapshot.empty(), target)
            assert (
                await db.migrations.apply(
                    revision,
                    plan,
                    allow_destructive=True,
                    checksum=f"checksum-{revision}",
                    description=f"{database.dialect} no-bound identity",
                )
                is True
            )
            applied = True

        live = db.migrations.live_snapshot(include_tables=[table_name])
        assert len(live.tables) == 1
        reflected_table = live.tables[0]
        reflected_columns = []
        for column in reflected_table.columns:
            if column.name.lower() != "id":
                reflected_columns.append(column)
                continue
            assert column.identity is True
            reflected_columns.append(
                replace(
                    column,
                    identity_min_value=None,
                    identity_max_value=None,
                    identity_no_min_value=True,
                    identity_no_max_value=True,
                )
            )
        no_bound_target = replace(reflected_table, columns=reflected_columns)
        before = SchemaSnapshot(tables=[reflected_table])
        after = SchemaSnapshot(tables=[no_bound_target])

        assert db.migrations.diff(before, after).summary() == []
        assert db.migrations.generate_plan(before, after).dry_run() == []
    finally:
        if database.dialect == "oracle":
            try:
                runtime.execute_native(url, f'DROP TABLE "{table_name}"', [])
            except Exception:
                pass
        else:
            if applied and plan is not None:
                try:
                    await db.migrations.rollback(
                        revision,
                        plan,
                        allow_destructive=True,
                    )
                except Exception:
                    pass
            if plan is not None:
                for statement in plan.rollback_sql():
                    try:
                        runtime.execute_native(url, statement, [])
                    except Exception:
                        pass
