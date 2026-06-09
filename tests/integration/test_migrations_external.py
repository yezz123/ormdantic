from __future__ import annotations

import os
from dataclasses import dataclass

import pytest
from pydantic import BaseModel

from ormdantic import Ormdantic
from ormdantic.migrations import MigrationArtifact, MigrationOperation, MigrationPlan


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
            "CREATE TABLE {parent} (id VARCHAR(255) PRIMARY KEY, code VARCHAR(255) UNIQUE, name VARCHAR(255) NOT NULL)",
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
            "CREATE TABLE {parent} (id VARCHAR(255) PRIMARY KEY, code VARCHAR(255) UNIQUE, name VARCHAR(255) NOT NULL)",
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
            'CREATE INDEX "{index}" ON "{parent}" ("name")',
            'CREATE TABLE "{child}" ("id" VARCHAR2(255) PRIMARY KEY, "parent_id" VARCHAR2(255) NOT NULL, FOREIGN KEY ("parent_id") REFERENCES "{parent}"("id"))',
        ),
        ('DROP TABLE "{child}"', 'DROP TABLE "{parent}"'),
    ),
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
        assert status["current"] == revision
        history = await db.migrations.history()
        assert history[-1].revision == revision
        assert history[-1].checksum == checksum
        assert history[-1].status == "applied"
        assert history[-1].dirty is False
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
        id: str
        name: str
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
