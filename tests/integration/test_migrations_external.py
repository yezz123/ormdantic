from __future__ import annotations

import os

import pytest

from ormdantic import Ormdantic
from ormdantic.migrations import MigrationOperation, MigrationPlan


@pytest.mark.parametrize(
    "env_var",
    [
        "ORMDANTIC_TEST_POSTGRES_URL",
        "ORMDANTIC_TEST_MYSQL_URL",
        "ORMDANTIC_TEST_MARIADB_URL",
    ],
)
@pytest.mark.asyncio
async def test_external_migration_execution_smoke(env_var: str) -> None:
    url = os.getenv(env_var)
    if not url:
        pytest.skip(f"{env_var} not configured")

    db = Ormdantic(url)
    table_name = "ormdantic_migration_smoke"
    plan = MigrationPlan(
        operations=[
            MigrationOperation(
                f"CREATE TABLE {table_name} (id VARCHAR(255) PRIMARY KEY)"
            )
        ],
        rollback_operations=[MigrationOperation(f"DROP TABLE {table_name}")],
    )
    revision = f"smoke_{env_var.lower()}"
    try:
        await db.migrations.apply(revision, plan, allow_destructive=True)
        assert revision in await db.migrations.applied_revisions()
        await db.migrations.rollback(revision, plan, allow_destructive=True)
    finally:
        try:
            import importlib

            runtime = importlib.import_module("ormdantic._ormdantic")
            runtime.execute_native(url, f"DROP TABLE IF EXISTS {table_name}", [])
        except Exception:
            pass
