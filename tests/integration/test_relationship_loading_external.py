from __future__ import annotations

import os
from dataclasses import dataclass

import pytest

from tests.integration.relationship_stress import run_relationship_loader_stress


@dataclass(frozen=True)
class RelationshipStressDatabase:
    dialect: str
    env_var: str

    @property
    def url(self) -> str | None:
        return os.getenv(self.env_var) or os.getenv(
            self.env_var.replace("ORMDANTIC_TEST_", "ORMDANTIC_")
        )


STRESS_DATABASES = [
    RelationshipStressDatabase("postgresql", "ORMDANTIC_TEST_POSTGRES_URL"),
    RelationshipStressDatabase("mysql", "ORMDANTIC_TEST_MYSQL_URL"),
    RelationshipStressDatabase("mariadb", "ORMDANTIC_TEST_MARIADB_URL"),
    RelationshipStressDatabase("mssql", "ORMDANTIC_TEST_MSSQL_URL"),
    RelationshipStressDatabase("oracle", "ORMDANTIC_TEST_ORACLE_URL"),
]


@pytest.mark.parametrize(
    "database",
    STRESS_DATABASES,
    ids=[database.dialect for database in STRESS_DATABASES],
)
@pytest.mark.asyncio
async def test_external_relationship_loader_stress(
    database: RelationshipStressDatabase,
) -> None:
    url = database.url
    if not url:
        pytest.skip(f"{database.env_var} not configured")

    await run_relationship_loader_stress(
        url,
        suffix=f"{database.dialect}_{os.getpid()}",
    )
