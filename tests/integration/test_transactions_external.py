from __future__ import annotations

import os
from dataclasses import dataclass

import pytest
from pydantic import BaseModel

from ormdantic import Ormdantic


@dataclass(frozen=True)
class TransactionDatabase:
    dialect: str
    env_var: str
    suffix: str

    @property
    def url(self) -> str | None:
        return os.getenv(self.env_var) or os.getenv(
            self.env_var.replace("ORMDANTIC_TEST_", "ORMDANTIC_")
        )


TRANSACTION_DATABASES = [
    TransactionDatabase("postgresql", "ORMDANTIC_TEST_POSTGRES_URL", "pg"),
    TransactionDatabase("mysql", "ORMDANTIC_TEST_MYSQL_URL", "my"),
    TransactionDatabase("mariadb", "ORMDANTIC_TEST_MARIADB_URL", "ma"),
    TransactionDatabase("mssql", "ORMDANTIC_TEST_MSSQL_URL", "ms"),
    TransactionDatabase("oracle", "ORMDANTIC_TEST_ORACLE_URL", "or"),
]


@pytest.mark.parametrize(
    "database",
    TRANSACTION_DATABASES,
    ids=[database.dialect for database in TRANSACTION_DATABASES],
)
async def test_external_session_savepoint_rollback(
    database: TransactionDatabase,
) -> None:
    url = database.url
    if not url:
        pytest.skip(f"{database.env_var} not configured")

    table_name = f"ext_tx_{database.suffix}_{os.getpid() % 10000}"
    db = Ormdantic(url)

    @db.table(table_name, pk="id")
    class ExternalFlavor(BaseModel):
        id: str
        name: str

    await db.init()
    await db.drop_all()
    await db.create_all()
    try:
        async with db.session() as session:
            session.add(ExternalFlavor(id="1", name="committed"))
            await session.flush()

            try:
                async with session.savepoint("optional_flavor"):
                    session.add(ExternalFlavor(id="2", name="rolled back"))
                    await session.flush()
                    raise RuntimeError("rollback savepoint")
            except RuntimeError:
                pass

        assert await db[ExternalFlavor].count() == 1
        assert await db[ExternalFlavor].find_one("1") is not None
        assert await db[ExternalFlavor].find_one("2") is None
    finally:
        await db.drop_all()
