from __future__ import annotations

import os
from contextlib import suppress
from dataclasses import dataclass
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import BaseModel, Field

from ormdantic import Ormdantic


@dataclass(frozen=True)
class ExternalValueFidelityDatabase:
    dialect: str
    env_var: str
    sql: str
    expected_decimal: Decimal
    expected_unsigned: int | None = None

    @property
    def url(self) -> str | None:
        return os.getenv(self.env_var) or os.getenv(
            self.env_var.replace("ORMDANTIC_TEST_", "ORMDANTIC_")
        )


EXTERNAL_VALUE_DATABASES = [
    ExternalValueFidelityDatabase(
        "postgresql",
        "ORMDANTIC_TEST_POSTGRES_URL",
        "SELECT CAST(123456789012345.123456789 AS NUMERIC(24,9)) AS decimal_value",
        Decimal("123456789012345.123456789"),
    ),
    ExternalValueFidelityDatabase(
        "mysql",
        "ORMDANTIC_TEST_MYSQL_URL",
        "SELECT CAST(123456789012345.123456789 AS DECIMAL(24,9)) AS decimal_value, "
        "CAST(18446744073709551615 AS UNSIGNED) AS unsigned_value",
        Decimal("123456789012345.123456789"),
        2**64 - 1,
    ),
    ExternalValueFidelityDatabase(
        "mariadb",
        "ORMDANTIC_TEST_MARIADB_URL",
        "SELECT CAST(123456789012345.123456789 AS DECIMAL(24,9)) AS decimal_value, "
        "CAST(18446744073709551615 AS UNSIGNED) AS unsigned_value",
        Decimal("123456789012345.123456789"),
        2**64 - 1,
    ),
    ExternalValueFidelityDatabase(
        "mssql",
        "ORMDANTIC_TEST_MSSQL_URL",
        "SELECT CAST(123456789012345.123456789 AS DECIMAL(24,9)) AS decimal_value",
        Decimal("123456789012345.123456789"),
    ),
    ExternalValueFidelityDatabase(
        "oracle",
        "ORMDANTIC_TEST_ORACLE_URL",
        "SELECT CAST(123456789012345.123456789 AS NUMBER(24,9)) AS decimal_value "
        "FROM dual",
        Decimal("123456789012345.123456789"),
    ),
]


async def test_sqlite_large_unsigned_integer_hydrates_strict_int_model(
    tmp_path,
) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'unsigned_hydration.sqlite3'}")
    unsigned = 2**64 - 1

    @db.table(pk="id")
    class Counter(BaseModel):
        id: str
        value: int = Field(strict=True)

    await db.init()
    await db.drop_all()
    await db.create_all()

    await db[Counter].insert(Counter(id="1", value=unsigned))

    stored = await db[Counter].find_one("1")

    assert stored is not None
    assert stored.value == unsigned
    assert isinstance(stored.value, int)


@pytest.mark.parametrize(
    "database",
    EXTERNAL_VALUE_DATABASES,
    ids=[database.dialect for database in EXTERNAL_VALUE_DATABASES],
)
def test_external_python_bridge_numeric_value_fidelity(
    database: ExternalValueFidelityDatabase,
) -> None:
    url = database.url
    if not url:
        pytest.skip(f"{database.env_var} not configured")

    runtime = pytest.importorskip("ormdantic._ormdantic")
    result = runtime.execute_native(url, database.sql, [])
    row = result["rows"][0]

    decimal_value = row[0]
    assert decimal_value == database.expected_decimal
    assert isinstance(decimal_value, Decimal)
    if database.expected_unsigned is not None:
        unsigned_value = row[1]
        assert unsigned_value == database.expected_unsigned
        assert isinstance(unsigned_value, int)


@pytest.mark.parametrize(
    "database",
    EXTERNAL_VALUE_DATABASES,
    ids=[database.dialect for database in EXTERNAL_VALUE_DATABASES],
)
@pytest.mark.asyncio
async def test_external_decimal_hydrates_model_without_precision_loss(
    database: ExternalValueFidelityDatabase,
) -> None:
    url = database.url
    if not url:
        pytest.skip(f"{database.env_var} not configured")

    table_name = f"orm_vd_{database.dialect[:3]}_{os.getpid()}_{uuid4().hex[:6]}"
    db = Ormdantic(url)

    @db.table(table_name, pk="id")
    class ExternalDecimalValue(BaseModel):
        id: int
        amount: Decimal = Field(max_digits=24, decimal_places=9)

    try:
        await db.init()
        await db[ExternalDecimalValue].insert(
            ExternalDecimalValue(id=1, amount=database.expected_decimal)
        )

        stored = await db[ExternalDecimalValue].find_one(1)

        assert stored is not None
        assert stored.amount == database.expected_decimal
        assert isinstance(stored.amount, Decimal)
    finally:
        with suppress(Exception):
            await db.drop_all()


@pytest.mark.parametrize(
    "database",
    [
        database
        for database in EXTERNAL_VALUE_DATABASES
        if database.expected_unsigned is not None
    ],
    ids=[
        database.dialect
        for database in EXTERNAL_VALUE_DATABASES
        if database.expected_unsigned is not None
    ],
)
@pytest.mark.asyncio
async def test_external_unsigned_integer_hydrates_strict_int_model(
    database: ExternalValueFidelityDatabase,
) -> None:
    url = database.url
    if not url:
        pytest.skip(f"{database.env_var} not configured")

    runtime = pytest.importorskip("ormdantic._ormdantic")
    table_name = f"orm_vu_{database.dialect[:3]}_{os.getpid()}_{uuid4().hex[:6]}"
    db = Ormdantic(url)
    assert database.expected_unsigned is not None

    @db.table(table_name, pk="id")
    class ExternalUnsignedValue(BaseModel):
        id: str
        value: int = Field(strict=True)

    try:
        runtime.execute_native(
            url,
            f"CREATE TABLE {table_name} "
            "(id VARCHAR(255) PRIMARY KEY, value BIGINT UNSIGNED NOT NULL)",
            [],
        )
        runtime.execute_native(
            url,
            f"INSERT INTO {table_name} (id, value) "
            f"VALUES ('1', {database.expected_unsigned})",
            [],
        )
        await db.init()

        stored = await db[ExternalUnsignedValue].find_one("1")

        assert stored is not None
        assert stored.value == database.expected_unsigned
        assert isinstance(stored.value, int)
    finally:
        with suppress(Exception):
            runtime.execute_native(url, f"DROP TABLE IF EXISTS {table_name}", [])
