from __future__ import annotations

from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field

from ormdantic import Ormdantic
from ormdantic.generator._rust_schema import compile_create_table_sql


class DdlFlavor(Enum):
    MOCHA = "mocha"


def test_compile_create_table_sql_includes_types_indexes_and_checks() -> None:
    db = Ormdantic("sqlite:///:memory:")

    @db.table(
        pk="id",
        indexed=["name"],
        unique=["code"],
        unique_constraints=[["name", "code"]],
    )
    class Flavor(BaseModel):
        id: str
        name: str = Field(min_length=2, max_length=63)
        code: bytes
        price: Decimal = Field(gt=0)
        flavor: DdlFlavor

    table = db._table_map.name_to_data["flavor"]
    statements = compile_create_table_sql(db._table_map, table.tablename, "sqlite")

    assert statements[0] == (
        'CREATE TABLE IF NOT EXISTS "flavor" ('
        '"id" TEXT PRIMARY KEY NOT NULL, '
        '"name" VARCHAR(63) NOT NULL CHECK (LENGTH(name) >= 2) CHECK (LENGTH(name) <= 63), '
        '"code" BLOB NOT NULL UNIQUE, '
        '"price" NUMERIC NOT NULL CHECK (price > 0), '
        '"flavor" TEXT NOT NULL, '
        'UNIQUE ("name", "code"))'
    )
    assert (
        'CREATE INDEX IF NOT EXISTS "flavor_name_idx" ON "flavor" ("name")'
        in statements
    )
    assert (
        'CREATE UNIQUE INDEX IF NOT EXISTS "flavor_code_unique_idx" ON "flavor" ("code")'
        in statements
    )
