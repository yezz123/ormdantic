from __future__ import annotations

import asyncio
import unittest
from typing import Callable
from uuid import UUID, uuid4

import pytest
from decouple import config
from pydantic import BaseModel, Field
from sqlalchemy import MetaData

from ormdantic import Ormdantic
from ormdantic.handler import (
    MismatchingBackReferenceError,
    MustUnionForeignKeyError,
    TypeConversionError,
    UndefinedBackReferenceError,
)

URL = config("DATABASE_URL")

connection = URL
db_1 = Ormdantic(connection)
db_2 = Ormdantic(connection)
db_3 = Ormdantic(connection)
db_4 = Ormdantic(connection)
db_5 = Ormdantic(connection)


@db_1.table(pk="id")
class UndefinedBackreference(BaseModel):
    """Missing explicit back-reference to raise exception."""

    id: UUID = Field(default_factory=uuid4)
    self_ref: list[UndefinedBackreference | UUID] | None


@db_2.table(pk="id", back_references={"other": "other"})
class MismatchedBackreferenceA(BaseModel):
    """Type of back-reference for "other" is not this model."""

    id: UUID = Field(default_factory=uuid4)
    other: list[MismatchedBackreferenceB] | None


@db_2.table(pk="id", back_references={"other": "other"})
class MismatchedBackreferenceB(BaseModel):
    """Type of back-reference for "other" is this model."""

    id: UUID = Field(default_factory=uuid4)
    other: list[MismatchedBackreferenceB] | None


@db_3.table(pk="id")
class Table_1(BaseModel):
    """A table."""

    id: UUID = Field(default_factory=uuid4)


@db_3.table(pk="id")
class Table_2(BaseModel):
    """Another table."""

    id: UUID = Field(default_factory=uuid4)
    table: Table_1


@db_4.table(pk="id")
class Table_3(BaseModel):
    """Another table."""

    id: UUID = Field(default_factory=uuid4)


@db_4.table(pk="id")
class Table_4(BaseModel):
    """Another table."""

    id: UUID = Field(default_factory=uuid4)
    table: Table_3 | int


@db_5.table(pk="id")
class Table_5(BaseModel):
    """Another table."""

    id: UUID = Field(default_factory=uuid4)
    table: Callable[[], int]


MismatchedBackreferenceA.update_forward_refs()
MismatchedBackreferenceB.update_forward_refs()
UndefinedBackreference.update_forward_refs()


class ormdanticErrorTesting(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        """Setup clean sqlite database."""

        async def _init(db: Ormdantic) -> None:
            metadata = MetaData()
            async with db._engine.begin() as conn:
                await conn.run_sync(metadata.drop_all)

        asyncio.run(_init(db_1))
        asyncio.run(_init(db_2))
        asyncio.run(_init(db_3))
        asyncio.run(_init(db_4))
        asyncio.run(_init(db_5))

    @staticmethod
    async def test_undefined_back_reference() -> None:
        with pytest.raises(UndefinedBackReferenceError) as e:
            await db_1.init()
        assert e.value.args[0] == (
            'Many relation defined on "undefined_backreference.self_ref" to table undefined_backreference" must be defined with a back reference on "undefined_backreference".'
        )

    @staticmethod
    async def test_mismatched_back_reference() -> None:
        with pytest.raises(MismatchingBackReferenceError) as e:
            await db_2.init()
        assert (
            e.value.args[0]
            == 'Many relation defined on "mismatched_backreference_a.other" to'
            ' "mismatched_backreference_b.other" must use the same model type'
            " back-referenced."
        )

    @staticmethod
    async def test_missing_foreign_key_union() -> None:
        with pytest.raises(MustUnionForeignKeyError) as e:
            await db_3.init()
        assert (
            e.value.args[0]
            == 'Relation defined on "table_2.table" to "table_1" must be a union type of "Model |'
            ' model_pk_type" e.g. "Table_1 | UUID"'
        )

    @staticmethod
    async def test_missing_wrong_pk_type() -> None:
        with pytest.raises(MustUnionForeignKeyError) as e:
            await db_4.init()
        assert (
            e.value.args[0]
            == 'Relation defined on "table_4.table" to "table_3" must be a union type of "Model |'
            ' model_pk_type" e.g. "Table_3 | UUID"'
        )

    @staticmethod
    async def test_conversion_type_error() -> None:
        with pytest.raises(TypeConversionError) as e:
            await db_5.init()
        assert (
            e.value.args[0]
            == "Type typing.Callable[[], int] is not supported by SQLAlchemy 1.4.42."
        )
