from __future__ import annotations

import asyncio
import unittest
from typing import Callable
from uuid import UUID, uuid4

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

    async def test_undefined_back_reference(self) -> None:
        correct_error = False
        try:
            await db_1.init()
        except UndefinedBackReferenceError:
            correct_error = True
        self.assertTrue(correct_error)

    async def test_mismatched_back_reference(self) -> None:
        correct_error = False
        try:
            await db_2.init()
        except MismatchingBackReferenceError:
            correct_error = True
        self.assertTrue(correct_error)

    async def test_missing_foreign_key_union(self) -> None:
        correct_error = False
        try:
            await db_3.init()
        except MustUnionForeignKeyError:
            correct_error = True
        self.assertTrue(correct_error)

    async def test_missing_wrong_pk_type(self) -> None:
        correct_error = False
        try:
            await db_4.init()
        except MustUnionForeignKeyError:
            correct_error = True
        self.assertTrue(correct_error)

    async def test_conversion_type_error(self) -> None:
        correct_error = False
        try:
            await db_5.init()
        except TypeConversionError:
            correct_error = True
        self.assertTrue(correct_error)
