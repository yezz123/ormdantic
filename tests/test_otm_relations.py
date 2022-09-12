from __future__ import annotations

import asyncio
import unittest
from uuid import UUID, uuid4

from decouple import config
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import create_async_engine

from ormdantic import Ormdantic

URL = config("DATABASE_URL")

connection = URL
database = Ormdantic(connection)


@database.table(pk="id", back_references={"many_a": "one_a", "many_b": "one_b"})
class One(BaseModel):
    """One will have many "Many"."""

    id: UUID = Field(default_factory=uuid4)
    many_a: list[Many] = Field(default_factory=lambda: [])
    many_b: list[Many] | None = None


@database.table(pk="id")
class Many(BaseModel):
    """Has a "One" parent and "Many" siblings."""

    id: UUID = Field(default_factory=uuid4)
    one_a: One | UUID
    one_b: One | UUID | None = None


One.update_forward_refs()
Many.update_forward_refs()


class ormdanticOneToManyRelationTesting(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        """Setup clean sqlite database."""

        async def _init() -> None:
            async with database._engine.begin() as conn:
                await database.init()
                await conn.run_sync(database._metadata.drop_all)  # type: ignore
                await conn.run_sync(database._metadata.create_all)  # type: ignore

        asyncio.run(_init())

    async def test_one_to_many_insert_and_get(self) -> None:
        one_a = One()
        one_b = One()
        await database[One].insert(one_a)
        await database[One].insert(one_b)
        many_a = [Many(one_a=one_a), Many(one_a=one_a)]
        many_b = [
            Many(one_a=one_a, one_b=one_b),
            Many(one_a=one_a, one_b=one_b),
            Many(one_a=one_a, one_b=one_b),
        ]
        for many in many_a + many_b:
            await database[Many].insert(many)
        find_one_a = await database[One].find_one(one_a.id, depth=2)
        many_a_plus_b = many_a + many_b
        many_a_plus_b.sort(key=lambda x: x.id)
        find_one_a.many_a.sort(key=lambda x: x.id)  # type: ignore
        self.assertListEqual(many_a_plus_b, find_one_a.many_a)  # type: ignore
        self.assertIsNone(find_one_a.many_b)  # type: ignore
        find_one_b = await database[One].find_one(one_b.id, depth=2)
        many_b.sort(key=lambda x: x.id)
        find_one_b.many_b.sort(key=lambda x: x.id)  # type: ignore
        self.assertListEqual(many_b, find_one_b.many_b)  # type: ignore
        self.assertListEqual([], find_one_b.many_a)  # type: ignore
        many_a_idx_zero = await database[Many].find_one(many_a[0].id, depth=3)
        many_a_idx_zero.one_a.many_a.sort(key=lambda x: x.id)  # type: ignore
        self.assertDictEqual(find_one_a.dict(), many_a_idx_zero.one_a.dict())  # type: ignore
