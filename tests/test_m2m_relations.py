from __future__ import annotations

import asyncio
import unittest
from uuid import UUID, uuid4

from decouple import config
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import create_async_engine

from ormdantic import Ormdantic

URL = config("DATABASE_URL")

engine = create_async_engine(URL)
db = Ormdantic(engine)


@db.table(pk="id", back_references={"many": "many", "many_two": "many_two"})
class ManyToManyA(BaseModel):
    """Has many-to-many relationship with ManyToManyB."""

    id: UUID = Field(default_factory=uuid4)
    many: list[ManyToManyB] | None = None
    many_two: list[ManyToManyB] | None = None
    value: str | None = None


@db.table(pk="id", back_references={"many": "many", "many_two": "many_two"})
class ManyToManyB(BaseModel):
    """Has many-to-many relationship with ManyToManyA."""

    id: UUID = Field(default_factory=uuid4)
    many: list[ManyToManyA]
    many_two: list[ManyToManyA] | None = None


@db.table(pk="id", back_references={"many": "many", "many_two": "many_two"})
class ManyToSelf(BaseModel):
    """Has many-to-many relationship with self."""

    id: UUID = Field(default_factory=uuid4)
    many: list[ManyToSelf] | None = None
    many_two: list[ManyToSelf] | None = None


ManyToManyA.update_forward_refs()
ManyToManyB.update_forward_refs()
ManyToSelf.update_forward_refs()


class PyDBManyRelationsTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        """Setup clean sqlite database."""

        async def _init() -> None:
            await db.init()
            async with engine.begin() as conn:
                await conn.run_sync(db.metadata.drop_all)
                await conn.run_sync(db.metadata.create_all)

        asyncio.run(_init())

    async def test_many_to_many_insert_and_get(self) -> None:
        many_a = [ManyToManyA(), ManyToManyA()]
        many_b = ManyToManyB(many=many_a)
        await db[ManyToManyB].insert(many_b)
        find_b = await db[ManyToManyB].find_one(many_b.id, depth=1)
        source_b_dict = many_b.dict()
        find_b_dict = find_b.dict()
        source_b_dict["many"].sort(key=lambda it: it["id"])
        find_b_dict["many"].sort(key=lambda it: it["id"])
        self.assertEqual(source_b_dict["id"], find_b_dict["id"])
        self.assertListEqual(source_b_dict["many"], find_b_dict["many"])
        find_a = await db[ManyToManyA].find_one(many_a[0].id, depth=2)
        self.assertDictEqual(find_a.many[0].dict(), find_b.dict())

    async def test_many_to_many_update(self) -> None:
        many_a = [ManyToManyA(value="coffee"), ManyToManyA(value="caramel")]
        many_b = ManyToManyB(many=many_a)
        await db[ManyToManyB].insert(many_b)
        many_b.many[0].value = "mocha"
        await db[ManyToManyB].update(many_b)
        find_b = await db[ManyToManyB].find_one(many_b.id, depth=1)
        flavors = [it.value for it in find_b.many]
        flavors.sort()
        self.assertListEqual(["caramel", "mocha"], flavors)
