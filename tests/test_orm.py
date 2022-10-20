from __future__ import annotations

import asyncio
import unittest
from datetime import date, datetime
from typing import Any
from uuid import UUID, uuid4

from decouple import config
from pydantic import BaseModel, Field
from pypika import Order

from ormdantic import Ormdantic

URL = config("DATABASE_URL")

connection = URL
database = Ormdantic(connection)


class Money(BaseModel):
    """3 floating point numbers."""

    currency: float = 1.0
    val: float = 1.0


@database.table(
    "flavors",
    pk="id",
    indexed=["strength"],
    unique_constraints=[["name", "strength"]],
)
class Flavor(BaseModel):
    """A coffee flavor."""

    id: UUID = Field(default_factory=uuid4)
    name: str = Field(..., max_length=63)
    strength: int | None = None
    coffee: Coffee | UUID | None = None
    created_at: date = Field(default_factory=date.today)
    updated_at: date = Field(default_factory=date.today)
    expire: datetime = Field(default_factory=datetime.now)
    exist: bool = False


@database.table(pk="id")
class Coffee(BaseModel):
    """Drink it in the morning."""

    id: UUID = Field(default_factory=uuid4)
    primary_flavor: Flavor | UUID
    secondary_flavor: Flavor | UUID | None
    sweetener: str
    cream: float
    place: dict  # type: ignore
    ice: list  # type: ignore
    size: Money
    attributes: dict[str, Any] | None = None
    exist: bool = False


@database.table(pk="id")
class Table(BaseModel):
    """Drink it in the morning."""

    id: UUID = Field(default_factory=uuid4)


Flavor.update_forward_refs()


class ormdanticTesting(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        """Setup clean sqlite database."""

        async def _init() -> None:
            async with database._engine.begin() as conn:
                await database.init()
                await conn.run_sync(database._metadata.drop_all)  # type: ignore
                await conn.run_sync(database._metadata.create_all)  # type: ignore

        asyncio.run(_init())

    async def test_find_nothing(self) -> None:
        self.assertEqual(None, (await database[Flavor].find_one(uuid4())))
        self.assertEqual(None, (await database[Coffee].find_one(uuid4(), depth=3)))

    async def test_no_relation_insert_and_fine_one(self) -> None:
        # Insert record.
        record = Table()
        find = await database[Table].insert(record)
        # Find new record and compare.
        self.assertDictEqual(
            find.dict(), (await database[Table].find_one(find.id, 1)).dict()  # type: ignore
        )

    async def test_insert_and_find_one(self) -> None:
        # Insert record.
        flavor = Flavor(name="mocha")
        mocha = await database[Flavor].insert(flavor)
        # Find new record and compare.
        self.assertDictEqual(
            mocha.dict(), (await database[Flavor].find_one(mocha.id)).dict()  # type: ignore
        )

    async def test_insert_and_find_one_date(self) -> None:
        # Test Date and Time fields
        flavor = Flavor(name="mocha", created_at=date(2021, 1, 1))
        mocha = await database[Flavor].insert(flavor)
        # Find new record and compare.
        self.assertDictEqual(
            mocha.dict(), (await database[Flavor].find_one(mocha.id)).dict()  # type: ignore
        )

    async def test_insert_and_find_one_bool(self) -> None:
        # Insert record.
        flavor = Flavor(name="mocha", exist=True)
        mocha = await database[Flavor].insert(flavor)
        # Find new record and compare.
        self.assertDictEqual(
            mocha.dict(), (await database[Flavor].find_one(mocha.id)).dict()  # type: ignore
        )

    async def test_count(self) -> None:
        # Insert 3 records.
        await database[Flavor].insert(Flavor(name="mocha"))
        await database[Flavor].insert(Flavor(name="mocha"))
        await database[Flavor].insert(Flavor(name="caramel"))
        # Count records.
        self.assertEqual(1, await database[Flavor].count(where={"name": "caramel"}))
        self.assertEqual(3, await database[Flavor].count())

    async def test_find_many(self) -> None:
        # Insert 3 records.
        mocha1 = await database[Flavor].insert(Flavor(name="mocha"))
        mocha2 = await database[Flavor].insert(Flavor(name="mocha"))
        caramel = await database[Flavor].insert(Flavor(name="caramel"))
        # Find two records with filter.
        mochas = await database[Flavor].find_many(where={"name": "mocha"})
        self.assertListEqual([mocha1, mocha2], mochas.data)
        flavors = await database[Flavor].find_many()
        self.assertListEqual([mocha1, mocha2, caramel], flavors.data)

    async def test_find_many_order(self) -> None:
        # Insert 3 records.
        mocha1 = await database[Flavor].insert(Flavor(name="mocha", strength=3))
        mocha2 = await database[Flavor].insert(Flavor(name="mocha", strength=2))
        caramel = await database[Flavor].insert(Flavor(name="caramel"))
        flavors = await database[Flavor].find_many(
            order_by=["name", "strength"], order=Order.desc
        )
        self.assertListEqual([mocha1, mocha2, caramel], flavors.data)

    async def test_find_many_pagination(self) -> None:
        # Insert 4 records.
        mocha1 = await database[Flavor].insert(Flavor(name="mocha"))
        mocha2 = await database[Flavor].insert(Flavor(name="mocha"))
        vanilla = await database[Flavor].insert(Flavor(name="vanilla"))
        caramel = await database[Flavor].insert(Flavor(name="caramel"))
        flavors_page_1 = await database[Flavor].find_many(limit=2)
        self.assertListEqual([mocha1, mocha2], flavors_page_1.data)
        flavors_page_2 = await database[Flavor].find_many(limit=2, offset=2)
        self.assertListEqual([vanilla, caramel], flavors_page_2.data)

    async def test_update(self) -> None:
        # Insert record.
        flavor = await database[Flavor].insert(Flavor(name="mocha"))
        # Update record.
        flavor.name = "caramel"
        await database[Flavor].update(flavor)
        # Find the updated record.
        self.assertEqual(flavor.name, (await database[Flavor].find_one(flavor.id)).name)  # type: ignore

    async def test_update_datetime(self) -> None:
        # Insert record.
        flavor = await database[Flavor].insert(
            Flavor(name="mocha", expire=datetime(2021, 1, 1, 1, 1, 1))
        )
        # Update record.
        flavor.expire = datetime(2021, 1, 1, 1, 1, 2)
        await database[Flavor].update(flavor)
        # Find the updated record.
        self.assertEqual(flavor.expire, (await database[Flavor].find_one(flavor.id)).expire)  # type: ignore

    async def test_upsert(self) -> None:
        # Upsert record as insert.
        flavor = await database[Flavor].upsert(Flavor(name="vanilla"))
        await database[Flavor].upsert(flavor)
        # Find all "vanilla" record.
        flavors = await database[Flavor].find_many(where={"id": flavor.id})
        self.assertEqual(1, len(flavors.data))
        # Upsert as update.
        flavor.name = "caramel"
        await database[Flavor].upsert(flavor)
        # Find one record.
        flavors = await database[Flavor].find_many(where={"id": flavor.id})
        self.assertEqual(1, len(flavors.data))
        self.assertDictEqual(flavor.dict(), flavors.data[0].dict())

    async def test_delete(self) -> None:
        # Insert record.
        caramel = Flavor(name="caramel")
        await database[Flavor].insert(caramel)
        # Delete record.
        await database[Flavor].delete(caramel.id)
        # Find one record.
        self.assertIsNone(await database[Flavor].find_one(caramel.id))

    async def test_insert_and_find_orm(self) -> None:
        mocha = Flavor(name="mocha")
        vanilla = Flavor(name="vanilla")
        await database[Flavor].insert(mocha)
        await database[Flavor].insert(vanilla)
        coffee = Coffee(
            primary_flavor=mocha,
            secondary_flavor=vanilla,
            sweetener="none",
            cream=0,
            place={"sum": 1},
            ice=["cubes"],
            size=Money(),
        )
        await database[Coffee].insert(coffee)
        # Find record and compare.
        coffee_dict = coffee.dict()
        find_coffee = await database[Coffee].find_one(coffee.id, depth=1)
        self.assertDictEqual(coffee_dict, find_coffee.dict())  # type: ignore
        coffee_dict["primary_flavor"] = coffee_dict["primary_flavor"]["id"]
        coffee_dict["secondary_flavor"] = coffee_dict["secondary_flavor"]["id"]
        self.assertDictEqual(coffee_dict, (await database[Coffee].find_one(coffee.id)).dict())  # type: ignore
