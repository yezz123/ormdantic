import asyncio
from functools import wraps
from uuid import UUID, uuid4

from decouple import config
from pydantic import BaseModel, Field

from ormdantic import Ormdantic

connection = config("DATABASE_URL")
db = Ormdantic(connection)


@db.table(pk="id", indexed=["name"])
class Flavor(BaseModel):
    """A coffee flavor."""

    id: UUID = Field(default_factory=uuid4)
    name: str = Field(max_length=63)


@db.table(pk="id")
class Coffee(BaseModel):
    """Drink it in the morning."""

    id: UUID = Field(default_factory=uuid4)
    sweetener: str | None = Field(max_length=63)
    sweetener_count: int | None = None
    flavor: Flavor | UUID


def sleep_after(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        result = await func(self, *args, **kwargs)
        await asyncio.sleep(self.sleep_duration)
        return result

    return wrapper


class CoffeeDemo:
    def __init__(self, sleep_duration=5):
        self.db = db
        self.sleep_duration = sleep_duration

    @sleep_after
    async def init_db(self):
        """Initialize the database."""
        async with self.db._engine.begin() as conn:
            await self.db.init()
            await conn.run_sync(self.db._metadata.drop_all)  # type: ignore
            await conn.run_sync(self.db._metadata.create_all)  # type: ignore

    @sleep_after
    async def insert_flavor(self, name):
        """Insert a new flavor."""
        flavor = Flavor(name=name)
        await self.db[Flavor].insert(flavor)
        return flavor

    @sleep_after
    async def insert_coffee(self, sweetener, flavor):
        """Insert a new coffee."""
        coffee = Coffee(sweetener=sweetener, flavor=flavor)
        await self.db[Coffee].insert(coffee)
        return coffee

    @sleep_after
    async def count_flavors(self):
        """Count all flavors."""
        count = await self.db[Flavor].count()
        print(f"Total flavors: {count}")

    @sleep_after
    async def count_coffees_with_condition(self, sweetener, depth=1):
        """Count coffees with a specific sweetener."""
        count = await self.db[Coffee].count(where={"sweetener": sweetener}, depth=depth)
        print(f"Coffees with sweetener {sweetener}: {count}")

    @sleep_after
    async def find_flavor(self, flavor_id):
        """Find a flavor by ID."""
        flavor = await self.db[Flavor].find_one(flavor_id)
        print(f"Found flavor: {flavor}")

    @sleep_after
    async def find_coffee(self, coffee_id, depth=1):
        """Find a coffee by ID."""
        coffee = await self.db[Coffee].find_one(coffee_id, depth=depth)
        print(f"Found coffee: {coffee}")

    @sleep_after
    async def find_all_flavors(self):
        """Find all flavors."""
        flavors = await self.db[Flavor].find_many()
        print(f"All flavors: {flavors}")

    @sleep_after
    async def find_flavors_paginated(self, name, limit=2, offset=2):
        """Find flavors with pagination."""
        flavors = await self.db[Flavor].find_many(
            where={"name": name}, order_by=["id", "name"], limit=limit, offset=offset
        )
        print(f"Paginated flavors: {flavors}")

    @sleep_after
    async def update_flavor(self, flavor, new_name):
        """Update a flavor's name."""
        flavor.name = new_name
        updated_flavor = await self.db[Flavor].update(flavor)
        print(f"Updated flavor: {updated_flavor}")

    @sleep_after
    async def upsert_flavor(self, flavor, new_name):
        """Upsert a flavor."""
        flavor.name = new_name
        upserted_flavor = await self.db[Flavor].upsert(flavor)
        print(f"Upserted flavor: {upserted_flavor}")

    @sleep_after
    async def delete_flavor(self, flavor_name):
        """Delete a flavor by name."""
        await self.db[Flavor].delete(flavor_name)
        print(f"Deleted flavor: {flavor_name}")

    async def run_demo(self):
        """Run the full demo."""
        await self.init_db()
        mocha = await self.insert_flavor("mocha")
        await self.insert_coffee(1, mocha)
        caramel = await self.insert_flavor("caramel")
        await self.insert_coffee(2, caramel)
        latte = await self.insert_flavor("latte")
        await self.insert_coffee(3, latte)
        mocha_chai = await self.insert_flavor("mocha-chai")
        await self.insert_coffee(6, mocha_chai)
        hot_chocolate = await self.insert_flavor("hot chocolate")
        coffee = await self.insert_coffee(None, hot_chocolate)
        await self.count_flavors()
        await self.count_coffees_with_condition("6")
        await self.find_flavor(mocha.id)
        await self.find_coffee(coffee.id)
        await self.find_all_flavors()
        await self.find_flavors_paginated("mocha")
        await self.update_flavor(mocha, "caramel")
        await self.upsert_flavor(mocha, "vanilla")
        await self.delete_flavor(mocha.name)


if __name__ == "__main__":
    demo = CoffeeDemo(sleep_duration=5)
    asyncio.run(demo.run_demo())
