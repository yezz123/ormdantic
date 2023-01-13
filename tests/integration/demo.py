import asyncio
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


async def demo() -> None:
    """Demo CRUD operations."""

    async def _init() -> None:
        async with db._engine.begin() as conn:
            await db.init()
            await conn.run_sync(db._metadata.drop_all)  # type: ignore
            await conn.run_sync(db._metadata.create_all)  # type: ignore

    await _init()

    await asyncio.sleep(5)

    # Insert
    flavor = Flavor(name="mocha")
    await db[Flavor].insert(flavor)
    await asyncio.sleep(5)
    coffee = Coffee(sweetener=1, flavor=flavor)
    await db[Coffee].insert(coffee)
    await asyncio.sleep(5)

    # Insert
    flavor = Flavor(name="caramel")
    await db[Flavor].insert(flavor)
    await asyncio.sleep(5)
    coffee = Coffee(sweetener=2, flavor=flavor)
    await db[Coffee].insert(coffee)
    await asyncio.sleep(5)

    # Insert
    flavor = Flavor(name="latte")
    await db[Flavor].insert(flavor)
    await asyncio.sleep(5)
    coffee = Coffee(sweetener=3, flavor=flavor)
    await db[Coffee].insert(coffee)
    await asyncio.sleep(5)

    # Insert
    flavor = Flavor(name="mocha-chai")
    await db[Flavor].insert(flavor)
    await asyncio.sleep(5)
    coffee = Coffee(sweetener=6, flavor=flavor)
    await db[Coffee].insert(coffee)
    await asyncio.sleep(5)

    # Insert
    flavor = Flavor(name="hot chocolate")
    await db[Flavor].insert(flavor)
    await asyncio.sleep(5)
    coffee = Coffee(sweetener=None, flavor=flavor)
    await db[Coffee].insert(coffee)
    await asyncio.sleep(5)

    # Count
    count = await db[Flavor].count()
    print(count)

    await asyncio.sleep(5)

    # Count Using Where and depth
    count = await db[Coffee].count(where={"sweetener": "6"}, depth=1)
    print(count)

    await asyncio.sleep(5)

    # Find one
    mocha = await db[Flavor].find_one(flavor.id)
    print(mocha)

    await asyncio.sleep(5)

    # Find one with depth.
    find_coffee = await db[Coffee].find_one(coffee.id, depth=1)
    print(find_coffee)

    await asyncio.sleep(5)

    # Find many
    await db[Flavor].find_many()  # Find all.

    await asyncio.sleep(5)

    # Get paginated results.
    await db[Flavor].find_many(
        where={"name": "mocha"}, order_by=["id", "name"], limit=2, offset=2
    )

    await asyncio.sleep(5)

    # Update
    flavor.name = "caramel"
    flavor = await db[Flavor].update(flavor)

    await asyncio.sleep(5)

    # Upsert
    flavor.name = "vanilla"
    flavor = await db[Flavor].upsert(flavor)

    await asyncio.sleep(5)

    # Delete
    await db[Flavor].delete(flavor.name)

    await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(demo())
