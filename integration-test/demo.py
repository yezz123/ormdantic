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
            await conn.run_sync(db._metadata.drop_all)
            await conn.run_sync(db._metadata.create_all)

    await _init()

    # Insert
    flavor = Flavor(name="mocha")
    await db[Flavor].insert(flavor)
    coffee = Coffee(sweetener=None, flavor=flavor)
    await db[Coffee].insert(coffee)

    # Find one
    mocha = await db[Flavor].find_one(flavor.id)
    print(mocha)

    # Find one with depth.
    find_coffee = await db[Coffee].find_one(coffee.id, depth=1)
    print(find_coffee)

    # Find many
    await db[Flavor].find_many()  # Find all.

    # Get paginated results.
    await db[Flavor].find_many(
        where={"name": "mocha"}, order_by=["id", "name"], limit=2, offset=2
    )

    # Update
    flavor.name = "caramel"
    flavor = await db[Flavor].update(flavor)

    # Upsert
    flavor.name = "vanilla"
    flavor = await db[Flavor].upsert(flavor)

    # Delete
    await db[Flavor].delete(flavor.id)


if __name__ == "__main__":
    asyncio.run(demo())
