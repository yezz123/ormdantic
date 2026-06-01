"""Basic Ormdantic CRUD example."""

import asyncio

from pydantic import BaseModel

from ormdantic import Ormdantic

db = Ormdantic("sqlite:///examples_basic_crud.sqlite3")


@db.table(pk="id", indexed=["name"])
class Flavor(BaseModel):
    id: str
    name: str
    strength: int


async def main() -> None:
    await db.init()
    await db.drop_all()
    await db.create_all()

    mocha = Flavor(id="1", name="mocha", strength=5)
    await db[Flavor].insert(mocha)

    found = await db[Flavor].find_one("1")
    assert found == mocha

    mocha.strength = 6
    await db[Flavor].update(mocha)

    assert await db[Flavor].count() == 1
    await db[Flavor].delete("1")


if __name__ == "__main__":
    asyncio.run(main())
