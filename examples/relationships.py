"""Relationship loading example."""

import asyncio

from pydantic import BaseModel, Field

from ormdantic import Ormdantic, lazy

db = Ormdantic("sqlite:///examples_relationships.sqlite3")


@db.table(pk="id")
class Flavor(BaseModel):
    id: str
    name: str


@db.table(pk="id")
class Coffee(BaseModel):
    id: str
    name: str
    flavor: Flavor | str = Field(...)


async def main() -> None:
    await db.init()
    await db.drop_all()
    await db.create_all()

    flavor = Flavor(id="flavor-1", name="mocha")
    await db[Flavor].insert(flavor)
    await db[Coffee].insert(Coffee(id="coffee-1", name="latte", flavor=flavor))

    loaded = await db[Coffee].find_one("coffee-1", depth=1)
    assert loaded is not None
    assert isinstance(loaded.flavor, Flavor)

    shallow = await db[Coffee].find_one("coffee-1", load=[lazy("flavor")])
    assert shallow is not None
    assert await db.load(shallow, "flavor") == flavor


if __name__ == "__main__":
    asyncio.run(main())
