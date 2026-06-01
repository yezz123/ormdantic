"""Query expression example."""

import asyncio

from pydantic import BaseModel

from ormdantic import Ormdantic, column

db = Ormdantic("sqlite:///examples_query_expressions.sqlite3")


@db.table(pk="id")
class Flavor(BaseModel):
    id: str
    name: str
    strength: int | None = None


async def main() -> None:
    await db.init()
    await db.drop_all()
    await db.create_all()

    await db[Flavor].insert(Flavor(id="1", name="mocha", strength=5))
    await db[Flavor].insert(Flavor(id="2", name="latte", strength=None))

    strong = await db[Flavor].find_many(
        where=(column("strength") >= 5) & column("name").like("mo%")
    )
    assert [flavor.name for flavor in strong.data] == ["mocha"]

    flexible = await db[Flavor].find_many(
        where=column("name").ilike("MO%") | column("strength").is_(None)
    )
    assert {flavor.name for flavor in flexible.data} == {"mocha", "latte"}


if __name__ == "__main__":
    asyncio.run(main())
