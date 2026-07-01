"""Transactions, savepoints, and sessions example."""

import asyncio

from pydantic import BaseModel

from ormdantic import Ormdantic

db = Ormdantic("sqlite:///examples_transactions_sessions.sqlite3")


@db.table(pk="id")
class Flavor(BaseModel):
    id: str
    name: str


async def main() -> None:
    await db.init()
    await db.drop_all()
    await db.create_all()

    async with db.transaction():
        await db[Flavor].insert(Flavor(id="1", name="mocha"))
        try:
            async with db.savepoint("optional_insert"):
                await db[Flavor].insert(Flavor(id="2", name="rollback"))
                raise RuntimeError("rollback only the savepoint")
        except RuntimeError:
            pass

    async with db.session() as session:
        try:
            async with session.savepoint("optional_session_insert"):
                session.add(Flavor(id="2", name="discarded"))
                await session.flush()
                raise RuntimeError("rollback only the session savepoint")
        except RuntimeError:
            pass
        session.add(Flavor(id="3", name="vanilla"))

    assert await db[Flavor].count() == 2


if __name__ == "__main__":
    asyncio.run(main())
