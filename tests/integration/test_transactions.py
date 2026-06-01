from __future__ import annotations

import pytest
from pydantic import BaseModel

from ormdantic import Ormdantic


@pytest.mark.asyncio
async def test_transaction_commits(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'tx.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    await db.init()
    await db.drop_all()
    await db.create_all()

    async with db.transaction():
        await db[Flavor].insert(Flavor(id="1", name="mocha"))

    assert (await db[Flavor].count()) == 1


@pytest.mark.asyncio
async def test_transaction_rolls_back(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'tx_rollback.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    await db.init()
    await db.drop_all()
    await db.create_all()

    with pytest.raises(RuntimeError):
        async with db.transaction():
            await db[Flavor].insert(Flavor(id="1", name="mocha"))
            raise RuntimeError("boom")

    assert (await db[Flavor].count()) == 0
