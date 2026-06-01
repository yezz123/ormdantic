from __future__ import annotations

from pydantic import BaseModel

from ormdantic import Ormdantic


async def test_session_flushes_and_commits(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'session.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    await db.init()
    await db.drop_all()
    await db.create_all()

    async with db.session() as session:
        session.add(Flavor(id="1", name="mocha"))

    assert (await db[Flavor].find_one("1")).name == "mocha"  # type: ignore[union-attr]


async def test_session_rolls_back_on_error(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'session_rollback.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    await db.init()
    await db.drop_all()
    await db.create_all()

    try:
        async with db.session() as session:
            session.add(Flavor(id="1", name="mocha"))
            raise RuntimeError("boom")
    except RuntimeError:
        pass

    assert await db[Flavor].count() == 0


async def test_session_refreshes_identity_map(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'session_refresh.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    await db.init()
    await db.drop_all()
    await db.create_all()
    flavor = await db[Flavor].insert(Flavor(id="1", name="mocha"))

    async with db.session() as session:
        refreshed = await session.refresh(flavor)
        assert refreshed == flavor
        assert session.get_cached(Flavor, "1") == flavor
