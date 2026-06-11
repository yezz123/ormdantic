from __future__ import annotations

from pydantic import BaseModel

from ormdantic import Ormdantic


async def test_crud_events_fire(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'events.sqlite3'}")
    seen: list[str] = []

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    db.on("before_insert", lambda **_: seen.append("before_insert"))
    db.on("after_insert", lambda **_: seen.append("after_insert"))
    db.on("before_update", lambda **_: seen.append("before_update"))
    db.on("after_update", lambda **_: seen.append("after_update"))
    db.on("before_delete", lambda **_: seen.append("before_delete"))
    db.on("after_delete", lambda **_: seen.append("after_delete"))

    await db.init()
    await db.drop_all()
    await db.create_all()
    flavor = await db[Flavor].insert(Flavor(id="1", name="mocha"))
    flavor.name = "vanilla"
    await db[Flavor].update(flavor)
    await db[Flavor].delete(flavor.id)

    assert seen == [
        "before_insert",
        "after_insert",
        "before_update",
        "after_update",
        "before_delete",
        "after_delete",
    ]


async def test_session_flush_events_fire(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'flush_events.sqlite3'}")
    seen: list[str] = []

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    async def before_flush(**_) -> None:
        seen.append("before_flush")

    db.on("before_flush", before_flush)
    db.on("after_flush", lambda **_: seen.append("after_flush"))

    await db.init()
    await db.drop_all()
    await db.create_all()
    async with db.session() as session:
        session.add(Flavor(id="1", name="mocha"))

    assert seen == ["before_flush", "after_flush"]


async def test_transaction_lifecycle_events_fire(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'transaction_events.sqlite3'}")
    seen: list[str] = []

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    for event in (
        "before_begin",
        "after_begin",
        "before_commit",
        "after_commit",
        "before_rollback",
        "after_rollback",
    ):
        db.on(event, lambda event=event, **_: seen.append(event))

    await db.init()
    await db.drop_all()
    await db.create_all()

    async with db.transaction():
        await db[Flavor].insert(Flavor(id="1", name="mocha"))

    try:
        async with db.transaction():
            await db[Flavor].insert(Flavor(id="2", name="latte"))
            raise RuntimeError("rollback")
    except RuntimeError:
        pass

    assert seen == [
        "before_begin",
        "after_begin",
        "before_commit",
        "after_commit",
        "before_begin",
        "after_begin",
        "before_rollback",
        "after_rollback",
    ]
