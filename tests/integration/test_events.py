from __future__ import annotations

from pydantic import BaseModel

from ormdantic import Ormdantic
from ormdantic.migrations import MigrationOperation, MigrationPlan


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


async def test_session_context_events_fire_in_order(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'session_event_order.sqlite3'}")
    seen: list[str] = []

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    for event in (
        "before_begin",
        "after_begin",
        "before_flush",
        "after_flush",
        "before_commit",
        "after_commit",
    ):
        db.on(event, lambda event=event, **_: seen.append(event))

    await db.init()
    await db.drop_all()
    await db.create_all()

    async with db.session() as session:
        session.add(Flavor(id="1", name="mocha"))

    assert seen == [
        "before_begin",
        "after_begin",
        "before_flush",
        "after_flush",
        "before_commit",
        "after_commit",
    ]


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


async def test_savepoint_lifecycle_events_fire(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'savepoint_events.sqlite3'}")
    seen: list[str] = []

    for event in (
        "before_savepoint",
        "after_savepoint",
        "before_release_savepoint",
        "after_release_savepoint",
        "before_rollback_to_savepoint",
        "after_rollback_to_savepoint",
    ):
        db.on(event, lambda event=event, **_: seen.append(event))

    await db.init()

    async with db.transaction():
        async with db.savepoint("keep"):
            pass

        try:
            async with db.savepoint("undo"):
                raise RuntimeError("rollback savepoint")
        except RuntimeError:
            pass

    assert seen == [
        "before_savepoint",
        "after_savepoint",
        "before_release_savepoint",
        "after_release_savepoint",
        "before_savepoint",
        "after_savepoint",
        "before_rollback_to_savepoint",
        "after_rollback_to_savepoint",
    ]


async def test_query_diagnostics_include_timing_sql_and_redacted_params(
    tmp_path,
) -> None:
    logged: list[dict[str, object]] = []
    db = Ormdantic(
        f"sqlite:///{tmp_path / 'query_diagnostics.sqlite3'}",
        debug=True,
        query_logger=lambda **payload: logged.append(payload),
    )
    execute_events: list[tuple[str, dict[str, object]]] = []
    hydration_events: list[tuple[str, dict[str, object]]] = []
    lifecycle: list[str] = []

    @db.table(pk="id")
    class SecretFlavor(BaseModel):
        id: str
        password: str
        name: str

    db.on(
        "before_execute",
        lambda **payload: execute_events.append(("before", dict(payload))),
    )
    db.on(
        "after_execute",
        lambda **payload: execute_events.append(("after", dict(payload))),
    )
    db.on(
        "after_hydration",
        lambda **payload: hydration_events.append(("after", dict(payload))),
    )
    db.on("before_create", lambda **_: lifecycle.append("before_create"))
    db.on("after_create", lambda **_: lifecycle.append("after_create"))

    await db.init()
    await db[SecretFlavor].insert(
        SecretFlavor(id="1", password="do-not-log", name="mocha")
    )
    await db[SecretFlavor].find_many(where={"name": "mocha"})
    diagnostics = db.runtime_diagnostics()

    insert_before = next(
        payload
        for phase, payload in execute_events
        if phase == "before" and payload["operation"] == "insert"
    )
    insert_after = next(
        payload
        for phase, payload in execute_events
        if phase == "after" and payload["operation"] == "insert"
    )

    assert insert_before["table_name"] == "secret_flavor"
    assert insert_before["backend"] == "sqlite"
    assert insert_before["bind_names"] == ["id", "password", "name"]
    assert insert_before["parameters"] == {
        "id": "1",
        "password": "<redacted>",
        "name": "mocha",
    }
    assert 'INSERT INTO "secret_flavor"' in str(insert_before["sql"])
    assert insert_after["duration_ms"] >= 0
    assert insert_after["error"] is None
    assert lifecycle == ["before_create", "after_create"]
    assert diagnostics["backend"] == "sqlite"
    assert diagnostics["debug"] is True
    assert "secret_flavor" in diagnostics["registered_tables"]
    assert logged
    assert any(
        payload["operation"] == "select_many" for _phase, payload in execute_events
    )
    assert any(payload["row_count"] == 1 for _phase, payload in hydration_events)


async def test_reflection_and_migration_events_include_timing(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'operation_events.sqlite3'}")
    reflection_events: list[tuple[str, dict[str, object]]] = []
    migration_events: list[tuple[str, dict[str, object]]] = []

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    db.on(
        "before_reflection",
        lambda **payload: reflection_events.append(("before", dict(payload))),
    )
    db.on(
        "after_reflection",
        lambda **payload: reflection_events.append(("after", dict(payload))),
    )
    db.on(
        "before_migration",
        lambda **payload: migration_events.append(("before", dict(payload))),
    )
    db.on(
        "after_migration",
        lambda **payload: migration_events.append(("after", dict(payload))),
    )

    await db.init()
    assert "flavor" in await db.inspect().table_names()

    plan = MigrationPlan(
        operations=[
            MigrationOperation(
                'CREATE TABLE "migration_event_extra" ("id" TEXT PRIMARY KEY)'
            )
        ],
        rollback_operations=[MigrationOperation('DROP TABLE "migration_event_extra"')],
    )
    assert await db.migrations.apply("001_event_extra", plan) is True

    assert reflection_events[0][0] == "before"
    assert reflection_events[-1][1]["duration_ms"] >= 0
    assert migration_events[0][1]["revision"] == "001_event_extra"
    assert migration_events[-1][1]["duration_ms"] >= 0
    assert migration_events[-1][1]["applied"] is True
