from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import BaseModel, Field

from ormdantic import Ormdantic


@pytest.fixture
async def database(tmp_path):
    db = Ormdantic(f"sqlite:///{tmp_path / 'filters.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str
        strength: int | None = None

    await db.init()
    await db.drop_all()
    await db.create_all()
    yield db, Flavor


async def test_find_many_supports_comparison_and_like_filters(database) -> None:
    db, Flavor = database
    mocha = await db[Flavor].insert(Flavor(name="mocha", strength=3))
    await db[Flavor].insert(Flavor(name="vanilla", strength=1))

    result = await db[Flavor].find_many(
        where={"strength__gt": 1, "name__like": "mo%"}
    )

    assert result.data == [mocha]


async def test_find_many_supports_in_and_null_filters(database) -> None:
    db, Flavor = database
    mocha = await db[Flavor].insert(Flavor(name="mocha"))
    vanilla = await db[Flavor].insert(Flavor(name="vanilla", strength=1))
    await db[Flavor].insert(Flavor(name="latte", strength=2))

    result = await db[Flavor].find_many(
        where={"id__in": [mocha.id, vanilla.id], "strength__is_not_null": True}
    )

    assert result.data == [vanilla]
