from __future__ import annotations

from uuid import uuid4

from pydantic import BaseModel, Field

from ormdantic import Ormdantic, joined, lazy, selectin


async def test_joined_and_selectin_loader_options(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'loaders.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str

    @db.table(pk="id")
    class Coffee(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        flavor: Flavor | str

    await db.init()
    await db.drop_all()
    await db.create_all()
    flavor = await db[Flavor].insert(Flavor(name="mocha"))
    coffee = await db[Coffee].insert(Coffee(flavor=flavor))

    joined_loaded = await db[Coffee].find_one(coffee.id, load=[joined("flavor")])
    selectin_loaded = await db[Coffee].find_one(coffee.id, load=[selectin("flavor")])

    assert joined_loaded is not None
    assert selectin_loaded is not None
    assert joined_loaded.flavor == flavor
    assert selectin_loaded.flavor == flavor


async def test_explicit_lazy_loader(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'lazy.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str

    @db.table(pk="id")
    class Coffee(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        flavor: Flavor | str

    await db.init()
    await db.drop_all()
    await db.create_all()
    flavor = await db[Flavor].insert(Flavor(name="mocha"))
    coffee = await db[Coffee].insert(Coffee(flavor=flavor))

    shallow = await db[Coffee].find_one(coffee.id, load=[lazy("flavor")])
    loaded_flavor = await db.load(shallow, "flavor")  # type: ignore[arg-type]

    assert shallow is not None
    assert shallow.flavor == flavor.id
    assert loaded_flavor == flavor
