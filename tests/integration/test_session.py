from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import BaseModel, Field

from ormdantic import Ormdantic, selectinload


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


async def test_session_tracks_dirty_loaded_models_automatically(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'session_dirty_tracking.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    await db.init()
    await db.drop_all()
    await db.create_all()
    await db[Flavor].insert(Flavor(id="1", name="mocha"))

    async with db.session() as session:
        flavor = await session.get(Flavor, "1")
        assert flavor is not None
        flavor.name = "vanilla"

    stored = await db[Flavor].find_one("1")
    assert stored is not None
    assert stored.name == "vanilla"


async def test_session_does_not_update_unchanged_remembered_models(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'session_clean_tracking.sqlite3'}")
    seen: list[str] = []

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    db.on("before_update", lambda **_: seen.append("before_update"))

    await db.init()
    await db.drop_all()
    await db.create_all()
    await db[Flavor].insert(Flavor(id="1", name="mocha"))

    async with db.session() as session:
        flavor = await session.get(Flavor, "1")
        assert flavor is not None

    assert seen == []


async def test_session_tracks_in_place_mutable_column_changes(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'session_mutable_dirty.sqlite3'}")

    @db.table(pk="id")
    class Recipe(BaseModel):
        id: str
        tags: list[str]

    await db.init()
    await db.drop_all()
    await db.create_all()
    await db[Recipe].insert(Recipe(id="1", tags=["mocha"]))

    async with db.session() as session:
        recipe = await session.get(Recipe, "1")
        assert recipe is not None
        recipe.tags.append("latte")

    stored = await db[Recipe].find_one("1")
    assert stored is not None
    assert stored.tags == ["mocha", "latte"]


async def test_session_expire_detaches_model_from_dirty_tracking(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'session_expire_detach.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    await db.init()
    await db.drop_all()
    await db.create_all()
    await db[Flavor].insert(Flavor(id="1", name="mocha"))

    async with db.session() as session:
        flavor = await session.get(Flavor, "1")
        assert flavor is not None
        session.expire(flavor)
        assert session.get_cached(Flavor, "1") is None
        flavor.name = "vanilla"
        fresh = await session.get(Flavor, "1")
        assert fresh is not None
        assert fresh is not flavor
        assert fresh.name == "mocha"

    stored = await db[Flavor].find_one("1")
    assert stored is not None
    assert stored.name == "mocha"


async def test_session_merge_detached_model_persists_updates(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'session_merge_detached.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    await db.init()
    await db.drop_all()
    await db.create_all()
    await db[Flavor].insert(Flavor(id="1", name="mocha"))

    async with db.session() as session:
        managed = await session.get(Flavor, "1")
        assert managed is not None
        merged = session.merge(Flavor(id="1", name="vanilla"))
        assert merged is managed
        assert managed.name == "vanilla"

    stored = await db[Flavor].find_one("1")
    assert stored is not None
    assert stored.name == "vanilla"


async def test_session_cascades_one_to_many_add_graph(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'session_add_cascade.sqlite3'}")

    @db.table(pk="id", back_references={"posts": "author"})
    class Author(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str
        posts: list[Post] = Field(default_factory=list)

    @db.table(pk="id")
    class Post(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        title: str
        author: Author | str | None = None

    Author.model_rebuild()
    Post.model_rebuild()

    await db.init()
    await db.drop_all()
    await db.create_all()

    author = Author(name="writer", posts=[Post(title="second"), Post(title="first")])
    async with db.session() as session:
        session.add(author)

    loaded = await db[Author].find_one(
        author.id, load=[selectinload("posts").sorted_by("title")]
    )
    assert loaded is not None
    assert [post.title for post in loaded.posts] == ["first", "second"]
    assert {post.author for post in loaded.posts} == {author.id}


async def test_session_orders_many_to_one_dependency_inserts(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'session_dependency_order.sqlite3'}")
    inserts: list[str] = []

    @db.table(pk="id")
    class Author(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str

    @db.table(pk="id")
    class Post(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        title: str
        author: Author | str

    db.on(
        "before_insert",
        lambda **kwargs: inserts.append(kwargs["table"].tablename),
    )

    await db.init()
    await db.drop_all()
    await db.create_all()

    author = Author(name="writer")
    post = Post(title="draft", author=author)
    async with db.session() as session:
        session.add(post)

    assert inserts == ["author", "post"]
    stored = await db[Post].find_one(post.id, load=[selectinload("author")])
    assert stored is not None
    assert stored.author == author


async def test_session_cascades_loaded_collection_deletes_before_parent(
    tmp_path,
) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'session_delete_cascade.sqlite3'}")
    deletes: list[str] = []

    @db.table(pk="id", back_references={"posts": "author"})
    class Author(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str
        posts: list[Post] = Field(default_factory=list)

    @db.table(pk="id")
    class Post(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        title: str
        author: Author | str

    Author.model_rebuild()
    Post.model_rebuild()

    db.on(
        "before_delete",
        lambda **kwargs: deletes.append(kwargs["table"].tablename),
    )

    await db.init()
    await db.drop_all()
    await db.create_all()

    author = await db[Author].insert(Author(name="writer"))
    await db[Post].insert(Post(title="first", author=author))
    await db[Post].insert(Post(title="second", author=author))
    loaded = await db[Author].find_one(author.id, load=[selectinload("posts")])
    assert loaded is not None
    assert len(loaded.posts) == 2

    async with db.session() as session:
        session.delete(loaded)

    assert deletes[:2] == ["post", "post"]
    assert deletes[2:] == ["author"]
    assert await db[Author].count() == 0
    assert await db[Post].count() == 0


async def test_session_mixed_loaded_graph_add_update_delete(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'session_mixed_graph.sqlite3'}")

    @db.table(pk="id", back_references={"posts": "author"})
    class Author(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str
        posts: list[Post] = Field(default_factory=list)

    @db.table(pk="id")
    class Post(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        title: str
        author: Author | str

    Author.model_rebuild()
    Post.model_rebuild()

    await db.init()
    await db.drop_all()
    await db.create_all()

    author = await db[Author].insert(Author(name="writer"))
    old_post = await db[Post].insert(Post(title="old", author=author))

    async with db.session() as session:
        managed = await session.get(Author, author.id, depth=1)
        assert managed is not None
        assert len(managed.posts) == 1
        managed.name = "updated"
        managed.posts = [Post(title="new", author=managed)]
        session.add(managed)
        session.delete(old_post)

    stored = await db[Author].find_one(
        author.id, load=[selectinload("posts").sorted_by("title")]
    )
    assert stored is not None
    assert stored.name == "updated"
    assert [post.title for post in stored.posts] == ["new"]
    assert stored.posts[0].author == author.id
    assert await db[Post].find_one(old_post.id) is None


async def test_session_rejects_operations_after_close(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'session_closed.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    await db.init()
    await db.drop_all()
    await db.create_all()

    session = db.session()
    async with session:
        session.add(Flavor(id="1", name="mocha"))

    try:
        session.add(Flavor(id="2", name="latte"))
    except RuntimeError as exc:
        assert str(exc) == "session is closed"
    else:  # pragma: no cover
        raise AssertionError("closed session accepted add()")


async def test_session_rejects_duplicate_staged_primary_keys(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'session_duplicate_pk.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    await db.init()
    await db.drop_all()
    await db.create_all()

    async with db.session() as session:
        session.add(Flavor(id="1", name="mocha"))
        with pytest.raises(ValueError, match="primary key '1' is already staged"):
            session.add(Flavor(id="1", name="latte"))


async def test_session_merge_updates_pending_model_with_same_primary_key(
    tmp_path,
) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'session_merge_pending.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    await db.init()
    await db.drop_all()
    await db.create_all()

    async with db.session() as session:
        pending = Flavor(id="1", name="mocha")
        session.add(pending)
        merged = session.merge(Flavor(id="1", name="vanilla"))
        assert merged is pending
        assert pending.name == "vanilla"

    stored = await db[Flavor].find_one("1")
    assert stored is not None
    assert stored.name == "vanilla"


async def test_session_failed_flush_requires_rollback_and_cleans_state(
    tmp_path,
) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'session_failed_flush.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    def fail_second_insert(**kwargs) -> None:
        if kwargs["model"].id == "2":
            raise RuntimeError("stop second insert")

    db.on("before_insert", fail_second_insert)

    await db.init()
    await db.drop_all()
    await db.create_all()

    session = db.session()
    async with session:
        session.add(Flavor(id="1", name="mocha"))
        session.add(Flavor(id="2", name="latte"))
        with pytest.raises(RuntimeError, match="stop second insert"):
            await session.flush()

        assert session.get_cached(Flavor, "1") is None
        with pytest.raises(
            RuntimeError, match="session flush failed; rollback required"
        ):
            session.add(Flavor(id="3", name="vanilla"))

        await session.rollback()

    assert await db[Flavor].count() == 0


async def test_session_savepoint_restores_flushed_and_pending_state(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'session_savepoint.sqlite3'}")

    @db.table(pk="id")
    class Flavor(BaseModel):
        id: str
        name: str

    await db.init()
    await db.drop_all()
    await db.create_all()

    async with db.session() as session:
        session.add(Flavor(id="1", name="mocha"))
        await session.flush()

        with pytest.raises(RuntimeError, match="rollback savepoint"):
            async with session.savepoint("optional_flavor"):
                session.add(Flavor(id="2", name="latte"))
                await session.flush()
                assert session.get_cached(Flavor, "2") is not None
                raise RuntimeError("rollback savepoint")

        assert session.get_cached(Flavor, "1") is not None
        assert session.get_cached(Flavor, "2") is None
        session.add(Flavor(id="3", name="vanilla"))

    assert await db[Flavor].count() == 2
    assert await db[Flavor].find_one("1") is not None
    assert await db[Flavor].find_one("2") is None
    assert await db[Flavor].find_one("3") is not None


async def test_session_tracks_collection_relationship_additions(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'session_relationship_changes.sqlite3'}")

    @db.table(pk="id", back_references={"posts": "author"})
    class Author(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str
        posts: list[Post] = Field(default_factory=list)

    @db.table(pk="id")
    class Post(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        title: str
        author: Author | str | None = None

    Author.model_rebuild()
    Post.model_rebuild()

    await db.init()
    await db.drop_all()
    await db.create_all()

    author = await db[Author].insert(Author(name="writer"))

    async with db.session() as session:
        managed = await session.get(Author, author.id, depth=1)
        assert managed is not None
        assert managed.posts == []
        managed.posts.append(Post(title="auto tracked"))

    stored = await db[Author].find_one(author.id, load=[selectinload("posts")])
    assert stored is not None
    assert [post.title for post in stored.posts] == ["auto tracked"]
    assert stored.posts[0].author == author.id
