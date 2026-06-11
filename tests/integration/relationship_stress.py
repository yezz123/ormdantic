from __future__ import annotations

from uuid import uuid4

from pydantic import BaseModel, Field

from ormdantic import Ormdantic, joinedload, selectinload


async def run_relationship_loader_stress(url: str, *, suffix: str = "") -> None:
    """Exercise large select-in batches and nested mixed loader graphs."""
    suffix = suffix or uuid4().hex[:8]
    db = Ormdantic(url)

    @db.table(
        f"orm_rel_stress_parent_{suffix}",
        pk="id",
        back_references={"children": "parent"},
    )
    class StressParent(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str
        children: list[StressChild] = Field(default_factory=list)

    @db.table(
        f"orm_rel_stress_child_{suffix}", pk="id", back_references={"leaves": "child"}
    )
    class StressChild(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str
        parent: StressParent | str
        leaves: list[StressLeaf] = Field(default_factory=list)

    @db.table(f"orm_rel_stress_leaf_{suffix}", pk="id")
    class StressLeaf(BaseModel):
        id: str = Field(default_factory=lambda: str(uuid4()))
        name: str
        child: StressChild | str

    StressParent.model_rebuild()
    StressChild.model_rebuild()
    StressLeaf.model_rebuild()

    await db.init()
    try:
        await db.drop_all()
        await db.create_all()

        parent_count = 18
        children_per_parent = 4
        leaves_per_child = 3
        for parent_index in range(parent_count):
            parent = await db[StressParent].insert(
                StressParent(name=f"parent-{parent_index:02d}")
            )
            for child_index in range(children_per_parent):
                child = await db[StressChild].insert(
                    StressChild(
                        name=f"child-{parent_index:02d}-{child_index:02d}",
                        parent=parent,
                    )
                )
                for leaf_index in range(leaves_per_child):
                    await db[StressLeaf].insert(
                        StressLeaf(
                            name=(
                                f"leaf-{parent_index:02d}-{child_index:02d}-"
                                f"{leaf_index:02d}"
                            ),
                            child=child,
                        )
                    )

        selectin_loaded = await db[StressParent].find_many(
            order_by=["name"],
            load=[
                selectinload("children").batched(5).sorted_by("name"),
                selectinload("children.leaves").batched(7).sorted_by("name"),
            ],
        )

        assert len(selectin_loaded.data) == parent_count
        assert [parent.name for parent in selectin_loaded.data] == [
            f"parent-{index:02d}" for index in range(parent_count)
        ]
        for parent_index, parent in enumerate(selectin_loaded.data):
            assert [child.name for child in parent.children] == [
                f"child-{parent_index:02d}-{child_index:02d}"
                for child_index in range(children_per_parent)
            ]
            for child_index, child in enumerate(parent.children):
                assert child.parent == parent.id
                assert [leaf.name for leaf in child.leaves] == [
                    f"leaf-{parent_index:02d}-{child_index:02d}-{leaf_index:02d}"
                    for leaf_index in range(leaves_per_child)
                ]
                assert {leaf.child for leaf in child.leaves} == {child.id}

        mixed_loaded = await db[StressParent].find_many(
            order_by=["name"],
            load=[
                joinedload("children").sorted_by("name"),
                selectinload("children.parent").batched(3),
                selectinload("children.leaves").batched(7).sorted_by("name"),
            ],
        )

        assert len(mixed_loaded.data) == parent_count
        for parent in mixed_loaded.data:
            assert len(parent.children) == children_per_parent
            assert {child.parent.id for child in parent.children} == {parent.id}  # type: ignore[union-attr]
            assert all(child.parent is parent for child in parent.children)
            assert all(
                len(child.leaves) == leaves_per_child for child in parent.children
            )
    finally:
        await db.drop_all()
