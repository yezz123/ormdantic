from __future__ import annotations

import pytest


def test_sqlmodel_relationship_mappers_configure() -> None:
    pytest.importorskip("sqlmodel")
    from sqlalchemy.orm import configure_mappers

    from benchmark import models as benchmark_models

    configure_mappers()

    parent_relationship = benchmark_models.SQLModelBenchParent.__mapper__.relationships[
        "children"
    ]
    child_relationship = benchmark_models.SQLModelBenchChild.__mapper__.relationships[
        "parent"
    ]
    assert parent_relationship.mapper.class_ is benchmark_models.SQLModelBenchChild
    assert child_relationship.mapper.class_ is benchmark_models.SQLModelBenchParent


def test_ormdantic_indexed_strings_are_bounded_for_mysql(tmp_path) -> None:
    from annotated_types import MaxLen

    from benchmark.models import register_ormdantic_models
    from ormdantic import Ormdantic

    db = Ormdantic(f"sqlite:///{tmp_path / 'models.sqlite3'}")
    models = register_ormdantic_models(db)

    assert MaxLen(32) in models.item.model_fields["id"].metadata
    assert MaxLen(24) in models.item.model_fields["category"].metadata
    assert MaxLen(96) in models.item.model_fields["name"].metadata
    assert MaxLen(160) in models.item.model_fields["payload"].metadata


def test_sqlmodel_benchmark_tables_are_scoped_to_benchmark_prefix() -> None:
    pytest.importorskip("sqlmodel")
    from benchmark.models import sqlmodel_benchmark_tables

    names = {table.name for table in sqlmodel_benchmark_tables()}

    assert names == {
        "ormdantic_bench_items",
        "ormdantic_bench_parents",
        "ormdantic_bench_children",
        "ormdantic_bench_leaves",
    }
