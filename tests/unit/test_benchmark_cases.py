from __future__ import annotations

from benchmark.cases import case_matrix
from benchmark.charts import ORMDANTIC, SQLALCHEMY, SQLMODEL


def test_case_matrix_covers_required_workload_categories() -> None:
    cases = case_matrix()
    categories = {case.category for case in cases}
    names = {case.name for case in cases}

    assert {
        "schema",
        "write",
        "read",
        "query",
        "hydration",
        "serialization",
        "relationship",
    } <= categories
    assert {
        "schema create/drop",
        "raw batch insert",
        "orm insert models",
        "orm update filtered",
        "orm upsert mixed",
        "orm delete filtered",
        "count all rows",
        "count equality filter",
        "count range filter",
        "aggregate filtered rows",
        "scalar projection read",
        "batched primary-key lookup",
        "paginated find_many",
        "ordered find_many",
        "hydrate flat rows",
        "serialize simple payloads",
        "serialize nested payloads",
        "one-to-many relationship loading",
        "many-to-one relationship loading",
        "nested relationship loading",
    } <= names


def test_case_matrix_declares_supported_orms_for_first_backends() -> None:
    for case in case_matrix():
        for backend in ("sqlite", "postgres", "mysql"):
            assert case.supports(ORMDANTIC, backend)
            assert case.supports(SQLALCHEMY, backend)
            assert case.supports(SQLMODEL, backend)


def test_case_rows_and_expected_values_are_profile_aware() -> None:
    cases = {case.name: case for case in case_matrix()}

    assert (
        cases["raw batch insert"].rows(read_rows=100, write_rows=7, lookup_count=3) == 7
    )
    assert (
        cases["count equality filter"].expected(
            read_rows=1_003,
            write_rows=7,
            lookup_count=3,
            category="cat-2",
        )
        == 101
    )
    assert (
        cases["batched primary-key lookup"].rows(
            read_rows=100,
            write_rows=7,
            lookup_count=3,
        )
        == 3
    )


def test_non_equivalent_serialization_cases_are_diagnostic() -> None:
    cases = {case.name: case for case in case_matrix()}

    assert cases["serialize simple payloads"].comparable is False
    assert cases["serialize nested payloads"].comparable is False
    assert cases["orm insert models"].comparable is True
