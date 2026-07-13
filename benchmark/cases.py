from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from benchmark.charts import ORMDANTIC, SQLALCHEMY, SQLMODEL
from benchmark.datasets import expected_category_count, expected_score_range_count

ReadRows = int
WriteRows = int
LookupCount = int


@dataclass(frozen=True)
class CaseDefinition:
    """One benchmark workload case."""

    name: str
    category: str
    row_counter: Callable[[ReadRows, WriteRows, LookupCount], int]
    expected_counter: Callable[[ReadRows, WriteRows, LookupCount, str], int]
    supported_orms: tuple[str, ...] = (ORMDANTIC, SQLALCHEMY, SQLMODEL)
    supported_backends: tuple[str, ...] = ("sqlite", "postgres", "mysql")

    def rows(self, *, read_rows: int, write_rows: int, lookup_count: int) -> int:
        return self.row_counter(read_rows, write_rows, lookup_count)

    def expected(
        self,
        *,
        read_rows: int,
        write_rows: int,
        lookup_count: int,
        category: str,
    ) -> int:
        return self.expected_counter(read_rows, write_rows, lookup_count, category)

    def supports(self, orm: str, backend: str) -> bool:
        return orm in self.supported_orms and backend in self.supported_backends


def case_matrix() -> tuple[CaseDefinition, ...]:
    """Return the full first-pass cross-database benchmark matrix."""
    return (
        _case("schema create/drop", "schema", _zero, _zero_expected),
        _case("raw batch insert", "write", _write_rows, _write_rows_expected),
        _case("orm insert models", "write", _write_rows, _write_rows_expected),
        _case("orm update filtered", "write", _read_rows, _category_expected),
        _case("orm upsert mixed", "write", _lookup_rows, _lookup_expected),
        _case("orm delete filtered", "write", _read_rows, _remaining_after_category),
        _case("count all rows", "read", _read_rows, _read_rows_expected),
        _case("count equality filter", "query", _read_rows, _category_expected),
        _case("count range filter", "query", _read_rows, _range_expected),
        _case("aggregate filtered rows", "query", _read_rows, _category_expected),
        _case("scalar projection read", "read", _read_rows, _category_expected),
        _case("batched primary-key lookup", "read", _lookup_rows, _lookup_expected),
        _case("paginated find_many", "read", _page_rows, _page_expected),
        _case("ordered find_many", "read", _page_rows, _page_expected),
        _case("hydrate flat rows", "hydration", _page_rows, _page_expected),
        _case("serialize simple payloads", "serialization", _page_rows, _page_expected),
        _case(
            "serialize nested payloads",
            "serialization",
            _relationship_rows,
            _relationship_parent_expected,
        ),
        _case(
            "hydrate relationship results",
            "hydration",
            _relationship_rows,
            _relationship_parent_expected,
        ),
        _case(
            "one-to-many relationship loading",
            "relationship",
            _relationship_rows,
            _relationship_parent_expected,
        ),
        _case(
            "many-to-one relationship loading",
            "relationship",
            _relationship_rows,
            _relationship_child_expected,
        ),
        _case(
            "nested relationship loading",
            "relationship",
            _relationship_rows,
            _relationship_parent_expected,
        ),
    )


def case_names() -> tuple[str, ...]:
    return tuple(case.name for case in case_matrix())


def _case(
    name: str,
    category: str,
    row_counter: Callable[[int, int, int], int],
    expected_counter: Callable[[int, int, int, str], int],
) -> CaseDefinition:
    return CaseDefinition(
        name=name,
        category=category,
        row_counter=row_counter,
        expected_counter=expected_counter,
    )


def _zero(read_rows: int, write_rows: int, lookup_count: int) -> int:
    return 0


def _read_rows(read_rows: int, write_rows: int, lookup_count: int) -> int:
    return read_rows


def _write_rows(read_rows: int, write_rows: int, lookup_count: int) -> int:
    return write_rows


def _lookup_rows(read_rows: int, write_rows: int, lookup_count: int) -> int:
    return min(read_rows, lookup_count)


def _page_rows(read_rows: int, write_rows: int, lookup_count: int) -> int:
    return min(read_rows, lookup_count, 1_000)


def _relationship_shape(read_rows: int) -> tuple[int, int, int]:
    parents = min(max(read_rows // 250, 4), 200)
    children_per_parent = 3 if read_rows < 1_000_000 else 5
    leaves_per_child = 2 if read_rows < 1_000_000 else 3
    return parents, children_per_parent, leaves_per_child


def _relationship_rows(read_rows: int, write_rows: int, lookup_count: int) -> int:
    parents, children_per_parent, leaves_per_child = _relationship_shape(read_rows)
    children = parents * children_per_parent
    return parents + children + children * leaves_per_child


def _zero_expected(
    read_rows: int, write_rows: int, lookup_count: int, category: str
) -> int:
    return 0


def _read_rows_expected(
    read_rows: int, write_rows: int, lookup_count: int, category: str
) -> int:
    return read_rows


def _write_rows_expected(
    read_rows: int, write_rows: int, lookup_count: int, category: str
) -> int:
    return write_rows


def _lookup_expected(
    read_rows: int, write_rows: int, lookup_count: int, category: str
) -> int:
    return min(read_rows, lookup_count)


def _category_expected(
    read_rows: int, write_rows: int, lookup_count: int, category: str
) -> int:
    return expected_category_count(read_rows, category)


def _range_expected(
    read_rows: int, write_rows: int, lookup_count: int, category: str
) -> int:
    return expected_score_range_count(read_rows)


def _remaining_after_category(
    read_rows: int, write_rows: int, lookup_count: int, category: str
) -> int:
    return read_rows - expected_category_count(read_rows, category)


def _page_expected(
    read_rows: int, write_rows: int, lookup_count: int, category: str
) -> int:
    return min(read_rows, lookup_count, 1_000)


def _relationship_parent_expected(
    read_rows: int, write_rows: int, lookup_count: int, category: str
) -> int:
    return _relationship_shape(read_rows)[0]


def _relationship_child_expected(
    read_rows: int, write_rows: int, lookup_count: int, category: str
) -> int:
    parents, children_per_parent, _leaves_per_child = _relationship_shape(read_rows)
    return parents * children_per_parent
