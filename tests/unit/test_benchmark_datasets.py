from __future__ import annotations

from benchmark.datasets import (
    expected_category_count,
    expected_score_range_count,
    lookup_ids,
    row_tuple,
    row_tuples,
)


def test_dataset_expected_counts_use_closed_form_math() -> None:
    assert expected_category_count(1_000_000_003, "cat-2") == 100_000_001
    assert expected_category_count(1_000_000_003, "cat-9") == 100_000_000
    assert expected_score_range_count(1_000_000_250, threshold=500) == 500_000_000


def test_lookup_ids_are_stable_and_bounded() -> None:
    assert lookup_ids(rows=10, lookup_count=4) == (
        "item-00000000",
        "item-00000002",
        "item-00000004",
        "item-00000006",
    )
    assert lookup_ids(rows=3, lookup_count=10) == (
        "item-00000000",
        "item-00000001",
        "item-00000002",
    )


def test_row_generation_is_deterministic() -> None:
    assert row_tuple(12) == (
        "item-00000012",
        "cat-2",
        "item-12",
        12,
        "payload-12-12",
    )
    assert list(row_tuples(2, start=10, prefix="write")) == [
        ("write-00000010", "cat-0", "write-10", 10, "payload-10-10"),
        ("write-00000011", "cat-1", "write-11", 11, "payload-11-11"),
    ]
