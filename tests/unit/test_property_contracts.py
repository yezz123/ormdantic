from __future__ import annotations

import pytest

from ormdantic.generator._rust_query import bind_compiled_query
from ormdantic.handler.snake import snake


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("Flavor", "flavor"),
        ("CoffeeFlavor", "coffee_flavor"),
        ("coffee_flavor", "coffee_flavor"),
        ("Coffee2Flavor", "coffee2_flavor"),
    ],
)
def test_snake_case_contract(source: str, expected: str) -> None:
    assert snake(source) == expected


def test_bind_compiled_query_preserves_parameter_order() -> None:
    query = bind_compiled_query(
        {"sql": "SELECT * FROM t WHERE a = ? AND b = ?", "params": ["b", "a"], "operation": "select"},
        {"a": 1, "b": 2},
    )

    assert query.values == (2, 1)
