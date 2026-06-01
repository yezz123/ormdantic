from __future__ import annotations

import pytest

from ormdantic.naming import snake_case


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
    assert snake_case(source) == expected


def test_python_dict_preserves_bind_parameter_order() -> None:
    params = ["b", "a"]
    values = {"a": 1, "b": 2}

    assert tuple(values[param] for param in params) == (2, 1)
