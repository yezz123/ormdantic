from __future__ import annotations

import pytest

from benchmark.config import (
    BenchmarkConfig,
    BenchmarkConfigurationError,
    Profile,
    build_config,
)


def test_named_profiles_match_cross_database_design() -> None:
    assert Profile.for_name("smoke").settings == {
        "rows": 1_000,
        "write_rows": 1_000,
        "lookup_count": 100,
        "iterations": 1,
        "warmups": 0,
    }
    assert Profile.for_name("default").settings == {
        "rows": 20_000,
        "write_rows": 20_000,
        "lookup_count": 1_000,
        "iterations": 5,
        "warmups": 1,
    }
    assert Profile.for_name("million").settings["rows"] == 1_000_000
    assert Profile.for_name("large").settings == {
        "rows": 10_000_000,
        "write_rows": 1_000_000,
        "lookup_count": 50_000,
        "iterations": 1,
        "warmups": 0,
    }
    assert Profile.for_name("billion").settings == {
        "rows": 1_000_000_000,
        "write_rows": 10_000_000,
        "lookup_count": 100_000,
        "iterations": 1,
        "warmups": 0,
    }


def test_billion_profile_requires_explicit_confirmation() -> None:
    with pytest.raises(BenchmarkConfigurationError, match="billion"):
        build_config(profile="billion", backend="sqlite")

    config = build_config(
        profile="billion",
        backend="sqlite",
        i_understand_this_may_be_expensive=True,
    )

    assert config.profile == "billion"
    assert config.materialized is True
    assert config.planner_scale is False


def test_billion_planner_scale_is_labeled_without_materializing_rows() -> None:
    config = build_config(
        profile="billion",
        backend="postgres",
        planner_scale=True,
        i_understand_this_may_be_expensive=True,
    )

    assert config.materialized is False
    assert config.planner_scale is True


def test_config_overrides_are_validated() -> None:
    config = build_config(
        profile="smoke",
        backend="mysql",
        rows=25,
        write_rows=12,
        lookup_count=5,
        iterations=2,
        warmups=1,
        batch_size=7,
    )

    assert config == BenchmarkConfig(
        backend="mysql",
        profile="smoke",
        rows=25,
        write_rows=12,
        lookup_count=5,
        iterations=2,
        warmups=1,
        batch_size=7,
        materialized=True,
        planner_scale=False,
    )

    with pytest.raises(BenchmarkConfigurationError, match="rows"):
        build_config(profile="smoke", backend="sqlite", rows=0)

    with pytest.raises(BenchmarkConfigurationError, match="backend"):
        build_config(profile="smoke", backend="oracle")
