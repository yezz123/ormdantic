from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Mapping

SUPPORTED_BACKENDS = ("sqlite", "postgres", "mysql")
DEFAULT_BATCH_SIZE = 5_000


class BenchmarkConfigurationError(ValueError):
    """Raised when a benchmark configuration is invalid or unsafe."""


@dataclass(frozen=True)
class Profile:
    """Named benchmark scale profile."""

    name: str
    rows: int
    write_rows: int
    lookup_count: int
    iterations: int
    warmups: int

    @property
    def settings(self) -> dict[str, int]:
        return {
            "rows": self.rows,
            "write_rows": self.write_rows,
            "lookup_count": self.lookup_count,
            "iterations": self.iterations,
            "warmups": self.warmups,
        }

    @classmethod
    def for_name(cls, name: str) -> "Profile":
        normalized = name.lower()
        if normalized == "huge":
            normalized = "million"
        try:
            return PROFILES[normalized]
        except KeyError as exc:
            choices = ", ".join(PROFILES)
            raise BenchmarkConfigurationError(
                f"profile must be one of: {choices}"
            ) from exc


PROFILES: Mapping[str, Profile] = {
    "smoke": Profile(
        name="smoke",
        rows=1_000,
        write_rows=1_000,
        lookup_count=100,
        iterations=1,
        warmups=0,
    ),
    "default": Profile(
        name="default",
        rows=20_000,
        write_rows=20_000,
        lookup_count=1_000,
        iterations=5,
        warmups=1,
    ),
    "million": Profile(
        name="million",
        rows=1_000_000,
        write_rows=1_000_000,
        lookup_count=10_000,
        iterations=1,
        warmups=0,
    ),
    "large": Profile(
        name="large",
        rows=10_000_000,
        write_rows=1_000_000,
        lookup_count=50_000,
        iterations=1,
        warmups=0,
    ),
    "billion": Profile(
        name="billion",
        rows=1_000_000_000,
        write_rows=10_000_000,
        lookup_count=100_000,
        iterations=1,
        warmups=0,
    ),
}


@dataclass(frozen=True)
class BenchmarkConfig:
    """Resolved benchmark settings for one backend/profile run."""

    backend: str
    profile: str
    rows: int
    write_rows: int
    lookup_count: int
    iterations: int
    warmups: int
    batch_size: int = DEFAULT_BATCH_SIZE
    category: str = "cat-3"
    materialized: bool = True
    planner_scale: bool = False


def build_config(
    *,
    profile: str,
    backend: str,
    rows: int | None = None,
    write_rows: int | None = None,
    lookup_count: int | None = None,
    iterations: int | None = None,
    warmups: int | None = None,
    batch_size: int | None = None,
    category: str | None = None,
    planner_scale: bool = False,
    i_understand_this_may_be_expensive: bool = False,
) -> BenchmarkConfig:
    """Build and validate benchmark settings from CLI/user inputs."""
    normalized_backend = backend.lower()
    if normalized_backend not in SUPPORTED_BACKENDS:
        choices = ", ".join(SUPPORTED_BACKENDS)
        raise BenchmarkConfigurationError(f"backend must be one of: {choices}")

    selected = Profile.for_name(profile)
    if selected.name == "billion" and not i_understand_this_may_be_expensive:
        raise BenchmarkConfigurationError(
            "The billion profile may require substantial disk, time, and Docker "
            "resources. Re-run with --i-understand-this-may-be-expensive."
        )

    config = BenchmarkConfig(
        backend=normalized_backend,
        profile=selected.name,
        rows=selected.rows,
        write_rows=selected.write_rows,
        lookup_count=selected.lookup_count,
        iterations=selected.iterations,
        warmups=selected.warmups,
        planner_scale=planner_scale,
        materialized=not planner_scale,
    )
    overrides = {
        "rows": rows,
        "write_rows": write_rows,
        "lookup_count": lookup_count,
        "iterations": iterations,
        "warmups": warmups,
        "batch_size": batch_size,
        "category": category,
    }
    config = replace(
        config,
        **{key: value for key, value in overrides.items() if value is not None},
    )
    validate_config(config)
    return config


def validate_config(config: BenchmarkConfig) -> None:
    """Validate positive numeric settings and supported labels."""
    if config.backend not in SUPPORTED_BACKENDS:
        choices = ", ".join(SUPPORTED_BACKENDS)
        raise BenchmarkConfigurationError(f"backend must be one of: {choices}")
    if config.rows <= 0:
        raise BenchmarkConfigurationError("rows must be greater than zero")
    if config.write_rows <= 0:
        raise BenchmarkConfigurationError("write_rows must be greater than zero")
    if config.lookup_count <= 0:
        raise BenchmarkConfigurationError("lookup_count must be greater than zero")
    if config.iterations <= 0:
        raise BenchmarkConfigurationError("iterations must be greater than zero")
    if config.warmups < 0:
        raise BenchmarkConfigurationError("warmups cannot be negative")
    if config.batch_size <= 0:
        raise BenchmarkConfigurationError("batch_size must be greater than zero")
    if not config.category.startswith("cat-"):
        raise BenchmarkConfigurationError("category must use the cat-N benchmark form")
