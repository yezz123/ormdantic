"""Hydration helpers backed by Rust."""

from __future__ import annotations

import importlib
from typing import Any, cast

_ormdantic: Any = importlib.import_module("ormdantic._ormdantic")


def hydrate_flat_payload(
    *,
    tablename: str,
    pk: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
    is_array: bool,
) -> dict[str, Any] | list[dict[str, Any]] | None:
    """Hydrate flat SQL rows into dict payloads."""
    return cast(
        dict[str, Any] | list[dict[str, Any]] | None,
        _ormdantic.hydrate_flat(
            tablename,
            pk,
            columns,
            [list(row) for row in rows],
            is_array,
        ),
    )


def hydrate_joined_payload(
    *,
    columns: list[str],
    rows: list[tuple[Any, ...]],
    path_pks: list[tuple[str, str]],
    array_paths: list[str],
) -> dict[str, Any] | None:
    """Hydrate joined SQL rows into nested dict payloads."""
    payload = cast(
        dict[str, Any],
        _ormdantic.hydrate_joined(
            columns,
            [list(row) for row in rows],
            path_pks,
            array_paths,
        ),
    )
    return payload or None


def plan_result_shape(
    *,
    root_table: str,
    columns: list[str],
    array_paths: list[str] | None = None,
) -> dict[str, Any]:
    """Describe joined result aliases with Rust when available."""
    array_paths = array_paths or []
    return cast(
        dict[str, Any],
        _ormdantic.plan_result_shape(root_table, columns, array_paths),
    )
