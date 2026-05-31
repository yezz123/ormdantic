"""Private hydration bridge with Rust fast paths and Python fallbacks."""

from __future__ import annotations

import importlib
from collections import OrderedDict
from typing import Any, cast

try:
    _ormdantic: Any | None = importlib.import_module("ormdantic._ormdantic")
except ImportError:  # pragma: no cover - exercised when extension is not built
    _ormdantic = None


def hydrate_flat_payload(
    *,
    tablename: str,
    pk: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
    is_array: bool,
) -> dict[str, Any] | list[dict[str, Any]] | None:
    """Hydrate flat SQL rows into dict payloads.

    The Rust extension owns the fast path when it is available. The Python
    fallback keeps source checkouts usable before the extension is built.
    """
    if _ormdantic is not None:
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
    return _hydrate_flat_payload_python(
        tablename=tablename,
        pk=pk,
        columns=columns,
        rows=rows,
        is_array=is_array,
    )


def hydrate_joined_payload(
    *,
    columns: list[str],
    rows: list[tuple[Any, ...]],
    path_pks: list[tuple[str, str]],
    array_paths: list[str],
) -> dict[str, Any] | None:
    """Hydrate joined SQL rows into the serializer's nested dict payload."""
    if _ormdantic is None or not hasattr(_ormdantic, "hydrate_joined"):
        raise RuntimeError(
            "Ormdantic vNext requires the Rust extension for joined hydration. "
            "Install the package with maturin or reinstall the wheel."
        )
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
    if _ormdantic is not None and hasattr(_ormdantic, "plan_result_shape"):
        return cast(
            dict[str, Any],
            _ormdantic.plan_result_shape(root_table, columns, array_paths),
        )
    return _plan_result_shape_python(root_table, columns, array_paths)


def _hydrate_flat_payload_python(
    *,
    tablename: str,
    pk: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
    is_array: bool,
) -> dict[str, Any] | list[dict[str, Any]] | None:
    parsed_columns = [_parse_column_alias(alias, tablename) for alias in columns]
    pk_idx = _get_pk_index(parsed_columns, tablename, pk)

    if not rows:
        return None

    if not is_array:
        return _row_to_dict(rows[0], parsed_columns)

    records: OrderedDict[Any, dict[str, Any]] = OrderedDict()
    for row in rows:
        pk_value = row[pk_idx]
        if pk_value not in records:
            records[pk_value] = _row_to_dict(row, parsed_columns)
    return list(records.values())


def _plan_result_shape_python(
    root_table: str, columns: list[str], array_paths: list[str]
) -> dict[str, Any]:
    parsed_columns = []
    relationship_paths = set()
    for alias in columns:
        table_path, _, column = alias.partition("\\")
        if not column:
            continue
        parsed_columns.append(
            {"alias": alias, "table_path": table_path, "column": column}
        )
        if table_path == root_table:
            continue
        current = ""
        for idx, branch in enumerate(table_path.split("/")):
            current = branch if idx == 0 else f"{current}/{branch}"
            if idx > 0:
                relationship_paths.add(current)
    return {
        "root_table": root_table,
        "columns": parsed_columns,
        "relationship_paths": sorted(relationship_paths),
        "array_paths": array_paths,
    }


def _parse_column_alias(alias: str, tablename: str) -> str | None:
    column_tree, _, column = alias.partition("\\")
    if column_tree != tablename or not column:
        return None
    return column


def _get_pk_index(parsed_columns: list[str | None], tablename: str, pk: str) -> int:
    try:
        return parsed_columns.index(pk)
    except ValueError as exc:
        raise ValueError(
            f"primary key column '{tablename}\\{pk}' was not found"
        ) from exc


def _row_to_dict(
    row: tuple[Any, ...], parsed_columns: list[str | None]
) -> dict[str, Any]:
    return {
        column: row[idx]
        for idx, column in enumerate(parsed_columns)
        if column is not None
    }
