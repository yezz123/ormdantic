from __future__ import annotations

import importlib
from typing import Any

from ormdantic.models import Map

try:
    _ormdantic: Any | None = importlib.import_module("ormdantic._ormdantic")
except ImportError:  # pragma: no cover - exercised when extension is not built
    _ormdantic = None


def validate_table_map(table_map: Map) -> int | None:
    """Validate current Python table metadata through Rust when available."""
    if _ormdantic is None or not hasattr(_ormdantic, "validate_schema_tables"):
        return None

    tables = [
        (table.tablename, table.pk, list(table.columns))
        for table in table_map.name_to_data.values()
    ]
    return int(_ormdantic.validate_schema_tables(tables))
