from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def sqlite_url(tmp_path: Path) -> str:
    return f"sqlite:///{tmp_path / 'ormdantic.sqlite3'}"
