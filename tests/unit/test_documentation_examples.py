from __future__ import annotations

import re
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[2]
PLAYGROUND_DOCS = ROOT / "docs" / "playground"
TOML_BLOCK = re.compile(r"```toml\n(.*?)```", re.DOTALL)
OBSOLETE_COLUMNS = re.compile(r"^ {4}columns\s*=\s*\{", re.MULTILINE)


def test_every_playground_toml_block_parses() -> None:
    paths = sorted(PLAYGROUND_DOCS.glob("*.md"))
    assert paths
    blocks = [
        (path, block)
        for path in paths
        for block in TOML_BLOCK.findall(path.read_text(encoding="utf-8"))
    ]
    assert blocks
    for path, block in blocks:
        try:
            tomllib.loads(block)
        except tomllib.TOMLDecodeError as exc:
            raise AssertionError(f"invalid TOML example in {path}: {exc}") from exc


def test_playground_navigation_targets_exist() -> None:
    source = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    expected = {
        "playground/index.md",
        "playground/configuration.md",
        "playground/schema-watching.md",
        "playground/migration-workflows.md",
        "playground/editor.md",
        "playground/safety.md",
        "playground/troubleshooting.md",
        "api/playground.md",
    }
    for target in expected:
        assert target in source
        assert (ROOT / "docs" / target).is_file()


def test_user_documentation_does_not_use_obsolete_table_columns_keyword() -> None:
    paths = [
        path
        for path in (ROOT / "docs").rglob("*.md")
        if "superpowers" not in path.parts and "development" not in path.parts
    ]
    violations = [
        path
        for path in paths
        if OBSOLETE_COLUMNS.search(path.read_text(encoding="utf-8"))
    ]
    assert violations == []
