from __future__ import annotations

import re
from pathlib import Path

from PIL import Image

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 compatibility
    import tomli as tomllib

ROOT = Path(__file__).resolve().parents[2]
PLAYGROUND_DOCS = ROOT / "docs" / "playground"
TOML_BLOCK = re.compile(r"```toml\n(.*?)```", re.DOTALL)
OBSOLETE_COLUMNS = re.compile(r"^ {4}columns\s*=\s*\{", re.MULTILINE)
SNIPPET = re.compile(r'--8<--\s+"([^"]+)"')
TUTORIAL_PAGES = {
    "tutorial/index.md",
    "tutorial/setup.md",
    "tutorial/configuration.md",
    "tutorial/models.md",
    "tutorial/crud-and-queries.md",
    "tutorial/migrations.md",
    "tutorial/postgresql.md",
    "tutorial/testing.md",
    "tutorial/production-checklist.md",
}


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


def test_todo_tutorial_navigation_and_source_includes_are_complete() -> None:
    navigation = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    old_targets = {
        "examples/basic-crud.md",
        "examples/relationships.md",
        "examples/transactions-sessions.md",
        "examples/query-expressions.md",
        "examples/migrations-reflection.md",
        "examples/enterprise-dialects.md",
    }

    for target in TUTORIAL_PAGES:
        assert target in navigation
        page = ROOT / "docs" / target
        assert page.is_file()
        for include in SNIPPET.findall(page.read_text(encoding="utf-8")):
            assert (ROOT / include).is_file(), f"missing include {include} in {target}"
    for target in old_targets:
        assert target not in navigation


def test_tutorial_uses_real_images_and_named_mermaid_diagrams() -> None:
    tutorial = "\n".join(
        (ROOT / "docs" / target).read_text(encoding="utf-8")
        for target in TUTORIAL_PAGES
    )
    for filename in ("openapi-overview.png", "playground-overview.png"):
        image_path = ROOT / "docs" / "assets" / "tutorial" / filename
        assert image_path.is_file()
        with Image.open(image_path) as image:
            assert image.width >= 1200
            assert image.height >= 700
        assert f"assets/tutorial/{filename}" in tutorial

    for diagram in (
        "architecture",
        "entities",
        "request flow",
        "migration flow",
        "Compose startup",
    ):
        assert f"%% {diagram}" in tutorial


def test_documentation_matrix_has_one_canonical_target_per_surface() -> None:
    matrix = ROOT / "docs" / "development" / "documentation-matrix.md"
    source = matrix.read_text(encoding="utf-8")
    required = {
        "public:Ormdantic",
        "public:Table",
        "public:QueryExpression",
        "cli:migrations",
        "cli:playground",
        "driver:sqlite",
        "driver:postgresql",
        "driver:mysql",
        "driver:mariadb",
        "driver:mssql",
        "driver:oracle",
        "playground:schema-watching",
        "playground:migration-workflows",
        "playground:editor",
        "playground:safety",
    }
    for key in required:
        assert source.count(f"`{key}`") == 1
    assert "development/documentation-matrix.md" in (ROOT / "mkdocs.yml").read_text(
        encoding="utf-8"
    )
