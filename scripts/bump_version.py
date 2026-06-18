#!/usr/bin/env python3
"""Bump Ormdantic's Python and Rust package versions together."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 fallback
    import tomli as tomllib

import typer
from rich.console import Console
from rich.table import Table

ROOT = Path(__file__).resolve().parents[1]
PY_INIT = ROOT / "ormdantic" / "__init__.py"
CARGO_TOML = ROOT / "Cargo.toml"
CARGO_LOCK = ROOT / "Cargo.lock"

ORMDANTIC_CRATES = (
    "ormdantic-core",
    "ormdantic-schema",
    "ormdantic-hydrate",
    "ormdantic-dialects",
    "ormdantic-engine",
    "ormdantic-sql",
    "ormdantic-py",
)
WORKSPACE_DEPENDENCY_CRATES = tuple(
    crate for crate in ORMDANTIC_CRATES if crate != "ormdantic-py"
)
STABLE_SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")

console = Console()


class BumpPart(str, Enum):
    major = "major"
    minor = "minor"
    patch = "patch"


@dataclass(frozen=True)
class VersionEntry:
    location: str
    version: str


@dataclass(frozen=True)
class FileUpdate:
    path: Path
    old: str
    new: str
    replacements: int


def parse_version(version: str) -> tuple[int, int, int]:
    if not STABLE_SEMVER_RE.fullmatch(version):
        raise typer.BadParameter(
            "Use a stable SemVer version like 2.1.0. "
            "The same value is published to both PyPI and crates.io."
        )

    major, minor, patch = version.split(".")
    return int(major), int(minor), int(patch)


def bump_version(version: str, part: BumpPart) -> str:
    major, minor, patch = parse_version(version)
    if part is BumpPart.major:
        return f"{major + 1}.0.0"
    if part is BumpPart.minor:
        return f"{major}.{minor + 1}.0"
    return f"{major}.{minor}.{patch + 1}"


def read_python_version() -> str:
    match = re.search(r'^__version__ = "([^"]+)"$', PY_INIT.read_text(), re.MULTILINE)
    if match is None:
        raise RuntimeError(f"Could not find __version__ in {PY_INIT.relative_to(ROOT)}")
    return match.group(1)


def read_cargo_workspace() -> dict[str, object]:
    with CARGO_TOML.open("rb") as cargo_toml:
        return tomllib.load(cargo_toml)["workspace"]


def collect_versions() -> list[VersionEntry]:
    workspace = read_cargo_workspace()
    entries = [
        VersionEntry("ormdantic/__init__.py __version__", read_python_version()),
        VersionEntry(
            "Cargo.toml workspace.package.version", workspace["package"]["version"]
        ),
    ]

    dependencies = workspace["dependencies"]
    for crate in ORMDANTIC_CRATES:
        dependency = dependencies.get(crate)
        if isinstance(dependency, dict) and "version" in dependency:
            entries.append(
                VersionEntry(
                    f"Cargo.toml workspace.dependencies.{crate}", dependency["version"]
                )
            )

    if CARGO_LOCK.exists():
        entries.extend(read_cargo_lock_versions())

    return entries


def read_cargo_lock_versions() -> list[VersionEntry]:
    entries: list[VersionEntry] = []
    current_package: Optional[str] = None

    for line in CARGO_LOCK.read_text().splitlines():
        if line == "[[package]]":
            current_package = None
            continue

        name_match = re.fullmatch(r'name = "([^"]+)"', line)
        if name_match is not None:
            current_package = name_match.group(1)
            continue

        version_match = re.fullmatch(r'version = "([^"]+)"', line)
        if version_match is not None and current_package in ORMDANTIC_CRATES:
            entries.append(
                VersionEntry(
                    f"Cargo.lock package.{current_package}", version_match.group(1)
                )
            )

    return entries


def render_versions(
    entries: list[VersionEntry], expected: Optional[str] = None
) -> None:
    table = Table(title="Ormdantic versions")
    table.add_column("Location", style="cyan")
    table.add_column("Version", style="green")
    if expected is not None:
        table.add_column("Status")

    for entry in entries:
        if expected is None:
            table.add_row(entry.location, entry.version)
        else:
            status = (
                "[green]ok[/green]"
                if entry.version == expected
                else "[red]mismatch[/red]"
            )
            table.add_row(entry.location, entry.version, status)

    console.print(table)


def ensure_consistent(
    entries: list[VersionEntry], expected: Optional[str] = None
) -> None:
    expected_version = expected or entries[0].version
    mismatches = [entry for entry in entries if entry.version != expected_version]
    if mismatches:
        render_versions(entries, expected_version)
        raise typer.Exit(1)


def replace_once(
    pattern: str, replacement: str, text: str, path: Path
) -> tuple[str, int]:
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.MULTILINE)
    if count != 1:
        raise RuntimeError(
            f"Expected exactly one replacement in {path.relative_to(ROOT)}"
        )
    return updated, count


def update_python_init(version: str) -> FileUpdate:
    old = PY_INIT.read_text()
    new, count = replace_once(
        r'^__version__ = "[^"]+"$',
        f'__version__ = "{version}"',
        old,
        PY_INIT,
    )
    return FileUpdate(PY_INIT, old, new, count)


def update_cargo_toml(version: str) -> FileUpdate:
    old = CARGO_TOML.read_text()
    lines = old.splitlines(keepends=True)
    current_section: Optional[str] = None
    replacements = 0
    updated_lines: list[str] = []

    for line in lines:
        section_match = re.match(r"\[([^\]]+)\]\s*$", line.strip())
        if section_match is not None:
            current_section = section_match.group(1)

        if current_section == "workspace.package":
            updated_line, count = re.subn(
                r'^(version\s*=\s*)"[^"]+"', rf'\g<1>"{version}"', line
            )
            replacements += count
            updated_lines.append(updated_line)
            continue

        if current_section == "workspace.dependencies":
            stripped = line.lstrip()
            crate = stripped.split("=", 1)[0].strip() if "=" in stripped else ""
            if crate in WORKSPACE_DEPENDENCY_CRATES:
                updated_line, count = re.subn(
                    r'(version\s*=\s*)"[^"]+"', rf'\g<1>"{version}"', line
                )
                replacements += count
                updated_lines.append(updated_line)
                continue

        updated_lines.append(line)

    expected_replacements = 1 + len(WORKSPACE_DEPENDENCY_CRATES)
    if replacements != expected_replacements:
        raise RuntimeError(
            f"Expected {expected_replacements} replacements in {CARGO_TOML.relative_to(ROOT)}, got {replacements}"
        )

    return FileUpdate(CARGO_TOML, old, "".join(updated_lines), replacements)


def update_cargo_lock(version: str) -> Optional[FileUpdate]:
    if not CARGO_LOCK.exists():
        return None

    old = CARGO_LOCK.read_text()
    lines = old.splitlines(keepends=True)
    current_package: Optional[str] = None
    replacements = 0
    updated_lines: list[str] = []

    for line in lines:
        if line.strip() == "[[package]]":
            current_package = None
            updated_lines.append(line)
            continue

        name_match = re.match(r'name = "([^"]+)"\s*$', line.strip())
        if name_match is not None:
            current_package = name_match.group(1)
            updated_lines.append(line)
            continue

        if current_package in ORMDANTIC_CRATES:
            updated_line, count = re.subn(
                r'^(version\s*=\s*)"[^"]+"', rf'\g<1>"{version}"', line
            )
            replacements += count
            updated_lines.append(updated_line)
            continue

        updated_lines.append(line)

    if replacements != len(ORMDANTIC_CRATES):
        raise RuntimeError(
            f"Expected {len(ORMDANTIC_CRATES)} replacements in {CARGO_LOCK.relative_to(ROOT)}, got {replacements}"
        )

    return FileUpdate(CARGO_LOCK, old, "".join(updated_lines), replacements)


def build_updates(version: str) -> list[FileUpdate]:
    updates = [update_python_init(version), update_cargo_toml(version)]
    cargo_lock_update = update_cargo_lock(version)
    if cargo_lock_update is not None:
        updates.append(cargo_lock_update)
    return updates


def render_updates(updates: list[FileUpdate]) -> None:
    table = Table(title="Planned version updates")
    table.add_column("File", style="cyan")
    table.add_column("Replacements", justify="right", style="green")

    for update in updates:
        table.add_row(str(update.path.relative_to(ROOT)), str(update.replacements))

    console.print(table)


def write_updates(updates: list[FileUpdate]) -> None:
    for update in updates:
        if update.old != update.new:
            update.path.write_text(update.new)


def main(
    version: Optional[str] = typer.Argument(
        None,
        help="Version to set, for example 2.1.0. Omit when using --part or --check.",
    ),
    part: Optional[BumpPart] = typer.Option(
        None,
        "--part",
        "-p",
        case_sensitive=False,
        help="Bump from the current version by major, minor, or patch.",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would change without writing files."
    ),
    check: bool = typer.Option(
        False, "--check", help="Check version consistency without writing files."
    ),
) -> None:
    current_entries = collect_versions()
    ensure_consistent(current_entries)
    current_version = current_entries[0].version

    if version is not None and part is not None:
        raise typer.BadParameter("Pass either VERSION or --part, not both.")

    target_version = version
    if part is not None:
        target_version = bump_version(current_version, part)

    if check:
        expected = target_version or current_version
        parse_version(expected)
        ensure_consistent(current_entries, expected)
        render_versions(current_entries, expected)
        console.print(f"[green]All versions are consistent at {expected}.[/green]")
        return

    if target_version is None:
        raise typer.BadParameter("Pass VERSION, --part, or --check.")

    parse_version(target_version)
    updates = build_updates(target_version)
    render_updates(updates)

    if dry_run:
        console.print(
            f"[yellow]Dry run only. Target version: {target_version}[/yellow]"
        )
        return

    write_updates(updates)
    render_versions(collect_versions(), target_version)
    console.print(f"[green]Bumped Ormdantic to {target_version}.[/green]")


if __name__ == "__main__":
    typer.run(main)
