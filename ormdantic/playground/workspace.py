"""Migration artifact discovery, editing, drafts, and atomic persistence."""

from __future__ import annotations

import json
import os
import re
from collections import Counter
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal

from ormdantic._migrations.artifacts import (
    operation_from_dict,
    operation_to_dict,
)
from ormdantic._migrations.documents import toml_loads
from ormdantic.migrations import MigrationArtifact
from ormdantic.playground.diagnostics import Diagnostic, Severity

ArtifactFormat = Literal["toml", "json"]


@dataclass(frozen=True)
class ArtifactDocument:
    """One migration file and its current editor state."""

    path: Path
    format: ArtifactFormat
    source: str
    artifact: MigrationArtifact | None
    last_valid_artifact: MigrationArtifact | None = None
    diagnostics: tuple[Diagnostic, ...] = ()
    dirty: bool = False


@dataclass(frozen=True)
class MigrationWorkspace:
    """Immutable set of migration documents and selection."""

    documents: tuple[ArtifactDocument, ...] = ()
    selected_path: Path | None = None


def load_workspace(directory: Path) -> MigrationWorkspace:
    """Load every top-level TOML and JSON migration without hiding errors."""
    root = directory.resolve()
    if not root.is_dir():
        return MigrationWorkspace()
    paths = sorted(
        (
            path
            for path in root.iterdir()
            if path.is_file() and path.suffix.casefold() in {".toml", ".json"}
        ),
        key=lambda path: path.name,
    )
    documents = [_load_document(path) for path in paths]
    documents = _annotate_revision_graph(documents)
    return MigrationWorkspace(documents=tuple(documents))


def select_document(
    workspace: MigrationWorkspace,
    path: Path,
) -> MigrationWorkspace:
    """Select a known document without mutating the workspace."""
    resolved = path.resolve()
    if not any(document.path == resolved for document in workspace.documents):
        raise ValueError(f"migration document is not in the workspace: {path}")
    return replace(workspace, selected_path=resolved)


def update_source(document: ArtifactDocument, source: str) -> ArtifactDocument:
    """Parse an edited TOML source while retaining the last valid artifact."""
    if document.format == "json":
        raise ValueError("Convert to TOML before editing a JSON migration artifact")
    last_valid = document.artifact or document.last_valid_artifact
    try:
        artifact = _parse_edited_source(source, document.format)
    except Exception as exc:
        return replace(
            document,
            source=source,
            artifact=None,
            last_valid_artifact=last_valid,
            diagnostics=(
                Diagnostic.create(
                    Severity.ERROR,
                    "artifact.edit_invalid",
                    str(exc),
                    source=str(document.path),
                    hint="Fix the TOML error before saving or running this migration.",
                ),
            ),
            dirty=True,
        )
    return replace(
        document,
        source=source,
        artifact=artifact,
        last_valid_artifact=artifact,
        diagnostics=(),
        dirty=True,
    )


def replace_operation_sql(
    document: ArtifactDocument,
    *,
    index: int,
    sql: str,
    rollback: bool = False,
) -> ArtifactDocument:
    """Replace one SQL operation and regenerate artifact checksum/source."""
    artifact = document.artifact
    if artifact is None:
        raise ValueError("Fix artifact validation errors before editing operation SQL")
    attribute = "rollback_operations" if rollback else "operations"
    operations = [
        operation_from_dict(operation_to_dict(operation))
        for operation in getattr(artifact, attribute)
    ]
    try:
        payload = operation_to_dict(operations[index])
    except IndexError as exc:
        raise IndexError(f"operation index out of range: {index}") from exc
    payload["sql"] = sql
    operations[index] = operation_from_dict(payload)
    updated = replace(artifact, **{attribute: operations}).with_checksum()
    source = updated.to_toml() if document.format == "toml" else updated.to_json()
    return replace(
        document,
        source=source,
        artifact=updated,
        last_valid_artifact=updated,
        diagnostics=(),
        dirty=True,
    )


def convert_to_toml(
    document: ArtifactDocument,
    destination: Path,
    *,
    overwrite: bool = False,
) -> ArtifactDocument:
    """Save a valid JSON artifact as a separate canonical TOML document."""
    if document.artifact is None:
        raise ValueError("Cannot convert an invalid migration artifact")
    artifact = document.artifact.with_checksum()
    converted = ArtifactDocument(
        path=destination.resolve(),
        format="toml",
        source=artifact.to_toml(),
        artifact=artifact,
        last_valid_artifact=artifact,
        dirty=True,
    )
    return save_document(converted, overwrite=overwrite)


def save_document(
    document: ArtifactDocument,
    *,
    destination: Path | None = None,
    overwrite: bool = False,
) -> ArtifactDocument:
    """Atomically save a valid document with a current checksum."""
    artifact = document.artifact
    if artifact is None:
        raise ValueError("Fix artifact validation errors before saving")
    path = (destination or document.path).resolve()
    if path.exists() and path != document.path and not overwrite:
        raise FileExistsError(path)
    format_value: ArtifactFormat = (
        "json" if path.suffix.casefold() == ".json" else "toml"
    )
    artifact = artifact.with_checksum()
    source = artifact.to_json() if format_value == "json" else artifact.to_toml()
    _atomic_write(path, source)
    return ArtifactDocument(
        path=path,
        format=format_value,
        source=source,
        artifact=artifact,
        last_valid_artifact=artifact,
        dirty=False,
    )


def draft_path(root: Path, document: ArtifactDocument) -> Path:
    """Return the project-local recovery path for a document."""
    artifact = document.artifact or document.last_valid_artifact
    identifier = artifact.revision if artifact is not None else document.path.stem
    safe_identifier = re.sub(r"[^A-Za-z0-9_.-]+", "_", identifier).strip(".")
    if not safe_identifier:
        safe_identifier = "untitled"
    return root.resolve() / ".ormdantic" / "drafts" / f"{safe_identifier}.toml"


def write_draft(root: Path, document: ArtifactDocument) -> Path:
    """Atomically persist the current editor source for crash recovery."""
    path = draft_path(root, document)
    _atomic_write(path, document.source)
    return path


def recover_draft(root: Path, document: ArtifactDocument) -> ArtifactDocument:
    """Load the current recovery source back into the editor document."""
    path = draft_path(root, document)
    source = path.read_text(encoding="utf-8")
    if document.format == "json":
        document = replace(
            document, format="toml", path=document.path.with_suffix(".toml")
        )
    return update_source(document, source)


def discard_draft(root: Path, document: ArtifactDocument) -> None:
    """Remove a recovery draft when the caller has confirmed the action."""
    try:
        draft_path(root, document).unlink()
    except FileNotFoundError:
        return


def _load_document(path: Path) -> ArtifactDocument:
    format_value: ArtifactFormat = (
        "json" if path.suffix.casefold() == ".json" else "toml"
    )
    source = path.read_text(encoding="utf-8")
    try:
        artifact = MigrationArtifact.read(path)
    except Exception as exc:
        return ArtifactDocument(
            path=path.resolve(),
            format=format_value,
            source=source,
            artifact=None,
            diagnostics=(
                Diagnostic.create(
                    Severity.ERROR,
                    "artifact.invalid",
                    str(exc),
                    source=str(path),
                    hint="Repair the artifact or restore it from version control.",
                ),
            ),
        )
    return ArtifactDocument(
        path=path.resolve(),
        format=format_value,
        source=source,
        artifact=artifact,
        last_valid_artifact=artifact,
    )


def _parse_edited_source(source: str, format: ArtifactFormat) -> MigrationArtifact:
    payload = json.loads(source) if format == "json" else toml_loads(source)
    payload.pop("checksum", None)
    return MigrationArtifact.from_dict(payload).with_checksum()


def _annotate_revision_graph(
    documents: list[ArtifactDocument],
) -> list[ArtifactDocument]:
    revisions = [
        document.artifact.revision
        for document in documents
        if document.artifact is not None
    ]
    counts = Counter(revisions)
    available = set(revisions)
    annotated: list[ArtifactDocument] = []
    for document in documents:
        artifact = document.artifact
        if artifact is None:
            annotated.append(document)
            continue
        diagnostics = list(document.diagnostics)
        if counts[artifact.revision] > 1:
            diagnostics.append(
                Diagnostic.create(
                    Severity.ERROR,
                    "artifact.duplicate_revision",
                    f"Revision {artifact.revision!r} appears more than once",
                    source=str(document.path),
                )
            )
        missing = [item for item in artifact.depends_on if item not in available]
        if missing:
            diagnostics.append(
                Diagnostic.create(
                    Severity.ERROR,
                    "artifact.missing_dependency",
                    "Missing dependencies: " + ", ".join(missing),
                    source=str(document.path),
                )
            )
        annotated.append(replace(document, diagnostics=tuple(diagnostics)))
    return annotated


def _atomic_write(path: Path, source: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        stream.write(source)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
