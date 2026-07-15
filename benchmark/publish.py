from __future__ import annotations

import argparse
import json
import re
import shutil
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any

from benchmark.compare import compare_results, load_result
from benchmark.report import ReportArtifacts, write_pr_report

MAX_ARCHIVE_BYTES = 25_000_000
MAX_FILES = 20
MAX_UNCOMPRESSED_BYTES = 50_000_000
_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class PublicationError(ValueError):
    """An untrusted benchmark artifact failed publication validation."""


def validate_publication_identity(
    *,
    artifact_pr: object,
    artifact_sha: object,
    workflow_pr: object,
    workflow_sha: object,
) -> tuple[int, str]:
    """Match artifact identity to the trusted workflow-run event."""
    for value in (artifact_pr, workflow_pr):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise PublicationError("invalid pull request number")
    if artifact_pr != workflow_pr:
        raise PublicationError("artifact pull request number does not match workflow")
    for value in (artifact_sha, workflow_sha):
        if not isinstance(value, str) or _SHA_PATTERN.fullmatch(value.lower()) is None:
            raise PublicationError("invalid head SHA")
    normalized_artifact_sha = artifact_sha.lower()
    if normalized_artifact_sha != workflow_sha.lower():
        raise PublicationError("artifact head SHA does not match workflow")
    return artifact_pr, normalized_artifact_sha


def safe_extract(
    archive: Path,
    destination: Path,
    *,
    max_archive_bytes: int = MAX_ARCHIVE_BYTES,
    max_files: int = MAX_FILES,
    max_uncompressed_bytes: int = MAX_UNCOMPRESSED_BYTES,
) -> Path:
    """Extract a small ZIP without following links or escaping the destination."""
    try:
        archive_size = archive.stat().st_size
    except OSError as error:
        raise PublicationError(f"cannot read artifact archive: {error}") from error
    if archive_size > max_archive_bytes:
        raise PublicationError("artifact archive exceeds size limit")

    destination.mkdir(parents=True, exist_ok=True)
    root = destination.resolve()
    seen: set[Path] = set()
    total_size = 0
    try:
        bundle = zipfile.ZipFile(archive)
    except (OSError, zipfile.BadZipFile) as error:
        raise PublicationError("artifact is not a valid ZIP archive") from error

    with bundle:
        regular_files = [member for member in bundle.infolist() if not member.is_dir()]
        if len(regular_files) > max_files:
            raise PublicationError("artifact file count exceeds limit")
        for member in bundle.infolist():
            relative = _safe_member_path(member)
            target = (root / relative).resolve()
            if root not in target.parents and target != root:
                raise PublicationError(f"unsafe artifact path: {member.filename!r}")
            if target in seen:
                raise PublicationError(f"duplicate artifact path: {member.filename!r}")
            seen.add(target)
            if member.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            total_size += member.file_size
            if total_size > max_uncompressed_bytes:
                raise PublicationError("artifact uncompressed size exceeds limit")
            target.parent.mkdir(parents=True, exist_ok=True)
            written = 0
            with bundle.open(member) as source, target.open("xb") as output:
                while chunk := source.read(64 * 1024):
                    written += len(chunk)
                    if (
                        written > member.file_size
                        or total_size > max_uncompressed_bytes
                    ):
                        raise PublicationError(
                            "artifact uncompressed size exceeds limit"
                        )
                    output.write(chunk)
            if written != member.file_size:
                raise PublicationError("artifact member size changed during extraction")
    return destination


def regenerate_publication(
    artifact_dir: Path,
    output_dir: Path,
    *,
    workflow_pr: int,
    workflow_sha: str,
) -> ReportArtifacts:
    """Validate untrusted JSON and render all publication files with trusted code."""
    identity = _load_identity(artifact_dir / "identity.json")
    pr_number, head_sha = validate_publication_identity(
        artifact_pr=identity.get("pr"),
        artifact_sha=identity.get("head_sha"),
        workflow_pr=workflow_pr,
        workflow_sha=workflow_sha,
    )
    base_path = artifact_dir / "base.json"
    head_path = artifact_dir / "head.json"
    base = load_result(base_path)
    head = load_result(head_path)
    head_metadata = head.get("metadata")
    if not isinstance(head_metadata, dict):
        raise PublicationError("head benchmark metadata must be an object")
    if str(head_metadata.get("git_commit", "")).lower() != head_sha:
        raise PublicationError("head benchmark commit does not match artifact head SHA")

    try:
        report = compare_results(base, head)
    except (TypeError, ValueError) as error:
        raise PublicationError(f"invalid benchmark result: {error}") from error
    artifacts = write_pr_report(report, output_dir)
    shutil.copyfile(base_path, output_dir / "base.json")
    shutil.copyfile(head_path, output_dir / "head.json")
    (output_dir / "identity.json").write_text(
        json.dumps({"pr": pr_number, "head_sha": head_sha}, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return artifacts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate and regenerate an untrusted benchmark artifact"
    )
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--extract-dir", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--workflow-pr", required=True, type=int)
    parser.add_argument("--workflow-sha", required=True)
    args = parser.parse_args(argv)
    artifact_dir = safe_extract(args.archive, args.extract_dir)
    regenerate_publication(
        artifact_dir,
        args.output_dir,
        workflow_pr=args.workflow_pr,
        workflow_sha=args.workflow_sha,
    )
    return 0


def _safe_member_path(member: zipfile.ZipInfo) -> Path:
    name = member.filename
    if not name or "\\" in name or "\x00" in name:
        raise PublicationError(f"unsafe artifact path: {name!r}")
    path = PurePosixPath(name)
    if path.is_absolute() or any(part in ("", ".", "..") for part in path.parts):
        raise PublicationError(f"unsafe artifact path: {name!r}")
    mode = member.external_attr >> 16
    if mode & 0o170000 == 0o120000:
        raise PublicationError(f"artifact symlink is not allowed: {name!r}")
    return Path(*path.parts)


def _load_identity(path: Path) -> dict[str, Any]:
    if path.stat().st_size > 1_024:
        raise PublicationError("identity file exceeds size limit")
    try:
        identity = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PublicationError("identity file is invalid") from error
    if not isinstance(identity, dict):
        raise PublicationError("identity file must contain an object")
    return identity


if __name__ == "__main__":
    raise SystemExit(main())
