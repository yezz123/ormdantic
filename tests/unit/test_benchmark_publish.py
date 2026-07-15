from __future__ import annotations

import json
import stat
import zipfile
from pathlib import Path

import pytest

from benchmark.publish import (
    PublicationError,
    regenerate_publication,
    safe_extract,
    validate_publication_identity,
)


def _archive(tmp_path: Path, members: dict[str, bytes]) -> Path:
    archive = tmp_path / "artifact.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        for name, contents in members.items():
            bundle.writestr(name, contents)
    return archive


def test_validate_publication_identity_rejects_mismatched_sha() -> None:
    with pytest.raises(PublicationError, match="head SHA"):
        validate_publication_identity(
            artifact_pr=42,
            artifact_sha="a" * 40,
            workflow_pr=42,
            workflow_sha="b" * 40,
        )


@pytest.mark.parametrize("artifact_pr", [0, -1, True, "42"])
def test_validate_publication_identity_rejects_invalid_pr(artifact_pr) -> None:
    with pytest.raises(PublicationError, match="pull request number"):
        validate_publication_identity(
            artifact_pr=artifact_pr,
            artifact_sha="a" * 40,
            workflow_pr=42,
            workflow_sha="a" * 40,
        )


def test_validate_publication_identity_rejects_invalid_sha() -> None:
    with pytest.raises(PublicationError, match="head SHA"):
        validate_publication_identity(
            artifact_pr=42,
            artifact_sha="not-a-sha",
            workflow_pr=42,
            workflow_sha="a" * 40,
        )


def test_safe_extract_rejects_path_traversal(tmp_path: Path) -> None:
    archive = _archive(tmp_path, {"../../owned": b"bad"})

    with pytest.raises(PublicationError, match="unsafe artifact path"):
        safe_extract(archive, tmp_path / "output")


def test_safe_extract_rejects_symlink(tmp_path: Path) -> None:
    archive = tmp_path / "artifact.zip"
    info = zipfile.ZipInfo("report.svg")
    info.create_system = 3
    info.external_attr = (stat.S_IFLNK | 0o777) << 16
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr(info, "target")

    with pytest.raises(PublicationError, match="symlink"):
        safe_extract(archive, tmp_path / "output")


def test_safe_extract_enforces_file_count_and_size(tmp_path: Path) -> None:
    too_many = _archive(
        tmp_path,
        {f"file-{index}.json": b"{}" for index in range(3)},
    )
    with pytest.raises(PublicationError, match="file count"):
        safe_extract(too_many, tmp_path / "many", max_files=2)

    too_large = _archive(tmp_path, {"large.json": b"12345"})
    with pytest.raises(PublicationError, match="uncompressed size"):
        safe_extract(too_large, tmp_path / "large", max_uncompressed_bytes=4)


def test_safe_extract_writes_regular_files(tmp_path: Path) -> None:
    identity = json.dumps({"pr": 42, "head_sha": "a" * 40}).encode()
    archive = _archive(
        tmp_path,
        {"identity.json": identity, "results/base.json": b"{}"},
    )

    output = safe_extract(archive, tmp_path / "output")

    assert (output / "identity.json").read_bytes() == identity
    assert (output / "results/base.json").read_bytes() == b"{}"


def test_regenerate_publication_ignores_untrusted_rendered_files(
    tmp_path: Path,
) -> None:
    head_sha = "a" * 40
    artifact = tmp_path / "artifact"
    artifact.mkdir()
    (artifact / "identity.json").write_text(
        json.dumps({"pr": 42, "head_sha": head_sha}), encoding="utf-8"
    )
    (artifact / "base.json").write_text(
        json.dumps(_result_payload("b" * 40, 10.0)), encoding="utf-8"
    )
    (artifact / "head.json").write_text(
        json.dumps(_result_payload(head_sha, 8.0)), encoding="utf-8"
    )
    (artifact / "report.svg").write_text(
        "<script>untrusted()</script>", encoding="utf-8"
    )

    artifacts = regenerate_publication(
        artifact,
        tmp_path / "trusted",
        workflow_pr=42,
        workflow_sha=head_sha,
    )

    svg = artifacts.svg.read_text(encoding="utf-8")
    assert "untrusted" not in svg
    assert "Ormdantic pull request benchmark" in svg
    assert (tmp_path / "trusted/base.json").exists()


def _result_payload(commit: str, ormdantic_ms: float) -> dict[str, object]:
    measurements = []
    for orm, median in (
        ("ormdantic", ormdantic_ms),
        ("sqlalchemy", 16.0),
        ("sqlmodel", 24.0),
    ):
        measurements.append(
            {
                "backend": "sqlite",
                "profile": "ci",
                "case": "count all rows",
                "rows": 10_000,
                "orm": orm,
                "median_ms": median,
                "samples_ms": [median] * 7,
                "comparable": True,
            }
        )
    return {
        "schema_version": 2,
        "metadata": {"git_commit": commit, "backend": "sqlite"},
        "config": {"profile": "ci"},
        "measurements": measurements,
    }
