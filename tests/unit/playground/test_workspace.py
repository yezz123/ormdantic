from __future__ import annotations

from pathlib import Path

import pytest

from ormdantic.migrations import (
    MigrationArtifact,
    MigrationOperation,
    MigrationPlan,
    SchemaSnapshot,
)
from ormdantic.playground.workspace import (
    convert_to_toml,
    discard_draft,
    draft_path,
    load_workspace,
    recover_draft,
    replace_operation_sql,
    save_document,
    select_document,
    update_source,
    write_draft,
)


def artifact(
    revision: str,
    *,
    depends_on: tuple[str, ...] = (),
    sql: str = "CREATE TABLE users (id INTEGER)",
) -> MigrationArtifact:
    plan = MigrationPlan(
        operations=[MigrationOperation(sql=sql, kind="create_table")],
        rollback_operations=[
            MigrationOperation(
                sql="DROP TABLE users",
                kind="drop_table",
                destructive=True,
            )
        ],
    )
    return MigrationArtifact.from_plan(
        revision,
        plan,
        SchemaSnapshot.empty(),
        SchemaSnapshot.empty(),
        dialect="sqlite",
        depends_on=depends_on,
    )


def test_load_workspace_discovers_json_and_toml_in_filename_order(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "migrations"
    artifact("002_second", depends_on=("001_initial",)).write(
        directory / "002_second.json"
    )
    artifact("001_initial").write(directory / "001_initial.toml")
    (directory / "README.md").write_text("ignored")

    workspace = load_workspace(directory)

    assert [item.path.name for item in workspace.documents] == [
        "001_initial.toml",
        "002_second.json",
    ]
    assert [item.format for item in workspace.documents] == ["toml", "json"]
    assert all(item.artifact is not None for item in workspace.documents)


def test_load_workspace_keeps_invalid_files_as_diagnostics(tmp_path: Path) -> None:
    directory = tmp_path / "migrations"
    directory.mkdir()
    (directory / "001_broken.toml").write_text("revision = [")
    artifact("002_valid").write(directory / "002_valid.toml")

    workspace = load_workspace(directory)

    assert len(workspace.documents) == 2
    broken = workspace.documents[0]
    assert broken.artifact is None
    assert broken.diagnostics[0].code == "artifact.invalid"
    assert workspace.documents[1].artifact is not None


def test_load_workspace_reports_duplicates_and_missing_dependencies(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "migrations"
    artifact("001_same").write(directory / "001_a.toml")
    artifact("001_same").write(directory / "001_b.json")
    artifact("002_next", depends_on=("000_missing",)).write(directory / "002_next.toml")

    workspace = load_workspace(directory)

    duplicate_codes = {
        item.path.name: {diagnostic.code for diagnostic in item.diagnostics}
        for item in workspace.documents
    }
    assert "artifact.duplicate_revision" in duplicate_codes["001_a.toml"]
    assert "artifact.duplicate_revision" in duplicate_codes["001_b.json"]
    assert "artifact.missing_dependency" in duplicate_codes["002_next.toml"]


def test_select_document_returns_a_new_workspace(tmp_path: Path) -> None:
    directory = tmp_path / "migrations"
    artifact("001_initial").write(directory / "001_initial.toml")
    workspace = load_workspace(directory)

    selected = select_document(workspace, workspace.documents[0].path)

    assert workspace.selected_path is None
    assert selected.selected_path == workspace.documents[0].path


def test_update_source_keeps_last_valid_artifact_on_toml_error(
    tmp_path: Path,
) -> None:
    path = tmp_path / "001_initial.toml"
    artifact("001_initial").write(path)
    document = load_workspace(tmp_path).documents[0]

    updated = update_source(document, "revision = [")

    assert updated.artifact is None
    assert updated.last_valid_artifact is document.artifact
    assert updated.dirty is True
    assert updated.diagnostics[0].code == "artifact.edit_invalid"


def test_json_document_requires_explicit_toml_conversion(tmp_path: Path) -> None:
    path = tmp_path / "001_initial.json"
    artifact("001_initial").write(path)
    document = load_workspace(tmp_path).documents[0]

    with pytest.raises(ValueError, match="Convert to TOML"):
        update_source(document, document.source)


def test_replace_operation_sql_rebuilds_artifact_without_mutating_original(
    tmp_path: Path,
) -> None:
    path = tmp_path / "001_initial.toml"
    artifact("001_initial").write(path)
    document = load_workspace(tmp_path).documents[0]
    original = document.artifact
    assert original is not None

    updated = replace_operation_sql(
        document,
        index=0,
        sql="CREATE TABLE accounts (id INTEGER)",
    )

    assert original.operations[0].sql == "CREATE TABLE users (id INTEGER)"
    assert updated.artifact is not None
    assert updated.artifact.operations[0].sql == "CREATE TABLE accounts (id INTEGER)"
    assert updated.artifact.checksum != original.checksum
    assert "CREATE TABLE accounts" in updated.source
    assert updated.dirty is True


def test_convert_json_to_toml_leaves_json_source_untouched(tmp_path: Path) -> None:
    source_path = tmp_path / "001_initial.json"
    destination = tmp_path / "001_initial.toml"
    artifact("001_initial").write(source_path)
    original_source = source_path.read_text()
    document = load_workspace(tmp_path).documents[0]

    converted = convert_to_toml(document, destination)

    assert source_path.read_text() == original_source
    assert destination.is_file()
    assert converted.path == destination
    assert converted.format == "toml"
    assert MigrationArtifact.read(destination).revision == "001_initial"


def test_atomic_save_refuses_overwrite_and_removes_temporary_file(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "001_initial.toml"
    artifact("001_initial").write(source_path)
    document = replace_operation_sql(
        load_workspace(tmp_path).documents[0],
        index=0,
        sql="CREATE TABLE changed (id INTEGER)",
    )
    destination = tmp_path / "copy.toml"
    destination.write_text("keep me")

    with pytest.raises(FileExistsError):
        save_document(document, destination=destination)

    saved = save_document(document, destination=destination, overwrite=True)

    assert saved.dirty is False
    assert "CREATE TABLE changed" in destination.read_text()
    assert not (tmp_path / ".copy.toml.tmp").exists()


def test_draft_can_be_written_recovered_and_discarded(tmp_path: Path) -> None:
    migration = tmp_path / "migrations" / "001_initial.toml"
    artifact("001_initial").write(migration)
    document = update_source(
        load_workspace(migration.parent).documents[0],
        "revision = [",
    )

    path = write_draft(tmp_path, document)

    assert path == draft_path(tmp_path, document)
    assert path.read_text() == "revision = ["
    recovered = recover_draft(tmp_path, document)
    assert recovered.source == "revision = ["
    assert recovered.dirty is True
    discard_draft(tmp_path, document)
    assert not path.exists()
