from __future__ import annotations

import pytest

from ormdantic import cli as root_cli
from ormdantic._migrations import cli as migration_cli


def test_root_cli_re_exports_migration_cli_group_and_handlers() -> None:
    assert root_cli.migrations_app is migration_cli.migrations_app
    assert root_cli.create_command is migration_cli.create_command
    assert root_cli.apply_command is migration_cli.apply_command
    assert root_cli.rollback_command is migration_cli.rollback_command
    assert root_cli._artifact_for_revision is migration_cli._artifact_for_revision


def test_load_object_validates_module_colon_object_syntax() -> None:
    assert migration_cli._load_object("ormdantic.cli:main") is root_cli.main

    with pytest.raises(ValueError, match="module:object"):
        migration_cli._load_object("ormdantic.cli")
