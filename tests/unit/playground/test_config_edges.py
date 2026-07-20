from __future__ import annotations

from pathlib import Path

import pytest

from ormdantic.playground import config


def test_discovery_accepts_a_file_start_and_reports_no_match(tmp_path: Path) -> None:
    nested = tmp_path / "nested"
    nested.mkdir()
    source = nested / "models.py"
    source.write_text("")

    assert config.discover_config(source) is None

    expected = tmp_path / "ormdantic.toml"
    expected.write_text("[project]\n")
    assert config.discover_config(source) == expected


def test_load_requires_a_nonempty_import_target(tmp_path: Path) -> None:
    path = tmp_path / "ormdantic.toml"
    path.write_text("[project]\n[environments.development]\n")

    with pytest.raises(config.ConfigError, match="import target is required"):
        config.load_config(path)


@pytest.mark.parametrize(
    ("source", "message"),
    [
        (
            "[project]\ntarget = 'app:db'\n[environments]\n",
            "at least one named environment",
        ),
        ("project = []\n[environments.dev]\n", "project: expected a TOML table"),
        (
            "[project]\ntarget='app:db'\nwatch=['']\n[environments.dev]\n",
            "project.watch",
        ),
        (
            "[project]\ntarget='app:db'\ndatabase_poll_seconds=0\n[environments.dev]\n",
            "project.database_poll_seconds",
        ),
        (
            "[project]\ntarget=1\n[environments.dev]\n",
            "project.target",
        ),
        (
            "[project]\ntarget='app:db'\ndatabase_poll_seconds=true\n"
            "[environments.dev]\n",
            "project.database_poll_seconds",
        ),
        (
            "[project]\ntarget='app:db'\ndebounce_milliseconds=true\n"
            "[environments.dev]\n",
            "project.debounce_milliseconds",
        ),
        (
            "[project]\ntarget='app:db'\n[environments.dev]\nproduction='yes'\n",
            "environments.dev.production",
        ),
    ],
)
def test_configuration_type_and_boundary_errors_are_keyed(
    source: str,
    message: str,
) -> None:
    with pytest.raises(config.ConfigError, match=message):
        config.parse_config_source(source)


def test_empty_environment_name_is_rejected() -> None:
    with pytest.raises(config.ConfigError, match="names cannot be empty"):
        config._parse_environment(" ", {})


def test_optional_string_and_path_helpers_cover_empty_and_absolute_values(
    tmp_path: Path,
) -> None:
    assert config._optional_nonempty_string(None, "value") is None
    assert (
        config._resolve_path(tmp_path, Path("/tmp/schema"))
        == Path("/tmp/schema").resolve()
    )
    assert config._clean_value(None) is None
    assert config._clean_value("  ") is None

    with pytest.raises(config.ConfigError, match="non-empty string"):
        config._string(" ", "value")


def test_dotenv_reader_handles_exports_quotes_comments_and_malformed_lines(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n# ignored\nexport QUOTED='sqlite:///quoted.db'\n"
        'DOUBLE="postgresql://localhost/app"\n'
        "PLAIN=sqlite:///plain.db # comment\nBROKEN\n=missing-key\n"
    )

    assert config._read_env_file(env_file) == {
        "QUOTED": "sqlite:///quoted.db",
        "DOUBLE": "postgresql://localhost/app",
        "PLAIN": "sqlite:///plain.db",
    }
    assert config._read_env_file(None) == {}
