from dataclasses import FrozenInstanceError
from urllib.parse import parse_qsl, urlsplit

import pytest

from examples.todo_app.app import config


def test_development_defaults_to_local_sqlite_database() -> None:
    settings = config.load_settings({})

    assert settings.environment == "development"
    assert settings.database_url == "sqlite:///todo-dev.sqlite3"


def test_explicit_test_database_url_is_accepted() -> None:
    settings = config.load_settings(
        {
            "APP_ENV": "test",
            "DATABASE_URL": "postgresql://tester:secret@db.example/todos",
        }
    )

    assert settings.environment == "test"
    assert settings.database_url == "postgresql://tester:secret@db.example/todos"


@pytest.mark.parametrize("environment", ["test", "production"])
def test_non_development_environments_require_database_url(
    environment: str,
) -> None:
    with pytest.raises(config.ConfigurationError, match="DATABASE_URL"):
        config.load_settings({"APP_ENV": environment, "DATABASE_URL": "   "})


def test_invalid_environment_names_app_env_and_value() -> None:
    with pytest.raises(config.ConfigurationError, match=r"APP_ENV.*staging"):
        config.load_settings({"APP_ENV": " staging "})


def test_environment_and_database_url_are_normalized() -> None:
    settings = config.load_settings(
        {
            "APP_ENV": "  TeSt  ",
            "DATABASE_URL": "  sqlite:///:memory:  ",
        }
    )

    assert settings.environment == "test"
    assert settings.database_url == "sqlite:///:memory:"


def test_settings_are_immutable() -> None:
    settings = config.Settings(
        environment="development",
        database_url="sqlite:///todo-dev.sqlite3",
    )

    with pytest.raises(FrozenInstanceError):
        settings.environment = "test"  # type: ignore[misc]


def test_database_password_is_absent_from_repr_and_safe_url() -> None:
    password = "do-not-leak"
    settings = config.Settings(
        environment="production",
        database_url=(
            f"postgresql://todo:{password}@db.example:5432/todos?sslmode=require"
        ),
    )

    assert password not in repr(settings)
    assert password not in settings.safe_database_url
    assert settings.safe_database_url.startswith("postgresql://")
    assert "db.example:5432/todos" in settings.safe_database_url


def test_safe_sqlite_url_remains_useful() -> None:
    settings = config.Settings(
        environment="development",
        database_url="sqlite:///todo-dev.sqlite3",
    )

    assert settings.safe_database_url == "sqlite:///todo-dev.sqlite3"


def test_safe_url_redacts_authority_username_and_password() -> None:
    username = "private-user"
    password = "private-password"
    settings = config.Settings(
        environment="production",
        database_url=f"postgresql://{username}:{password}@db.example/todos",
    )

    assert username not in settings.safe_database_url
    assert password not in settings.safe_database_url
    assert "db.example/todos" in settings.safe_database_url


def test_safe_url_redacts_percent_encoded_authority_credentials() -> None:
    settings = config.Settings(
        environment="production",
        database_url=("postgresql://private%40user:p%40ss%2Fword@db.example/todos"),
    )

    assert "private%40user" not in settings.safe_database_url
    assert "p%40ss%2Fword" not in settings.safe_database_url
    assert "db.example/todos" in settings.safe_database_url


def test_safe_url_preserves_bracketed_ipv6_host_and_port() -> None:
    settings = config.Settings(
        environment="production",
        database_url=(
            "postgresql://private-user:private-password@[2001:db8::1]:5432/todos"
        ),
    )

    assert settings.safe_database_url.endswith("@[2001:db8::1]:5432/todos")
    assert "private-user" not in settings.safe_database_url
    assert "private-password" not in settings.safe_database_url


def test_safe_url_redacts_query_only_secrets_and_preserves_fragment() -> None:
    secret = "query-only-secret"
    settings = config.Settings(
        environment="production",
        database_url=f"postgresql://db.example/todos?token={secret}#diagnostics",
    )

    assert secret not in settings.safe_database_url
    assert settings.safe_database_url.endswith("#diagnostics")


def test_safe_url_redacts_mixed_and_percent_encoded_query_keys() -> None:
    secrets = {
        "password": "password-value",
        "passwd": "passwd-value",
        "access_token": "access-token-value",
        "token": "token-value",
        "api_key": "api-key-value",
        "secret": "secret-value",
    }
    settings = config.Settings(
        environment="production",
        database_url=(
            "postgresql://db.example/todos?sslmode=require"
            f"&PassWord={secrets['password']}"
            f"&passwd={secrets['passwd']}"
            f"&access%5Ftoken={secrets['access_token']}"
            f"&TOKEN={secrets['token']}"
            f"&api%5Fkey={secrets['api_key']}"
            f"&secret={secrets['secret']}"
            "&application_name=todo-app"
        ),
    )

    safe_url = settings.safe_database_url
    safe_query = dict(parse_qsl(urlsplit(safe_url).query, keep_blank_values=True))

    assert all(secret not in safe_url for secret in secrets.values())
    assert safe_query["sslmode"] == "require"
    assert safe_query["application_name"] == "todo-app"
    assert safe_query["PassWord"] == "***"
    assert safe_query["passwd"] == "***"
    assert safe_query["access_token"] == "***"
    assert safe_query["TOKEN"] == "***"
    assert safe_query["api_key"] == "***"
    assert safe_query["secret"] == "***"
