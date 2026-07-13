from __future__ import annotations

from benchmark.backends import redact_url, resolve_backend


def test_backend_url_resolution_prefers_benchmark_environment() -> None:
    backend = resolve_backend(
        "postgres",
        env={
            "ORMDANTIC_BENCH_POSTGRES_URL": "postgresql://bench:secret@db.example/app",
            "ORMDANTIC_TEST_POSTGRES_URL": "postgresql://test:test@localhost/postgres",
        },
    )

    assert backend.name == "postgres"
    assert backend.url == "postgresql://bench:secret@db.example/app"
    assert backend.redacted_url == "postgresql://bench:***@db.example/app"
    assert backend.sqlalchemy_url == "postgresql+asyncpg://bench:secret@db.example/app"


def test_backend_url_resolution_uses_test_env_then_default() -> None:
    mysql = resolve_backend(
        "mysql",
        env={"ORMDANTIC_TEST_MYSQL_URL": "mysql://root:mysql@localhost:3307/mysql"},
    )
    postgres = resolve_backend("postgres", env={})
    sqlite = resolve_backend("sqlite", env={})

    assert mysql.url == "mysql://root:mysql@localhost:3307/mysql"
    assert mysql.sqlalchemy_url == "mysql+aiomysql://root:mysql@localhost:3307/mysql"
    assert postgres.url == "postgresql://postgres:postgres@localhost:5432/postgres"
    assert sqlite.url == "sqlite:///:benchmark:"
    assert sqlite.sqlalchemy_url == "sqlite+aiosqlite:///:benchmark:"


def test_redact_url_masks_credentials_without_losing_shape() -> None:
    assert (
        redact_url("postgresql://user:p%40ss@example.com:5432/app?sslmode=disable")
        == "postgresql://user:***@example.com:5432/app?sslmode=disable"
    )
    assert redact_url("sqlite:////tmp/bench.sqlite3") == "sqlite:////tmp/bench.sqlite3"


def test_redact_url_masks_sensitive_query_values() -> None:
    redacted = redact_url(
        "mysql://user:pass@example.com/app?sslmode=require&token=abc&ssl_key=/tmp/client.key&password=querypass"
    )

    assert redacted == (
        "mysql://user:***@example.com/app?"
        "sslmode=require&token=***&ssl_key=***&password=***"
    )
