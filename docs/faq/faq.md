# Frequently Asked Questions

![Logo](https://raw.githubusercontent.com/yezz123/ormdantic/main/.github/logo.png)

## What is the purpose of this project?

**Ormdantic** is an async ORM that uses Pydantic v2 models as database table models. The Python API handles model declarations, decorators, sessions, events, and final Pydantic object construction, while the Rust core handles schema validation, SQL compilation, row hydration, and native database execution.

## What are the key differences between Ormdantic and SQLModel?

| Area | SQLModel | Ormdantic |
| --- | --- | --- |
| Model style | Uses Pydantic models with SQLAlchemy table metadata. | Uses regular Pydantic v2 models decorated with `database.table`. |
| Runtime | Builds on SQLAlchemy. | Uses Rust SQL compilation and native Rust execution. |
| Async API | Uses SQLAlchemy-style sessions and engines. | Creates an async database abstraction from a connection URL and exposes table-scoped CRUD helpers. |
| Query path | SQLAlchemy expression/session model. | Rust-compiled CRUD, filters, counts, joins, and DDL behind a Python API. |
| Relationship loading | SQLAlchemy relationship machinery. | Explicit `depth` and loader options; no hidden synchronous lazy loading. |

Ormdantic is inspired by SQLModel's Pydantic-first ergonomics, but its internals are intentionally different after the Rust-core migration.

## Does Ormdantic still depend on SQLAlchemy?

No. The vNext runtime removed SQLAlchemy and PyPika from runtime dependencies. Dialect parsing still accepts SQLAlchemy-style database URLs for compatibility with familiar connection strings.

## How to Support Project?

You can financially support the author (me) through
[![](https://img.shields.io/static/v1?label=Sponsor&message=%E2%9D%A4&logo=GitHub&color=%23fe8e86)](https://github.com/sponsors/yezz123) Paypal
sponsors</a>.

There you could buy me a [coffee ☕️](https://www.buymeacoffee.com/tahiri) to
say thanks. 😄

And you can also become a Silver or Gold sponsor for Ormdantic. 🏅🎉
