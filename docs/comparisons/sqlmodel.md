# Ormdantic vs SQLModel

SQLModel combines Pydantic models with SQLAlchemy. Ormdantic keeps Pydantic as the public model layer and moves SQL planning, execution, and hydration into Rust.

| Area | SQLModel | Ormdantic |
| --- | --- | --- |
| Public models | Pydantic + SQLAlchemy metadata | Pydantic v2 models with `database.table` |
| Runtime | SQLAlchemy | Native Rust crates through PyO3 |
| Dialects | SQLAlchemy dialect ecosystem | Rust runtime for SQLite, PostgreSQL, MySQL, MariaDB, SQL Server, Oracle |
| Loading | SQLAlchemy relationship APIs | Explicit async-safe loading |
| Performance focus | Python ORM ergonomics | Rust table handles, SQL compilation, hydration |

Ormdantic is inspired by Pydantic-first ergonomics, but it is not a SQLAlchemy wrapper.
