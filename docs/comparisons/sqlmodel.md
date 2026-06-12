# Ormdantic vs SQLModel

SQLModel combines Pydantic-style models with SQLAlchemy. Ormdantic keeps Pydantic as the public model layer and moves planning, execution, and hydration into Rust.

| Area | SQLModel | Ormdantic |
| --- | --- | --- |
| Public model | Pydantic model with SQLAlchemy metadata. | Plain Pydantic v2 model registered by `@db.table`. |
| Runtime | SQLAlchemy. | Native Rust crates through PyO3. |
| Drivers | SQLAlchemy dialect and DBAPI ecosystem. | Rust runtime for SQLite, PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle. |
| Query style | SQLAlchemy/SQLModel expressions and sessions. | Table handles, dict filters, and serializable expression helpers. |
| Loading | SQLAlchemy relationship APIs. | Explicit async-safe loaders. |
| Migrations | Usually Alembic. | Built-in snapshot and migration manager. |

## The Practical Difference

SQLModel is the right choice if you want SQLAlchemy compatibility with a Pydantic-friendly declaration style.

Ormdantic is the right choice if you want Pydantic models to remain the application object layer while a Rust runtime owns the database hot path.
