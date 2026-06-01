# Ormdantic vs SQLAlchemy

SQLAlchemy is the most complete Python ORM and SQL toolkit. Ormdantic takes a narrower path: Pydantic models in Python and a Rust runtime for ORM execution.

| Area | SQLAlchemy | Ormdantic |
| --- | --- | --- |
| Model layer | SQLAlchemy declarative models | Pydantic v2 models |
| Runtime | Python SQL toolkit and ORM | Rust SQL compiler and native execution |
| Async behavior | Async APIs over explicit engines/sessions | Async-first facade with explicit loading |
| Relationships | Mature loader strategies | Rust-owned joined/depth loading and explicit lazy loading |
| Scope | Broad SQL toolkit | Focused ORM for Pydantic-first applications |

Choose Ormdantic when you want validation-first data models and a smaller Rust-backed runtime surface. Choose SQLAlchemy when you need the deepest SQL toolkit and ecosystem coverage.
