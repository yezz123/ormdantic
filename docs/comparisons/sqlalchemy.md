# Ormdantic vs SQLAlchemy

SQLAlchemy is the most complete Python SQL toolkit and ORM. Ormdantic is narrower: Pydantic models in Python, Rust runtime underneath.

| Area | SQLAlchemy | Ormdantic |
| --- | --- | --- |
| Model layer | SQLAlchemy declarative or imperative mappings. | Pydantic v2 models registered with `@db.table`. |
| Runtime | Python SQL toolkit, ORM, unit of work, dialects, and drivers. | Rust SQL compiler, hydration planner, and native execution behind a Python facade. |
| Query scope | Very broad SQL expression system. | Focused expression helpers for application ORM use cases. |
| Relationships | Mature relationship mapper and loader strategies. | Explicit async loader options and no hidden attribute-triggered I/O. |
| Migrations | Usually Alembic. | Built-in snapshot, diff, plan, artifact, history, rollback, and repair APIs. |
| Ecosystem | Large and mature. | Smaller, purpose-built API. |

## Benchmark Snapshot

The local report in `benchmark/` compares Ormdantic, SQLAlchemy, and SQLModel
on read and write cases. These are median timings from the committed benchmark
artifacts, not a universal claim about every ORM workflow.

![Ormdantic speedup over SQLAlchemy and SQLModel](../assets/benchmarks/default/ormdantic-orm-benchmark-speedup.svg)

## Choose Ormdantic When

- your application models are already Pydantic models;
- you want a smaller async ORM surface;
- you want Rust-owned query compilation and hydration;
- you value explicit relationship loading over transparent lazy access;
- you want migrations tied to the same schema metadata used by the ORM.

## Choose SQLAlchemy When

- you need the deepest Python SQL toolkit;
- you rely on SQLAlchemy plugins or Alembic workflows;
- you need advanced mapper features outside Ormdantic's modeled surface;
- you prefer SQLAlchemy's mature ecosystem over a smaller Rust-backed runtime.
