# Why Ormdantic

Ormdantic is for Python teams that want Pydantic models at the API boundary and a native Rust runtime underneath the ORM.

<div class="brand-grid">
  <div class="brand-card">
    <h3>Rust Runtime</h3>
    <p>SQL compilation, relationship planning, hydration, and database execution run through focused Rust crates.</p>
  </div>
  <div class="brand-card">
    <h3>Pydantic First</h3>
    <p>Your table models remain normal Pydantic v2 models, so validation and serialization stay familiar.</p>
  </div>
  <div class="brand-card">
    <h3>Async Safe</h3>
    <p>Relationship loading is explicit. There is no hidden synchronous lazy loading on attribute access.</p>
  </div>
</div>

## What Makes It Different

- A single private PyO3 extension powers the Python package, similar to the architecture used by `pydantic-core`.
- The Python layer is intentionally small: decorators, event callbacks, final model construction, and ergonomic facades.
- SQLite, PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle are supported by the Rust runtime.
- Query expressions, migrations, reflection, sessions, events, association proxies, and hybrid attributes are exposed through Python facades while core execution stays native.
