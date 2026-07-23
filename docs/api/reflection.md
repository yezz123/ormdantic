# Reflection

Reflection reads live database metadata through the native runtime and exposes an
async inspector API for table names, columns, indexes, foreign keys,
constraints, namespaces, reflected snapshots, model comparison, and model
scaffolding.

::: ormdantic.reflection.Inspector

For an onboarding walkthrough, see [Work with an existing database](../how-to/existing-databases.md).
For direct migration workflows, use `db.migrations.live_snapshot()` and
`db.migrations.autogenerate()`.
