# Enterprise Dialects

This guide checks runtime driver availability for PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle.

## What The Example Covers

- `runtime_capabilities()`;
- supported dialect names;
- detecting source builds that omit optional engines.

```python
--8<-- "examples/enterprise_dialects.py"
```

Run it locally:

```console
python examples/enterprise_dialects.py
```

## Driver-Specific Documentation

Read the [Drivers](../drivers/index.md) section before relying on backend-specific DDL behavior. Each backend has different identifier, type, transaction, and reflection details.
