# MySQL

MySQL support targets native execution, common DDL, reflection, and migration workflows.

## URL

```python
Ormdantic("mysql://root:password@localhost:3306/app")
Ormdantic("mysql+pymysql://root:password@localhost:3306/app")
```

## Supported Metadata

MySQL support includes:

- table engine, charset, collation, row format, and storage options;
- `AUTO_INCREMENT` reflection and migration normalization;
- index comments;
- index prefix lengths;
- index `USING` methods;
- visible and invisible index metadata;
- check constraints where supported by the server version;
- regular views;
- migration history storage.

## Type Notes

MySQL cannot use unbounded `TEXT` columns as keys without a prefix length. Ormdantic renders keyable string columns as bounded `VARCHAR` when needed.

Common behavior:

- ordinary unbounded strings can use `TEXT`;
- primary-key strings use bounded `VARCHAR`;
- UUID fallback uses `VARCHAR(36)` when keyable;
- decimal values use exact decimal decoding.

## Transactions

Transaction behavior depends on storage engine and DDL operation. InnoDB is expected for normal transactional application tables.
