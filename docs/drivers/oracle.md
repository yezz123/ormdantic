# Oracle

Oracle support is available when the Rust extension is built with the Oracle engine feature.

## URL

```python
Ormdantic("oracle://system:oracle@localhost:1521/FREEPDB1")
Ormdantic("oracle+oracledb://user:password@localhost:1521/service")
```

You can also use query parameters such as `service_name`, `service`, `sid`, or `tls` depending on the deployment.

## Supported Metadata

Oracle support includes:

- table and index tablespaces;
- table compression;
- index compression and bitmap indexes;
- identity options, including `BY DEFAULT ON NULL`;
- sequences;
- regular views and materialized views;
- comments;
- foreign keys, unique constraints, and checks;
- `MERGE` upserts;
- migration history with paginated metadata reads.

## Type Notes

- Strings render as `VARCHAR2` for ordinary model strings.
- UUID fallback renders as `VARCHAR2(36)`.
- Exact `NUMBER` values hydrate as decimals when they contain a decimal point.
- Oracle table aliases do not use `AS`.

## Runtime Notes

Oracle can close a connection for some protocol-level error cases without returning the original Oracle code through the driver dependency. Ormdantic maps known cases exercised by the driver matrix and reconnects periodically for non-transactional committed write batches.

Oracle `RETURNING INTO` requires out-bind handling and is not compiled through Ormdantic's generic `RETURNING` API yet.
