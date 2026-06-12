# MariaDB

MariaDB is treated as its own dialect while sharing much of the MySQL-family runtime behavior.

## URL

```python
Ormdantic("mariadb://root:password@localhost:3306/app")
Ormdantic("mariadb+mariadbconnector://root:password@localhost:3306/app")
```

## Differences From MySQL

MariaDB and MySQL overlap heavily, but they differ in server features, sequence behavior, reflection catalogs, and some DDL support.

Ormdantic keeps a separate dialect name so migrations can make backend-aware choices.

## Supported Metadata

MariaDB support includes:

- table engine, charset, collation, and row format metadata;
- indexes, unique constraints, checks, and foreign keys;
- index comments and prefix lengths;
- regular views;
- sequence reflection where supported;
- migration history and repair commands.

## Type Notes

Like MySQL, MariaDB cannot freely use unbounded `TEXT` columns in key positions. Ormdantic renders keyable strings as bounded `VARCHAR` where required.
