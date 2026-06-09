use ormdantic_dialects::{normalize_dialect_name, AnyDialect, Dialect, DialectKind};

#[test]
fn normalizes_driver_names_and_connection_urls() {
    let cases = [
        ("postgresql+asyncpg://user:pass@localhost/db", "postgresql"),
        ("mysql-connector://localhost/db", "mysqlconnector"),
        (" cx_oracle ", "cxoracle"),
        ("maria-db", "mariadb"),
    ];

    for (input, expected) in cases {
        assert_eq!(normalize_dialect_name(input), expected);
    }
}

#[test]
fn dialect_kind_parses_driver_aliases() {
    let cases = [
        ("sqlite3", DialectKind::Sqlite),
        ("aiosqlite", DialectKind::Sqlite),
        ("postgres", DialectKind::Postgres),
        ("asyncpg", DialectKind::Postgres),
        ("psycopg2", DialectKind::Postgres),
        ("pg8000", DialectKind::Postgres),
        ("mysqlconnector", DialectKind::MySql),
        ("aiomysql", DialectKind::MySql),
        ("asyncmy", DialectKind::MySql),
        ("mariadbconnector", DialectKind::MariaDb),
        ("pyodbc", DialectKind::MsSql),
        ("pymssql", DialectKind::MsSql),
        ("aioodbc", DialectKind::MsSql),
        ("oracledb", DialectKind::Oracle),
        ("cx_oracle", DialectKind::Oracle),
    ];

    for (input, expected) in cases {
        assert_eq!(DialectKind::parse(input).unwrap(), expected);
    }
}

#[test]
fn any_dialect_parses_connection_urls_to_canonical_names() {
    let cases = [
        ("sqlite+aiosqlite:///db.sqlite3", "sqlite"),
        ("postgresql+asyncpg://user:pass@host/db", "postgresql"),
        ("mysql+pymysql://user:pass@host/db", "mysql"),
        ("mariadb+mariadbconnector://user:pass@host/db", "mariadb"),
        ("mssql+pyodbc://user:pass@host/db", "mssql"),
        ("oracle+oracledb://user:pass@host/db", "oracle"),
    ];

    for (input, expected) in cases {
        assert_eq!(AnyDialect::parse(input).unwrap().name(), expected);
    }
}

#[test]
fn rejects_unknown_dialects_with_normalized_name() {
    let error = AnyDialect::parse("DB-2").expect_err("dialect should fail");

    assert_eq!(error.to_string(), "dialect 'db2' is not supported");
}
