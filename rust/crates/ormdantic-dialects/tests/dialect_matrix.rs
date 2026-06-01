use ormdantic_dialects::{AnyDialect, Dialect};

#[test]
fn accepts_sqlalchemy_style_connection_schemes() {
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
fn renders_driver_placeholder_styles() {
    assert_eq!(
        AnyDialect::parse("postgresql").unwrap().placeholder(3),
        "$3"
    );
    assert_eq!(AnyDialect::parse("mysql").unwrap().placeholder(3), "?");
    assert_eq!(AnyDialect::parse("mssql").unwrap().placeholder(3), "@P3");
    assert_eq!(AnyDialect::parse("oracle").unwrap().placeholder(3), ":3");
}
