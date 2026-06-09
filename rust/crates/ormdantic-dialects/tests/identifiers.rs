use ormdantic_dialects::{
    Dialect, MariaDbDialect, MsSqlDialect, MySqlDialect, OracleDialect, PostgresDialect,
    SqliteDialect,
};

#[test]
fn quotes_identifiers_by_backend() {
    assert_eq!(SqliteDialect.quote_ident("user"), r#""user""#);
    assert_eq!(
        PostgresDialect.quote_ident("weird\"name"),
        r#""weird""name""#
    );
    assert_eq!(MySqlDialect.quote_ident("weird`name"), "`weird``name`");
    assert_eq!(MariaDbDialect.quote_ident("weird`name"), "`weird``name`");
    assert_eq!(MsSqlDialect.quote_ident("weird]name"), "[weird]]name]");
    assert_eq!(OracleDialect.quote_ident("user"), r#""user""#);
}

#[test]
fn renders_placeholder_styles_by_backend() {
    assert_eq!(SqliteDialect.placeholder(3), "?");
    assert_eq!(PostgresDialect.placeholder(3), "$3");
    assert_eq!(MySqlDialect.placeholder(3), "?");
    assert_eq!(MariaDbDialect.placeholder(3), "?");
    assert_eq!(MsSqlDialect.placeholder(3), "@P3");
    assert_eq!(OracleDialect.placeholder(3), ":3");
}
