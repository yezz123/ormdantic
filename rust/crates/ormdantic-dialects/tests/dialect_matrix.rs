use ormdantic_dialects::{AnyDialect, Dialect};
use ormdantic_schema::{ColumnDef, FieldKind, SchemaOperation};

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

#[test]
fn exposes_backend_bind_parameter_limits() {
    let cases = [
        ("sqlite", Some(32_766)),
        ("postgresql", Some(65_535)),
        ("mysql", Some(65_535)),
        ("mariadb", Some(65_535)),
        ("mssql", Some(2_100)),
        ("oracle", Some(65_535)),
    ];

    for (dialect, expected) in cases {
        assert_eq!(
            AnyDialect::parse(dialect).unwrap().max_bind_parameters(),
            expected
        );
    }
}

#[test]
fn renders_add_column_schema_operations_for_each_dialect() {
    let column = ColumnDef::new("rating", FieldKind::Integer).nullable(true);
    let operation = SchemaOperation::AddColumn {
        table: "flavor".to_string(),
        column,
    };
    let cases = [
        (
            "sqlite",
            r#"ALTER TABLE "flavor" ADD COLUMN "rating" INTEGER"#,
        ),
        (
            "postgresql",
            r#"ALTER TABLE "flavor" ADD COLUMN "rating" INTEGER"#,
        ),
        ("mysql", "ALTER TABLE `flavor` ADD COLUMN `rating` INTEGER"),
        (
            "mariadb",
            "ALTER TABLE `flavor` ADD COLUMN `rating` INTEGER",
        ),
        ("mssql", "ALTER TABLE [flavor] ADD [rating] INTEGER"),
        ("oracle", r#"ALTER TABLE "flavor" ADD ("rating" INTEGER)"#),
    ];

    for (dialect, expected) in cases {
        let sql = AnyDialect::parse(dialect)
            .unwrap()
            .compile_schema_operation(&operation)
            .unwrap();
        assert_eq!(sql, vec![expected.to_string()]);
    }
}
