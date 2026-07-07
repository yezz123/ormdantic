use ormdantic_core::{
    BackendFeature, DeferrableMode, IsolationLevel, SavepointName, TransactionOptions,
};
use ormdantic_dialects::{AnyDialect, Dialect, ReflectionScope};
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

#[test]
fn any_dialect_delegates_common_dialect_methods_for_every_backend() {
    let savepoint = SavepointName::new("sp_1").unwrap();
    let string_column = ColumnDef::new("name", FieldKind::String).with_max_length(12);
    let json_column = ColumnDef::new("payload", FieldKind::Json);
    let scope = ReflectionScope::new()
        .schema("inventory")
        .tables(vec!["flavor".to_string()]);

    let cases = [
        (
            "sqlite",
            "\"flavor\"",
            "?",
            true,
            false,
            true,
            "TEXT",
            "JSON",
            r#"SAVEPOINT "sp_1""#,
            r#"ROLLBACK TO SAVEPOINT "sp_1""#,
            r#"RELEASE SAVEPOINT "sp_1""#,
        ),
        (
            "postgresql",
            "\"flavor\"",
            "$4",
            true,
            true,
            true,
            "VARCHAR(12)",
            "JSON",
            r#"SAVEPOINT "sp_1""#,
            r#"ROLLBACK TO SAVEPOINT "sp_1""#,
            r#"RELEASE SAVEPOINT "sp_1""#,
        ),
        (
            "mysql",
            "`flavor`",
            "?",
            false,
            false,
            true,
            "VARCHAR(12)",
            "JSON",
            "SAVEPOINT `sp_1`",
            "ROLLBACK TO SAVEPOINT `sp_1`",
            "RELEASE SAVEPOINT `sp_1`",
        ),
        (
            "mariadb",
            "`flavor`",
            "?",
            true,
            false,
            true,
            "VARCHAR(12)",
            "JSON",
            "SAVEPOINT `sp_1`",
            "ROLLBACK TO SAVEPOINT `sp_1`",
            "RELEASE SAVEPOINT `sp_1`",
        ),
        (
            "mssql",
            "[flavor]",
            "@P4",
            false,
            true,
            false,
            "NVARCHAR(12)",
            "TEXT",
            "SAVE TRANSACTION [sp_1]",
            "ROLLBACK TRANSACTION [sp_1]",
            "",
        ),
        (
            "oracle",
            "\"flavor\"",
            ":4",
            false,
            false,
            true,
            "VARCHAR2(12)",
            "JSON",
            r#"SAVEPOINT "sp_1""#,
            r#"ROLLBACK TO SAVEPOINT "sp_1""#,
            "",
        ),
    ];

    for (
        name,
        quoted,
        placeholder,
        returning,
        native_uuid,
        native_json,
        string_type,
        json_type,
        savepoint_sql,
        rollback_sql,
        release_sql,
    ) in cases
    {
        let dialect = AnyDialect::parse(name).unwrap();
        assert_eq!(
            AnyDialect::parse(dialect.name()).unwrap().kind(),
            dialect.kind()
        );
        assert_eq!(dialect.quote_ident("flavor"), quoted);
        assert_eq!(dialect.placeholder(4), placeholder);
        assert_eq!(dialect.supports_returning(), returning);
        assert_eq!(dialect.supports_native_uuid(), native_uuid);
        assert_eq!(dialect.supports_json(), native_json);
        assert_eq!(
            dialect.supports_feature(BackendFeature::Returning),
            returning
        );
        assert_eq!(
            dialect.supports_feature(BackendFeature::NativeJson),
            native_json
        );
        assert_eq!(dialect.render_column_type(&string_column), string_type);
        assert_eq!(dialect.render_column_type(&json_column), json_type);
        assert_eq!(
            dialect.set_isolation_sql(IsolationLevel::Serializable),
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
        );
        assert_eq!(dialect.savepoint_sql(&savepoint), savepoint_sql);
        assert_eq!(dialect.rollback_to_savepoint_sql(&savepoint), rollback_sql);
        assert_eq!(dialect.release_savepoint_sql(&savepoint), release_sql);
        assert_eq!(dialect.reflection_queries(&scope).len(), 5);
    }
}

#[test]
fn any_dialect_delegates_transactions_and_upsert_for_every_backend() {
    let begin_options = TransactionOptions::new()
        .with_isolation_level(IsolationLevel::Serializable)
        .with_deferrable_mode(DeferrableMode::NotDeferrable);
    let update_columns = vec!["name".to_string()];

    for name in [
        "sqlite",
        "postgresql",
        "mysql",
        "mariadb",
        "mssql",
        "oracle",
    ] {
        let dialect = AnyDialect::parse(name).unwrap();
        let begin = dialect.begin_transaction_sql(&begin_options);
        assert!(!begin.is_empty() || name == "oracle");

        match name {
            "mssql" | "oracle" => {
                let error = dialect
                    .upsert_conflict_clause("id", &update_columns)
                    .expect_err("merge dialects reject insert conflict clauses");
                assert!(error.to_string().contains("INSERT conflict-clause upsert"));
            }
            _ => {
                let conflict = dialect
                    .upsert_conflict_clause("id", &update_columns)
                    .expect("insert conflict dialect should render upsert clause");
                assert!(conflict.contains("name"));
            }
        }
    }
}
