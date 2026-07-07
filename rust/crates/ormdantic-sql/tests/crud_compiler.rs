use ormdantic_dialects::{AnyDialect, MsSqlDialect, OracleDialect, PostgresDialect, SqliteDialect};
use ormdantic_sql::{Filter, QueryAst, SelectColumn, TableRef};

#[test]
fn compiles_flat_select_with_aliases() {
    let query = QueryAst::Select {
        table: TableRef::new("flavors"),
        columns: vec![
            SelectColumn::aliased("id", "flavors\\id"),
            SelectColumn::aliased("name", "flavors\\name"),
        ],
        filters: vec![Filter::Eq {
            column: "id".to_string(),
            param: "id".to_string(),
        }],
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }
    .compile(&PostgresDialect)
    .expect("select should compile");

    assert_eq!(
        query.sql(),
        "SELECT \"flavors\".\"id\" AS \"flavors\\id\", \"flavors\".\"name\" AS \"flavors\\name\" FROM \"flavors\" WHERE \"id\" = $1"
    );
}

#[test]
fn compiles_mysql_insert_for_driver_sql() {
    let dialect = AnyDialect::parse("mysql+pymysql://localhost/db").unwrap();
    let query = QueryAst::Insert {
        table: TableRef::new("flavors"),
        columns: vec!["id".to_string(), "name".to_string()],
    }
    .compile(&dialect)
    .expect("insert should compile");

    assert_eq!(
        query.sql(),
        "INSERT INTO `flavors` (`id`, `name`) VALUES (?, ?)"
    );
}

#[test]
fn compiles_core_dml_for_each_backend() {
    let cases = [
        (
            "sqlite",
            r#"INSERT INTO "flavors" ("id", "name") VALUES (?, ?)"#,
            r#"UPDATE "flavors" SET "name" = ? WHERE "id" = ?"#,
            r#"DELETE FROM "flavors" WHERE "id" = ?"#,
            r#"SELECT COUNT(*) FROM "flavors""#,
        ),
        (
            "postgresql",
            r#"INSERT INTO "flavors" ("id", "name") VALUES ($1, $2)"#,
            r#"UPDATE "flavors" SET "name" = $1 WHERE "id" = $2"#,
            r#"DELETE FROM "flavors" WHERE "id" = $1"#,
            r#"SELECT COUNT(*) FROM "flavors""#,
        ),
        (
            "mysql",
            "INSERT INTO `flavors` (`id`, `name`) VALUES (?, ?)",
            "UPDATE `flavors` SET `name` = ? WHERE `id` = ?",
            "DELETE FROM `flavors` WHERE `id` = ?",
            "SELECT COUNT(*) FROM `flavors`",
        ),
        (
            "mariadb",
            "INSERT INTO `flavors` (`id`, `name`) VALUES (?, ?)",
            "UPDATE `flavors` SET `name` = ? WHERE `id` = ?",
            "DELETE FROM `flavors` WHERE `id` = ?",
            "SELECT COUNT(*) FROM `flavors`",
        ),
        (
            "mssql",
            "INSERT INTO [flavors] ([id], [name]) VALUES (@P1, @P2)",
            "UPDATE [flavors] SET [name] = @P1 WHERE [id] = @P2",
            "DELETE FROM [flavors] WHERE [id] = @P1",
            "SELECT COUNT(*) FROM [flavors]",
        ),
        (
            "oracle",
            r#"INSERT INTO "flavors" ("id", "name") VALUES (:1, :2)"#,
            r#"UPDATE "flavors" SET "name" = :1 WHERE "id" = :2"#,
            r#"DELETE FROM "flavors" WHERE "id" = :1"#,
            r#"SELECT COUNT(*) FROM "flavors""#,
        ),
    ];

    for (dialect_name, insert_sql, update_sql, delete_sql, count_sql) in cases {
        let dialect = AnyDialect::parse(dialect_name).unwrap();
        let insert = QueryAst::Insert {
            table: TableRef::new("flavors"),
            columns: vec!["id".to_string(), "name".to_string()],
        }
        .compile(&dialect)
        .unwrap();
        let update = QueryAst::Update {
            table: TableRef::new("flavors"),
            columns: vec!["name".to_string()],
            pk: "id".to_string(),
        }
        .compile(&dialect)
        .unwrap();
        let delete = QueryAst::Delete {
            table: TableRef::new("flavors"),
            pk: "id".to_string(),
        }
        .compile(&dialect)
        .unwrap();
        let count = QueryAst::Count {
            table: TableRef::new("flavors"),
            filters: Vec::new(),
        }
        .compile(&dialect)
        .unwrap();

        assert_eq!(insert.sql(), insert_sql);
        assert_eq!(insert.params(), &["id".to_string(), "name".to_string()]);
        assert_eq!(update.sql(), update_sql);
        assert_eq!(update.params(), &["name".to_string(), "id".to_string()]);
        assert_eq!(delete.sql(), delete_sql);
        assert_eq!(delete.params(), &["id".to_string()]);
        assert_eq!(count.sql(), count_sql);
        assert!(count.params().is_empty());
    }
}

#[test]
fn compiles_schema_qualified_table_refs() {
    let select = QueryAst::Select {
        table: TableRef::new("inventory.flavors"),
        columns: vec![SelectColumn::aliased("id", "flavors\\id")],
        filters: vec![Filter::Eq {
            column: "id".to_string(),
            param: "id".to_string(),
        }],
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }
    .compile(&PostgresDialect)
    .expect("schema-qualified select should compile");
    let insert = QueryAst::Insert {
        table: TableRef::new("inventory.flavors"),
        columns: vec!["id".to_string(), "name".to_string()],
    }
    .compile(&PostgresDialect)
    .expect("schema-qualified insert should compile");

    assert_eq!(
        select.sql(),
        "SELECT \"inventory\".\"flavors\".\"id\" AS \"flavors\\id\" FROM \"inventory\".\"flavors\" WHERE \"id\" = $1"
    );
    assert_eq!(
        insert.sql(),
        "INSERT INTO \"inventory\".\"flavors\" (\"id\", \"name\") VALUES ($1, $2)"
    );
}

#[test]
fn compiles_sqlite_delete() {
    let query = QueryAst::Delete {
        table: TableRef::new("flavors"),
        pk: "id".to_string(),
    }
    .compile(&SqliteDialect)
    .expect("delete should compile");

    assert_eq!(query.sql(), "DELETE FROM \"flavors\" WHERE \"id\" = ?");
}

#[test]
fn query_ast_validation_errors_cover_empty_column_sets() {
    let empty_select = QueryAst::Select {
        table: TableRef::new("flavors"),
        columns: Vec::new(),
        filters: Vec::new(),
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }
    .compile(&SqliteDialect)
    .expect_err("select without columns should fail");
    assert!(empty_select
        .to_string()
        .contains("select query requires at least one column"));

    for (operation, error) in [
        (
            QueryAst::Insert {
                table: TableRef::new("flavors"),
                columns: Vec::new(),
            },
            "insert query requires at least one column",
        ),
        (
            QueryAst::Update {
                table: TableRef::new("flavors"),
                columns: Vec::new(),
                pk: "id".to_string(),
            },
            "update query requires at least one column",
        ),
        (
            QueryAst::Upsert {
                table: TableRef::new("flavors"),
                columns: Vec::new(),
                pk: "id".to_string(),
            },
            "upsert query requires at least one column",
        ),
    ] {
        let actual = operation
            .compile(&SqliteDialect)
            .expect_err("empty column operation should fail");
        assert!(actual.to_string().contains(error), "{actual}");
    }
}

#[test]
fn compiles_sql_server_upsert_as_merge() {
    let query = QueryAst::Upsert {
        table: TableRef::new("flavors"),
        columns: vec!["id".to_string(), "name".to_string()],
        pk: "id".to_string(),
    }
    .compile(&MsSqlDialect)
    .expect("sql server upsert should compile as MERGE");

    assert_eq!(
        query.sql(),
        "MERGE INTO [flavors] AS target USING (VALUES (@P1, @P2)) AS source ([id], [name]) ON (target.[id] = source.[id]) WHEN MATCHED THEN UPDATE SET target.[name] = source.[name] WHEN NOT MATCHED THEN INSERT ([id], [name]) VALUES (source.[id], source.[name]);"
    );
    assert_eq!(query.params(), &["id".to_string(), "name".to_string()]);
}

#[test]
fn compiles_oracle_upsert_as_merge() {
    let query = QueryAst::Upsert {
        table: TableRef::new("flavors"),
        columns: vec!["id".to_string(), "name".to_string()],
        pk: "id".to_string(),
    }
    .compile(&OracleDialect)
    .expect("oracle upsert should compile as MERGE");

    assert_eq!(
        query.sql(),
        r#"MERGE INTO "flavors" target USING (SELECT :1 AS "id", :2 AS "name" FROM dual) source ON (target."id" = source."id") WHEN MATCHED THEN UPDATE SET target."name" = source."name" WHEN NOT MATCHED THEN INSERT ("id", "name") VALUES (source."id", source."name")"#
    );
    assert_eq!(query.params(), &["id".to_string(), "name".to_string()]);
}
