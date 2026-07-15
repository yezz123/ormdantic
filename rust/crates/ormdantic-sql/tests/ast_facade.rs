use ormdantic_dialects::{
    MsSqlDialect, MySqlDialect, OracleDialect, PostgresDialect, SqliteDialect,
};
use ormdantic_schema::{SchemaDiff, SchemaOperation};
use ormdantic_sql::{
    BinaryOp, CompiledQuery, DdlAst, DmlAst, Expr, OrderBy, Projection, QueryAst, QueryOperation,
    SelectAst, SelectColumn, SelectInPlan, SortDirection, SqlLiteral, TableRef, TableSource,
};

#[test]
fn facade_constructors_expose_stable_accessors_and_ddl_compilation() {
    let compiled = CompiledQuery::new(
        "SELECT 1".to_string(),
        vec!["one".to_string()],
        QueryOperation::Select,
    );
    assert_eq!(compiled.sql(), "SELECT 1");
    assert_eq!(compiled.params(), &["one".to_string()]);
    assert_eq!(compiled.operation(), &QueryOperation::Select);

    let table = TableRef::new("flavors");
    assert_eq!(table.name(), "flavors");

    let plain = SelectColumn::new("id");
    let aliased = SelectColumn::aliased("name", "flavors\\name");
    assert_eq!(plain.name(), "id");
    assert_eq!(plain.alias(), None);
    assert_eq!(aliased.name(), "name");
    assert_eq!(aliased.alias(), Some("flavors\\name"));

    let order = OrderBy::new("amount", SortDirection::Desc).decimal(true);
    assert_eq!(order.column(), "amount");
    assert_eq!(order.direction(), &SortDirection::Desc);
    assert!(order.is_decimal());

    let ddl = DdlAst::from_diff(SchemaDiff::new(vec![SchemaOperation::DropTable {
        name: "flavors".to_string(),
    }]));
    let queries = ddl
        .compile(&SqliteDialect)
        .expect("ddl ast should compile through facade");
    assert_eq!(queries.len(), 1);
    assert_eq!(queries[0].operation(), &QueryOperation::Ddl);
    assert_eq!(queries[0].sql(), r#"DROP TABLE IF EXISTS "flavors""#);

    assert!(DdlAst::new(vec![SchemaOperation::SetTableComment {
        table: "flavors".to_string(),
        comment: None,
    }])
    .compile(&SqliteDialect)
    .expect("sqlite comment operations should be skipped")
    .is_empty());
}

#[test]
fn public_facade_compiles_select_ast_after_module_split() {
    let query = SelectAst::new(vec![
        Projection::new(Expr::qualified_column("flavors", "id")),
        Projection::aliased(
            Expr::Binary {
                left: Box::new(Expr::qualified_column("flavors", "strength")),
                op: BinaryOp::Add,
                right: Box::new(Expr::Literal(SqlLiteral::Integer(1))),
            },
            "next_strength",
        ),
    ])
    .from(TableSource::aliased_table("flavors", "flavors"))
    .where_expr(Expr::eq(
        Expr::qualified_column("flavors", "id"),
        Expr::param("id"),
    ));

    let compiled = query
        .compile(&PostgresDialect)
        .expect("select AST should compile through facade");

    assert_eq!(
        compiled.sql(),
        "SELECT \"flavors\".\"id\", (\"flavors\".\"strength\" + 1) AS \"next_strength\" FROM \"flavors\" AS \"flavors\" WHERE (\"flavors\".\"id\" = $1)"
    );
    assert_eq!(compiled.params(), &["id".to_string()]);
}

#[test]
fn select_in_plan_compiles_batch_parameters() {
    let compiled = SelectInPlan::new(
        "flavors",
        "coffee",
        vec!["id".to_string()],
        vec!["flavor_id".to_string()],
    )
    .batch_size(0)
    .query_for_batch(vec!["first".to_string(), "second".to_string()])
    .compile(&PostgresDialect)
    .expect("select-in query should compile");

    assert_eq!(
        compiled.sql(),
        "SELECT * FROM \"coffee\" WHERE \"flavor_id\" IN ($1, $2)"
    );
    assert_eq!(
        compiled.params(),
        &["first".to_string(), "second".to_string()]
    );
}

#[test]
fn rejects_queries_that_exceed_backend_bind_parameter_limits() {
    let allowed = SelectInPlan::new(
        "flavors",
        "coffee",
        vec!["id".to_string()],
        vec!["flavor_id".to_string()],
    )
    .query_for_batch((0..2_100).map(|index| format!("id_{index}")).collect())
    .compile(&MsSqlDialect)
    .expect("sql server max bind count should compile");
    let error = SelectInPlan::new(
        "flavors",
        "coffee",
        vec!["id".to_string()],
        vec!["flavor_id".to_string()],
    )
    .query_for_batch((0..2_101).map(|index| format!("id_{index}")).collect())
    .compile(&MsSqlDialect)
    .expect_err("sql server bind parameter overflow should fail");

    assert_eq!(allowed.params().len(), 2_100);
    assert!(error.to_string().contains("exceeding the 2100 limit"));
    assert!(error.to_string().contains("mssql"));
}

#[test]
fn pk_only_upsert_compiles_as_do_nothing() {
    let compiled = QueryAst::Upsert {
        table: TableRef::new("flavors"),
        columns: vec!["id".to_string()],
        pk: "id".to_string(),
    }
    .compile(&SqliteDialect)
    .expect("pk-only upsert should compile");

    assert_eq!(
        compiled.sql(),
        "INSERT INTO \"flavors\" (\"id\") VALUES (?) ON CONFLICT (\"id\") DO NOTHING"
    );
}

#[test]
fn dml_returning_requires_dialect_support() {
    let dml = DmlAst::Insert {
        table: TableSource::table("flavors"),
        columns: vec!["id".to_string()],
        rows: vec![vec![Expr::param("id")]],
        returning: vec![Expr::column("id")],
    };

    let postgres = dml
        .compile(&PostgresDialect)
        .expect("postgres returning should compile");
    assert_eq!(
        postgres.sql(),
        "INSERT INTO \"flavors\" (\"id\") VALUES ($1) RETURNING \"id\""
    );

    let mysql_error = dml
        .compile(&MySqlDialect)
        .expect_err("mysql returning should fail");
    assert!(mysql_error.to_string().contains("RETURNING"));
}

#[test]
fn upsert_rejects_row_width_mismatches() {
    let dml = DmlAst::Upsert {
        table: TableSource::table("flavors"),
        columns: vec!["id".to_string(), "name".to_string()],
        rows: vec![vec![Expr::param("id")]],
        conflict_target: vec!["id".to_string()],
        update_assignments: Vec::new(),
        returning: Vec::new(),
    };

    let error = dml
        .compile(&PostgresDialect)
        .expect_err("upsert row width mismatch should fail before rendering SQL");

    assert!(error.to_string().contains("1 values for 2 columns"));
}

#[test]
fn empty_projection_error_surfaces_from_public_ast() {
    let error = SelectAst::new(Vec::new())
        .compile(&PostgresDialect)
        .expect_err("empty projection should fail");

    assert_eq!(
        error.to_string(),
        "select query requires at least one projection"
    );
}

#[test]
fn dml_ast_validation_edges_are_reported() {
    let empty_insert_rows = DmlAst::Insert {
        table: TableSource::table("flavors"),
        columns: vec!["id".to_string()],
        rows: Vec::new(),
        returning: Vec::new(),
    }
    .compile(&PostgresDialect)
    .expect_err("insert with no rows should fail");
    assert!(empty_insert_rows
        .to_string()
        .contains("insert query requires at least one row"));

    let empty_update = DmlAst::Update {
        table: TableSource::table("flavors"),
        assignments: Vec::new(),
        where_expr: None,
        returning: Vec::new(),
    }
    .compile(&PostgresDialect)
    .expect_err("update with no assignments should fail");
    assert!(empty_update
        .to_string()
        .contains("update query requires at least one assignment"));

    let empty_upsert_rows = DmlAst::Upsert {
        table: TableSource::table("flavors"),
        columns: vec!["id".to_string()],
        rows: Vec::new(),
        conflict_target: vec!["id".to_string()],
        update_assignments: Vec::new(),
        returning: Vec::new(),
    }
    .compile(&PostgresDialect)
    .expect_err("upsert with no rows should fail");
    assert!(empty_upsert_rows
        .to_string()
        .contains("upsert query requires at least one row"));

    let merge_returning = DmlAst::Upsert {
        table: TableSource::table("flavors"),
        columns: vec!["id".to_string(), "name".to_string()],
        rows: vec![vec![Expr::param("id"), Expr::param("name")]],
        conflict_target: vec!["id".to_string()],
        update_assignments: Vec::new(),
        returning: vec![Expr::column("id")],
    }
    .compile(&MsSqlDialect)
    .expect_err("MERGE returning should fail");
    assert!(merge_returning.to_string().contains("RETURNING from MERGE"));
}

#[test]
fn dml_ast_delete_and_explicit_merge_update_edges_compile() {
    let deleted = DmlAst::Delete {
        table: TableSource::table("flavors"),
        where_expr: Some(Expr::eq(Expr::column("id"), Expr::param("id"))),
        returning: Vec::new(),
    }
    .compile(&PostgresDialect)
    .expect("delete with where should compile");
    assert_eq!(deleted.sql(), "DELETE FROM \"flavors\" WHERE (\"id\" = $1)");
    assert_eq!(deleted.params(), &["id".to_string()]);

    let update_merge = DmlAst::Upsert {
        table: TableSource::table("flavors"),
        columns: vec!["id".to_string(), "name".to_string()],
        rows: vec![
            vec![Expr::param("id_1"), Expr::param("name_1")],
            vec![Expr::param("id_2"), Expr::param("name_2")],
        ],
        conflict_target: vec!["id".to_string()],
        update_assignments: vec![("name".to_string(), Expr::param("replacement_name"))],
        returning: Vec::new(),
    }
    .compile(&OracleDialect)
    .expect("oracle MERGE with explicit assignments should compile");
    assert!(update_merge
        .sql()
        .contains(r#"WHEN MATCHED THEN UPDATE SET target."name" = :5"#));
    assert_eq!(
        update_merge.params(),
        &[
            "id_1".to_string(),
            "name_1".to_string(),
            "id_2".to_string(),
            "name_2".to_string(),
            "replacement_name".to_string(),
        ]
    );
}
