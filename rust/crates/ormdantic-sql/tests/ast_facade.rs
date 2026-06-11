use ormdantic_dialects::{MsSqlDialect, MySqlDialect, PostgresDialect, SqliteDialect};
use ormdantic_sql::{
    BinaryOp, DmlAst, Expr, Projection, QueryAst, SelectAst, SelectInPlan, SqlLiteral, TableRef,
    TableSource,
};

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
fn empty_projection_error_surfaces_from_public_ast() {
    let error = SelectAst::new(Vec::new())
        .compile(&PostgresDialect)
        .expect_err("empty projection should fail");

    assert_eq!(
        error.to_string(),
        "select query requires at least one projection"
    );
}
