use ormdantic_dialects::{
    Dialect, OracleDialect, PostgresDialect, ReflectionQuery, ReflectionQueryKind, ReflectionScope,
};

#[test]
fn renders_information_schema_queries_without_scope() {
    let queries = PostgresDialect.reflection_queries(&ReflectionScope::new());
    let kinds = queries.iter().map(|query| query.kind()).collect::<Vec<_>>();

    assert_eq!(
        kinds,
        vec![
            ReflectionQueryKind::Tables,
            ReflectionQueryKind::Columns,
            ReflectionQueryKind::Constraints,
        ]
    );
    assert_eq!(
        queries[0].sql(),
        "SELECT table_name FROM information_schema.tables"
    );
    assert_eq!(
        queries[1].sql(),
        "SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns"
    );
    assert_eq!(
        queries[2].sql(),
        "SELECT table_name, constraint_name, constraint_type FROM information_schema.table_constraints"
    );
}

#[test]
fn renders_schema_scoped_information_schema_queries() {
    let scope = ReflectionScope::new()
        .schema("pub'lic")
        .tables(vec!["flavor".to_string(), "supplier".to_string()]);
    let queries = PostgresDialect.reflection_queries(&scope);

    assert_eq!(scope.schema_name(), Some("pub'lic"));
    assert_eq!(scope.table_names(), ["flavor", "supplier"]);
    assert_eq!(
        queries[0].sql(),
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'pub''lic'"
    );
}

#[test]
fn renders_oracle_catalog_queries_without_scope() {
    let queries = OracleDialect.reflection_queries(&ReflectionScope::new());

    assert_eq!(
        queries.iter().map(|query| query.kind()).collect::<Vec<_>>(),
        vec![
            ReflectionQueryKind::Tables,
            ReflectionQueryKind::Columns,
            ReflectionQueryKind::Constraints,
        ]
    );
    assert_eq!(queries[0].sql(), "SELECT table_name FROM user_tables");
    assert_eq!(
        queries[1].sql(),
        "SELECT table_name, column_name, data_type, nullable FROM user_tab_columns"
    );
    assert_eq!(
        queries[2].sql(),
        "SELECT table_name, constraint_name, constraint_type FROM user_constraints"
    );
}

#[test]
fn renders_schema_scoped_oracle_catalog_queries() {
    let queries = OracleDialect.reflection_queries(&ReflectionScope::new().schema("app"));

    assert_eq!(
        queries[0].sql(),
        "SELECT table_name FROM all_tables WHERE owner = 'APP'"
    );
    assert_eq!(
        queries[1].sql(),
        "SELECT table_name, column_name, data_type, nullable FROM all_tab_columns WHERE owner = 'APP'"
    );
    assert_eq!(
        queries[2].sql(),
        "SELECT table_name, constraint_name, constraint_type FROM all_constraints WHERE owner = 'APP'"
    );
}

#[test]
fn reflection_query_exposes_kind_and_sql() {
    let query = ReflectionQuery::new(ReflectionQueryKind::ForeignKeys, "SELECT 1");

    assert_eq!(query.kind(), ReflectionQueryKind::ForeignKeys);
    assert_eq!(query.sql(), "SELECT 1");
}
