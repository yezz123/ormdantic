use ormdantic_dialects::{
    Dialect, MariaDbDialect, MySqlDialect, OracleDialect, PostgresDialect, ReflectionQuery,
    ReflectionQueryKind, ReflectionScope, SqliteDialect,
};

#[test]
fn renders_postgres_catalog_queries_without_scope() {
    let queries = PostgresDialect.reflection_queries(&ReflectionScope::new());
    let kinds = queries.iter().map(|query| query.kind()).collect::<Vec<_>>();

    assert_eq!(
        kinds,
        vec![
            ReflectionQueryKind::Tables,
            ReflectionQueryKind::Columns,
            ReflectionQueryKind::Indexes,
            ReflectionQueryKind::ForeignKeys,
            ReflectionQueryKind::Constraints,
        ]
    );
    assert!(queries[0].sql().contains("information_schema.tables"));
    assert!(queries[1].sql().contains("information_schema.columns"));
    assert!(queries[2].sql().contains("pg_indexes"));
    assert!(queries[3].sql().contains("FOREIGN KEY"));
    assert!(queries[4]
        .sql()
        .contains("information_schema.table_constraints"));
}

#[test]
fn renders_schema_and_table_scoped_postgres_queries() {
    let scope = ReflectionScope::new()
        .schema("pub'lic")
        .tables(vec!["flavor".to_string(), "supplier".to_string()]);
    let queries = PostgresDialect.reflection_queries(&scope);

    assert_eq!(scope.schema_name(), Some("pub'lic"));
    assert_eq!(scope.table_names(), ["flavor", "supplier"]);
    assert!(queries[0].sql().contains("table_schema = 'pub''lic'"));
    assert!(queries[0]
        .sql()
        .contains("table_name IN ('flavor', 'supplier')"));
}

#[test]
fn renders_sqlite_catalog_queries() {
    let queries = SqliteDialect
        .reflection_queries(&ReflectionScope::new().tables(vec!["flavor".to_string()]));

    assert_eq!(
        queries.iter().map(|query| query.kind()).collect::<Vec<_>>(),
        vec![
            ReflectionQueryKind::Tables,
            ReflectionQueryKind::Columns,
            ReflectionQueryKind::Indexes,
            ReflectionQueryKind::ForeignKeys,
            ReflectionQueryKind::Constraints,
        ]
    );
    assert!(queries[0].sql().contains("sqlite_master"));
    assert!(queries[1].sql().contains("pragma_table_xinfo"));
    assert!(queries[2].sql().contains("pragma_index_list"));
    assert!(queries[3].sql().contains("pragma_foreign_key_list"));
    assert!(queries[0].sql().contains("m.name IN ('flavor')"));
}

#[test]
fn renders_mysql_and_mariadb_catalog_queries_with_default_database_scope() {
    let mysql = MySqlDialect.reflection_queries(&ReflectionScope::new());
    let mariadb = MariaDbDialect.reflection_queries(&ReflectionScope::new());

    assert!(mysql[0].sql().contains("table_schema = DATABASE()"));
    assert!(mysql[2].sql().contains("information_schema.statistics"));
    assert!(mysql[3].sql().contains("referential_constraints"));
    assert_eq!(
        mysql.iter().map(|query| query.kind()).collect::<Vec<_>>(),
        mariadb.iter().map(|query| query.kind()).collect::<Vec<_>>()
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
            ReflectionQueryKind::Indexes,
            ReflectionQueryKind::ForeignKeys,
            ReflectionQueryKind::Constraints,
        ]
    );
    assert!(queries[0].sql().contains("FROM user_tables"));
    assert!(queries[1].sql().contains("FROM user_tab_columns"));
    assert!(queries[2].sql().contains("FROM user_indexes"));
    assert!(queries[3].sql().contains("FROM user_constraints c"));
    assert!(queries[4].sql().contains("FROM user_constraints"));
}

#[test]
fn renders_schema_scoped_oracle_catalog_queries() {
    let queries = OracleDialect.reflection_queries(&ReflectionScope::new().schema("app"));

    assert!(queries[0].sql().contains("FROM all_tables"));
    assert!(queries[0].sql().contains("owner = 'APP'"));
    assert!(queries[1].sql().contains("FROM all_tab_columns"));
    assert!(queries[3].sql().contains("c.owner = 'APP'"));
}

#[test]
fn reflection_query_exposes_kind_and_sql() {
    let query = ReflectionQuery::new(ReflectionQueryKind::ForeignKeys, "SELECT 1");

    assert_eq!(query.kind(), ReflectionQueryKind::ForeignKeys);
    assert_eq!(query.sql(), "SELECT 1");
}
