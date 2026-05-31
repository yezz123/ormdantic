use ormdantic_dialects::{PostgresDialect, SqliteDialect};
use ormdantic_sql::{Filter, QueryAst, SelectColumn, TableRef};

#[test]
fn compiles_comparison_and_like_filters() {
    let query = QueryAst::Select {
        table: TableRef::new("flavors"),
        columns: vec![SelectColumn::new("id")],
        filters: vec![
            Filter::Gt {
                column: "strength".to_string(),
                param: "strength__gt".to_string(),
            },
            Filter::Like {
                column: "name".to_string(),
                param: "name__like".to_string(),
            },
        ],
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }
    .compile(&SqliteDialect)
    .expect("query should compile");

    assert_eq!(
        query.sql(),
        "SELECT \"flavors\".\"id\" FROM \"flavors\" WHERE \"strength\" > ? AND \"name\" LIKE ?"
    );
    assert_eq!(
        query.params(),
        &["strength__gt".to_string(), "name__like".to_string()]
    );
}

#[test]
fn compiles_in_and_null_filters() {
    let query = QueryAst::Select {
        table: TableRef::new("flavors"),
        columns: vec![SelectColumn::new("id")],
        filters: vec![
            Filter::In {
                column: "id".to_string(),
                params: vec!["id__in_0".to_string(), "id__in_1".to_string()],
            },
            Filter::IsNotNull {
                column: "name".to_string(),
            },
        ],
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }
    .compile(&PostgresDialect)
    .expect("query should compile");

    assert_eq!(
        query.sql(),
        "SELECT \"flavors\".\"id\" FROM \"flavors\" WHERE \"id\" IN ($1, $2) AND \"name\" IS NOT NULL"
    );
}
