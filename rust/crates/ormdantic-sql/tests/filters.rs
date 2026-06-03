use ormdantic_dialects::{PostgresDialect, SqliteDialect};
use ormdantic_sql::{Filter, JoinedSelectColumn, QueryAst, SelectColumn, TableRef};

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

#[test]
fn compiles_nested_boolean_filters_with_stable_param_order() {
    let query = QueryAst::Select {
        table: TableRef::new("flavors"),
        columns: vec![SelectColumn::new("id")],
        filters: vec![Filter::Or(vec![
            Filter::And(vec![
                Filter::Ge {
                    column: "strength".to_string(),
                    param: "strength__ge".to_string(),
                },
                Filter::Like {
                    column: "name".to_string(),
                    param: "name__like".to_string(),
                },
            ]),
            Filter::Eq {
                column: "name".to_string(),
                param: "fallback_name".to_string(),
            },
        ])],
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }
    .compile(&SqliteDialect)
    .expect("query should compile");

    assert_eq!(
        query.sql(),
        "SELECT \"flavors\".\"id\" FROM \"flavors\" WHERE ((\"strength\" >= ? AND \"name\" LIKE ?) OR \"name\" = ?)"
    );
    assert_eq!(
        query.params(),
        &[
            "strength__ge".to_string(),
            "name__like".to_string(),
            "fallback_name".to_string()
        ]
    );
}

#[test]
fn compiles_ilike_and_empty_in_filters() {
    let query = QueryAst::Select {
        table: TableRef::new("flavors"),
        columns: vec![SelectColumn::new("id")],
        filters: vec![
            Filter::ILike {
                column: "name".to_string(),
                param: "name__ilike".to_string(),
            },
            Filter::In {
                column: "id".to_string(),
                params: Vec::new(),
            },
            Filter::NotIn {
                column: "kind".to_string(),
                params: Vec::new(),
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
        "SELECT \"flavors\".\"id\" FROM \"flavors\" WHERE LOWER(\"name\") LIKE LOWER(?) AND 1 = 0 AND 1 = 1"
    );
    assert_eq!(query.params(), &["name__ilike".to_string()]);
}

#[test]
fn compiles_joined_boolean_filters_against_root_alias() {
    let query = QueryAst::JoinedSelect {
        table: TableRef::new("coffee"),
        columns: vec![JoinedSelectColumn::aliased("coffee", "id", "coffee\\id")],
        joins: Vec::new(),
        filters: vec![Filter::Or(vec![
            Filter::Eq {
                column: "name".to_string(),
                param: "name".to_string(),
            },
            Filter::IsNull {
                column: "strength".to_string(),
            },
        ])],
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }
    .compile(&PostgresDialect)
    .expect("joined query should compile");

    assert_eq!(
        query.sql(),
        "SELECT \"coffee\".\"id\" AS \"coffee\\id\" FROM \"coffee\" WHERE (\"coffee\".\"name\" = $1 OR \"coffee\".\"strength\" IS NULL)"
    );
    assert_eq!(query.params(), &["name".to_string()]);
}
