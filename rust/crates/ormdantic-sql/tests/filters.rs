use ormdantic_dialects::{PostgresDialect, SqliteDialect};
use ormdantic_sql::{
    Filter, JoinedSelectColumn, OrderBy, QueryAst, SelectColumn, SortDirection, TableRef,
};

#[test]
fn compiles_comparison_and_like_filters() {
    let query = QueryAst::Select {
        table: TableRef::new("flavors"),
        columns: vec![SelectColumn::new("id")],
        filters: vec![
            Filter::Ne {
                column: "name".to_string(),
                param: "name__ne".to_string(),
            },
            Filter::Lt {
                column: "strength".to_string(),
                param: "strength__lt".to_string(),
            },
            Filter::Le {
                column: "strength".to_string(),
                param: "strength__le".to_string(),
            },
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
        "SELECT \"flavors\".\"id\" FROM \"flavors\" WHERE \"name\" != ? AND \"strength\" < ? AND \"strength\" <= ? AND \"strength\" > ? AND \"name\" LIKE ?"
    );
    assert_eq!(
        query.params(),
        &[
            "name__ne".to_string(),
            "strength__lt".to_string(),
            "strength__le".to_string(),
            "strength__gt".to_string(),
            "name__like".to_string(),
        ]
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
        relationship_filters: Vec::new(),
        order_by: Vec::new(),
        relationship_order_by: Vec::new(),
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

#[test]
fn compiles_decimal_filters_for_sqlite_with_numeric_comparators() {
    let query = QueryAst::Select {
        table: TableRef::new("prices"),
        columns: vec![SelectColumn::new("id")],
        filters: vec![
            Filter::DecimalEq {
                column: "amount".to_string(),
                param: "eq".to_string(),
            },
            Filter::DecimalNe {
                column: "amount".to_string(),
                param: "ne".to_string(),
            },
            Filter::DecimalLt {
                column: "amount".to_string(),
                param: "lt".to_string(),
            },
            Filter::DecimalLe {
                column: "amount".to_string(),
                param: "le".to_string(),
            },
            Filter::DecimalGt {
                column: "amount".to_string(),
                param: "gt".to_string(),
            },
            Filter::DecimalGe {
                column: "amount".to_string(),
                param: "ge".to_string(),
            },
        ],
        order_by: vec![OrderBy::new("amount", SortDirection::Asc).decimal(true)],
        limit: None,
        offset: None,
    }
    .compile(&SqliteDialect)
    .expect("decimal filters should compile");

    assert_eq!(
        query.sql(),
        "SELECT \"prices\".\"id\" FROM \"prices\" WHERE ormdantic_decimal_cmp(\"amount\", ?) = 0 AND ormdantic_decimal_cmp(\"amount\", ?) != 0 AND ormdantic_decimal_cmp(\"amount\", ?) < 0 AND ormdantic_decimal_cmp(\"amount\", ?) <= 0 AND ormdantic_decimal_cmp(\"amount\", ?) > 0 AND ormdantic_decimal_cmp(\"amount\", ?) >= 0 ORDER BY ormdantic_decimal_sort_key(\"amount\") ASC"
    );
    assert_eq!(
        query.params(),
        &[
            "eq".to_string(),
            "ne".to_string(),
            "lt".to_string(),
            "le".to_string(),
            "gt".to_string(),
            "ge".to_string(),
        ]
    );
}

#[test]
fn compiles_decimal_in_filters_for_sqlite_and_other_backends() {
    let sqlite_query = QueryAst::Select {
        table: TableRef::new("prices"),
        columns: vec![SelectColumn::new("id")],
        filters: vec![
            Filter::DecimalIn {
                column: "amount".to_string(),
                params: vec!["low".to_string(), "high".to_string()],
            },
            Filter::DecimalNotIn {
                column: "tax".to_string(),
                params: vec!["tax".to_string()],
            },
            Filter::DecimalIn {
                column: "empty".to_string(),
                params: Vec::new(),
            },
            Filter::DecimalNotIn {
                column: "empty_not".to_string(),
                params: Vec::new(),
            },
        ],
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }
    .compile(&SqliteDialect)
    .expect("sqlite decimal IN filters should compile");
    assert_eq!(
        sqlite_query.sql(),
        "SELECT \"prices\".\"id\" FROM \"prices\" WHERE (ormdantic_decimal_cmp(\"amount\", ?) = 0 OR ormdantic_decimal_cmp(\"amount\", ?) = 0) AND NOT (ormdantic_decimal_cmp(\"tax\", ?) = 0) AND 1 = 0 AND 1 = 1"
    );
    assert_eq!(
        sqlite_query.params(),
        &["low".to_string(), "high".to_string(), "tax".to_string()]
    );

    let postgres_query = QueryAst::Select {
        table: TableRef::new("prices"),
        columns: vec![SelectColumn::new("id")],
        filters: vec![
            Filter::DecimalIn {
                column: "amount".to_string(),
                params: vec!["low".to_string(), "high".to_string()],
            },
            Filter::DecimalNotIn {
                column: "tax".to_string(),
                params: vec!["tax".to_string()],
            },
        ],
        order_by: vec![OrderBy::new("amount", SortDirection::Desc).decimal(true)],
        limit: None,
        offset: None,
    }
    .compile(&PostgresDialect)
    .expect("postgres decimal IN filters should compile as ordinary IN filters");
    assert_eq!(
        postgres_query.sql(),
        "SELECT \"prices\".\"id\" FROM \"prices\" WHERE \"amount\" IN ($1, $2) AND \"tax\" NOT IN ($3) ORDER BY \"amount\" DESC"
    );
}

#[test]
fn compiles_decimal_comparisons_for_non_sqlite_backends_without_rewrite() {
    let query = QueryAst::Select {
        table: TableRef::new("prices"),
        columns: vec![SelectColumn::new("id")],
        filters: vec![
            Filter::DecimalEq {
                column: "amount".to_string(),
                param: "amount".to_string(),
            },
            Filter::DecimalGe {
                column: "tax".to_string(),
                param: "tax".to_string(),
            },
        ],
        order_by: vec![OrderBy::new("amount", SortDirection::Asc).decimal(true)],
        limit: Some(10),
        offset: Some(5),
    }
    .compile(&PostgresDialect)
    .expect("postgres decimal comparisons should compile without sqlite functions");

    assert_eq!(
        query.sql(),
        "SELECT \"prices\".\"id\" FROM \"prices\" WHERE \"amount\" = $1 AND \"tax\" >= $2 ORDER BY \"amount\" ASC LIMIT 10 OFFSET 5"
    );
    assert_eq!(query.params(), &["amount".to_string(), "tax".to_string()]);
}
