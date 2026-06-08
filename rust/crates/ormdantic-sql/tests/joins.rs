use ormdantic_dialects::SqliteDialect;
use ormdantic_sql::{
    Filter, JoinSpec, JoinedFilter, JoinedOrderBy, JoinedSelectColumn, OrderBy, QueryAst,
    SortDirection, TableRef,
};

#[test]
fn compiles_many_to_one_join_with_current_alias_contract() {
    let query = QueryAst::JoinedSelect {
        table: TableRef::new("coffee"),
        columns: vec![
            JoinedSelectColumn::aliased("coffee", "id", "coffee\\id"),
            JoinedSelectColumn::aliased("coffee/flavor", "id", "coffee/flavor\\id"),
            JoinedSelectColumn::aliased("coffee/flavor", "name", "coffee/flavor\\name"),
        ],
        joins: vec![JoinSpec::left_join(
            "flavors",
            "coffee/flavor",
            "coffee",
            "flavor",
            "coffee/flavor",
            "id",
        )],
        filters: vec![Filter::Eq {
            column: "id".to_string(),
            param: "id".to_string(),
        }],
        relationship_filters: Vec::new(),
        order_by: Vec::new(),
        relationship_order_by: Vec::new(),
        limit: None,
        offset: None,
    }
    .compile(&SqliteDialect)
    .expect("join query should compile");

    assert_eq!(
        query.sql(),
        "SELECT \"coffee\".\"id\" AS \"coffee\\id\", \"coffee/flavor\".\"id\" AS \"coffee/flavor\\id\", \"coffee/flavor\".\"name\" AS \"coffee/flavor\\name\" FROM \"coffee\" LEFT JOIN \"flavors\" AS \"coffee/flavor\" ON \"coffee\".\"flavor\" = \"coffee/flavor\".\"id\" WHERE \"coffee\".\"id\" = ?"
    );
}

#[test]
fn compiles_one_to_many_back_reference_join() {
    let query = QueryAst::JoinedSelect {
        table: TableRef::new("one"),
        columns: vec![
            JoinedSelectColumn::aliased("one", "id", "one\\id"),
            JoinedSelectColumn::aliased("one/many", "id", "one/many\\id"),
            JoinedSelectColumn::aliased("one/many", "one_a", "one/many\\one_a"),
        ],
        joins: vec![JoinSpec::left_join(
            "many", "one/many", "one", "id", "one/many", "one_a",
        )],
        filters: Vec::new(),
        relationship_filters: Vec::new(),
        order_by: Vec::new(),
        relationship_order_by: Vec::new(),
        limit: None,
        offset: None,
    }
    .compile(&SqliteDialect)
    .expect("back reference join query should compile");

    assert_eq!(
        query.sql(),
        "SELECT \"one\".\"id\" AS \"one\\id\", \"one/many\".\"id\" AS \"one/many\\id\", \"one/many\".\"one_a\" AS \"one/many\\one_a\" FROM \"one\" LEFT JOIN \"many\" AS \"one/many\" ON \"one\".\"id\" = \"one/many\".\"one_a\""
    );
}

#[test]
fn compiles_joined_relationship_filters_in_join_on_clause() {
    let query = QueryAst::JoinedSelect {
        table: TableRef::new("one"),
        columns: vec![
            JoinedSelectColumn::aliased("one", "id", "one\\id"),
            JoinedSelectColumn::aliased("one/many", "id", "one/many\\id"),
            JoinedSelectColumn::aliased("one/many", "kind", "one/many\\kind"),
        ],
        joins: vec![JoinSpec::left_join(
            "many", "one/many", "one", "id", "one/many", "one_a",
        )],
        filters: vec![Filter::Eq {
            column: "id".to_string(),
            param: "id".to_string(),
        }],
        relationship_filters: vec![JoinedFilter::new(
            "one/many",
            Filter::Eq {
                column: "kind".to_string(),
                param: "loader_0__kind".to_string(),
            },
        )],
        order_by: Vec::new(),
        relationship_order_by: vec![JoinedOrderBy::new(
            "one/many",
            OrderBy::new("kind", SortDirection::Desc),
        )],
        limit: None,
        offset: None,
    }
    .compile(&SqliteDialect)
    .expect("join query should compile");

    assert_eq!(
        query.sql(),
        "SELECT \"one\".\"id\" AS \"one\\id\", \"one/many\".\"id\" AS \"one/many\\id\", \"one/many\".\"kind\" AS \"one/many\\kind\" FROM \"one\" LEFT JOIN \"many\" AS \"one/many\" ON \"one\".\"id\" = \"one/many\".\"one_a\" AND \"one/many\".\"kind\" = ? WHERE \"one\".\"id\" = ? ORDER BY \"one/many\".\"kind\" DESC"
    );
    assert_eq!(
        query.params(),
        &["loader_0__kind".to_string(), "id".to_string()]
    );
}
