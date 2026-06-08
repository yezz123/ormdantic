use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ormdantic_dialects::PostgresDialect;
use ormdantic_sql::{
    Filter, JoinSpec, JoinedSelectColumn, OrderBy, QueryAst, SelectColumn, SortDirection, TableRef,
};

fn bench_crud_compilation(c: &mut Criterion) {
    let dialect = PostgresDialect;
    c.bench_function("compile_insert_update_upsert_delete", |b| {
        b.iter(|| {
            let columns = vec!["id".to_string(), "name".to_string(), "strength".to_string()];
            black_box(
                QueryAst::Insert {
                    table: TableRef::new("flavors"),
                    columns: columns.clone(),
                }
                .compile(&dialect)
                .unwrap(),
            );
            black_box(
                QueryAst::Update {
                    table: TableRef::new("flavors"),
                    columns: columns.clone(),
                    pk: "id".to_string(),
                }
                .compile(&dialect)
                .unwrap(),
            );
            black_box(
                QueryAst::Upsert {
                    table: TableRef::new("flavors"),
                    columns: columns.clone(),
                    pk: "id".to_string(),
                }
                .compile(&dialect)
                .unwrap(),
            );
            black_box(
                QueryAst::Delete {
                    table: TableRef::new("flavors"),
                    pk: "id".to_string(),
                }
                .compile(&dialect)
                .unwrap(),
            );
        })
    });
}

fn bench_filter_compilation(c: &mut Criterion) {
    let dialect = PostgresDialect;
    c.bench_function("compile_grouped_filters", |b| {
        b.iter(|| {
            black_box(
                QueryAst::Select {
                    table: TableRef::new("flavors"),
                    columns: vec![
                        SelectColumn::new("id"),
                        SelectColumn::new("name"),
                        SelectColumn::new("strength"),
                    ],
                    filters: vec![Filter::Or(vec![
                        Filter::And(vec![
                            Filter::Ge {
                                column: "strength".to_string(),
                                param: "strength".to_string(),
                            },
                            Filter::Like {
                                column: "name".to_string(),
                                param: "name_like".to_string(),
                            },
                        ]),
                        Filter::In {
                            column: "id".to_string(),
                            params: vec!["first".to_string(), "second".to_string()],
                        },
                    ])],
                    order_by: vec![OrderBy::new("name", SortDirection::Asc)],
                    limit: Some(50),
                    offset: Some(10),
                }
                .compile(&dialect)
                .unwrap(),
            );
        })
    });
}

fn bench_joined_compilation(c: &mut Criterion) {
    let dialect = PostgresDialect;
    c.bench_function("compile_joined_select", |b| {
        b.iter(|| {
            black_box(
                QueryAst::JoinedSelect {
                    table: TableRef::new("coffee"),
                    columns: vec![
                        JoinedSelectColumn::aliased("coffee", "id", "coffee\\id"),
                        JoinedSelectColumn::aliased("coffee", "name", "coffee\\name"),
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
                    order_by: vec![],
                    relationship_order_by: Vec::new(),
                    limit: None,
                    offset: None,
                }
                .compile(&dialect)
                .unwrap(),
            );
        })
    });
}

criterion_group!(
    benches,
    bench_crud_compilation,
    bench_filter_compilation,
    bench_joined_compilation
);
criterion_main!(benches);
