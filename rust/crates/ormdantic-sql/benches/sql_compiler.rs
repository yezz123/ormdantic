use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ormdantic_dialects::PostgresDialect;
use ormdantic_sql::{
    Filter, JoinSpec, JoinedFilter, JoinedOrderBy, JoinedSelectColumn, OrderBy, QueryAst,
    SelectColumn, SelectInPlan, SortDirection, TableRef,
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

fn bench_large_filter_compilation(c: &mut Criterion) {
    let dialect = PostgresDialect;
    c.bench_function("compile_large_filter_tree", |b| {
        b.iter(|| {
            let mut columns = vec![SelectColumn::new("id"), SelectColumn::new("name")];
            columns.extend((0..32).map(|index| SelectColumn::new(format!("metric_{index}"))));

            let filters = vec![Filter::Or(
                (0..32)
                    .map(|index| {
                        Filter::And(vec![
                            Filter::Ge {
                                column: format!("metric_{index}"),
                                param: format!("metric_{index}_min"),
                            },
                            Filter::Le {
                                column: format!("metric_{index}"),
                                param: format!("metric_{index}_max"),
                            },
                            Filter::Like {
                                column: "name".to_string(),
                                param: format!("name_like_{index}"),
                            },
                        ])
                    })
                    .collect(),
            )];

            black_box(
                QueryAst::Select {
                    table: TableRef::new("flavors"),
                    columns,
                    filters,
                    order_by: vec![
                        OrderBy::new("name", SortDirection::Asc),
                        OrderBy::new("strength", SortDirection::Desc),
                    ],
                    limit: Some(500),
                    offset: Some(250),
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

fn bench_joined_relationship_modifier_compilation(c: &mut Criterion) {
    let dialect = PostgresDialect;
    c.bench_function("compile_joined_select_with_relationship_modifiers", |b| {
        b.iter(|| {
            black_box(
                QueryAst::JoinedSelect {
                    table: TableRef::new("coffee"),
                    columns: vec![
                        JoinedSelectColumn::aliased("coffee", "id", "coffee\\id"),
                        JoinedSelectColumn::aliased("coffee", "name", "coffee\\name"),
                        JoinedSelectColumn::aliased("coffee", "strength", "coffee\\strength"),
                        JoinedSelectColumn::aliased("coffee/flavor", "id", "coffee/flavor\\id"),
                        JoinedSelectColumn::aliased("coffee/flavor", "name", "coffee/flavor\\name"),
                        JoinedSelectColumn::aliased(
                            "coffee/flavor/origin",
                            "id",
                            "coffee/flavor/origin\\id",
                        ),
                        JoinedSelectColumn::aliased(
                            "coffee/flavor/origin",
                            "region",
                            "coffee/flavor/origin\\region",
                        ),
                        JoinedSelectColumn::aliased("coffee/roaster", "id", "coffee/roaster\\id"),
                        JoinedSelectColumn::aliased(
                            "coffee/roaster",
                            "name",
                            "coffee/roaster\\name",
                        ),
                    ],
                    joins: vec![
                        JoinSpec::left_join(
                            "flavors",
                            "coffee/flavor",
                            "coffee",
                            "flavor",
                            "coffee/flavor",
                            "id",
                        ),
                        JoinSpec::left_join(
                            "origins",
                            "coffee/flavor/origin",
                            "coffee/flavor",
                            "origin",
                            "coffee/flavor/origin",
                            "id",
                        ),
                        JoinSpec::left_join(
                            "roasters",
                            "coffee/roaster",
                            "coffee",
                            "roaster",
                            "coffee/roaster",
                            "id",
                        ),
                    ],
                    filters: vec![Filter::And(vec![
                        Filter::Ge {
                            column: "strength".to_string(),
                            param: "coffee_strength_min".to_string(),
                        },
                        Filter::Like {
                            column: "name".to_string(),
                            param: "coffee_name_like".to_string(),
                        },
                    ])],
                    relationship_filters: vec![
                        JoinedFilter::new(
                            "coffee/flavor",
                            Filter::And(vec![
                                Filter::Ge {
                                    column: "strength".to_string(),
                                    param: "flavor_strength_min".to_string(),
                                },
                                Filter::Like {
                                    column: "name".to_string(),
                                    param: "flavor_name_like".to_string(),
                                },
                            ]),
                        ),
                        JoinedFilter::new(
                            "coffee/flavor/origin",
                            Filter::In {
                                column: "region".to_string(),
                                params: vec![
                                    "origin_region_first".to_string(),
                                    "origin_region_second".to_string(),
                                    "origin_region_third".to_string(),
                                ],
                            },
                        ),
                        JoinedFilter::new(
                            "coffee/roaster",
                            Filter::Or(vec![
                                Filter::Eq {
                                    column: "active".to_string(),
                                    param: "roaster_active".to_string(),
                                },
                                Filter::IsNull {
                                    column: "archived_at".to_string(),
                                },
                            ]),
                        ),
                    ],
                    order_by: vec![OrderBy::new("name", SortDirection::Asc)],
                    relationship_order_by: vec![
                        JoinedOrderBy::new(
                            "coffee/flavor",
                            OrderBy::new("strength", SortDirection::Desc),
                        ),
                        JoinedOrderBy::new(
                            "coffee/flavor/origin",
                            OrderBy::new("region", SortDirection::Asc),
                        ),
                        JoinedOrderBy::new(
                            "coffee/roaster",
                            OrderBy::new("name", SortDirection::Asc),
                        ),
                    ],
                    limit: Some(250),
                    offset: Some(50),
                }
                .compile(&dialect)
                .unwrap(),
            );
        })
    });
}

fn bench_select_in_compilation(c: &mut Criterion) {
    let dialect = PostgresDialect;
    c.bench_function("compile_select_in_batch_500", |b| {
        b.iter(|| {
            let params = (0..500)
                .map(|index| format!("parent_id_{index}"))
                .collect::<Vec<_>>();

            black_box(
                SelectInPlan::new(
                    "bench_parents",
                    "bench_children",
                    vec!["id".to_string()],
                    vec!["parent".to_string()],
                )
                .query_for_batch(params)
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
    bench_large_filter_compilation,
    bench_joined_compilation,
    bench_joined_relationship_modifier_compilation,
    bench_select_in_compilation
);
criterion_main!(benches);
