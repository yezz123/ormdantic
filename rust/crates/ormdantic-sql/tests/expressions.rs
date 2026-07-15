use std::collections::HashSet;

use ormdantic_dialects::{PostgresDialect, SqliteDialect};
use ormdantic_sql::{
    BinaryOp, CommonTableExpr, DmlAst, Expr, JoinAst, JoinKind, OrderExpr, OrderNulls, Projection,
    SelectAst, SortDirection, TableSource, UnaryOp,
};

#[test]
fn compiles_grouped_aggregate_select_with_null_ordering() {
    let total = Expr::Function {
        name: "SUM".to_string(),
        args: vec![Expr::column("total")],
    };
    let query = SelectAst::new(vec![
        Projection::new(Expr::column("customer_id")),
        Projection::aliased(total.clone(), "total_sum"),
        Projection::aliased(
            Expr::Function {
                name: "AVG".to_string(),
                args: vec![Expr::column("total")],
            },
            "average_total",
        ),
        Projection::aliased(
            Expr::Function {
                name: "MIN".to_string(),
                args: vec![Expr::column("total")],
            },
            "minimum_total",
        ),
        Projection::aliased(
            Expr::Function {
                name: "MAX".to_string(),
                args: vec![Expr::column("total")],
            },
            "maximum_total",
        ),
        Projection::aliased(
            Expr::Function {
                name: "COUNT".to_string(),
                args: vec![Expr::RawSafe("*".to_string())],
            },
            "row_count",
        ),
        Projection::aliased(
            Expr::Literal(ormdantic_sql::SqlLiteral::String("orders".to_string())),
            "source",
        ),
    ])
    .from(TableSource::table("orders"))
    .where_expr(Expr::InList {
        expr: Box::new(Expr::column("status")),
        values: vec![Expr::param("status_0"), Expr::param("status_1")],
        negated: false,
    })
    .group_by(vec![Expr::column("customer_id")])
    .having(Expr::Binary {
        left: Box::new(total.clone()),
        op: BinaryOp::Gt,
        right: Box::new(Expr::param("minimum_total")),
    })
    .order_by(vec![
        OrderExpr::new(total, SortDirection::Desc).nulls(OrderNulls::Last)
    ]);

    let compiled = query
        .compile(&PostgresDialect)
        .expect("expression query should compile");

    assert_eq!(
        compiled.sql(),
        "SELECT \"customer_id\", SUM(\"total\") AS \"total_sum\", AVG(\"total\") AS \"average_total\", MIN(\"total\") AS \"minimum_total\", MAX(\"total\") AS \"maximum_total\", COUNT(*) AS \"row_count\", 'orders' AS \"source\" FROM \"orders\" WHERE (\"status\" IN ($1, $2)) GROUP BY \"customer_id\" HAVING (SUM(\"total\") > $3) ORDER BY SUM(\"total\") DESC NULLS LAST"
    );
    assert_eq!(
        compiled.params(),
        &[
            "status_0".to_string(),
            "status_1".to_string(),
            "minimum_total".to_string()
        ]
    );
}

#[test]
fn compiles_empty_expression_in_lists_as_constants() {
    let query = SelectAst::new(vec![Projection::new(Expr::column("id"))])
        .from(TableSource::table("orders"))
        .where_expr(Expr::Binary {
            left: Box::new(Expr::InList {
                expr: Box::new(Expr::column("status")),
                values: Vec::new(),
                negated: false,
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::InList {
                expr: Box::new(Expr::column("kind")),
                values: Vec::new(),
                negated: true,
            }),
        });

    let compiled = query
        .compile(&PostgresDialect)
        .expect("expression query should compile");

    assert_eq!(
        compiled.sql(),
        "SELECT \"id\" FROM \"orders\" WHERE ((1 = 0) AND (1 = 1))"
    );
    assert!(compiled.params().is_empty());
}

#[test]
fn compiles_remaining_literal_unary_and_binary_expression_arms() {
    let query = SelectAst::new(vec![
        Projection::aliased(
            Expr::Binary {
                left: Box::new(Expr::column("name")),
                op: BinaryOp::Ne,
                right: Box::new(Expr::Literal(ormdantic_sql::SqlLiteral::String(
                    "mint".to_string(),
                ))),
            },
            "not_mint",
        ),
        Projection::aliased(
            Expr::Binary {
                left: Box::new(Expr::column("score")),
                op: BinaryOp::Le,
                right: Box::new(Expr::Literal(ormdantic_sql::SqlLiteral::Integer(10))),
            },
            "score_le",
        ),
        Projection::aliased(
            Expr::Binary {
                left: Box::new(Expr::column("score")),
                op: BinaryOp::Ge,
                right: Box::new(Expr::Literal(ormdantic_sql::SqlLiteral::Integer(1))),
            },
            "score_ge",
        ),
        Projection::aliased(
            Expr::Binary {
                left: Box::new(Expr::Binary {
                    left: Box::new(Expr::column("score")),
                    op: BinaryOp::Sub,
                    right: Box::new(Expr::Literal(ormdantic_sql::SqlLiteral::Integer(1))),
                }),
                op: BinaryOp::Mul,
                right: Box::new(Expr::Binary {
                    left: Box::new(Expr::Literal(ormdantic_sql::SqlLiteral::Integer(2))),
                    op: BinaryOp::Div,
                    right: Box::new(Expr::Literal(ormdantic_sql::SqlLiteral::Integer(1))),
                }),
            },
            "score_math",
        ),
        Projection::aliased(
            Expr::Binary {
                left: Box::new(Expr::column("name")),
                op: BinaryOp::Like,
                right: Box::new(Expr::Literal(ormdantic_sql::SqlLiteral::String(
                    "m%".to_string(),
                ))),
            },
            "like_name",
        ),
        Projection::aliased(
            Expr::Binary {
                left: Box::new(Expr::column("name")),
                op: BinaryOp::ILike,
                right: Box::new(Expr::Literal(ormdantic_sql::SqlLiteral::String(
                    "M%".to_string(),
                ))),
            },
            "ilike_name",
        ),
        Projection::aliased(
            Expr::Unary {
                op: UnaryOp::IsNull,
                expr: Box::new(Expr::column("deleted_at")),
            },
            "deleted_null",
        ),
        Projection::aliased(
            Expr::Unary {
                op: UnaryOp::IsNotNull,
                expr: Box::new(Expr::column("created_at")),
            },
            "created_not_null",
        ),
        Projection::aliased(
            Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(Expr::Literal(ormdantic_sql::SqlLiteral::Boolean(false))),
            },
            "not_false",
        ),
    ])
    .from(TableSource::table("flavors"))
    .where_expr(Expr::Binary {
        left: Box::new(Expr::Binary {
            left: Box::new(Expr::column("score")),
            op: BinaryOp::Lt,
            right: Box::new(Expr::param("max_score")),
        }),
        op: BinaryOp::Or,
        right: Box::new(Expr::Binary {
            left: Box::new(Expr::column("active")),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(ormdantic_sql::SqlLiteral::Boolean(true))),
        }),
    });

    let compiled = query
        .compile(&PostgresDialect)
        .expect("remaining expression arms should compile");

    assert_eq!(
        compiled.sql(),
        "SELECT (\"name\" != 'mint') AS \"not_mint\", (\"score\" <= 10) AS \"score_le\", (\"score\" >= 1) AS \"score_ge\", ((\"score\" - 1) * (2 / 1)) AS \"score_math\", (\"name\" LIKE 'm%') AS \"like_name\", (\"name\" ILIKE 'M%') AS \"ilike_name\", (\"deleted_at\" IS NULL) AS \"deleted_null\", (\"created_at\" IS NOT NULL) AS \"created_not_null\", (NOT FALSE) AS \"not_false\" FROM \"flavors\" WHERE ((\"score\" < $1) OR (\"active\" = TRUE))"
    );
    assert_eq!(compiled.params(), &["max_score".to_string()]);
}

#[test]
fn rewrites_sqlite_decimal_expression_comparisons_and_ordering() {
    let decimal_columns = HashSet::from(["amount".to_string()]);
    let table_names = vec!["prices".to_string()];
    let query = SelectAst::new(vec![Projection::new(Expr::column("id"))])
        .from(TableSource::table("prices"))
        .where_expr(Expr::Binary {
            left: Box::new(Expr::InList {
                expr: Box::new(Expr::column("amount")),
                values: vec![Expr::param("exact_0"), Expr::param("exact_1")],
                negated: false,
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::Binary {
                left: Box::new(Expr::column("amount")),
                op: BinaryOp::Gt,
                right: Box::new(Expr::param("minimum")),
            }),
        })
        .order_by(vec![OrderExpr::new(
            Expr::column("amount"),
            SortDirection::Asc,
        )])
        .rewrite_sqlite_decimal_columns(&decimal_columns, &table_names);

    let compiled = query
        .compile(&SqliteDialect)
        .expect("rewritten decimal expression query should compile");

    assert_eq!(
        compiled.sql(),
        "SELECT \"id\" FROM \"prices\" WHERE (((ormdantic_decimal_cmp(\"amount\", ?) = 0) OR (ormdantic_decimal_cmp(\"amount\", ?) = 0)) AND (ormdantic_decimal_cmp(\"amount\", ?) > 0)) ORDER BY ormdantic_decimal_sort_key(\"amount\") ASC"
    );
    assert_eq!(
        compiled.params(),
        &[
            "exact_0".to_string(),
            "exact_1".to_string(),
            "minimum".to_string()
        ]
    );
}

#[test]
fn rewrites_sqlite_decimal_nested_expression_shapes() {
    let decimal_columns = HashSet::from(["amount".to_string()]);
    let table_names = vec!["prices".to_string(), "discounts".to_string()];
    let subquery = SelectAst::new(vec![Projection::new(Expr::column("id"))])
        .from(TableSource::table("limits"));
    let query = SelectAst::new(vec![
        Projection::aliased(
            Expr::Case {
                whens: vec![(
                    Expr::InList {
                        expr: Box::new(Expr::qualified_column("prices", "amount")),
                        values: vec![Expr::param("blocked_amount")],
                        negated: true,
                    },
                    Expr::Cast {
                        expr: Box::new(Expr::qualified_column("prices", "amount")),
                        type_name: "TEXT".to_string(),
                    },
                )],
                else_expr: Some(Box::new(Expr::Tuple(vec![
                    Expr::Function {
                        name: "ABS".to_string(),
                        args: vec![Expr::Unary {
                            op: UnaryOp::Neg,
                            expr: Box::new(Expr::qualified_column("prices", "amount")),
                        }],
                    },
                    Expr::Literal(ormdantic_sql::SqlLiteral::Null),
                ]))),
            },
            "amount_bucket",
        ),
        Projection::aliased(
            Expr::Window {
                expr: Box::new(Expr::Function {
                    name: "COUNT".to_string(),
                    args: vec![Expr::RawSafe("*".to_string())],
                }),
                partition_by: vec![Expr::qualified_column("prices", "amount")],
                order_by: vec![OrderExpr::new(
                    Expr::qualified_column("prices", "amount"),
                    SortDirection::Desc,
                )],
            },
            "amount_window",
        ),
        Projection::aliased(Expr::Subquery(Box::new(subquery.clone())), "limit_id"),
        Projection::aliased(
            Expr::Exists {
                select: Box::new(subquery.clone()),
                negated: false,
            },
            "has_limit",
        ),
    ])
    .from(TableSource::aliased_table("prices", "prices"))
    .join(JoinAst::new(
        JoinKind::Inner,
        TableSource::aliased_table("discounts", "discounts"),
        Some(Expr::eq(
            Expr::qualified_column("discounts", "amount"),
            Expr::qualified_column("prices", "amount"),
        )),
    ))
    .where_expr(Expr::Binary {
        left: Box::new(Expr::Between {
            expr: Box::new(Expr::column("other_amount")),
            low: Box::new(Expr::param("other_low")),
            high: Box::new(Expr::param("other_high")),
        }),
        op: BinaryOp::And,
        right: Box::new(Expr::InSubquery {
            expr: Box::new(Expr::qualified_column("prices", "amount")),
            select: Box::new(subquery),
            negated: true,
        }),
    })
    .rewrite_sqlite_decimal_columns(&decimal_columns, &table_names);

    let compiled = query
        .compile(&SqliteDialect)
        .expect("nested decimal expression query should compile");
    let sql = compiled.sql();

    assert!(
        sql.contains("NOT (ormdantic_decimal_cmp(\"prices\".\"amount\", ?) = 0)"),
        "{sql}"
    );
    assert!(sql.contains("CAST(\"prices\".\"amount\" AS TEXT)"), "{sql}");
    assert!(sql.contains("ABS((-\"prices\".\"amount\")), NULL"), "{sql}");
    assert!(
        sql.contains("(SELECT \"id\" FROM \"limits\") AS \"limit_id\""),
        "{sql}"
    );
    assert!(
        sql.contains("(EXISTS (SELECT \"id\" FROM \"limits\")) AS \"has_limit\""),
        "{sql}"
    );
    assert!(
        sql.contains(
            "COUNT(*) OVER (PARTITION BY \"prices\".\"amount\" ORDER BY ormdantic_decimal_sort_key(\"prices\".\"amount\") DESC)"
        ),
        "{sql}"
    );
    assert!(
        sql.contains("(\"prices\".\"amount\" NOT IN (SELECT \"id\" FROM \"limits\"))"),
        "{sql}"
    );
    assert_eq!(
        compiled.params(),
        &[
            "blocked_amount".to_string(),
            "other_low".to_string(),
            "other_high".to_string(),
        ]
    );
}

#[test]
fn dml_decimal_rewrite_preserves_insert_upsert_and_empty_column_sets() {
    let decimal_columns = HashSet::from(["amount".to_string()]);
    let table_names = vec!["prices".to_string()];
    let insert = DmlAst::Insert {
        table: TableSource::table("prices"),
        columns: vec!["id".to_string(), "amount".to_string()],
        rows: vec![vec![Expr::param("id"), Expr::param("amount")]],
        returning: Vec::new(),
    }
    .rewrite_sqlite_decimal_columns(&decimal_columns, &table_names);
    let upsert = DmlAst::Upsert {
        table: TableSource::table("prices"),
        columns: vec!["id".to_string(), "amount".to_string()],
        rows: vec![vec![Expr::param("id"), Expr::param("amount")]],
        conflict_target: vec!["id".to_string()],
        update_assignments: vec![("amount".to_string(), Expr::param("amount"))],
        returning: Vec::new(),
    }
    .rewrite_sqlite_decimal_columns(&decimal_columns, &table_names);
    let update = DmlAst::Update {
        table: TableSource::table("prices"),
        assignments: vec![("amount".to_string(), Expr::param("amount"))],
        where_expr: Some(Expr::Binary {
            left: Box::new(Expr::eq(Expr::column("amount"), Expr::param("old_amount"))),
            op: BinaryOp::And,
            right: Box::new(Expr::InList {
                expr: Box::new(Expr::column("status")),
                values: vec![Expr::param("status")],
                negated: false,
            }),
        }),
        returning: Vec::new(),
    }
    .rewrite_sqlite_decimal_columns(&decimal_columns, &table_names);

    assert_eq!(
        insert.compile(&SqliteDialect).unwrap().sql(),
        "INSERT INTO \"prices\" (\"id\", \"amount\") VALUES (?, ?)"
    );
    assert_eq!(
        upsert.compile(&SqliteDialect).unwrap().sql(),
        "INSERT INTO \"prices\" (\"id\", \"amount\") VALUES (?, ?) ON CONFLICT (\"id\") DO UPDATE SET \"amount\" = excluded.\"amount\""
    );
    assert_eq!(
        update.compile(&SqliteDialect).unwrap().sql(),
        "UPDATE \"prices\" SET \"amount\" = ? WHERE ((ormdantic_decimal_cmp(\"amount\", ?) = 0) AND (\"status\" IN (?)))"
    );
}

#[test]
fn compiles_update_expression_with_arithmetic_assignment() {
    let query = DmlAst::Update {
        table: TableSource::table("orders"),
        assignments: vec![
            (
                "total".to_string(),
                Expr::Binary {
                    left: Box::new(Expr::column("total")),
                    op: BinaryOp::Add,
                    right: Box::new(Expr::param("increment")),
                },
            ),
            ("status".to_string(), Expr::param("status")),
        ],
        where_expr: Some(Expr::Binary {
            left: Box::new(Expr::column("customer_id")),
            op: BinaryOp::Eq,
            right: Box::new(Expr::param("customer_id")),
        }),
        returning: Vec::new(),
    };

    let compiled = query
        .compile(&PostgresDialect)
        .expect("update expression should compile");

    assert_eq!(
        compiled.sql(),
        "UPDATE \"orders\" SET \"total\" = (\"total\" + $1), \"status\" = $2 WHERE (\"customer_id\" = $3)"
    );
    assert_eq!(
        compiled.params(),
        &[
            "increment".to_string(),
            "status".to_string(),
            "customer_id".to_string()
        ]
    );
}

#[test]
fn compiles_case_cast_tuple_and_null_predicate_expressions() {
    let query = SelectAst::new(vec![
        Projection::new(Expr::column("id")),
        Projection::aliased(
            Expr::Cast {
                expr: Box::new(Expr::column("created_at")),
                type_name: "TEXT".to_string(),
            },
            "created_at_text",
        ),
        Projection::aliased(
            Expr::Case {
                whens: vec![(
                    Expr::Binary {
                        left: Box::new(Expr::column("tier")),
                        op: BinaryOp::Eq,
                        right: Box::new(Expr::param("tier")),
                    },
                    Expr::Literal(ormdantic_sql::SqlLiteral::String("priority".to_string())),
                )],
                else_expr: Some(Box::new(Expr::Literal(ormdantic_sql::SqlLiteral::String(
                    "standard".to_string(),
                )))),
            },
            "service_level",
        ),
        Projection::aliased(
            Expr::Tuple(vec![Expr::column("country"), Expr::column("city")]),
            "location_key",
        ),
    ])
    .from(TableSource::table("customers"))
    .where_expr(Expr::Binary {
        left: Box::new(Expr::Binary {
            left: Box::new(Expr::column("tier")),
            op: BinaryOp::Eq,
            right: Box::new(Expr::param("tier_filter")),
        }),
        op: BinaryOp::And,
        right: Box::new(Expr::Unary {
            op: UnaryOp::IsNotNull,
            expr: Box::new(Expr::column("id")),
        }),
    });

    let compiled = query
        .compile(&PostgresDialect)
        .expect("rich expression query should compile");

    assert_eq!(
        compiled.sql(),
        "SELECT \"id\", CAST(\"created_at\" AS TEXT) AS \"created_at_text\", CASE WHEN (\"tier\" = $1) THEN 'priority' ELSE 'standard' END AS \"service_level\", (\"country\", \"city\") AS \"location_key\" FROM \"customers\" WHERE ((\"tier\" = $2) AND (\"id\" IS NOT NULL))"
    );
    assert_eq!(
        compiled.params(),
        &["tier".to_string(), "tier_filter".to_string()]
    );
}

#[test]
fn compiles_subquery_predicates_and_scalar_subqueries() {
    let order_count = SelectAst::new(vec![Projection::new(Expr::Function {
        name: "COUNT".to_string(),
        args: vec![Expr::RawSafe("*".to_string())],
    })])
    .from(TableSource::table("orders"))
    .where_expr(Expr::Binary {
        left: Box::new(Expr::qualified_column("orders", "customer_id")),
        op: BinaryOp::Eq,
        right: Box::new(Expr::qualified_column("customers", "id")),
    });
    let paid_customer_ids = SelectAst::new(vec![Projection::new(Expr::column("customer_id"))])
        .from(TableSource::table("orders"))
        .where_expr(Expr::Binary {
            left: Box::new(Expr::column("status")),
            op: BinaryOp::Eq,
            right: Box::new(Expr::param("paid_status")),
        });
    let banned_customers = SelectAst::new(vec![Projection::new(Expr::Literal(
        ormdantic_sql::SqlLiteral::Integer(1),
    ))])
    .from(TableSource::table("bans"))
    .where_expr(Expr::Binary {
        left: Box::new(Expr::qualified_column("bans", "customer_id")),
        op: BinaryOp::Eq,
        right: Box::new(Expr::qualified_column("customers", "id")),
    });
    let query = SelectAst::new(vec![
        Projection::new(Expr::column("id")),
        Projection::aliased(Expr::Subquery(Box::new(order_count)), "order_count"),
    ])
    .from(TableSource::table("customers"))
    .where_expr(Expr::Binary {
        left: Box::new(Expr::Binary {
            left: Box::new(Expr::Exists {
                select: Box::new(paid_customer_ids.clone()),
                negated: false,
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::InSubquery {
                expr: Box::new(Expr::column("id")),
                select: Box::new(paid_customer_ids),
                negated: false,
            }),
        }),
        op: BinaryOp::And,
        right: Box::new(Expr::Exists {
            select: Box::new(banned_customers),
            negated: true,
        }),
    });

    let compiled = query
        .compile(&PostgresDialect)
        .expect("subquery expression query should compile");

    assert_eq!(
        compiled.sql(),
        "SELECT \"id\", (SELECT COUNT(*) FROM \"orders\" WHERE (\"orders\".\"customer_id\" = \"customers\".\"id\")) AS \"order_count\" FROM \"customers\" WHERE (((EXISTS (SELECT \"customer_id\" FROM \"orders\" WHERE (\"status\" = $1))) AND (\"id\" IN (SELECT \"customer_id\" FROM \"orders\" WHERE (\"status\" = $2)))) AND (NOT EXISTS (SELECT 1 FROM \"bans\" WHERE (\"bans\".\"customer_id\" = \"customers\".\"id\"))))"
    );
    assert_eq!(
        compiled.params(),
        &["paid_status".to_string(), "paid_status".to_string()]
    );
}

#[test]
fn compiles_ctes_and_window_expressions() {
    let paid_orders = SelectAst::new(vec![
        Projection::new(Expr::column("customer_id")),
        Projection::aliased(
            Expr::Function {
                name: "SUM".to_string(),
                args: vec![Expr::column("total")],
            },
            "paid_total",
        ),
    ])
    .from(TableSource::table("orders"))
    .where_expr(Expr::Binary {
        left: Box::new(Expr::column("status")),
        op: BinaryOp::Eq,
        right: Box::new(Expr::param("status")),
    })
    .group_by(vec![Expr::column("customer_id")]);
    let query = SelectAst::new(vec![
        Projection::new(Expr::column("customer_id")),
        Projection::new(Expr::column("paid_total")),
        Projection::aliased(
            Expr::Window {
                expr: Box::new(Expr::Function {
                    name: "SUM".to_string(),
                    args: vec![Expr::column("paid_total")],
                }),
                partition_by: Vec::new(),
                order_by: vec![OrderExpr::new(
                    Expr::column("paid_total"),
                    SortDirection::Desc,
                )],
            },
            "running_total",
        ),
    ])
    .with_cte(CommonTableExpr::new("paid_orders", paid_orders))
    .from(TableSource::table("paid_orders"))
    .where_expr(Expr::Binary {
        left: Box::new(Expr::column("paid_total")),
        op: BinaryOp::Gt,
        right: Box::new(Expr::param("minimum_total")),
    })
    .order_by(vec![OrderExpr::new(
        Expr::column("paid_total"),
        SortDirection::Desc,
    )]);

    let compiled = query
        .compile(&PostgresDialect)
        .expect("cte window expression query should compile");

    assert_eq!(
        compiled.sql(),
        "WITH \"paid_orders\" AS (SELECT \"customer_id\", SUM(\"total\") AS \"paid_total\" FROM \"orders\" WHERE (\"status\" = $1) GROUP BY \"customer_id\") SELECT \"customer_id\", \"paid_total\", SUM(\"paid_total\") OVER (ORDER BY \"paid_total\" DESC) AS \"running_total\" FROM \"paid_orders\" WHERE (\"paid_total\" > $2) ORDER BY \"paid_total\" DESC"
    );
    assert_eq!(
        compiled.params(),
        &["status".to_string(), "minimum_total".to_string()]
    );
}

#[test]
fn compiles_recursive_ctes_join_variants_distinct_and_window_partitions() {
    let recursive_seed = SelectAst::new(vec![
        Projection::new(Expr::Literal(ormdantic_sql::SqlLiteral::Integer(1))),
        Projection::new(Expr::Literal(ormdantic_sql::SqlLiteral::Integer(0))),
    ]);
    let query = SelectAst::new(vec![
        Projection::new(Expr::qualified_column("tree", "id")),
        Projection::aliased(
            Expr::Window {
                expr: Box::new(Expr::Function {
                    name: "COUNT".to_string(),
                    args: vec![Expr::RawSafe("*".to_string())],
                }),
                partition_by: vec![Expr::qualified_column("tree", "depth")],
                order_by: vec![OrderExpr::new(
                    Expr::qualified_column("tree", "id"),
                    SortDirection::Asc,
                )
                .nulls(OrderNulls::First)],
            },
            "depth_count",
        ),
        Projection::aliased(
            Expr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(Expr::qualified_column("tree", "depth")),
            },
            "negative_depth",
        ),
    ])
    .with_cte(
        CommonTableExpr::new("tree", recursive_seed)
            .columns(vec!["id".to_string(), "depth".to_string()])
            .recursive(true),
    )
    .from(TableSource::aliased_table("tree", "tree"))
    .join(JoinAst::new(
        JoinKind::Left,
        TableSource::aliased_table("labels", "labels"),
        Some(Expr::eq(
            Expr::qualified_column("labels", "id"),
            Expr::qualified_column("tree", "id"),
        )),
    ))
    .join(JoinAst::new(
        JoinKind::Right,
        TableSource::RawSafe("LATERAL (SELECT 1 AS marker) AS marker".to_string()),
        Some(Expr::Literal(ormdantic_sql::SqlLiteral::Boolean(true))),
    ))
    .join(JoinAst::new(
        JoinKind::Full,
        TableSource::aliased_table("fallback", "fallback"),
        Some(Expr::Unary {
            op: UnaryOp::Not,
            expr: Box::new(Expr::Binary {
                left: Box::new(Expr::qualified_column("fallback", "id")),
                op: BinaryOp::Eq,
                right: Box::new(Expr::qualified_column("tree", "id")),
            }),
        }),
    ))
    .join(JoinAst::new(
        JoinKind::Cross,
        TableSource::RawSafe("(SELECT 'x' AS tag) AS tags".to_string()),
        None,
    ))
    .where_expr(Expr::Between {
        expr: Box::new(Expr::qualified_column("tree", "depth")),
        low: Box::new(Expr::param("min_depth")),
        high: Box::new(Expr::param("max_depth")),
    })
    .distinct(true)
    .limit(25)
    .offset(50);

    let compiled = query
        .compile(&PostgresDialect)
        .expect("recursive join query should compile");

    assert_eq!(
        compiled.sql(),
        "WITH RECURSIVE \"tree\" (\"id\", \"depth\") AS (SELECT 1, 0) SELECT DISTINCT \"tree\".\"id\", COUNT(*) OVER (PARTITION BY \"tree\".\"depth\" ORDER BY \"tree\".\"id\" ASC NULLS FIRST) AS \"depth_count\", (-\"tree\".\"depth\") AS \"negative_depth\" FROM \"tree\" AS \"tree\" LEFT JOIN \"labels\" AS \"labels\" ON (\"labels\".\"id\" = \"tree\".\"id\") RIGHT JOIN LATERAL (SELECT 1 AS marker) AS marker ON TRUE FULL JOIN \"fallback\" AS \"fallback\" ON (NOT (\"fallback\".\"id\" = \"tree\".\"id\")) CROSS JOIN (SELECT 'x' AS tag) AS tags WHERE (\"tree\".\"depth\" BETWEEN $1 AND $2) LIMIT 25 OFFSET 50"
    );
    assert_eq!(
        compiled.params(),
        &["min_depth".to_string(), "max_depth".to_string()]
    );
}

#[test]
fn rewrites_sqlite_decimal_branches_for_select_and_dml() {
    let decimal_columns = HashSet::from(["amount".to_string()]);
    let table_names = vec!["prices".to_string()];
    let untouched = SelectAst::new(vec![Projection::new(Expr::column("amount"))])
        .from(TableSource::table("prices"))
        .rewrite_sqlite_decimal_columns(&HashSet::new(), &table_names)
        .compile(&SqliteDialect)
        .expect("empty decimal set should leave query untouched");

    assert_eq!(untouched.sql(), "SELECT \"amount\" FROM \"prices\"");

    let query = SelectAst::new(vec![
        Projection::aliased(
            Expr::Cast {
                expr: Box::new(Expr::Case {
                    whens: vec![(
                        Expr::InList {
                            expr: Box::new(Expr::column("amount")),
                            values: vec![Expr::param("amount_a"), Expr::param("amount_b")],
                            negated: true,
                        },
                        Expr::Tuple(vec![Expr::column("amount"), Expr::param("fallback")]),
                    )],
                    else_expr: Some(Box::new(Expr::Function {
                        name: "ABS".to_string(),
                        args: vec![Expr::column("amount")],
                    })),
                }),
                type_name: "TEXT".to_string(),
            },
            "amount_key",
        ),
        Projection::aliased(
            Expr::InSubquery {
                expr: Box::new(Expr::column("amount")),
                select: Box::new(
                    SelectAst::new(vec![Projection::new(Expr::column("amount"))])
                        .from(TableSource::table("archived_prices")),
                ),
                negated: true,
            },
            "not_archived",
        ),
    ])
    .from(TableSource::table("prices"))
    .join(JoinAst::new(
        JoinKind::Inner,
        TableSource::aliased_table("thresholds", "thresholds"),
        Some(Expr::Binary {
            left: Box::new(Expr::qualified_column("prices", "amount")),
            op: BinaryOp::Le,
            right: Box::new(Expr::qualified_column("thresholds", "amount")),
        }),
    ))
    .where_expr(Expr::Between {
        expr: Box::new(Expr::column("amount")),
        low: Box::new(Expr::param("low")),
        high: Box::new(Expr::param("high")),
    })
    .group_by(vec![Expr::column("amount")])
    .having(Expr::Binary {
        left: Box::new(Expr::column("amount")),
        op: BinaryOp::Ne,
        right: Box::new(Expr::param("excluded")),
    })
    .order_by(vec![OrderExpr::new(
        Expr::Function {
            name: "COALESCE".to_string(),
            args: vec![Expr::column("amount"), Expr::param("fallback_order")],
        },
        SortDirection::Desc,
    )])
    .rewrite_sqlite_decimal_columns(&decimal_columns, &table_names);

    let compiled = query
        .compile(&SqliteDialect)
        .expect("decimal rewrite should compile");

    assert!(compiled.sql().contains("NOT ((ormdantic_decimal_cmp"));
    assert!(compiled
        .sql()
        .contains("ormdantic_decimal_cmp(\"amount\", ?) >= 0"));
    assert!(compiled
        .sql()
        .contains("ormdantic_decimal_cmp(\"amount\", ?) <= 0"));
    assert!(compiled.sql().contains("GROUP BY \"amount\""));
    assert!(compiled
        .sql()
        .contains("HAVING (ormdantic_decimal_cmp(\"amount\", ?) != 0)"));
    assert!(compiled
        .sql()
        .contains("ORDER BY COALESCE(\"amount\", ?) DESC"));
    assert_eq!(
        compiled.params(),
        &[
            "amount_a".to_string(),
            "amount_b".to_string(),
            "fallback".to_string(),
            "low".to_string(),
            "high".to_string(),
            "excluded".to_string(),
            "fallback_order".to_string()
        ]
    );

    let delete = DmlAst::Delete {
        table: TableSource::table("prices"),
        where_expr: Some(Expr::Between {
            expr: Box::new(Expr::column("amount")),
            low: Box::new(Expr::param("low")),
            high: Box::new(Expr::param("high")),
        }),
        returning: Vec::new(),
    }
    .rewrite_sqlite_decimal_columns(&decimal_columns, &table_names)
    .compile(&SqliteDialect)
    .expect("rewritten decimal delete should compile");

    assert_eq!(
        delete.sql(),
        "DELETE FROM \"prices\" WHERE ((ormdantic_decimal_cmp(\"amount\", ?) >= 0) AND (ormdantic_decimal_cmp(\"amount\", ?) <= 0))"
    );
    assert_eq!(delete.params(), &["low".to_string(), "high".to_string()]);
}
