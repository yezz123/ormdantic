use std::collections::HashSet;

use ormdantic_dialects::{PostgresDialect, SqliteDialect};
use ormdantic_sql::{
    BinaryOp, CommonTableExpr, DmlAst, Expr, OrderExpr, OrderNulls, Projection, SelectAst,
    SortDirection, TableSource, UnaryOp,
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
