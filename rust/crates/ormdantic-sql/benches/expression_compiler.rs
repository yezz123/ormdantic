use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ormdantic_dialects::PostgresDialect;
use ormdantic_sql::{
    BinaryOp, DmlAst, Expr, OrderExpr, OrderNulls, Projection, SelectAst, SortDirection,
    TableSource,
};

fn bench_expression_select(c: &mut Criterion) {
    let dialect = PostgresDialect;
    c.bench_function("compile_expression_select", |b| {
        b.iter(|| {
            let total = Expr::Function {
                name: "SUM".to_string(),
                args: vec![Expr::column("total")],
            };
            black_box(
                SelectAst::new(vec![
                    Projection::new(Expr::column("customer_id")),
                    Projection::aliased(total.clone(), "total_sum"),
                    Projection::aliased(
                        Expr::Function {
                            name: "COUNT".to_string(),
                            args: vec![Expr::RawSafe("*".to_string())],
                        },
                        "row_count",
                    ),
                ])
                .from(TableSource::table("orders"))
                .where_expr(Expr::Binary {
                    left: Box::new(Expr::column("status")),
                    op: BinaryOp::Eq,
                    right: Box::new(Expr::param("status")),
                })
                .group_by(vec![Expr::column("customer_id")])
                .having(Expr::Binary {
                    left: Box::new(total.clone()),
                    op: BinaryOp::Gt,
                    right: Box::new(Expr::param("minimum_total")),
                })
                .order_by(vec![
                    OrderExpr::new(total, SortDirection::Desc).nulls(OrderNulls::Last)
                ])
                .compile(&dialect)
                .unwrap(),
            );
        })
    });
}

fn bench_dml_expression_returning(c: &mut Criterion) {
    let dialect = PostgresDialect;
    c.bench_function("compile_dml_expression_returning", |b| {
        b.iter(|| {
            black_box(
                DmlAst::Update {
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
                    returning: vec![Expr::column("id"), Expr::column("total")],
                }
                .compile(&dialect)
                .unwrap(),
            );
        })
    });
}

criterion_group!(
    benches,
    bench_expression_select,
    bench_dml_expression_returning
);
criterion_main!(benches);
