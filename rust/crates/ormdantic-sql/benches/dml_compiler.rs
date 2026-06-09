use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ormdantic_dialects::PostgresDialect;
use ormdantic_sql::{BinaryOp, DmlAst, Expr, TableSource};

fn metric_columns() -> Vec<String> {
    (0..8).map(|idx| format!("metric_{idx}")).collect()
}

fn bulk_rows(row_count: usize, columns: &[String]) -> Vec<Vec<Expr>> {
    (0..row_count)
        .map(|row| {
            columns
                .iter()
                .map(|column| Expr::param(format!("{column}_{row}")))
                .collect()
        })
        .collect()
}

fn bench_bulk_insert_compilation(c: &mut Criterion) {
    let dialect = PostgresDialect;
    c.bench_function("compile_bulk_insert_250x8", |bench| {
        bench.iter(|| {
            let columns = metric_columns();
            black_box(
                DmlAst::Insert {
                    table: TableSource::table("metric_samples"),
                    rows: bulk_rows(250, &columns),
                    columns,
                    returning: vec![Expr::column("id")],
                }
                .compile(&dialect)
                .unwrap(),
            );
        });
    });
}

fn bench_bulk_upsert_compilation(c: &mut Criterion) {
    let dialect = PostgresDialect;
    c.bench_function("compile_bulk_upsert_250x8", |bench| {
        bench.iter(|| {
            let columns = metric_columns();
            let update_assignments = columns
                .iter()
                .filter(|column| column.as_str() != "metric_0")
                .map(|column| (column.clone(), Expr::qualified_column("excluded", column)))
                .collect::<Vec<_>>();
            black_box(
                DmlAst::Upsert {
                    table: TableSource::table("metric_samples"),
                    rows: bulk_rows(250, &columns),
                    columns,
                    conflict_target: vec!["metric_0".to_string()],
                    update_assignments,
                    returning: vec![Expr::column("metric_0")],
                }
                .compile(&dialect)
                .unwrap(),
            );
        });
    });
}

fn bench_wide_update_and_delete_compilation(c: &mut Criterion) {
    let dialect = PostgresDialect;
    c.bench_function("compile_wide_update_and_delete", |bench| {
        bench.iter(|| {
            let assignments = (0..32)
                .map(|idx| {
                    (
                        format!("metric_{idx}"),
                        Expr::Binary {
                            left: Box::new(Expr::column(format!("metric_{idx}"))),
                            op: BinaryOp::Add,
                            right: Box::new(Expr::param(format!("metric_{idx}_delta"))),
                        },
                    )
                })
                .collect::<Vec<_>>();
            let where_expr = Expr::InList {
                expr: Box::new(Expr::column("sample_id")),
                values: (0..128)
                    .map(|idx| Expr::param(format!("sample_id_{idx}")))
                    .collect(),
                negated: false,
            };

            black_box(
                DmlAst::Update {
                    table: TableSource::table("metric_samples"),
                    assignments,
                    where_expr: Some(where_expr.clone()),
                    returning: vec![Expr::column("sample_id")],
                }
                .compile(&dialect)
                .unwrap(),
            );
            black_box(
                DmlAst::Delete {
                    table: TableSource::table("metric_samples"),
                    where_expr: Some(where_expr),
                    returning: vec![Expr::column("sample_id")],
                }
                .compile(&dialect)
                .unwrap(),
            );
        });
    });
}

criterion_group!(
    benches,
    bench_bulk_insert_compilation,
    bench_bulk_upsert_compilation,
    bench_wide_update_and_delete_compilation
);
criterion_main!(benches);
