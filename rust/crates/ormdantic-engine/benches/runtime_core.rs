use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ormdantic_engine::{DbValue, NativeConnection, QueryResult, StatementResult};

fn bench_statement_result_conversion(c: &mut Criterion) {
    c.bench_function("statement_result_from_query_result", |bench| {
        bench.iter(|| {
            let result = QueryResult::new(
                vec!["id".to_string(), "name".to_string()],
                vec![
                    vec![DbValue::Integer(1), DbValue::Text("vanilla".to_string())],
                    vec![DbValue::Integer(2), DbValue::Text("mocha".to_string())],
                ],
            );
            black_box(StatementResult::from_query_result(result));
        });
    });
}

fn bench_sqlite_native_select(c: &mut Criterion) {
    let mut connection =
        NativeConnection::open("sqlite:///:memory:").expect("sqlite connection should open");
    connection
        .execute(
            "CREATE TABLE flavors (id INTEGER PRIMARY KEY, name TEXT)",
            &[],
        )
        .expect("create table should work");
    connection
        .execute(
            "INSERT INTO flavors (id, name) VALUES (?1, ?2)",
            &[DbValue::Integer(1), DbValue::Text("vanilla".to_string())],
        )
        .expect("insert should work");

    c.bench_function("sqlite_native_select", |bench| {
        bench.iter(|| {
            let result = connection
                .execute(
                    "SELECT name FROM flavors WHERE id = ?1",
                    &[DbValue::Integer(1)],
                )
                .expect("select should work");
            black_box(result);
        });
    });
}

criterion_group!(
    benches,
    bench_statement_result_conversion,
    bench_sqlite_native_select
);
criterion_main!(benches);
