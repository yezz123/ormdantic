use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ormdantic_core::{IsolationLevel, SavepointName, TransactionOptions};
use ormdantic_dialects::{AnyDialect, Dialect, PostgresDialect};
use ormdantic_schema::{
    CheckConstraintDef, ColumnDef, FieldKind, IndexDef, SchemaOperation, TableDef,
    UniqueConstraintDef,
};

fn bench_identifier_and_placeholder_rendering(c: &mut Criterion) {
    let dialects = all_dialects();
    c.bench_function("dialect_identifier_and_placeholder_rendering", |bench| {
        bench.iter(|| {
            for (index, dialect) in dialects.iter().enumerate() {
                black_box(dialect.quote_ident(black_box("flavor_name")));
                black_box(dialect.placeholder(black_box(index + 1)));
            }
        });
    });
}

fn bench_create_table_rendering(c: &mut Criterion) {
    let dialect = PostgresDialect;
    let operation = SchemaOperation::CreateTable(sample_table());

    c.bench_function("dialect_create_table_rendering", |bench| {
        bench.iter(|| {
            black_box(
                dialect
                    .compile_schema_operation(black_box(&operation))
                    .unwrap(),
            );
        });
    });
}

fn bench_transaction_rendering(c: &mut Criterion) {
    let dialects = all_dialects();
    let options = TransactionOptions::new().with_isolation_level(IsolationLevel::Serializable);
    let savepoint = SavepointName::new("sp_1").unwrap();

    c.bench_function("dialect_transaction_rendering", |bench| {
        bench.iter(|| {
            for dialect in &dialects {
                black_box(dialect.begin_transaction_sql(black_box(&options)));
                black_box(dialect.savepoint_sql(black_box(&savepoint)));
                black_box(dialect.rollback_to_savepoint_sql(black_box(&savepoint)));
                black_box(dialect.release_savepoint_sql(black_box(&savepoint)));
            }
        });
    });
}

fn all_dialects() -> Vec<AnyDialect> {
    [
        "sqlite",
        "postgresql",
        "mysql",
        "mariadb",
        "mssql",
        "oracle",
    ]
    .into_iter()
    .map(|name| AnyDialect::parse(name).unwrap())
    .collect()
}

fn sample_table() -> TableDef {
    TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![
            ColumnDef::new("id", FieldKind::Integer)
                .primary_key(true)
                .autoincrement(true),
            ColumnDef::new("name", FieldKind::String),
            ColumnDef::new("rating", FieldKind::Decimal).numeric(5, 2),
        ],
        vec![IndexDef::new("flavor_name_idx", vec!["name".to_string()])],
        vec![UniqueConstraintDef::new(
            "flavor_name_unique",
            vec!["name".to_string()],
        )],
        Vec::new(),
    )
    .with_check_constraints(vec![
        CheckConstraintDef::new("rating >= 0").named("flavor_rating_check")
    ])
}

criterion_group!(
    benches,
    bench_identifier_and_placeholder_rendering,
    bench_create_table_rendering,
    bench_transaction_rendering
);
criterion_main!(benches);
