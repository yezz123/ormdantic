use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ormdantic_schema::{
    ColumnDef, FieldKind, IndexDef, SchemaDef, SchemaDiffer, SchemaRegistry, SchemaSnapshot,
    TableDef, UniqueConstraintDef,
};

fn bench_registry_validation(c: &mut Criterion) {
    c.bench_function("register_100_tables", |b| {
        b.iter(|| {
            let mut registry = SchemaRegistry::new();
            for index in 0..100 {
                registry
                    .register_table(TableDef::from_parts(
                        format!("table_{index}"),
                        format!("Model{index}"),
                        "id",
                        vec![
                            ColumnDef::new("id", FieldKind::Uuid).primary_key(true),
                            ColumnDef::new("name", FieldKind::String),
                            ColumnDef::new("score", FieldKind::Integer),
                        ],
                        vec![IndexDef::new(
                            format!("table_{index}_name_idx"),
                            vec!["name".to_string()],
                        )],
                        vec![UniqueConstraintDef::new(
                            format!("table_{index}_name_unique"),
                            vec!["name".to_string()],
                        )],
                        Vec::new(),
                    ))
                    .unwrap();
            }
            black_box(registry);
        })
    });
}

fn bench_schema_diff(c: &mut Criterion) {
    c.bench_function("diff_100_tables_with_column_changes", |b| {
        b.iter(|| {
            let before = schema_with_tables(100, false);
            let after = schema_with_tables(100, true);
            black_box(
                SchemaDiffer::diff(&SchemaSnapshot::new(before), &SchemaSnapshot::new(after))
                    .unwrap(),
            );
        })
    });
}

fn schema_with_tables(count: usize, expanded: bool) -> SchemaDef {
    SchemaDef::from_tables(
        (0..count)
            .map(|index| {
                let mut columns = vec![
                    ColumnDef::new("id", FieldKind::Uuid).primary_key(true),
                    ColumnDef::new("name", FieldKind::String),
                ];
                if expanded {
                    columns.push(ColumnDef::new("score", FieldKind::Integer));
                }
                TableDef::from_parts(
                    format!("table_{index}"),
                    format!("Model{index}"),
                    "id",
                    columns,
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                )
            })
            .collect(),
    )
}

criterion_group!(benches, bench_registry_validation, bench_schema_diff);
criterion_main!(benches);
