use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ormdantic_hydrate::{
    merge_selectin_results, FlatHydrationPlan, HydratedRow, ResultShape, SelectInHydrationPlan,
};
use ormdantic_schema::{RelationshipCardinality, RelationshipDef, TableDef};

fn bench_result_shape_planning(c: &mut Criterion) {
    let aliases = (0..50)
        .flat_map(|index| {
            [
                format!("coffee\\root_{index}"),
                format!("coffee/flavor_{index}\\id"),
                format!("coffee/flavor_{index}/roast\\name"),
            ]
        })
        .collect::<Vec<_>>();

    c.bench_function("hydrate_result_shape_planning", |bench| {
        bench.iter(|| {
            black_box(ResultShape::new(
                "coffee",
                black_box(&aliases),
                vec!["coffee/flavor_1".to_string()],
            ));
        });
    });
}

fn bench_flat_hydration_plan(c: &mut Criterion) {
    let table = TableDef::new(
        "coffee",
        "id",
        vec!["id".to_string(), "name".to_string(), "rating".to_string()],
    );
    let aliases = vec![
        "coffee\\name".to_string(),
        "coffee\\id".to_string(),
        "coffee\\rating".to_string(),
    ];

    c.bench_function("hydrate_flat_plan", |bench| {
        bench.iter(|| {
            black_box(
                FlatHydrationPlan::new(black_box(table.clone()), black_box(&aliases)).unwrap(),
            );
        });
    });
}

fn bench_selectin_merge(c: &mut Criterion) {
    let plan = SelectInHydrationPlan::new(
        vec!["id".to_string()],
        vec!["coffee_id".to_string()],
        RelationshipDef::new("flavors", "flavor", "id", RelationshipCardinality::Many),
    );
    let parents = (0..100)
        .map(|index| row(&[("id", &index.to_string())]))
        .collect::<Vec<_>>();
    let children = (0..500)
        .map(|index| {
            row(&[
                ("coffee_id", &(index % 100).to_string()),
                ("id", &index.to_string()),
                ("name", "flavor"),
            ])
        })
        .collect::<Vec<_>>();

    c.bench_function("hydrate_selectin_merge", |bench| {
        bench.iter(|| {
            black_box(merge_selectin_results(
                black_box(parents.clone()),
                black_box(children.clone()),
                black_box(&plan),
            ));
        });
    });
}

fn row(values: &[(&str, &str)]) -> HydratedRow {
    values
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

criterion_group!(
    benches,
    bench_result_shape_planning,
    bench_flat_hydration_plan,
    bench_selectin_merge
);
criterion_main!(benches);
