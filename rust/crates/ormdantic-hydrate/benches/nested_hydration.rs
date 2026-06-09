use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ormdantic_hydrate::{
    merge_selectin_results, HydratedRow, HydrationGraph, RelationshipNode, SelectInHydrationPlan,
};
use ormdantic_schema::{RelationshipCardinality, RelationshipDef, TableDef};

fn bench_nested_selectin_hydration(c: &mut Criterion) {
    let profile_plan = SelectInHydrationPlan::new(
        vec!["id".to_string()],
        vec!["coffee_id".to_string()],
        relationship("profile", "profile", RelationshipCardinality::One),
    );
    let flavor_plan = SelectInHydrationPlan::new(
        vec!["id".to_string()],
        vec!["coffee_id".to_string()],
        relationship("flavors", "flavor", RelationshipCardinality::Many),
    );
    let roast_plan = SelectInHydrationPlan::new(
        vec!["id".to_string()],
        vec!["flavor_id".to_string()],
        relationship("roasts", "roast", RelationshipCardinality::Many),
    );

    let coffees = (0..200)
        .map(|index| {
            row(&[
                ("id", &index.to_string()),
                ("tenant_id", &(index % 10).to_string()),
                ("name", &format!("coffee-{index}")),
            ])
        })
        .collect::<Vec<_>>();
    let profiles = (0..200)
        .flat_map(|index| {
            let profile = row(&[
                ("coffee_id", &index.to_string()),
                ("id", &format!("profile-{index}")),
                (
                    "tier",
                    if index % 2 == 0 {
                        "single-origin"
                    } else {
                        "blend"
                    },
                ),
            ]);
            [profile.clone(), profile]
        })
        .collect::<Vec<_>>();
    let flavors = (0..800)
        .flat_map(|index| {
            let flavor = row(&[
                ("coffee_id", &(index % 200).to_string()),
                ("id", &index.to_string()),
                ("name", &format!("flavor-{index}")),
            ]);
            [flavor.clone(), flavor]
        })
        .collect::<Vec<_>>();
    let roasts = (0..1600)
        .flat_map(|index| {
            let roast = row(&[
                ("flavor_id", &(index % 800).to_string()),
                ("id", &index.to_string()),
                (
                    "level",
                    match index % 3 {
                        0 => "light",
                        1 => "medium",
                        _ => "dark",
                    },
                ),
            ]);
            [roast.clone(), roast]
        })
        .collect::<Vec<_>>();

    c.bench_function("hydrate_nested_selectin_one_and_many", |bench| {
        bench.iter(|| {
            let flavors_with_roasts = merge_selectin_results(
                black_box(flavors.clone()),
                black_box(roasts.clone()),
                black_box(&roast_plan),
            );
            let coffees_with_profile = merge_selectin_results(
                black_box(coffees.clone()),
                black_box(profiles.clone()),
                black_box(&profile_plan),
            );
            black_box(merge_selectin_results(
                coffees_with_profile,
                flavors_with_roasts,
                black_box(&flavor_plan),
            ));
        });
    });
}

fn bench_hydration_graph_duplicate_folding(c: &mut Criterion) {
    let table = TableDef::new(
        "coffee",
        "id",
        vec![
            "tenant_id".to_string(),
            "id".to_string(),
            "name".to_string(),
        ],
    );
    let graph = HydrationGraph::new(table)
        .composite_key(vec!["tenant_id".to_string(), "id".to_string()])
        .with_relationships(vec![
            RelationshipNode::new(
                "coffee/profile",
                relationship("profile", "profile", RelationshipCardinality::One),
            ),
            RelationshipNode::new(
                "coffee/flavors",
                relationship("flavors", "flavor", RelationshipCardinality::Many),
            )
            .with_children(vec![RelationshipNode::new(
                "coffee/flavors/roasts",
                relationship("roasts", "roast", RelationshipCardinality::Many),
            )]),
        ]);
    let joined_rows = (0..2000)
        .flat_map(|index| {
            let root = row(&[
                ("tenant_id", &(index % 8).to_string()),
                ("id", &(index % 250).to_string()),
                ("name", &format!("coffee-{}", index % 250)),
                ("flavor_id", &index.to_string()),
                ("roast_id", &(index * 2).to_string()),
            ]);
            [root.clone(), root]
        })
        .collect::<Vec<_>>();

    c.bench_function("hydrate_graph_nested_duplicate_folding", |bench| {
        bench.iter(|| {
            black_box(graph.deduplicate_rows(black_box(joined_rows.clone())));
        });
    });
}

fn relationship(
    field: &str,
    target: &str,
    cardinality: RelationshipCardinality,
) -> RelationshipDef {
    RelationshipDef::new(field, target, "id", cardinality)
}

fn row(values: &[(&str, &str)]) -> HydratedRow {
    values
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

criterion_group!(
    benches,
    bench_nested_selectin_hydration,
    bench_hydration_graph_duplicate_folding
);
criterion_main!(benches);
