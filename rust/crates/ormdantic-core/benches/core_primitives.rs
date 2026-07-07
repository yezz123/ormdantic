use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ormdantic_core::{
    BackendFeature, EventKind, EventPayload, FeatureSet, Identifier, IdentityKey, IsolationLevel,
    QualifiedName, RevisionId, SavepointName, TransactionOptions,
};

fn bench_identifier_validation(c: &mut Criterion) {
    let names = (0..1_000)
        .map(|index| format!("flavor_column_{index}"))
        .collect::<Vec<_>>();

    c.bench_function("core_identifier_validation", |bench| {
        bench.iter(|| {
            for name in &names {
                black_box(Identifier::new(black_box(name)).unwrap());
                black_box(QualifiedName::with_schema("inventory", black_box(name)).unwrap());
            }
        });
    });
}

fn bench_feature_set_normalization(c: &mut Criterion) {
    let features = [
        BackendFeature::Returning,
        BackendFeature::NativeJson,
        BackendFeature::Ctes,
        BackendFeature::Returning,
        BackendFeature::Savepoints,
        BackendFeature::NativeJson,
        BackendFeature::TransactionalDdl,
    ];

    c.bench_function("core_feature_set_normalization", |bench| {
        bench.iter(|| {
            let mut set = FeatureSet::new(black_box(features));
            set.insert(BackendFeature::PartialIndexes);
            set.insert(BackendFeature::Ctes);
            black_box(set);
        });
    });
}

fn bench_identity_and_event_payloads(c: &mut Criterion) {
    c.bench_function("core_identity_event_payloads", |bench| {
        bench.iter(|| {
            for index in 0..500 {
                black_box(IdentityKey::new(
                    "Flavor",
                    vec![format!("flavor-{index}"), format!("tenant-{index}")],
                ));
                black_box(
                    EventPayload::new(EventKind::BeforeExecute)
                        .with_target(format!("flavor_{index}"))
                        .with_message("SELECT 1"),
                );
                black_box(RevisionId::new(format!("202607071430{index:04}")).unwrap());
                black_box(SavepointName::new(format!("sp_{index}")).unwrap());
                black_box(
                    TransactionOptions::new()
                        .with_isolation_level(IsolationLevel::Serializable)
                        .read_only(),
                );
            }
        });
    });
}

criterion_group!(
    benches,
    bench_identifier_validation,
    bench_feature_set_normalization,
    bench_identity_and_event_payloads
);
criterion_main!(benches);
