use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ormdantic_core::RevisionId;
use ormdantic_dialects::ReflectionScope;
use ormdantic_engine::{MigrationStore, NativeConnection, Reflector};

fn bench_migration_store_revisions(c: &mut Criterion) {
    let mut connection =
        NativeConnection::open("sqlite:///:memory:").expect("sqlite connection should open");
    {
        let mut store =
            MigrationStore::new(&mut connection).with_table_name("bench_migration_store");
        for idx in 0..100 {
            let revision =
                RevisionId::new(format!("bench_{idx:03}")).expect("revision id should be valid");
            store
                .record_revision(&revision)
                .expect("revision should record");
        }
    }

    c.bench_function("migration_store_revisions_100", |bench| {
        bench.iter(|| {
            let revisions = MigrationStore::new(&mut connection)
                .with_table_name("bench_migration_store")
                .revisions()
                .expect("revisions should load");
            black_box(revisions);
        });
    });
}

fn bench_reflection_query_planning(c: &mut Criterion) {
    let reflectors = [
        Reflector::for_url("sqlite:///:memory:").expect("sqlite reflector should parse"),
        Reflector::for_url("postgresql://user:pass@localhost/db")
            .expect("postgres reflector should parse"),
        Reflector::for_url("mysql://user:pass@localhost/db").expect("mysql reflector should parse"),
        Reflector::for_url("mariadb://user:pass@localhost/db")
            .expect("mariadb reflector should parse"),
        Reflector::for_url("mssql://user:pass@localhost/db").expect("mssql reflector should parse"),
        Reflector::for_url("oracle://user:pass@localhost/FREEPDB1")
            .expect("oracle reflector should parse"),
    ];
    let scope = ReflectionScope::new().schema("app");

    c.bench_function("reflection_query_planning_all_dialects", |bench| {
        bench.iter(|| {
            for reflector in &reflectors {
                black_box(reflector.reflection_queries(&scope));
            }
        });
    });
}

criterion_group!(
    benches,
    bench_migration_store_revisions,
    bench_reflection_query_planning
);
criterion_main!(benches);
