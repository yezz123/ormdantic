use ormdantic_core::{
    BackendFeature, DeferrableMode, EventKind, EventPayload, ExecutionErrorKind, FeatureSet,
    Identifier, IdentityKey, IsolationLevel, OrmdanticError, QualifiedName, RevisionId,
    SavepointName, TableId, TransactionAccessMode, TransactionOptions,
};

#[test]
fn table_ids_are_copyable_handles() {
    let first = TableId(0);
    let copied = first;

    assert_eq!(first, copied);
}

#[test]
fn schema_errors_have_actionable_messages() {
    let error = OrmdanticError::MissingPrimaryKey {
        tablename: "flavors".to_string(),
        primary_key: "id".to_string(),
    };

    assert_eq!(
        error.to_string(),
        "table 'flavors' does not define primary key column 'id'"
    );
}

#[test]
fn identifiers_and_qualified_names_expose_owned_and_borrowed_forms() {
    let identifier = Identifier::new("flavor_id").expect("identifier should be valid");
    let unchecked = Identifier::unchecked("unchecked-name");
    let qualified =
        QualifiedName::with_schema("inventory", "flavors").expect("qualified name should be valid");
    let unqualified = QualifiedName::new("flavors").expect("name should be valid");
    let unchecked_qualified = QualifiedName::unchecked(Some("bad-schema"), "bad-name");

    assert_eq!(identifier.as_str(), "flavor_id");
    assert_eq!(String::from(identifier.clone()), "flavor_id");
    assert_eq!(identifier.into_string(), "flavor_id");
    assert_eq!(unchecked.to_string(), "unchecked-name");
    assert_eq!(qualified.schema().unwrap().as_str(), "inventory");
    assert_eq!(qualified.name().as_str(), "flavors");
    assert_eq!(qualified.to_string(), "inventory.flavors");
    assert_eq!(unqualified.schema(), None);
    assert_eq!(unqualified.to_string(), "flavors");
    assert_eq!(unchecked_qualified.to_string(), "bad-schema.bad-name");
    assert!(Identifier::new("").is_err());
}

#[test]
fn feature_sets_transaction_options_and_payload_helpers_are_stable() {
    let mut features = FeatureSet::new([
        BackendFeature::Returning,
        BackendFeature::Ctes,
        BackendFeature::Returning,
    ]);
    features.insert(BackendFeature::Ctes);
    features.insert(BackendFeature::NativeJson);

    let options = TransactionOptions::new()
        .with_isolation_level(IsolationLevel::RepeatableRead)
        .with_access_mode(TransactionAccessMode::ReadOnly)
        .with_deferrable_mode(DeferrableMode::Deferrable);
    let read_only = TransactionOptions::new().read_only();
    let savepoint = SavepointName::new("sp_1").expect("savepoint should be valid");
    let payload = EventPayload::new(EventKind::BeforeExecute)
        .with_target("flavors")
        .with_message("SELECT 1");
    let key = IdentityKey::new("Flavor", vec!["id-1".to_string(), "id-2".to_string()]);
    let revision = RevisionId::new("20260707143000").expect("revision should be valid");

    assert_eq!(
        features.features(),
        &[
            BackendFeature::Ctes,
            BackendFeature::NativeJson,
            BackendFeature::Returning,
        ]
    );
    assert_eq!(
        options.isolation_level(),
        Some(IsolationLevel::RepeatableRead)
    );
    assert_eq!(options.access_mode(), TransactionAccessMode::ReadOnly);
    assert_eq!(options.deferrable_mode(), Some(DeferrableMode::Deferrable));
    assert_eq!(read_only.access_mode(), TransactionAccessMode::ReadOnly);
    assert_eq!(savepoint.to_string(), "sp_1");
    assert_eq!(payload.kind(), EventKind::BeforeExecute);
    assert_eq!(payload.target(), Some("flavors"));
    assert_eq!(payload.message(), Some("SELECT 1"));
    assert_eq!(key.model_key(), "Flavor");
    assert_eq!(key.primary_key(), &["id-1".to_string(), "id-2".to_string()]);
    assert_eq!(revision.as_str(), "20260707143000");
    assert_eq!(revision.to_string(), "20260707143000");
    assert!(RevisionId::new("   ").is_err());
}

#[test]
fn all_error_variants_keep_actionable_display_messages() {
    let cases = [
        (
            OrmdanticError::MissingPrimaryKeyAlias {
                tablename: "flavors".to_string(),
                primary_key: "id".to_string(),
            },
            "primary key column 'flavors\\id' was not found",
        ),
        (
            OrmdanticError::DuplicateTable {
                tablename: "flavors".to_string(),
            },
            "table 'flavors' is already registered",
        ),
        (
            OrmdanticError::DuplicateColumn {
                tablename: "flavors".to_string(),
                column: "name".to_string(),
            },
            "column 'flavors.name' is already registered",
        ),
        (
            OrmdanticError::UnknownTable {
                tablename: "missing".to_string(),
            },
            "table 'missing' is not registered",
        ),
        (
            OrmdanticError::InvalidRelationship {
                table: "coffee".to_string(),
                field: "flavor".to_string(),
                target_table: "flavors".to_string(),
            },
            "relationship 'coffee.flavor' targets unknown table 'flavors'",
        ),
        (
            OrmdanticError::UnsupportedDialect {
                dialect: "db2".to_string(),
            },
            "dialect 'db2' is not supported",
        ),
        (
            OrmdanticError::InvalidIdentifier {
                identifier: "bad-name".to_string(),
            },
            "identifier 'bad-name' is not valid",
        ),
        (
            OrmdanticError::UnsupportedFeature {
                feature: "RETURNING".to_string(),
                dialect: "mysql".to_string(),
            },
            "feature 'RETURNING' is not supported by dialect 'mysql'",
        ),
        (
            OrmdanticError::TransactionError {
                message: "nested transaction failed".to_string(),
            },
            "transaction error: nested transaction failed",
        ),
        (
            OrmdanticError::ReflectionError {
                message: "catalog unavailable".to_string(),
            },
            "reflection error: catalog unavailable",
        ),
        (
            OrmdanticError::MigrationError {
                message: "dirty migration history".to_string(),
            },
            "migration error: dirty migration history",
        ),
        (
            OrmdanticError::SchemaDiffError {
                message: "column drift".to_string(),
            },
            "schema diff error: column drift",
        ),
        (
            OrmdanticError::UnitOfWorkError {
                message: "flush failed".to_string(),
            },
            "unit of work error: flush failed",
        ),
        (
            OrmdanticError::EventError {
                message: "listener failed".to_string(),
            },
            "event error: listener failed",
        ),
        (
            OrmdanticError::SqlCompile {
                message: "select query requires columns".to_string(),
            },
            "select query requires columns",
        ),
    ];

    for (error, message) in cases {
        assert_eq!(error.to_string(), message);
    }
}

#[test]
fn execution_errors_keep_display_stable_with_structured_kind() {
    let error = OrmdanticError::ExecutionError {
        kind: ExecutionErrorKind::UniqueViolation,
        message: "duplicate key".to_string(),
    };

    assert_eq!(error.to_string(), "execution error: duplicate key");
    assert!(matches!(
        error,
        OrmdanticError::ExecutionError {
            kind: ExecutionErrorKind::UniqueViolation,
            ..
        }
    ));
}
