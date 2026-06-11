use ormdantic_core::{ExecutionErrorKind, OrmdanticError, TableId};

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
