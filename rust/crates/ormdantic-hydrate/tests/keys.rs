use ormdantic_hydrate::{HydratedRow, HydrationKey};

fn row(values: &[(&str, &str)]) -> HydratedRow {
    values
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

#[test]
fn builds_hydration_keys_from_complete_rows() {
    let key = HydrationKey::from_row(
        "coffee",
        &["tenant_id".to_string(), "id".to_string()],
        &row(&[("tenant_id", "north"), ("id", "42")]),
    )
    .expect("key should build");
    let identity = key.identity_key();

    assert_eq!(key.table_path(), "coffee");
    assert_eq!(key.values(), &["north".to_string(), "42".to_string()]);
    assert_eq!(identity.model_key(), "coffee");
    assert_eq!(
        identity.primary_key(),
        &["north".to_string(), "42".to_string()]
    );
}

#[test]
fn rejects_missing_or_empty_key_values() {
    assert!(
        HydrationKey::from_row("coffee", &["id".to_string()], &row(&[("name", "mocha")])).is_none()
    );
    assert!(HydrationKey::from_row("coffee", &["id".to_string()], &row(&[("id", "")])).is_none());
}
