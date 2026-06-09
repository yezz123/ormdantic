use std::collections::BTreeMap;

pub type HydratedRow = BTreeMap<String, String>;

pub(crate) fn key_values(columns: &[String], row: &HydratedRow) -> Option<Vec<String>> {
    columns
        .iter()
        .map(|column| row.get(column).cloned())
        .collect::<Option<Vec<_>>>()
}

pub(crate) fn format_collection(rows: Vec<HydratedRow>) -> String {
    rows.into_iter()
        .map(format_row)
        .collect::<Vec<_>>()
        .join(";")
}

pub(crate) fn format_row(row: HydratedRow) -> String {
    row.into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn row_fingerprint(row: &HydratedRow) -> Vec<(String, String)> {
    row.iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}
