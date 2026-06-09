use ormdantic_hydrate::{
    merge_selectin_results, FlatHydrationPlan, HydratedRow, ResultShape, SelectInHydrationPlan,
};
use ormdantic_schema::{RelationshipCardinality, RelationshipDef, TableDef};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::{BTreeMap, HashMap, HashSet};

#[pyfunction]
pub(crate) fn hydrate_flat(
    py: Python<'_>,
    tablename: &str,
    pk: &str,
    columns: Vec<String>,
    rows: Vec<Vec<Py<PyAny>>>,
    is_array: bool,
) -> PyResult<Py<PyAny>> {
    let table = TableDef::new(tablename, pk, Vec::new());
    let plan = FlatHydrationPlan::new(table, &columns)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;

    if rows.is_empty() {
        return Ok(py.None());
    }

    if !is_array {
        return row_to_dict(py, &rows[0], plan.parsed_columns())
            .map(|record| record.into_any().unbind());
    }

    let seen = PyDict::new(py);
    let mut records = Vec::new();
    for row in rows {
        let Some(pk_value) = row.get(plan.primary_key_index()) else {
            continue;
        };
        let bound_pk = pk_value.bind(py);
        if seen.contains(bound_pk)? {
            continue;
        }

        let record = row_to_dict(py, &row, plan.parsed_columns())?;
        seen.set_item(bound_pk, &record)?;
        records.push(record.into_any().unbind());
    }

    Ok(PyList::new(py, records)?.into_any().unbind())
}

#[pyfunction]
pub(crate) fn hydrate_joined(
    py: Python<'_>,
    columns: Vec<String>,
    rows: Vec<Vec<Py<PyAny>>>,
    path_pks: Vec<(String, String)>,
    array_paths: Vec<String>,
) -> PyResult<Py<PyAny>> {
    let parsed_columns = columns
        .iter()
        .map(|alias| split_alias(alias))
        .collect::<PyResult<Vec<_>>>()?;
    let path_pk_map = path_pks.into_iter().collect::<HashMap<_, _>>();
    let array_paths = array_paths.into_iter().collect::<HashSet<_>>();
    let root = PyDict::new(py);

    for row in rows {
        let row_pks = collect_row_pks(py, &row, &parsed_columns, &path_pk_map);
        for (idx, (path, column)) in parsed_columns.iter().enumerate() {
            let mut node = root.clone();
            let mut current_path = String::new();
            let mut should_set = true;
            for (branch_idx, branch) in path.split('/').enumerate() {
                if branch_idx == 0 {
                    current_path.push_str(branch);
                } else {
                    current_path.push('/');
                    current_path.push_str(branch);
                }
                let Some(pk_value) = row_pks.get(&current_path) else {
                    should_set = false;
                    break;
                };
                if pk_value.bind(py).is_none() {
                    should_set = false;
                    break;
                }
                if !node.contains(branch)? {
                    node.set_item(branch, PyDict::new(py))?;
                }
                let branch_node = node
                    .get_item(branch)?
                    .expect("branch exists")
                    .downcast_into::<PyDict>()?;
                if array_paths.contains(&current_path) {
                    if !branch_node.contains(pk_value.bind(py))? {
                        branch_node.set_item(pk_value.bind(py), PyDict::new(py))?;
                    }
                    node = branch_node
                        .get_item(pk_value.bind(py))?
                        .expect("array item exists")
                        .downcast_into::<PyDict>()?;
                } else {
                    node = branch_node;
                }
            }
            if should_set && !column.is_empty() {
                if let Some(value) = row.get(idx) {
                    node.set_item(column, value.bind(py))?;
                }
            }
        }
    }

    Ok(root.into_any().unbind())
}

#[pyfunction]
pub(crate) fn plan_result_shape(
    py: Python<'_>,
    root_table: &str,
    columns: Vec<String>,
    array_paths: Vec<String>,
) -> PyResult<Py<PyAny>> {
    let shape = ResultShape::new(root_table, &columns, array_paths);
    let result = PyDict::new(py);
    let columns = PyList::empty(py);
    for column in shape.columns() {
        let item = PyDict::new(py);
        item.set_item("alias", column.alias())?;
        item.set_item("table_path", column.table_path())?;
        item.set_item("column", column.column())?;
        columns.append(item)?;
    }
    result.set_item("root_table", shape.root_table())?;
    result.set_item("columns", columns)?;
    result.set_item("relationship_paths", shape.relationship_paths())?;
    result.set_item("array_paths", shape.array_paths())?;
    Ok(result.into_any().unbind())
}

#[allow(clippy::too_many_arguments)]
#[pyfunction]
pub(crate) fn execute_selectin_load(
    py: Python<'_>,
    parent_rows: Vec<HashMap<String, String>>,
    child_rows: Vec<HashMap<String, String>>,
    parent_key_columns: Vec<String>,
    child_key_columns: Vec<String>,
    relationship_field: &str,
    target_table: &str,
    target_field: &str,
    uselist: bool,
) -> PyResult<Py<PyAny>> {
    let relationship = RelationshipDef::new(
        relationship_field,
        target_table,
        target_field,
        if uselist {
            RelationshipCardinality::Many
        } else {
            RelationshipCardinality::One
        },
    )
    .uselist(uselist);
    let plan = SelectInHydrationPlan::new(parent_key_columns, child_key_columns, relationship);
    let merged = merge_selectin_results(
        parent_rows.into_iter().map(hash_to_hydrated_row).collect(),
        child_rows.into_iter().map(hash_to_hydrated_row).collect(),
        &plan,
    );
    hydrated_rows_to_python(py, merged)
}

fn split_alias(alias: &str) -> PyResult<(String, String)> {
    alias.split_once('\\').map_or_else(
        || {
            Err(PyValueError::new_err(format!(
                "invalid result alias '{alias}'"
            )))
        },
        |(path, column)| Ok((path.to_string(), column.to_string())),
    )
}

fn collect_row_pks(
    py: Python<'_>,
    row: &[Py<PyAny>],
    parsed_columns: &[(String, String)],
    path_pk_map: &HashMap<String, String>,
) -> HashMap<String, Py<PyAny>> {
    let mut row_pks = HashMap::new();
    for (idx, (path, column)) in parsed_columns.iter().enumerate() {
        if path_pk_map.get(path) == Some(column) {
            if let Some(value) = row.get(idx) {
                row_pks.insert(path.clone(), value.clone_ref(py));
            }
        }
    }
    row_pks
}

fn hash_to_hydrated_row(row: HashMap<String, String>) -> HydratedRow {
    row.into_iter().collect::<BTreeMap<_, _>>()
}

fn hydrated_rows_to_python(py: Python<'_>, rows: Vec<HydratedRow>) -> PyResult<Py<PyAny>> {
    let output = PyList::empty(py);
    for row in rows {
        let item = PyDict::new(py);
        for (key, value) in row {
            item.set_item(key, value)?;
        }
        output.append(item)?;
    }
    Ok(output.into_any().unbind())
}

fn row_to_dict<'py>(
    py: Python<'py>,
    row: &[Py<PyAny>],
    parsed_columns: &[Option<String>],
) -> PyResult<Bound<'py, PyDict>> {
    let record = PyDict::new(py);
    for (idx, column) in parsed_columns.iter().enumerate() {
        let Some(column) = column else {
            continue;
        };
        let Some(value) = row.get(idx) else {
            continue;
        };
        record.set_item(column, value.bind(py))?;
    }
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_alias_returns_path_and_column() {
        assert_eq!(
            split_alias("users/posts\\id").unwrap(),
            ("users/posts".to_string(), "id".to_string())
        );
    }

    #[test]
    fn split_alias_rejects_unqualified_alias() {
        assert!(split_alias("id").is_err());
    }

    #[test]
    fn hash_to_hydrated_row_orders_keys() {
        let row = HashMap::from([
            ("name".to_string(), "Ada".to_string()),
            ("id".to_string(), "1".to_string()),
        ]);

        let keys = hash_to_hydrated_row(row).into_keys().collect::<Vec<_>>();

        assert_eq!(keys, vec!["id".to_string(), "name".to_string()]);
    }
}
