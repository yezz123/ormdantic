use crate::runtime::py_to_db_value;
use ormdantic_engine::DbValue;
use pyo3::prelude::*;
use pyo3::types::PyString;
use pyo3::IntoPyObjectExt;

#[pyfunction]
pub(crate) fn snake_case(value: &str) -> String {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut previous: Option<char> = None;
    for ch in value.chars() {
        if ch == '_' || ch == '-' || ch.is_whitespace() {
            push_word(&mut words, &mut current);
            previous = None;
            continue;
        }
        if let Some(prev) = previous {
            if (prev.is_lowercase() && ch.is_uppercase())
                || (prev.is_ascii_digit() && ch.is_alphabetic())
            {
                push_word(&mut words, &mut current);
            }
        }
        current.push(ch);
        previous = Some(ch);
    }
    push_word(&mut words, &mut current);
    words
        .into_iter()
        .map(|word| word.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join("_")
}

#[pyfunction]
pub(crate) fn sql_value(py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
    match py_to_db_value(py, value)? {
        DbValue::Null => Ok(py.None()),
        DbValue::Integer(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        DbValue::Real(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        DbValue::Text(value) => Ok(PyString::new(py, &value).into_any().unbind()),
        DbValue::Bool(value) => Ok(value.into_py_any(py)?),
    }
}

fn push_word(words: &mut Vec<String>, current: &mut String) {
    if !current.is_empty() {
        words.push(std::mem::take(current));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snake_case_splits_common_word_boundaries() {
        assert_eq!(snake_case("UserID"), "user_id");
        assert_eq!(snake_case("user-name value"), "user_name_value");
        assert_eq!(snake_case("field1Name"), "field1_name");
    }
}
