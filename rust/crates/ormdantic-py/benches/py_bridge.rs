use _ormdantic::register_module;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyModule};
use pyo3::IntoPyObjectExt;

fn bench_sql_value_scalar_bridge(c: &mut Criterion) {
    let (sql_value, values) = Python::attach(|py| {
        let module = registered_module(py);
        let sql_value = function(&module, "sql_value");
        let values = vec![
            py.None(),
            42_i64.into_py_any(py).expect("integer should convert"),
            3.25_f64.into_py_any(py).expect("float should convert"),
            "vanilla".into_py_any(py).expect("string should convert"),
            true.into_py_any(py).expect("bool should convert"),
        ];
        (sql_value, values)
    });

    c.bench_function("py_bridge_sql_value_mixed_scalars", |bench| {
        bench.iter(|| {
            Python::attach(|py| {
                let sql_value = sql_value.bind(py);
                for value in &values {
                    let converted = sql_value
                        .call1((value.clone_ref(py),))
                        .expect("sql_value should convert");
                    black_box(converted);
                }
            });
        });
    });
}

fn bench_query_payload_bridge(c: &mut Criterion) {
    let fixtures = Python::attach(|py| {
        let module = registered_module(py);
        QueryFixtures {
            normalize_filters: function(&module, "normalize_filters"),
            compile_find_many: function(&module, "compile_find_many"),
            compile_typed_expression_query: function(&module, "compile_typed_expression_query"),
            filter_tree: filter_tree_payload(py),
            find_many_columns: vec!["id", "name", "rating", "created_at"]
                .into_py_any(py)
                .expect("columns should convert"),
            find_many_filters: vec![
                (
                    "rating".to_string(),
                    "ge".to_string(),
                    vec!["min_rating".to_string()],
                ),
                (
                    "status".to_string(),
                    "in".to_string(),
                    vec!["status_0".to_string(), "status_1".to_string()],
                ),
            ]
            .into_py_any(py)
            .expect("filters should convert"),
            find_many_order: vec!["created_at"]
                .into_py_any(py)
                .expect("order columns should convert"),
            typed_query: typed_select_payload(py),
        }
    });

    c.bench_function("py_bridge_query_payload_conversion", |bench| {
        bench.iter(|| {
            Python::attach(|py| {
                let normalized = fixtures
                    .normalize_filters
                    .bind(py)
                    .call1((fixtures.filter_tree.clone_ref(py),))
                    .expect("filters should normalize");
                black_box(normalized);

                let find_many = fixtures
                    .compile_find_many
                    .bind(py)
                    .call1((
                        "postgresql",
                        "coffee",
                        fixtures.find_many_columns.clone_ref(py),
                        fixtures.find_many_filters.clone_ref(py),
                        fixtures.find_many_order.clone_ref(py),
                        "desc",
                        Some(50_usize),
                        Some(10_usize),
                        Option::<Vec<String>>::None,
                    ))
                    .expect("find_many should compile");
                black_box(find_many);

                let typed = fixtures
                    .compile_typed_expression_query
                    .bind(py)
                    .call1(("postgresql", fixtures.typed_query.clone_ref(py)))
                    .expect("typed expression query should compile");
                black_box(typed);
            });
        });
    });
}

fn bench_schema_hydration_payload_bridge(c: &mut Criterion) {
    let fixtures = Python::attach(|py| {
        let module = registered_module(py);
        SchemaHydrationFixtures {
            validate_schema_tables: function(&module, "validate_schema_tables"),
            plan_result_shape: function(&module, "plan_result_shape"),
            hydrate_flat: function(&module, "hydrate_flat"),
            hydrate_joined: function(&module, "hydrate_joined"),
            execute_selectin_load: function(&module, "execute_selectin_load"),
            schema_tables: vec![
                (
                    "coffee".to_string(),
                    "id".to_string(),
                    vec![
                        "id".to_string(),
                        "name".to_string(),
                        "rating".to_string(),
                        "created_at".to_string(),
                    ],
                ),
                (
                    "flavor".to_string(),
                    "id".to_string(),
                    vec![
                        "id".to_string(),
                        "coffee_id".to_string(),
                        "name".to_string(),
                    ],
                ),
            ]
            .into_py_any(py)
            .expect("schema tables should convert"),
            shape_columns: vec![
                "coffee\\id",
                "coffee\\name",
                "coffee/flavors\\id",
                "coffee/flavors\\name",
            ]
            .into_py_any(py)
            .expect("shape columns should convert"),
            flat_columns: vec!["coffee\\id", "coffee\\name", "coffee\\rating"]
                .into_py_any(py)
                .expect("flat columns should convert"),
            flat_rows: flat_rows_payload(py),
            joined_columns: vec![
                "coffee\\id",
                "coffee\\name",
                "coffee/flavors\\id",
                "coffee/flavors\\name",
            ]
            .into_py_any(py)
            .expect("joined columns should convert"),
            joined_rows: joined_rows_payload(py),
            path_pks: vec![
                ("coffee".to_string(), "id".to_string()),
                ("coffee/flavors".to_string(), "id".to_string()),
            ]
            .into_py_any(py)
            .expect("path primary keys should convert"),
            array_paths: vec!["coffee/flavors"]
                .into_py_any(py)
                .expect("array paths should convert"),
            parent_rows: parent_rows_payload(py),
            child_rows: child_rows_payload(py),
            parent_key_columns: vec!["id"]
                .into_py_any(py)
                .expect("parent keys should convert"),
            child_key_columns: vec!["coffee_id"]
                .into_py_any(py)
                .expect("child keys should convert"),
        }
    });

    c.bench_function("py_bridge_schema_hydration_payload_conversion", |bench| {
        bench.iter(|| {
            Python::attach(|py| {
                let validated = fixtures
                    .validate_schema_tables
                    .bind(py)
                    .call1((fixtures.schema_tables.clone_ref(py),))
                    .expect("schema should validate");
                black_box(validated);

                let shape = fixtures
                    .plan_result_shape
                    .bind(py)
                    .call1((
                        "coffee",
                        fixtures.shape_columns.clone_ref(py),
                        fixtures.array_paths.clone_ref(py),
                    ))
                    .expect("shape should plan");
                black_box(shape);

                let flat = fixtures
                    .hydrate_flat
                    .bind(py)
                    .call1((
                        "coffee",
                        "id",
                        fixtures.flat_columns.clone_ref(py),
                        fixtures.flat_rows.clone_ref(py),
                        true,
                    ))
                    .expect("flat rows should hydrate");
                black_box(flat);

                let joined = fixtures
                    .hydrate_joined
                    .bind(py)
                    .call1((
                        fixtures.joined_columns.clone_ref(py),
                        fixtures.joined_rows.clone_ref(py),
                        fixtures.path_pks.clone_ref(py),
                        fixtures.array_paths.clone_ref(py),
                    ))
                    .expect("joined rows should hydrate");
                black_box(joined);

                let selectin = fixtures
                    .execute_selectin_load
                    .bind(py)
                    .call1((
                        fixtures.parent_rows.clone_ref(py),
                        fixtures.child_rows.clone_ref(py),
                        fixtures.parent_key_columns.clone_ref(py),
                        fixtures.child_key_columns.clone_ref(py),
                        "flavors",
                        "flavor",
                        "id",
                        true,
                    ))
                    .expect("select-in rows should merge");
                black_box(selectin);
            });
        });
    });
}

struct QueryFixtures {
    normalize_filters: Py<PyAny>,
    compile_find_many: Py<PyAny>,
    compile_typed_expression_query: Py<PyAny>,
    filter_tree: Py<PyAny>,
    find_many_columns: Py<PyAny>,
    find_many_filters: Py<PyAny>,
    find_many_order: Py<PyAny>,
    typed_query: Py<PyAny>,
}

struct SchemaHydrationFixtures {
    validate_schema_tables: Py<PyAny>,
    plan_result_shape: Py<PyAny>,
    hydrate_flat: Py<PyAny>,
    hydrate_joined: Py<PyAny>,
    execute_selectin_load: Py<PyAny>,
    schema_tables: Py<PyAny>,
    shape_columns: Py<PyAny>,
    flat_columns: Py<PyAny>,
    flat_rows: Py<PyAny>,
    joined_columns: Py<PyAny>,
    joined_rows: Py<PyAny>,
    path_pks: Py<PyAny>,
    array_paths: Py<PyAny>,
    parent_rows: Py<PyAny>,
    child_rows: Py<PyAny>,
    parent_key_columns: Py<PyAny>,
    child_key_columns: Py<PyAny>,
}

fn registered_module(py: Python<'_>) -> Bound<'_, PyModule> {
    let module = PyModule::new(py, "_ormdantic_bench").expect("module should create");
    register_module(&module).expect("module should register");
    module
}

fn function(module: &Bound<'_, PyModule>, name: &str) -> Py<PyAny> {
    module
        .getattr(name)
        .unwrap_or_else(|_| panic!("{name} should be registered"))
        .unbind()
}

fn filter_tree_payload(py: Python<'_>) -> Py<PyAny> {
    let active_filters = PyDict::new(py);
    active_filters
        .set_item("status__in", vec!["draft", "published"])
        .expect("status filter should set");
    active_filters
        .set_item("rating__ge", 80)
        .expect("rating filter should set");

    let active_leaf = PyDict::new(py);
    active_leaf
        .set_item("connector", "leaf")
        .expect("connector should set");
    active_leaf
        .set_item("filters", active_filters)
        .expect("filters should set");

    let owner_filters = PyDict::new(py);
    owner_filters
        .set_item("owner_id", 42)
        .expect("owner filter should set");

    let owner_leaf = PyDict::new(py);
    owner_leaf
        .set_item("connector", "leaf")
        .expect("connector should set");
    owner_leaf
        .set_item("filters", owner_filters)
        .expect("filters should set");

    let children = PyList::empty(py);
    children
        .append(active_leaf)
        .expect("active leaf should append");
    children
        .append(owner_leaf)
        .expect("owner leaf should append");

    let tree = PyDict::new(py);
    tree.set_item("connector", "and")
        .expect("connector should set");
    tree.set_item("children", children)
        .expect("children should set");
    tree.into_any().unbind()
}

fn typed_select_payload(py: Python<'_>) -> Py<PyAny> {
    let query = PyDict::new(py);
    query.set_item("table", "coffee").expect("table should set");
    query
        .set_item("distinct", true)
        .expect("distinct should set");
    query.set_item("limit", 50).expect("limit should set");
    query.set_item("offset", 10).expect("offset should set");

    let projections = PyList::empty(py);
    projections
        .append(projection(py, column_expr(py, None, "id"), None))
        .expect("id projection should append");
    projections
        .append(projection(
            py,
            function_expr(py, "LOWER", vec![column_expr(py, None, "name")]),
            Some("lower_name"),
        ))
        .expect("name projection should append");
    projections
        .append(projection(
            py,
            function_expr(py, "COUNT", vec![column_expr(py, None, "id")]),
            Some("total"),
        ))
        .expect("count projection should append");
    query
        .set_item("projections", projections)
        .expect("projections should set");

    let status_filter = in_list_expr(
        py,
        column_expr(py, None, "status"),
        vec![literal_expr(py, "draft"), literal_expr(py, "published")],
    );
    let rating_filter = binary_expr(
        py,
        column_expr(py, None, "rating"),
        "ge",
        param_expr(py, "min_rating"),
    );
    query
        .set_item(
            "where",
            binary_expr(py, status_filter, "and", rating_filter),
        )
        .expect("where should set");

    let group_by = PyList::empty(py);
    group_by
        .append(column_expr(py, None, "id"))
        .expect("group id should append");
    group_by
        .append(column_expr(py, None, "name"))
        .expect("group name should append");
    query
        .set_item("group_by", group_by)
        .expect("group should set");

    let order_by = PyList::empty(py);
    let order = PyDict::new(py);
    order
        .set_item("expr", column_expr(py, None, "created_at"))
        .expect("order expr should set");
    order
        .set_item("direction", "desc")
        .expect("order direction should set");
    order
        .set_item("nulls", "last")
        .expect("order nulls should set");
    order_by.append(order).expect("order should append");
    query
        .set_item("order_by", order_by)
        .expect("order should set");

    let values = PyDict::new(py);
    values.set_item("min_rating", 80).expect("value should set");
    query.set_item("values", values).expect("values should set");
    query.into_any().unbind()
}

fn flat_rows_payload(py: Python<'_>) -> Py<PyAny> {
    let rows = PyList::empty(py);
    for index in 0..120 {
        rows.append(
            PyList::new(
                py,
                vec![
                    (index % 40).into_py_any(py).expect("id should convert"),
                    format!("coffee-{}", index % 40)
                        .into_py_any(py)
                        .expect("name should convert"),
                    (index % 100)
                        .into_py_any(py)
                        .expect("rating should convert"),
                ],
            )
            .expect("row should create"),
        )
        .expect("row should append");
    }
    rows.into_any().unbind()
}

fn joined_rows_payload(py: Python<'_>) -> Py<PyAny> {
    let rows = PyList::empty(py);
    for index in 0..240 {
        rows.append(
            PyList::new(
                py,
                vec![
                    (index % 80).into_py_any(py).expect("id should convert"),
                    format!("coffee-{}", index % 80)
                        .into_py_any(py)
                        .expect("name should convert"),
                    index.into_py_any(py).expect("flavor id should convert"),
                    format!("flavor-{index}")
                        .into_py_any(py)
                        .expect("flavor should convert"),
                ],
            )
            .expect("row should create"),
        )
        .expect("row should append");
    }
    rows.into_any().unbind()
}

fn parent_rows_payload(py: Python<'_>) -> Py<PyAny> {
    let rows = PyList::empty(py);
    for index in 0..100 {
        let row = PyDict::new(py);
        row.set_item("id", index.to_string())
            .expect("id should set");
        row.set_item("name", format!("coffee-{index}"))
            .expect("name should set");
        rows.append(row).expect("row should append");
    }
    rows.into_any().unbind()
}

fn child_rows_payload(py: Python<'_>) -> Py<PyAny> {
    let rows = PyList::empty(py);
    for index in 0..400 {
        let row = PyDict::new(py);
        row.set_item("coffee_id", (index % 100).to_string())
            .expect("parent id should set");
        row.set_item("id", index.to_string())
            .expect("id should set");
        row.set_item("name", format!("flavor-{index}"))
            .expect("name should set");
        rows.append(&row).expect("row should append");
        rows.append(row).expect("duplicate row should append");
    }
    rows.into_any().unbind()
}

fn projection<'py>(
    py: Python<'py>,
    expr: Bound<'py, PyDict>,
    alias: Option<&str>,
) -> Bound<'py, PyDict> {
    let projection = PyDict::new(py);
    projection.set_item("expr", expr).expect("expr should set");
    if let Some(alias) = alias {
        projection
            .set_item("alias", alias)
            .expect("alias should set");
    }
    projection
}

fn column_expr<'py>(py: Python<'py>, table: Option<&str>, name: &str) -> Bound<'py, PyDict> {
    let expr = PyDict::new(py);
    expr.set_item("kind", "column").expect("kind should set");
    expr.set_item("name", name).expect("name should set");
    if let Some(table) = table {
        expr.set_item("table", table).expect("table should set");
    }
    expr
}

fn param_expr<'py>(py: Python<'py>, name: &str) -> Bound<'py, PyDict> {
    let expr = PyDict::new(py);
    expr.set_item("kind", "param").expect("kind should set");
    expr.set_item("name", name).expect("name should set");
    expr
}

fn literal_expr<'py, T>(py: Python<'py>, value: T) -> Bound<'py, PyDict>
where
    T: IntoPyObjectExt<'py>,
{
    let expr = PyDict::new(py);
    expr.set_item("kind", "literal").expect("kind should set");
    expr.set_item("value", value).expect("value should set");
    expr
}

fn binary_expr<'py>(
    py: Python<'py>,
    left: Bound<'py, PyDict>,
    op: &str,
    right: Bound<'py, PyDict>,
) -> Bound<'py, PyDict> {
    let expr = PyDict::new(py);
    expr.set_item("kind", "binary").expect("kind should set");
    expr.set_item("left", left).expect("left should set");
    expr.set_item("op", op).expect("op should set");
    expr.set_item("right", right).expect("right should set");
    expr
}

fn function_expr<'py>(
    py: Python<'py>,
    name: &str,
    args: Vec<Bound<'py, PyDict>>,
) -> Bound<'py, PyDict> {
    let expr = PyDict::new(py);
    expr.set_item("kind", "function").expect("kind should set");
    expr.set_item("name", name).expect("name should set");
    let args_list = PyList::empty(py);
    for arg in args {
        args_list.append(arg).expect("arg should append");
    }
    expr.set_item("args", args_list).expect("args should set");
    expr
}

fn in_list_expr<'py>(
    py: Python<'py>,
    expr: Bound<'py, PyDict>,
    values: Vec<Bound<'py, PyDict>>,
) -> Bound<'py, PyDict> {
    let payload = PyDict::new(py);
    payload
        .set_item("kind", "in_list")
        .expect("kind should set");
    payload.set_item("expr", expr).expect("expr should set");
    let value_list = PyList::empty(py);
    for value in values {
        value_list.append(value).expect("value should append");
    }
    payload
        .set_item("values", value_list)
        .expect("values should set");
    payload
}

criterion_group!(
    benches,
    bench_sql_value_scalar_bridge,
    bench_query_payload_bridge,
    bench_schema_hydration_payload_bridge
);
criterion_main!(benches);
