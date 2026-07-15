use ormdantic_dialects::AnyDialect;
use ormdantic_sql::{
    BinaryOp, CommonTableExpr, CompiledQuery, DmlAst, Expr, Filter, JoinSpec, JoinedFilter,
    JoinedOrderBy, JoinedSelectColumn, OrderBy, OrderExpr, OrderNulls, Projection, QueryAst,
    QueryOperation, SelectAst, SelectColumn, SelectInPlan as SqlSelectInPlan, SortDirection,
    SqlLiteral, TableRef, TableSource, UnaryOp,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

pub(crate) type FilterSpec = (String, String, Vec<String>);
pub(crate) type RuntimeJoinedFilter = (String, Vec<FilterSpec>);
pub(crate) type RuntimeJoinedOrder = (String, String, String);

pub(crate) struct RuntimeJoinedQuery {
    pub(crate) filters: Vec<Filter>,
    pub(crate) order_by: Vec<String>,
    pub(crate) direction: SortDirection,
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<usize>,
    pub(crate) paths: Vec<String>,
    pub(crate) relationship_filters: Vec<RuntimeJoinedFilter>,
    pub(crate) relationship_order_by: Vec<RuntimeJoinedOrder>,
}

const FILTER_OPERATORS: &[&str] = &[
    "eq",
    "ne",
    "lt",
    "le",
    "gt",
    "ge",
    "like",
    "ilike",
    "in",
    "not_in",
    "is_null",
    "is_not_null",
];

pub(crate) fn compiled_queries_to_list(
    py: Python<'_>,
    queries: Vec<CompiledQuery>,
) -> PyResult<Py<PyAny>> {
    let output = PyList::empty(py);
    for query in queries {
        output.append(compiled_query_to_dict(py, query)?)?;
    }
    Ok(output.into_any().unbind())
}

#[pyfunction]
pub(crate) fn normalize_filters(py: Python<'_>, filters: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let values = PyDict::new(py);
    let normalized_filters = normalize_filter_input_for_python(py, filters, &values, None)?;
    let output = PyDict::new(py);
    output.set_item("filters", normalized_filters)?;
    output.set_item("values", values)?;
    Ok(output.into_any().unbind())
}

fn normalize_filter_input_for_python(
    py: Python<'_>,
    filters: &Bound<'_, PyAny>,
    values: &Bound<'_, PyDict>,
    prefix: Option<&str>,
) -> PyResult<Py<PyAny>> {
    if filters.is_none() {
        return Ok(PyList::empty(py).into_any().unbind());
    }
    if filters.extract::<Vec<FilterSpec>>().is_ok() {
        return Ok(filters.clone().unbind());
    }
    let dict = filters.cast::<PyDict>()?;
    if dict.contains("connector")? {
        return normalize_filter_tree_for_python(py, dict, values, prefix.unwrap_or("expr"));
    }
    normalize_filter_dict_for_python(py, dict, values, prefix)
}

fn normalize_filter_tree_for_python(
    py: Python<'_>,
    tree: &Bound<'_, PyDict>,
    values: &Bound<'_, PyDict>,
    prefix: &str,
) -> PyResult<Py<PyAny>> {
    let connector: String = tree
        .get_item("connector")?
        .ok_or_else(|| PyValueError::new_err("filter tree missing connector"))?
        .extract()?;
    let output = PyDict::new(py);
    output.set_item("connector", connector.as_str())?;
    match connector.as_str() {
        "leaf" => {
            let filters = tree
                .get_item("filters")?
                .ok_or_else(|| PyValueError::new_err("leaf filter tree missing filters"))?;
            let normalized = normalize_filter_input_for_python(py, &filters, values, Some(prefix))?;
            output.set_item("filters", normalized)?;
        }
        "and" | "or" => {
            let children = tree
                .get_item("children")?
                .ok_or_else(|| PyValueError::new_err("group filter tree missing children"))?;
            let children = children.extract::<Vec<Py<PyAny>>>()?;
            let normalized_children = PyList::empty(py);
            for (idx, child) in children.into_iter().enumerate() {
                let child_prefix = format!("{prefix}_{idx}");
                normalized_children.append(normalize_filter_input_for_python(
                    py,
                    child.bind(py),
                    values,
                    Some(&child_prefix),
                )?)?;
            }
            output.set_item("children", normalized_children)?;
        }
        other => {
            return Err(PyValueError::new_err(format!(
                "unsupported filter connector '{other}'"
            )))
        }
    }
    Ok(output.into_any().unbind())
}

fn normalize_filter_dict_for_python(
    py: Python<'_>,
    filters: &Bound<'_, PyDict>,
    values: &Bound<'_, PyDict>,
    prefix: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let specs = PyList::empty(py);
    for (key, value) in filters.iter() {
        let key: String = key.extract()?;
        let (column, operator) = split_filter_key(&key);
        match operator.as_str() {
            "is_null" | "is_not_null" => {
                specs.append((column, operator, Vec::<String>::new()).into_pyobject(py)?)?;
            }
            "in" | "not_in" => {
                let items = value.extract::<Vec<Py<PyAny>>>()?;
                let mut params = Vec::with_capacity(items.len());
                for (idx, item) in items.into_iter().enumerate() {
                    let param = prefixed_param(prefix, &format!("{column}__{operator}_{idx}"));
                    values.set_item(&param, item.bind(py))?;
                    params.push(param);
                }
                specs.append((column, operator, params).into_pyobject(py)?)?;
            }
            _ => {
                let param = prefixed_param(prefix, &key);
                values.set_item(&param, value)?;
                specs.append((column, operator, vec![param]).into_pyobject(py)?)?;
            }
        }
    }
    Ok(specs.into_any().unbind())
}

fn split_filter_key(key: &str) -> (String, String) {
    let Some((column, operator)) = key.rsplit_once("__") else {
        return (key.to_string(), "eq".to_string());
    };
    if FILTER_OPERATORS.contains(&operator) {
        (column.to_string(), operator.to_string())
    } else {
        (key.to_string(), "eq".to_string())
    }
}

fn prefixed_param(prefix: Option<&str>, param: &str) -> String {
    prefix.map_or_else(|| param.to_string(), |prefix| format!("{prefix}__{param}"))
}

pub(crate) fn bind_select_columns(
    columns: Vec<String>,
    aliases: Option<Vec<String>>,
) -> PyResult<Vec<SelectColumn>> {
    let Some(aliases) = aliases else {
        return Ok(columns.into_iter().map(SelectColumn::new).collect());
    };
    if columns.len() != aliases.len() {
        return Err(PyValueError::new_err(
            "select column aliases must match selected columns",
        ));
    }
    Ok(columns
        .into_iter()
        .zip(aliases)
        .map(|(column, alias)| SelectColumn::aliased(column, alias))
        .collect())
}

pub(crate) fn filter_specs(filters: Vec<FilterSpec>) -> PyResult<Vec<Filter>> {
    filters
        .into_iter()
        .map(|(column, operator, params)| {
            let first = params.first().cloned().unwrap_or_else(|| column.clone());
            match operator.as_str() {
                "eq" => Ok(Filter::Eq {
                    column,
                    param: first,
                }),
                "ne" => Ok(Filter::Ne {
                    column,
                    param: first,
                }),
                "lt" => Ok(Filter::Lt {
                    column,
                    param: first,
                }),
                "le" => Ok(Filter::Le {
                    column,
                    param: first,
                }),
                "gt" => Ok(Filter::Gt {
                    column,
                    param: first,
                }),
                "ge" => Ok(Filter::Ge {
                    column,
                    param: first,
                }),
                "like" => Ok(Filter::Like {
                    column,
                    param: first,
                }),
                "ilike" => Ok(Filter::ILike {
                    column,
                    param: first,
                }),
                "in" => Ok(Filter::In { column, params }),
                "not_in" => Ok(Filter::NotIn { column, params }),
                "is_null" => Ok(Filter::IsNull { column }),
                "is_not_null" => Ok(Filter::IsNotNull { column }),
                other => Err(PyValueError::new_err(format!(
                    "unsupported filter operator '{other}'"
                ))),
            }
        })
        .collect()
}

pub(crate) fn joined_filters(filters: Vec<RuntimeJoinedFilter>) -> PyResult<Vec<JoinedFilter>> {
    let mut output = Vec::new();
    for (table_alias, specs) in filters {
        for filter in filter_specs(specs)? {
            output.push(JoinedFilter::new(table_alias.clone(), filter));
        }
    }
    Ok(output)
}

pub(crate) fn joined_order_by(order_by: Vec<RuntimeJoinedOrder>) -> PyResult<Vec<JoinedOrderBy>> {
    order_by
        .into_iter()
        .map(|(table_alias, column, direction)| {
            Ok(JoinedOrderBy::new(
                table_alias,
                OrderBy::new(column, parse_sort_direction(&direction)?),
            ))
        })
        .collect()
}

pub(crate) fn parse_filter_input(filters: &Bound<'_, PyAny>) -> PyResult<Vec<Filter>> {
    if let Ok(specs) = filters.extract::<Vec<FilterSpec>>() {
        return filter_specs(specs);
    }

    let dict = filters.cast::<PyDict>()?;
    let connector: String = dict
        .get_item("connector")?
        .ok_or_else(|| PyValueError::new_err("filter tree missing connector"))?
        .extract()?;

    match connector.as_str() {
        "leaf" => {
            let filters = dict
                .get_item("filters")?
                .ok_or_else(|| PyValueError::new_err("leaf filter tree missing filters"))?;
            let specs = filters.extract::<Vec<FilterSpec>>()?;
            filter_specs(specs)
        }
        "and" | "or" => {
            let children = dict
                .get_item("children")?
                .ok_or_else(|| PyValueError::new_err("group filter tree missing children"))?;
            let children = children.extract::<Vec<Py<PyAny>>>()?;
            let mut parsed = Vec::new();
            for child in children {
                parsed.extend(parse_filter_input(child.bind(filters.py()))?);
            }
            if connector == "and" {
                Ok(vec![Filter::And(parsed)])
            } else {
                Ok(vec![Filter::Or(parsed)])
            }
        }
        other => Err(PyValueError::new_err(format!(
            "unsupported filter connector '{other}'"
        ))),
    }
}

pub(crate) fn parse_sort_direction(direction: &str) -> PyResult<SortDirection> {
    match direction {
        "asc" | "ASC" => Ok(SortDirection::Asc),
        "desc" | "DESC" => Ok(SortDirection::Desc),
        other => Err(PyValueError::new_err(format!(
            "unsupported sort direction '{other}'"
        ))),
    }
}

pub(crate) fn compile_to_python(
    py: Python<'_>,
    dialect: &str,
    query: QueryAst,
) -> PyResult<Py<PyAny>> {
    let dialect =
        AnyDialect::parse(dialect).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let compiled = query
        .compile(&dialect)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    compiled_query_to_dict(py, compiled)
}

pub(crate) fn compiled_query_to_dict(py: Python<'_>, query: CompiledQuery) -> PyResult<Py<PyAny>> {
    let result = PyDict::new(py);
    result.set_item("sql", query.sql())?;
    result.set_item("params", query.params())?;
    result.set_item("operation", operation_name(query.operation()))?;
    Ok(result.into_any().unbind())
}

#[pyfunction]
pub(crate) fn compile_selectin_plan(
    py: Python<'_>,
    dialect: &str,
    parent_table: &str,
    child_table: &str,
    parent_key_columns: Vec<String>,
    child_key_columns: Vec<String>,
    param_names: Vec<String>,
) -> PyResult<Py<PyAny>> {
    let dialect =
        AnyDialect::parse(dialect).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let plan = SqlSelectInPlan::new(
        parent_table,
        child_table,
        parent_key_columns,
        child_key_columns,
    );
    let compiled = plan
        .query_for_batch(param_names)
        .compile(&dialect)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    compiled_query_to_dict(py, compiled)
}

#[pyfunction(signature = (dialect, table, primary_key, columns, aliases=None))]
pub(crate) fn compile_select_pk(
    py: Python<'_>,
    dialect: &str,
    table: &str,
    primary_key: &str,
    columns: Vec<String>,
    aliases: Option<Vec<String>>,
) -> PyResult<Py<PyAny>> {
    let query = QueryAst::Select {
        table: TableRef::new(table),
        columns: bind_select_columns(columns, aliases)?,
        filters: vec![Filter::Eq {
            column: primary_key.to_string(),
            param: primary_key.to_string(),
        }],
        order_by: Vec::new(),
        limit: None,
        offset: None,
    };
    compile_to_python(py, dialect, query)
}

#[pyfunction(signature = (
    dialect,
    table,
    columns,
    filter_columns,
    order_columns,
    order_direction,
    limit=None,
    offset=None,
    aliases=None
))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn compile_find_many(
    py: Python<'_>,
    dialect: &str,
    table: &str,
    columns: Vec<String>,
    filter_columns: Vec<FilterSpec>,
    order_columns: Vec<String>,
    order_direction: &str,
    limit: Option<usize>,
    offset: Option<usize>,
    aliases: Option<Vec<String>>,
) -> PyResult<Py<PyAny>> {
    let direction = parse_sort_direction(order_direction)?;
    let query = QueryAst::Select {
        table: TableRef::new(table),
        columns: bind_select_columns(columns, aliases)?,
        filters: filter_specs(filter_columns)?,
        order_by: order_columns
            .into_iter()
            .map(|column| OrderBy::new(column, direction.clone()))
            .collect(),
        limit,
        offset,
    };
    compile_to_python(py, dialect, query)
}

#[pyfunction(signature = (
    dialect,
    table,
    columns,
    joins,
    filter_columns,
    order_columns,
    order_direction,
    limit=None,
    offset=None
))]
#[allow(clippy::too_many_arguments)]
pub(crate) fn compile_joined_find_many(
    py: Python<'_>,
    dialect: &str,
    table: &str,
    columns: Vec<(String, String, String)>,
    joins: Vec<(String, String, String, String, String, String)>,
    filter_columns: Vec<FilterSpec>,
    order_columns: Vec<String>,
    order_direction: &str,
    limit: Option<usize>,
    offset: Option<usize>,
) -> PyResult<Py<PyAny>> {
    let direction = parse_sort_direction(order_direction)?;
    let query = QueryAst::JoinedSelect {
        table: TableRef::new(table),
        columns: columns
            .into_iter()
            .map(|(table_alias, column, alias)| {
                JoinedSelectColumn::aliased(table_alias, column, alias)
            })
            .collect(),
        joins: joins
            .into_iter()
            .map(
                |(table, alias, left_alias, left_column, right_alias, right_column)| {
                    JoinSpec::left_join(
                        table,
                        alias,
                        left_alias,
                        left_column,
                        right_alias,
                        right_column,
                    )
                },
            )
            .collect(),
        filters: filter_specs(filter_columns)?,
        relationship_filters: Vec::new(),
        order_by: order_columns
            .into_iter()
            .map(|column| OrderBy::new(column, direction.clone()))
            .collect(),
        relationship_order_by: Vec::new(),
        limit,
        offset,
    };
    compile_to_python(py, dialect, query)
}

#[pyfunction]
pub(crate) fn compile_count(
    py: Python<'_>,
    dialect: &str,
    table: &str,
    filter_columns: Vec<FilterSpec>,
) -> PyResult<Py<PyAny>> {
    compile_to_python(
        py,
        dialect,
        QueryAst::Count {
            table: TableRef::new(table),
            filters: filter_specs(filter_columns)?,
        },
    )
}

#[pyfunction]
pub(crate) fn compile_insert(
    py: Python<'_>,
    dialect: &str,
    table: &str,
    columns: Vec<String>,
) -> PyResult<Py<PyAny>> {
    compile_to_python(
        py,
        dialect,
        QueryAst::Insert {
            table: TableRef::new(table),
            columns,
        },
    )
}

#[pyfunction]
pub(crate) fn compile_update(
    py: Python<'_>,
    dialect: &str,
    table: &str,
    primary_key: &str,
    columns: Vec<String>,
) -> PyResult<Py<PyAny>> {
    compile_to_python(
        py,
        dialect,
        QueryAst::Update {
            table: TableRef::new(table),
            columns,
            pk: primary_key.to_string(),
        },
    )
}

#[pyfunction]
pub(crate) fn compile_upsert(
    py: Python<'_>,
    dialect: &str,
    table: &str,
    primary_key: &str,
    columns: Vec<String>,
) -> PyResult<Py<PyAny>> {
    compile_to_python(
        py,
        dialect,
        QueryAst::Upsert {
            table: TableRef::new(table),
            columns,
            pk: primary_key.to_string(),
        },
    )
}

#[pyfunction]
pub(crate) fn compile_delete_pk(
    py: Python<'_>,
    dialect: &str,
    table: &str,
    primary_key: &str,
) -> PyResult<Py<PyAny>> {
    compile_to_python(
        py,
        dialect,
        QueryAst::Delete {
            table: TableRef::new(table),
            pk: primary_key.to_string(),
        },
    )
}

#[allow(clippy::too_many_arguments)]
#[pyfunction(signature = (dialect, table, projections, where_column=None, where_param=None, limit=None, offset=None))]
pub(crate) fn compile_expression_query(
    py: Python<'_>,
    dialect: &str,
    table: &str,
    projections: Vec<(String, Option<String>)>,
    where_column: Option<String>,
    where_param: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> PyResult<Py<PyAny>> {
    let projections = projections
        .into_iter()
        .map(|(column, alias)| match alias {
            Some(alias) => Projection::aliased(Expr::column(column), alias),
            None => Projection::new(Expr::column(column)),
        })
        .collect::<Vec<_>>();
    let mut query = SelectAst::new(projections).from(TableSource::table(table));
    if let (Some(column), Some(param)) = (where_column, where_param) {
        query = query.where_expr(Expr::eq(Expr::column(column), Expr::param(param)));
    }
    if let Some(limit) = limit {
        query = query.limit(limit);
    }
    if let Some(offset) = offset {
        query = query.offset(offset);
    }
    let dialect =
        AnyDialect::parse(dialect).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let compiled = query
        .compile(&dialect)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    compiled_query_to_dict(py, compiled)
}

#[pyfunction]
pub(crate) fn compile_typed_expression_query(
    py: Python<'_>,
    dialect: &str,
    query: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let query = query.cast::<PyDict>()?;
    let dialect =
        AnyDialect::parse(dialect).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let compiled = select_ast_from_payload(py, query)?
        .compile(&dialect)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    let result = PyDict::new(py);
    result.set_item("sql", compiled.sql())?;
    result.set_item("params", compiled.params())?;
    result.set_item("operation", operation_name(compiled.operation()))?;
    if let Some(values) = query.get_item("values")? {
        result.set_item("values", values)?;
    }
    Ok(result.into_any().unbind())
}

#[pyfunction]
pub(crate) fn compile_typed_update_query(
    py: Python<'_>,
    dialect: &str,
    query: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let query = query.cast::<PyDict>()?;
    let dialect =
        AnyDialect::parse(dialect).map_err(|error| PyValueError::new_err(error.to_string()))?;
    let compiled = update_ast_from_payload(py, query)?
        .compile(&dialect)
        .map_err(|error| PyValueError::new_err(error.to_string()))?;
    let result = PyDict::new(py);
    result.set_item("sql", compiled.sql())?;
    result.set_item("params", compiled.params())?;
    result.set_item("operation", operation_name(compiled.operation()))?;
    if let Some(values) = query.get_item("values")? {
        result.set_item("values", values)?;
    }
    Ok(result.into_any().unbind())
}

pub(crate) fn select_ast_from_payload(
    py: Python<'_>,
    query: &Bound<'_, PyDict>,
) -> PyResult<SelectAst> {
    let table: String = required_item(query, "table")?.extract()?;
    let projections = required_item(query, "projections")?.extract::<Vec<Py<PyAny>>>()?;
    let source = match query.get_item("table_alias")? {
        Some(alias) => TableSource::aliased_table(table, alias.extract::<String>()?),
        None => TableSource::table(table),
    };
    let mut select = SelectAst::new(parse_projections(py, projections)?).from(source);

    if let Some(ctes) = query.get_item("ctes")? {
        for cte in parse_ctes(py, ctes)? {
            select = select.with_cte(cte);
        }
    }
    if let Some(where_expr) = query.get_item("where")? {
        select = select.where_expr(parse_expression(where_expr)?);
    }
    if let Some(group_by) = query.get_item("group_by")? {
        select = select.group_by(parse_expression_list(py, group_by)?);
    }
    if let Some(having) = query.get_item("having")? {
        select = select.having(parse_expression(having)?);
    }
    if let Some(order_by) = query.get_item("order_by")? {
        select = select.order_by(parse_order_expressions(py, order_by)?);
    }
    if let Some(distinct) = query.get_item("distinct")? {
        select = select.distinct(distinct.extract()?);
    }
    if let Some(limit) = query.get_item("limit")? {
        select = select.limit(limit.extract()?);
    }
    if let Some(offset) = query.get_item("offset")? {
        select = select.offset(offset.extract()?);
    }
    Ok(select)
}

pub(crate) fn update_ast_from_payload(
    py: Python<'_>,
    query: &Bound<'_, PyDict>,
) -> PyResult<DmlAst> {
    let table: String = required_item(query, "table")?.extract()?;
    let assignments = required_item(query, "assignments")?
        .extract::<Vec<Py<PyAny>>>()?
        .into_iter()
        .map(|assignment| {
            let assignment = assignment.bind(py).cast::<PyDict>()?;
            Ok((
                required_item(assignment, "column")?.extract::<String>()?,
                parse_expression(required_item(assignment, "expr")?)?,
            ))
        })
        .collect::<PyResult<Vec<_>>>()?;
    let where_expr = query.get_item("where")?.map(parse_expression).transpose()?;
    Ok(DmlAst::Update {
        table: TableSource::table(table),
        assignments,
        where_expr,
        returning: Vec::new(),
    })
}

pub(crate) fn delete_ast_from_payload(query: &Bound<'_, PyDict>) -> PyResult<DmlAst> {
    let table: String = required_item(query, "table")?.extract()?;
    let where_expr = query.get_item("where")?.map(parse_expression).transpose()?;
    Ok(DmlAst::Delete {
        table: TableSource::table(table),
        where_expr,
        returning: Vec::new(),
    })
}

fn parse_projections(py: Python<'_>, projections: Vec<Py<PyAny>>) -> PyResult<Vec<Projection>> {
    projections
        .into_iter()
        .map(|projection| {
            let projection = projection.bind(py).cast::<PyDict>()?;
            let expr = parse_expression(required_item(projection, "expr")?)?;
            match projection.get_item("alias")? {
                Some(alias) => Ok(Projection::aliased(expr, alias.extract::<String>()?)),
                None => Ok(Projection::new(expr)),
            }
        })
        .collect()
}

fn parse_expression_list(py: Python<'_>, value: Bound<'_, PyAny>) -> PyResult<Vec<Expr>> {
    value
        .extract::<Vec<Py<PyAny>>>()?
        .into_iter()
        .map(|expr| parse_expression(expr.bind(py).clone()))
        .collect()
}

fn parse_order_expressions(py: Python<'_>, value: Bound<'_, PyAny>) -> PyResult<Vec<OrderExpr>> {
    value
        .extract::<Vec<Py<PyAny>>>()?
        .into_iter()
        .map(|order| {
            let order_payload = order.bind(py).cast::<PyDict>()?;
            let expr = parse_expression(required_item(order_payload, "expr")?)?;
            let direction: String = required_item(order_payload, "direction")?.extract()?;
            let mut order_expr = OrderExpr::new(expr, parse_sort_direction(&direction)?);
            if let Some(nulls) = order_payload.get_item("nulls")? {
                order_expr = order_expr.nulls(parse_order_nulls(&nulls.extract::<String>()?)?);
            }
            Ok(order_expr)
        })
        .collect()
}

fn parse_ctes(py: Python<'_>, value: Bound<'_, PyAny>) -> PyResult<Vec<CommonTableExpr>> {
    value
        .extract::<Vec<Py<PyAny>>>()?
        .into_iter()
        .map(|cte| {
            let cte_payload = cte.bind(py).cast::<PyDict>()?;
            let query = required_item(cte_payload, "query")?;
            let query = query.cast::<PyDict>()?;
            let mut cte = CommonTableExpr::new(
                required_item(cte_payload, "name")?.extract::<String>()?,
                select_ast_from_payload(py, query)?,
            );
            if let Some(columns) = cte_payload.get_item("columns")? {
                cte = cte.columns(columns.extract()?);
            }
            if let Some(recursive) = cte_payload.get_item("recursive")? {
                cte = cte.recursive(recursive.extract()?);
            }
            Ok(cte)
        })
        .collect()
}

fn parse_expression(expr: Bound<'_, PyAny>) -> PyResult<Expr> {
    let expr = expr.cast::<PyDict>()?;
    let kind: String = required_item(expr, "kind")?.extract()?;
    match kind.as_str() {
        "column" => {
            let name: String = required_item(expr, "name")?.extract()?;
            match expr.get_item("table")? {
                Some(table) => Ok(Expr::qualified_column(table.extract::<String>()?, name)),
                None => Ok(Expr::column(name)),
            }
        }
        "param" => Ok(Expr::param(
            required_item(expr, "name")?.extract::<String>()?,
        )),
        "literal" => parse_literal_expr(required_item(expr, "value")?),
        "raw_safe" => Ok(Expr::RawSafe(required_item(expr, "sql")?.extract()?)),
        "binary" => Ok(Expr::Binary {
            left: Box::new(parse_expression(required_item(expr, "left")?)?),
            op: parse_binary_op(&required_item(expr, "op")?.extract::<String>()?)?,
            right: Box::new(parse_expression(required_item(expr, "right")?)?),
        }),
        "unary" => Ok(Expr::Unary {
            op: parse_unary_op(&required_item(expr, "op")?.extract::<String>()?)?,
            expr: Box::new(parse_expression(required_item(expr, "expr")?)?),
        }),
        "function" => {
            let args = match expr.get_item("args")? {
                Some(args) => args
                    .extract::<Vec<Py<PyAny>>>()?
                    .into_iter()
                    .map(|arg| parse_expression(arg.bind(expr.py()).clone()))
                    .collect::<PyResult<Vec<_>>>()?,
                None => Vec::new(),
            };
            Ok(Expr::Function {
                name: required_item(expr, "name")?.extract()?,
                args,
            })
        }
        "window" => {
            let partition_by = match expr.get_item("partition_by")? {
                Some(partition_by) => parse_expression_list(expr.py(), partition_by)?,
                None => Vec::new(),
            };
            let order_by = match expr.get_item("order_by")? {
                Some(order_by) => parse_order_expressions(expr.py(), order_by)?,
                None => Vec::new(),
            };
            Ok(Expr::Window {
                expr: Box::new(parse_expression(required_item(expr, "expr")?)?),
                partition_by,
                order_by,
            })
        }
        "between" => Ok(Expr::Between {
            expr: Box::new(parse_expression(required_item(expr, "expr")?)?),
            low: Box::new(parse_expression(required_item(expr, "low")?)?),
            high: Box::new(parse_expression(required_item(expr, "high")?)?),
        }),
        "in_list" => {
            let values = required_item(expr, "values")?
                .extract::<Vec<Py<PyAny>>>()?
                .into_iter()
                .map(|value| parse_expression(value.bind(expr.py()).clone()))
                .collect::<PyResult<Vec<_>>>()?;
            let negated = expr
                .get_item("negated")?
                .map(|value| value.extract::<bool>())
                .transpose()?
                .unwrap_or(false);
            Ok(Expr::InList {
                expr: Box::new(parse_expression(required_item(expr, "expr")?)?),
                values,
                negated,
            })
        }
        "case" => {
            let whens = required_item(expr, "whens")?
                .extract::<Vec<Py<PyAny>>>()?
                .into_iter()
                .map(|when| {
                    let when = when.bind(expr.py()).cast::<PyDict>()?;
                    Ok((
                        parse_expression(required_item(when, "when")?)?,
                        parse_expression(required_item(when, "then")?)?,
                    ))
                })
                .collect::<PyResult<Vec<_>>>()?;
            let else_expr = match expr.get_item("else")? {
                Some(value) if value.is_none() => None,
                Some(value) => Some(Box::new(parse_expression(value)?)),
                None => None,
            };
            Ok(Expr::Case { whens, else_expr })
        }
        "cast" => Ok(Expr::Cast {
            expr: Box::new(parse_expression(required_item(expr, "expr")?)?),
            type_name: required_item(expr, "type")?.extract()?,
        }),
        "tuple" => {
            let values = required_item(expr, "values")?
                .extract::<Vec<Py<PyAny>>>()?
                .into_iter()
                .map(|value| parse_expression(value.bind(expr.py()).clone()))
                .collect::<PyResult<Vec<_>>>()?;
            Ok(Expr::Tuple(values))
        }
        "subquery" => Ok(Expr::Subquery(Box::new(parse_select_subquery(expr)?))),
        "exists" => {
            let negated = expr
                .get_item("negated")?
                .map(|value| value.extract::<bool>())
                .transpose()?
                .unwrap_or(false);
            Ok(Expr::Exists {
                select: Box::new(parse_select_subquery(expr)?),
                negated,
            })
        }
        "in_subquery" => {
            let negated = expr
                .get_item("negated")?
                .map(|value| value.extract::<bool>())
                .transpose()?
                .unwrap_or(false);
            Ok(Expr::InSubquery {
                expr: Box::new(parse_expression(required_item(expr, "expr")?)?),
                select: Box::new(parse_select_subquery(expr)?),
                negated,
            })
        }
        other => Err(PyValueError::new_err(format!(
            "unsupported expression kind '{other}'"
        ))),
    }
}

fn parse_select_subquery(expr: &Bound<'_, PyDict>) -> PyResult<SelectAst> {
    let query = required_item(expr, "query")?;
    let query = query.cast::<PyDict>()?;
    select_ast_from_payload(expr.py(), query)
}

fn parse_literal_expr(value: Bound<'_, PyAny>) -> PyResult<Expr> {
    if value.is_none() {
        return Ok(Expr::Literal(SqlLiteral::Null));
    }
    if let Ok(value) = value.extract::<bool>() {
        return Ok(Expr::Literal(SqlLiteral::Boolean(value)));
    }
    if let Ok(value) = value.extract::<i64>() {
        return Ok(Expr::Literal(SqlLiteral::Integer(value)));
    }
    if let Ok(value) = value.extract::<String>() {
        return Ok(Expr::Literal(SqlLiteral::String(value)));
    }
    Err(PyValueError::new_err(
        "literal expressions support None, bool, int, and str values",
    ))
}

fn parse_binary_op(op: &str) -> PyResult<BinaryOp> {
    match op {
        "eq" => Ok(BinaryOp::Eq),
        "ne" => Ok(BinaryOp::Ne),
        "lt" => Ok(BinaryOp::Lt),
        "le" => Ok(BinaryOp::Le),
        "gt" => Ok(BinaryOp::Gt),
        "ge" => Ok(BinaryOp::Ge),
        "add" => Ok(BinaryOp::Add),
        "sub" => Ok(BinaryOp::Sub),
        "mul" => Ok(BinaryOp::Mul),
        "div" => Ok(BinaryOp::Div),
        "and" => Ok(BinaryOp::And),
        "or" => Ok(BinaryOp::Or),
        "like" => Ok(BinaryOp::Like),
        "ilike" => Ok(BinaryOp::ILike),
        other => Err(PyValueError::new_err(format!(
            "unsupported binary operator '{other}'"
        ))),
    }
}

fn parse_unary_op(op: &str) -> PyResult<UnaryOp> {
    match op {
        "not" => Ok(UnaryOp::Not),
        "neg" => Ok(UnaryOp::Neg),
        "is_null" => Ok(UnaryOp::IsNull),
        "is_not_null" => Ok(UnaryOp::IsNotNull),
        other => Err(PyValueError::new_err(format!(
            "unsupported unary operator '{other}'"
        ))),
    }
}

fn parse_order_nulls(nulls: &str) -> PyResult<OrderNulls> {
    match nulls {
        "first" | "FIRST" => Ok(OrderNulls::First),
        "last" | "LAST" => Ok(OrderNulls::Last),
        other => Err(PyValueError::new_err(format!(
            "unsupported null ordering '{other}'"
        ))),
    }
}

fn required_item<'py>(dict: &Bound<'py, PyDict>, key: &str) -> PyResult<Bound<'py, PyAny>> {
    dict.get_item(key)?
        .ok_or_else(|| PyValueError::new_err(format!("expression payload missing '{key}'")))
}

pub(crate) fn operation_name(operation: &QueryOperation) -> &'static str {
    match operation {
        QueryOperation::Select => "select",
        QueryOperation::Insert => "insert",
        QueryOperation::Update => "update",
        QueryOperation::Upsert => "upsert",
        QueryOperation::Delete => "delete",
        QueryOperation::Count => "count",
        QueryOperation::Ddl => "ddl",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_columns_require_matching_aliases() {
        assert!(bind_select_columns(vec!["id".to_string()], None).is_ok());
        assert!(bind_select_columns(vec!["id".to_string()], Some(Vec::new())).is_err());
    }

    #[test]
    fn filter_specs_parse_supported_operators() {
        let filters = filter_specs(vec![
            ("id".to_string(), "eq".to_string(), vec!["id".to_string()]),
            (
                "name".to_string(),
                "in".to_string(),
                vec!["name_0".to_string(), "name_1".to_string()],
            ),
            ("deleted_at".to_string(), "is_null".to_string(), Vec::new()),
        ])
        .expect("filters should parse");

        assert!(matches!(filters[0], Filter::Eq { .. }));
        assert!(matches!(filters[1], Filter::In { .. }));
        assert!(matches!(filters[2], Filter::IsNull { .. }));
        assert!(filter_specs(vec![(
            "id".to_string(),
            "regex".to_string(),
            vec!["id".to_string()]
        )])
        .is_err());
    }

    #[test]
    fn sort_direction_and_operation_names_are_stable() {
        assert_eq!(parse_sort_direction("ASC").unwrap(), SortDirection::Asc);
        assert_eq!(parse_sort_direction("desc").unwrap(), SortDirection::Desc);
        assert!(parse_sort_direction("random").is_err());
        assert_eq!(operation_name(&QueryOperation::Upsert), "upsert");
    }
}
