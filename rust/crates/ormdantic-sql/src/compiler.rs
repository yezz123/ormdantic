use ormdantic_core::{OrmdanticError, OrmdanticResult};
use ormdantic_dialects::{Dialect, DialectKind};
use ormdantic_schema::SchemaOperation;

use crate::ast::{
    BinaryOp, CommonTableExpr, CompiledQuery, DmlAst, Expr, JoinKind, JoinSpec, JoinedFilter,
    JoinedOrderBy, JoinedSelectColumn, OrderBy, OrderExpr, OrderNulls, QueryAst, QueryOperation,
    SelectAst, SelectColumn, SelectInQuery, SortDirection, SqlLiteral, TableRef, TableSource,
    UnaryOp,
};
use crate::filters::{BindParam, BoolOp, ComparisonOp, Filter, PredicateExpr};

pub struct DdlCompiler;

impl DdlCompiler {
    pub fn compile(
        dialect: &impl Dialect,
        operations: &[SchemaOperation],
    ) -> OrmdanticResult<Vec<CompiledQuery>> {
        let mut compiled = Vec::new();
        for operation in operations {
            for sql in dialect.compile_schema_operation(operation)? {
                if !sql.is_empty() {
                    compiled.push(CompiledQuery::new(sql, Vec::new(), QueryOperation::Ddl));
                }
            }
        }
        Ok(compiled)
    }
}

fn compiled_query(
    dialect: &impl Dialect,
    sql: String,
    params: Vec<String>,
    operation: QueryOperation,
) -> OrmdanticResult<CompiledQuery> {
    if let Some(max_params) = dialect.max_bind_parameters() {
        if params.len() > max_params {
            return Err(OrmdanticError::SqlCompile {
                message: format!(
                    "query uses {} bind parameters, exceeding the {} limit for {}",
                    params.len(),
                    max_params,
                    dialect.name()
                ),
            });
        }
    }
    Ok(CompiledQuery::new(sql, params, operation))
}

pub(crate) fn compile_query_ast(
    dialect: &impl Dialect,
    query: &QueryAst,
) -> OrmdanticResult<CompiledQuery> {
    match query {
        QueryAst::Select {
            table,
            columns,
            filters,
            order_by,
            limit,
            offset,
        } => compile_select(dialect, table, columns, filters, order_by, *limit, *offset),
        QueryAst::JoinedSelect {
            table,
            columns,
            joins,
            filters,
            relationship_filters,
            order_by,
            relationship_order_by,
            limit,
            offset,
        } => compile_joined_select(
            dialect,
            table,
            columns,
            joins,
            filters,
            relationship_filters,
            order_by,
            relationship_order_by,
            *limit,
            *offset,
        ),
        QueryAst::Count { table, filters } => compile_count(dialect, table, filters),
        QueryAst::Insert { table, columns } => compile_insert(dialect, table, columns),
        QueryAst::Update { table, columns, pk } => compile_update(dialect, table, columns, pk),
        QueryAst::Upsert { table, columns, pk } => compile_upsert(dialect, table, columns, pk),
        QueryAst::Delete { table, pk } => compile_delete(dialect, table, pk),
    }
}

pub(crate) fn compile_select_ast(
    dialect: &impl Dialect,
    select: &SelectAst,
) -> OrmdanticResult<CompiledQuery> {
    let mut params = Vec::new();
    let mut bind_index = 1;
    let sql = render_select_ast(dialect, select, &mut params, &mut bind_index)?;
    compiled_query(dialect, sql, params, QueryOperation::Select)
}

pub(crate) fn compile_dml_ast(
    dialect: &impl Dialect,
    dml: &DmlAst,
) -> OrmdanticResult<CompiledQuery> {
    let mut params = Vec::new();
    let mut bind_index = 1;
    let (sql, operation) = render_dml_ast(dialect, dml, &mut params, &mut bind_index)?;
    compiled_query(dialect, sql, params, operation)
}

pub(crate) fn compile_select_in_query(
    dialect: &impl Dialect,
    query: &SelectInQuery,
) -> OrmdanticResult<CompiledQuery> {
    let mut bind_index = 1;
    let selected = "*";
    let mut params = Vec::new();
    let predicates = query
        .plan
        .child_key_columns
        .iter()
        .map(|column| {
            let placeholders = query
                .param_names
                .iter()
                .map(|param| {
                    params.push(param.clone());
                    let placeholder = dialect.placeholder(bind_index);
                    bind_index += 1;
                    placeholder
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("{} IN ({placeholders})", dialect.quote_ident(column))
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    compiled_query(
        dialect,
        format!(
            "SELECT {selected} FROM {} WHERE {predicates}",
            quote_table_name(dialect, &query.plan.child_table)
        ),
        params,
        QueryOperation::Select,
    )
}

fn render_select_ast(
    dialect: &impl Dialect,
    select: &SelectAst,
    params: &mut Vec<String>,
    bind_index: &mut usize,
) -> OrmdanticResult<String> {
    if select.projections.is_empty() {
        return Err(OrmdanticError::SqlCompile {
            message: "select query requires at least one projection".to_string(),
        });
    }

    let mut sql = String::new();
    append_ctes(dialect, &mut sql, &select.ctes, params, bind_index)?;
    sql.push_str("SELECT ");
    if select.distinct {
        sql.push_str("DISTINCT ");
    }
    sql.push_str(
        &select
            .projections
            .iter()
            .map(|projection| {
                let rendered = render_expr(dialect, &projection.expr, params, bind_index)?;
                Ok(match &projection.alias {
                    Some(alias) => format!("{rendered} AS {}", dialect.quote_ident(alias)),
                    None => rendered,
                })
            })
            .collect::<OrmdanticResult<Vec<_>>>()?
            .join(", "),
    );

    if let Some(source) = &select.from {
        sql.push_str(" FROM ");
        sql.push_str(&render_table_source(dialect, source));
    }
    for join in &select.joins {
        sql.push(' ');
        sql.push_str(match join.kind {
            JoinKind::Inner => "JOIN",
            JoinKind::Left => "LEFT JOIN",
            JoinKind::Right => "RIGHT JOIN",
            JoinKind::Full => "FULL JOIN",
            JoinKind::Cross => "CROSS JOIN",
        });
        sql.push(' ');
        sql.push_str(&render_table_source(dialect, &join.source));
        if let Some(on) = &join.on {
            sql.push_str(" ON ");
            sql.push_str(&render_expr(dialect, on, params, bind_index)?);
        }
    }
    if let Some(where_expr) = &select.where_expr {
        sql.push_str(" WHERE ");
        sql.push_str(&render_expr(dialect, where_expr, params, bind_index)?);
    }
    if !select.group_by.is_empty() {
        sql.push_str(" GROUP BY ");
        sql.push_str(
            &select
                .group_by
                .iter()
                .map(|expr| render_expr(dialect, expr, params, bind_index))
                .collect::<OrmdanticResult<Vec<_>>>()?
                .join(", "),
        );
    }
    if let Some(having) = &select.having {
        sql.push_str(" HAVING ");
        sql.push_str(&render_expr(dialect, having, params, bind_index)?);
    }
    if !select.order_by.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(
            &select
                .order_by
                .iter()
                .map(|order| render_order_expr(dialect, order, params, bind_index))
                .collect::<OrmdanticResult<Vec<_>>>()?
                .join(", "),
        );
    }
    if let Some(limit) = select.limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    if let Some(offset) = select.offset {
        sql.push_str(&format!(" OFFSET {offset}"));
    }
    Ok(sql)
}

fn render_dml_ast(
    dialect: &impl Dialect,
    dml: &DmlAst,
    params: &mut Vec<String>,
    bind_index: &mut usize,
) -> OrmdanticResult<(String, QueryOperation)> {
    match dml {
        DmlAst::Insert {
            table,
            columns,
            rows,
            returning,
        } => {
            require_columns(columns, "insert")?;
            if rows.is_empty() {
                return Err(OrmdanticError::SqlCompile {
                    message: "insert query requires at least one row".to_string(),
                });
            }
            let rendered_rows = rows
                .iter()
                .map(|row| {
                    Ok(format!(
                        "({})",
                        row.iter()
                            .map(|expr| render_expr(dialect, expr, params, bind_index))
                            .collect::<OrmdanticResult<Vec<_>>>()?
                            .join(", ")
                    ))
                })
                .collect::<OrmdanticResult<Vec<_>>>()?
                .join(", ");
            let mut sql = format!(
                "INSERT INTO {} ({}) VALUES {rendered_rows}",
                render_table_source(dialect, table),
                column_list(dialect, columns)
            );
            append_returning(dialect, &mut sql, returning, params, bind_index)?;
            Ok((sql, QueryOperation::Insert))
        }
        DmlAst::Update {
            table,
            assignments,
            where_expr,
            returning,
        } => {
            if assignments.is_empty() {
                return Err(OrmdanticError::SqlCompile {
                    message: "update query requires at least one assignment".to_string(),
                });
            }
            let assignments = assignments
                .iter()
                .map(|(column, expr)| {
                    Ok(format!(
                        "{} = {}",
                        dialect.quote_ident(column),
                        render_expr(dialect, expr, params, bind_index)?
                    ))
                })
                .collect::<OrmdanticResult<Vec<_>>>()?
                .join(", ");
            let mut sql = format!(
                "UPDATE {} SET {assignments}",
                render_table_source(dialect, table)
            );
            if let Some(where_expr) = where_expr {
                sql.push_str(" WHERE ");
                sql.push_str(&render_expr(dialect, where_expr, params, bind_index)?);
            }
            append_returning(dialect, &mut sql, returning, params, bind_index)?;
            Ok((sql, QueryOperation::Update))
        }
        DmlAst::Delete {
            table,
            where_expr,
            returning,
        } => {
            let mut sql = format!("DELETE FROM {}", render_table_source(dialect, table));
            if let Some(where_expr) = where_expr {
                sql.push_str(" WHERE ");
                sql.push_str(&render_expr(dialect, where_expr, params, bind_index)?);
            }
            append_returning(dialect, &mut sql, returning, params, bind_index)?;
            Ok((sql, QueryOperation::Delete))
        }
        DmlAst::Upsert {
            table,
            columns,
            rows,
            conflict_target,
            update_assignments,
            returning,
        } => {
            require_columns(columns, "upsert")?;
            if rows.is_empty() {
                return Err(OrmdanticError::SqlCompile {
                    message: "upsert query requires at least one row".to_string(),
                });
            }
            let rendered_rows = rows
                .iter()
                .map(|row| {
                    Ok(format!(
                        "({})",
                        row.iter()
                            .map(|expr| render_expr(dialect, expr, params, bind_index))
                            .collect::<OrmdanticResult<Vec<_>>>()?
                            .join(", ")
                    ))
                })
                .collect::<OrmdanticResult<Vec<_>>>()?
                .join(", ");
            let mut sql = format!(
                "INSERT INTO {} ({}) VALUES {rendered_rows}",
                render_table_source(dialect, table),
                column_list(dialect, columns)
            );
            let target = conflict_target
                .first()
                .or_else(|| columns.first())
                .ok_or_else(|| OrmdanticError::SqlCompile {
                    message: "upsert query requires a conflict target".to_string(),
                })?;
            let update_columns = if update_assignments.is_empty() {
                columns
                    .iter()
                    .filter(|column| *column != target)
                    .cloned()
                    .collect::<Vec<_>>()
            } else {
                update_assignments
                    .iter()
                    .map(|(column, _)| column.clone())
                    .collect::<Vec<_>>()
            };
            sql.push(' ');
            sql.push_str(&dialect.upsert_conflict_clause(target, &update_columns));
            append_returning(dialect, &mut sql, returning, params, bind_index)?;
            Ok((sql, QueryOperation::Upsert))
        }
    }
}

fn render_expr(
    dialect: &impl Dialect,
    expr: &Expr,
    params: &mut Vec<String>,
    bind_index: &mut usize,
) -> OrmdanticResult<String> {
    Ok(match expr {
        Expr::Column { table, name } => match table {
            Some(table) => format!(
                "{}.{}",
                quote_table_name(dialect, table),
                dialect.quote_ident(name)
            ),
            None => dialect.quote_ident(name),
        },
        Expr::Param(name) => {
            params.push(name.clone());
            let placeholder = dialect.placeholder(*bind_index);
            *bind_index += 1;
            placeholder
        }
        Expr::Literal(literal) => render_literal(literal),
        Expr::Binary { left, op, right } => format!(
            "({} {} {})",
            render_expr(dialect, left, params, bind_index)?,
            render_binary_op(op),
            render_expr(dialect, right, params, bind_index)?
        ),
        Expr::Unary { op, expr } => match op {
            UnaryOp::Not => format!("(NOT {})", render_expr(dialect, expr, params, bind_index)?),
            UnaryOp::Neg => format!("(-{})", render_expr(dialect, expr, params, bind_index)?),
            UnaryOp::IsNull => {
                format!(
                    "({} IS NULL)",
                    render_expr(dialect, expr, params, bind_index)?
                )
            }
            UnaryOp::IsNotNull => format!(
                "({} IS NOT NULL)",
                render_expr(dialect, expr, params, bind_index)?
            ),
        },
        Expr::Function { name, args } => {
            let args = args
                .iter()
                .map(|arg| render_expr(dialect, arg, params, bind_index))
                .collect::<OrmdanticResult<Vec<_>>>()?
                .join(", ");
            format!("{name}({args})")
        }
        Expr::Window {
            expr,
            partition_by,
            order_by,
        } => {
            let mut clauses = Vec::new();
            if !partition_by.is_empty() {
                clauses.push(format!(
                    "PARTITION BY {}",
                    partition_by
                        .iter()
                        .map(|expr| render_expr(dialect, expr, params, bind_index))
                        .collect::<OrmdanticResult<Vec<_>>>()?
                        .join(", ")
                ));
            }
            if !order_by.is_empty() {
                clauses.push(format!(
                    "ORDER BY {}",
                    order_by
                        .iter()
                        .map(|order| render_order_expr(dialect, order, params, bind_index))
                        .collect::<OrmdanticResult<Vec<_>>>()?
                        .join(", ")
                ));
            }
            format!(
                "{} OVER ({})",
                render_expr(dialect, expr, params, bind_index)?,
                clauses.join(" ")
            )
        }
        Expr::Between { expr, low, high } => format!(
            "({} BETWEEN {} AND {})",
            render_expr(dialect, expr, params, bind_index)?,
            render_expr(dialect, low, params, bind_index)?,
            render_expr(dialect, high, params, bind_index)?
        ),
        Expr::InList {
            expr,
            values,
            negated,
        } => {
            if values.is_empty() {
                return Ok(if *negated {
                    "(1 = 1)".to_string()
                } else {
                    "(1 = 0)".to_string()
                });
            }
            let operator = if *negated { "NOT IN" } else { "IN" };
            format!(
                "({} {operator} ({}))",
                render_expr(dialect, expr, params, bind_index)?,
                values
                    .iter()
                    .map(|value| render_expr(dialect, value, params, bind_index))
                    .collect::<OrmdanticResult<Vec<_>>>()?
                    .join(", ")
            )
        }
        Expr::Case { whens, else_expr } => {
            let mut sql = "CASE".to_string();
            for (condition, value) in whens {
                sql.push_str(" WHEN ");
                sql.push_str(&render_expr(dialect, condition, params, bind_index)?);
                sql.push_str(" THEN ");
                sql.push_str(&render_expr(dialect, value, params, bind_index)?);
            }
            if let Some(else_expr) = else_expr {
                sql.push_str(" ELSE ");
                sql.push_str(&render_expr(dialect, else_expr, params, bind_index)?);
            }
            sql.push_str(" END");
            sql
        }
        Expr::Cast { expr, type_name } => format!(
            "CAST({} AS {type_name})",
            render_expr(dialect, expr, params, bind_index)?
        ),
        Expr::Tuple(values) => format!(
            "({})",
            values
                .iter()
                .map(|value| render_expr(dialect, value, params, bind_index))
                .collect::<OrmdanticResult<Vec<_>>>()?
                .join(", ")
        ),
        Expr::Subquery(select) => format!(
            "({})",
            render_select_ast(dialect, select, params, bind_index)?
        ),
        Expr::Exists { select, negated } => {
            let operator = if *negated { "NOT EXISTS" } else { "EXISTS" };
            format!(
                "({operator} ({}))",
                render_select_ast(dialect, select, params, bind_index)?
            )
        }
        Expr::InSubquery {
            expr,
            select,
            negated,
        } => {
            let operator = if *negated { "NOT IN" } else { "IN" };
            format!(
                "({} {operator} ({}))",
                render_expr(dialect, expr, params, bind_index)?,
                render_select_ast(dialect, select, params, bind_index)?
            )
        }
        Expr::RawSafe(sql) => sql.clone(),
    })
}

fn append_ctes(
    dialect: &impl Dialect,
    sql: &mut String,
    ctes: &[CommonTableExpr],
    params: &mut Vec<String>,
    bind_index: &mut usize,
) -> OrmdanticResult<()> {
    if ctes.is_empty() {
        return Ok(());
    }
    sql.push_str("WITH ");
    if ctes.iter().any(|cte| cte.recursive) {
        sql.push_str("RECURSIVE ");
    }
    sql.push_str(
        &ctes
            .iter()
            .map(|cte| {
                let columns = if cte.columns.is_empty() {
                    String::new()
                } else {
                    format!(
                        " ({})",
                        cte.columns
                            .iter()
                            .map(|column| dialect.quote_ident(column))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };
                Ok(format!(
                    "{}{columns} AS ({})",
                    dialect.quote_ident(&cte.name),
                    render_select_ast(dialect, &cte.query, params, bind_index)?
                ))
            })
            .collect::<OrmdanticResult<Vec<_>>>()?
            .join(", "),
    );
    sql.push(' ');
    Ok(())
}

fn render_table_source(dialect: &impl Dialect, source: &TableSource) -> String {
    match source {
        TableSource::Table { name, alias } => match alias {
            Some(alias) => format!(
                "{} AS {}",
                quote_table_name(dialect, name),
                dialect.quote_ident(alias)
            ),
            None => quote_table_name(dialect, name),
        },
        TableSource::RawSafe(sql) => sql.clone(),
    }
}

fn render_order_expr(
    dialect: &impl Dialect,
    order: &OrderExpr,
    params: &mut Vec<String>,
    bind_index: &mut usize,
) -> OrmdanticResult<String> {
    let direction = match order.direction {
        SortDirection::Asc => "ASC",
        SortDirection::Desc => "DESC",
    };
    let nulls = match order.nulls {
        Some(OrderNulls::First) => " NULLS FIRST",
        Some(OrderNulls::Last) => " NULLS LAST",
        None => "",
    };
    Ok(format!(
        "{} {direction}{nulls}",
        render_expr(dialect, &order.expr, params, bind_index)?
    ))
}

fn append_returning(
    dialect: &impl Dialect,
    sql: &mut String,
    returning: &[Expr],
    params: &mut Vec<String>,
    bind_index: &mut usize,
) -> OrmdanticResult<()> {
    if returning.is_empty() {
        return Ok(());
    }
    if !dialect.supports_returning() {
        return Err(OrmdanticError::UnsupportedFeature {
            feature: "RETURNING".to_string(),
            dialect: dialect.name().to_string(),
        });
    }
    sql.push_str(" RETURNING ");
    sql.push_str(
        &returning
            .iter()
            .map(|expr| render_expr(dialect, expr, params, bind_index))
            .collect::<OrmdanticResult<Vec<_>>>()?
            .join(", "),
    );
    Ok(())
}

fn render_literal(literal: &SqlLiteral) -> String {
    match literal {
        SqlLiteral::Null => "NULL".to_string(),
        SqlLiteral::Integer(value) => value.to_string(),
        SqlLiteral::String(value) => format!("'{}'", value.replace('\'', "''")),
        SqlLiteral::Boolean(true) => "TRUE".to_string(),
        SqlLiteral::Boolean(false) => "FALSE".to_string(),
    }
}

fn render_binary_op(op: &BinaryOp) -> &'static str {
    match op {
        BinaryOp::Eq => "=",
        BinaryOp::Ne => "!=",
        BinaryOp::Lt => "<",
        BinaryOp::Le => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::Ge => ">=",
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::And => "AND",
        BinaryOp::Or => "OR",
        BinaryOp::Like => "LIKE",
        BinaryOp::ILike => "ILIKE",
    }
}

#[allow(clippy::too_many_arguments)]
fn compile_joined_select(
    dialect: &impl Dialect,
    table: &TableRef,
    columns: &[JoinedSelectColumn],
    joins: &[JoinSpec],
    filters: &[Filter],
    relationship_filters: &[JoinedFilter],
    order_by: &[OrderBy],
    relationship_order_by: &[JoinedOrderBy],
    limit: Option<usize>,
    offset: Option<usize>,
) -> OrmdanticResult<CompiledQuery> {
    require_joined_select_columns(columns)?;
    let mut bind_index = 1;
    let mut params = Vec::new();
    let selected = columns
        .iter()
        .map(|column| joined_selected_column(dialect, column))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!("SELECT {selected} FROM {}", quote_table_ref(dialect, table));
    for join in joins {
        sql.push_str(" LEFT JOIN ");
        sql.push_str(&quote_table_name(dialect, join.table()));
        sql.push_str(" AS ");
        sql.push_str(&dialect.quote_ident(join.alias()));
        sql.push_str(" ON ");
        sql.push_str(&qualified_alias_column(
            dialect,
            join.left_alias(),
            join.left_column(),
        ));
        sql.push_str(" = ");
        sql.push_str(&qualified_alias_column(
            dialect,
            join.right_alias(),
            join.right_column(),
        ));
        for filter in relationship_filters
            .iter()
            .filter(|filter| filter.table_alias() == join.alias())
        {
            sql.push_str(" AND ");
            sql.push_str(&render_filter(
                dialect,
                filter.filter(),
                &mut bind_index,
                &mut params,
                |column| qualified_alias_column(dialect, join.alias(), column),
            ));
        }
    }
    params.extend(append_filters_with_alias(
        dialect,
        &mut sql,
        filters,
        &mut bind_index,
        table.name(),
    ));
    append_joined_ordering(
        dialect,
        &mut sql,
        order_by,
        table.name(),
        relationship_order_by,
    );
    if let Some(limit) = limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    if let Some(offset) = offset {
        sql.push_str(&format!(" OFFSET {offset}"));
    }
    compiled_query(dialect, sql, params, QueryOperation::Select)
}

fn compile_select(
    dialect: &impl Dialect,
    table: &TableRef,
    columns: &[SelectColumn],
    filters: &[Filter],
    order_by: &[OrderBy],
    limit: Option<usize>,
    offset: Option<usize>,
) -> OrmdanticResult<CompiledQuery> {
    require_select_columns(columns)?;
    let mut bind_index = 1;
    let selected = columns
        .iter()
        .map(|column| selected_column(dialect, table, column))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!("SELECT {selected} FROM {}", quote_table_ref(dialect, table));
    let params = append_filters(dialect, &mut sql, filters, &mut bind_index);
    append_ordering(dialect, &mut sql, order_by);
    if let Some(limit) = limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    if let Some(offset) = offset {
        sql.push_str(&format!(" OFFSET {offset}"));
    }
    compiled_query(dialect, sql, params, QueryOperation::Select)
}

fn compile_count(
    dialect: &impl Dialect,
    table: &TableRef,
    filters: &[Filter],
) -> OrmdanticResult<CompiledQuery> {
    let mut bind_index = 1;
    let mut sql = format!("SELECT COUNT(*) FROM {}", quote_table_ref(dialect, table));
    let params = append_filters(dialect, &mut sql, filters, &mut bind_index);
    compiled_query(dialect, sql, params, QueryOperation::Count)
}

fn compile_insert(
    dialect: &impl Dialect,
    table: &TableRef,
    columns: &[String],
) -> OrmdanticResult<CompiledQuery> {
    require_columns(columns, "insert")?;
    let rendered_columns = column_list(dialect, columns);
    let placeholders = placeholders(dialect, 1, columns.len());
    compiled_query(
        dialect,
        format!(
            "INSERT INTO {} ({rendered_columns}) VALUES ({placeholders})",
            quote_table_ref(dialect, table)
        ),
        columns.to_vec(),
        QueryOperation::Insert,
    )
}

fn compile_update(
    dialect: &impl Dialect,
    table: &TableRef,
    columns: &[String],
    pk: &str,
) -> OrmdanticResult<CompiledQuery> {
    require_columns(columns, "update")?;
    let assignments = columns
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            format!(
                "{} = {}",
                dialect.quote_ident(column),
                dialect.placeholder(idx + 1)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let pk_placeholder = dialect.placeholder(columns.len() + 1);
    let mut params = columns.to_vec();
    params.push(pk.to_string());
    compiled_query(
        dialect,
        format!(
            "UPDATE {} SET {assignments} WHERE {} = {pk_placeholder}",
            quote_table_ref(dialect, table),
            dialect.quote_ident(pk)
        ),
        params,
        QueryOperation::Update,
    )
}

fn compile_upsert(
    dialect: &impl Dialect,
    table: &TableRef,
    columns: &[String],
    pk: &str,
) -> OrmdanticResult<CompiledQuery> {
    require_columns(columns, "upsert")?;
    let insert = compile_insert(dialect, table, columns)?;
    let update_columns = columns
        .iter()
        .filter(|column| column.as_str() != pk)
        .cloned()
        .collect::<Vec<_>>();
    compiled_query(
        dialect,
        format!(
            "{} {}",
            insert.sql(),
            dialect.upsert_conflict_clause(pk, &update_columns)
        ),
        columns.to_vec(),
        QueryOperation::Upsert,
    )
}

fn compile_delete(
    dialect: &impl Dialect,
    table: &TableRef,
    pk: &str,
) -> OrmdanticResult<CompiledQuery> {
    compiled_query(
        dialect,
        format!(
            "DELETE FROM {} WHERE {} = {}",
            quote_table_ref(dialect, table),
            dialect.quote_ident(pk),
            dialect.placeholder(1)
        ),
        vec![pk.to_string()],
        QueryOperation::Delete,
    )
}

fn append_filters(
    dialect: &impl Dialect,
    sql: &mut String,
    filters: &[Filter],
    bind_index: &mut usize,
) -> Vec<String> {
    if filters.is_empty() {
        return Vec::new();
    }

    let mut params = Vec::new();
    let rendered = filters
        .iter()
        .map(|filter| {
            render_filter(dialect, filter, bind_index, &mut params, |column| {
                dialect.quote_ident(column)
            })
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    sql.push_str(" WHERE ");
    sql.push_str(&rendered);
    params
}

fn append_ordering(dialect: &impl Dialect, sql: &mut String, order_by: &[OrderBy]) {
    if order_by.is_empty() {
        return;
    }

    let rendered = order_by
        .iter()
        .map(|order| {
            let direction = match order.direction() {
                SortDirection::Asc => "ASC",
                SortDirection::Desc => "DESC",
            };
            format!(
                "{} {direction}",
                render_order_column(dialect, dialect.quote_ident(order.column()), order)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    sql.push_str(" ORDER BY ");
    sql.push_str(&rendered);
}

fn append_filters_with_alias(
    dialect: &impl Dialect,
    sql: &mut String,
    filters: &[Filter],
    bind_index: &mut usize,
    table_alias: &str,
) -> Vec<String> {
    if filters.is_empty() {
        return Vec::new();
    }

    let mut params = Vec::new();
    let rendered = filters
        .iter()
        .map(|filter| {
            render_filter(dialect, filter, bind_index, &mut params, |column| {
                qualified_alias_column(dialect, table_alias, column)
            })
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    sql.push_str(" WHERE ");
    sql.push_str(&rendered);
    params
}

fn render_filter(
    dialect: &impl Dialect,
    filter: &Filter,
    bind_index: &mut usize,
    params: &mut Vec<String>,
    render_column: impl Fn(&str) -> String + Copy,
) -> String {
    let expression = PredicateExpr::from(filter);
    render_expression(dialect, &expression, bind_index, params, render_column)
}

fn render_expression(
    dialect: &impl Dialect,
    expression: &PredicateExpr,
    bind_index: &mut usize,
    params: &mut Vec<String>,
    render_column: impl Fn(&str) -> String + Copy,
) -> String {
    match expression {
        PredicateExpr::Compare { left, op, right } => render_comparison_expression(
            dialect,
            render_column(left.name()),
            op,
            right,
            bind_index,
            params,
        ),
        PredicateExpr::DecimalCompare { left, op, right } => render_decimal_comparison_expression(
            dialect,
            render_column(left.name()),
            op,
            right,
            bind_index,
            params,
        ),
        PredicateExpr::InList {
            left,
            params: names,
            negated,
        } => render_in_expression(
            dialect,
            render_column(left.name()),
            *negated,
            names,
            bind_index,
            params,
        ),
        PredicateExpr::DecimalInList {
            left,
            params: names,
            negated,
        } => render_decimal_in_expression(
            dialect,
            render_column(left.name()),
            *negated,
            names,
            bind_index,
            params,
        ),
        PredicateExpr::NullCheck { expr, negated } => {
            let operator = if *negated { "IS NOT NULL" } else { "IS NULL" };
            format!("{} {operator}", render_column(expr.name()))
        }
        PredicateExpr::Bool { op, exprs } => {
            render_bool_expression(dialect, op, exprs, bind_index, params, render_column)
        }
    }
}

fn render_comparison_expression(
    dialect: &impl Dialect,
    column: String,
    operator: &ComparisonOp,
    param: &BindParam,
    bind_index: &mut usize,
    params: &mut Vec<String>,
) -> String {
    params.push(param.name().to_string());
    let placeholder = dialect.placeholder(*bind_index);
    *bind_index += 1;
    if operator == &ComparisonOp::ILike {
        return format!("LOWER({column}) LIKE LOWER({placeholder})");
    }
    format!("{} {} {placeholder}", column, operator.sql_operator())
}

fn render_decimal_comparison_expression(
    dialect: &impl Dialect,
    column: String,
    operator: &ComparisonOp,
    param: &BindParam,
    bind_index: &mut usize,
    params: &mut Vec<String>,
) -> String {
    params.push(param.name().to_string());
    let placeholder = dialect.placeholder(*bind_index);
    *bind_index += 1;
    if dialect.kind() != DialectKind::Sqlite {
        return format!("{} {} {placeholder}", column, operator.sql_operator());
    }
    let comparison = format!("ormdantic_decimal_cmp({column}, {placeholder})");
    match operator {
        ComparisonOp::Eq => format!("{comparison} = 0"),
        ComparisonOp::Ne => format!("{comparison} != 0"),
        ComparisonOp::Lt => format!("{comparison} < 0"),
        ComparisonOp::Le => format!("{comparison} <= 0"),
        ComparisonOp::Gt => format!("{comparison} > 0"),
        ComparisonOp::Ge => format!("{comparison} >= 0"),
        ComparisonOp::Like | ComparisonOp::ILike => {
            format!("{} {} {placeholder}", column, operator.sql_operator())
        }
    }
}

fn render_in_expression(
    dialect: &impl Dialect,
    column: String,
    negated: bool,
    names: &[BindParam],
    bind_index: &mut usize,
    params: &mut Vec<String>,
) -> String {
    let operator = if negated { "NOT IN" } else { "IN" };
    if names.is_empty() {
        return if negated {
            "1 = 1".to_string()
        } else {
            "1 = 0".to_string()
        };
    }
    let placeholders = names
        .iter()
        .map(|name| {
            params.push(name.name().to_string());
            let placeholder = dialect.placeholder(*bind_index);
            *bind_index += 1;
            placeholder
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("{column} {operator} ({placeholders})")
}

fn render_decimal_in_expression(
    dialect: &impl Dialect,
    column: String,
    negated: bool,
    names: &[BindParam],
    bind_index: &mut usize,
    params: &mut Vec<String>,
) -> String {
    if dialect.kind() != DialectKind::Sqlite {
        return render_in_expression(dialect, column, negated, names, bind_index, params);
    }
    if names.is_empty() {
        return if negated {
            "1 = 1".to_string()
        } else {
            "1 = 0".to_string()
        };
    }
    let comparisons = names
        .iter()
        .map(|name| {
            params.push(name.name().to_string());
            let placeholder = dialect.placeholder(*bind_index);
            *bind_index += 1;
            format!("ormdantic_decimal_cmp({column}, {placeholder}) = 0")
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    if negated {
        format!("NOT ({comparisons})")
    } else {
        format!("({comparisons})")
    }
}

fn render_bool_expression(
    dialect: &impl Dialect,
    operator: &BoolOp,
    expressions: &[PredicateExpr],
    bind_index: &mut usize,
    params: &mut Vec<String>,
    render_column: impl Fn(&str) -> String + Copy,
) -> String {
    let rendered = expressions
        .iter()
        .map(|expression| render_expression(dialect, expression, bind_index, params, render_column))
        .collect::<Vec<_>>()
        .join(&format!(" {} ", operator.sql_operator()));
    format!("({rendered})")
}

fn append_joined_ordering(
    dialect: &impl Dialect,
    sql: &mut String,
    order_by: &[OrderBy],
    table_alias: &str,
    relationship_order_by: &[JoinedOrderBy],
) {
    if order_by.is_empty() && relationship_order_by.is_empty() {
        return;
    }

    let mut rendered = order_by
        .iter()
        .map(|order| render_order_for_alias(dialect, table_alias, order))
        .collect::<Vec<_>>();
    rendered.extend(
        relationship_order_by
            .iter()
            .map(|order| render_order_for_alias(dialect, order.table_alias(), order.order_by())),
    );
    sql.push_str(" ORDER BY ");
    sql.push_str(&rendered.join(", "));
}

fn render_order_for_alias(dialect: &impl Dialect, table_alias: &str, order: &OrderBy) -> String {
    let direction = match order.direction() {
        SortDirection::Asc => "ASC",
        SortDirection::Desc => "DESC",
    };
    format!(
        "{} {direction}",
        render_order_column(
            dialect,
            qualified_alias_column(dialect, table_alias, order.column()),
            order,
        )
    )
}

fn render_order_column(dialect: &impl Dialect, column: String, order: &OrderBy) -> String {
    if order.is_decimal() && dialect.kind() == DialectKind::Sqlite {
        format!("ormdantic_decimal_sort_key({column})")
    } else {
        column
    }
}

fn require_columns(columns: &[String], operation: &str) -> OrmdanticResult<()> {
    if columns.is_empty() {
        return Err(OrmdanticError::SqlCompile {
            message: format!("{operation} query requires at least one column"),
        });
    }
    Ok(())
}

fn require_select_columns(columns: &[SelectColumn]) -> OrmdanticResult<()> {
    if columns.is_empty() {
        return Err(OrmdanticError::SqlCompile {
            message: "select query requires at least one column".to_string(),
        });
    }
    Ok(())
}

fn require_joined_select_columns(columns: &[JoinedSelectColumn]) -> OrmdanticResult<()> {
    if columns.is_empty() {
        return Err(OrmdanticError::SqlCompile {
            message: "joined select query requires at least one column".to_string(),
        });
    }
    Ok(())
}

fn selected_column(dialect: &impl Dialect, table: &TableRef, column: &SelectColumn) -> String {
    let rendered = qualified_column(dialect, table, column.name());
    match column.alias() {
        Some(alias) => format!("{rendered} AS {}", dialect.quote_ident(alias)),
        None => rendered,
    }
}

fn qualified_column(dialect: &impl Dialect, table: &TableRef, column: &str) -> String {
    qualified_table_column(dialect, table.name(), column)
}

fn qualified_table_column(dialect: &impl Dialect, table: &str, column: &str) -> String {
    format!(
        "{}.{}",
        quote_table_name(dialect, table),
        dialect.quote_ident(column)
    )
}

fn qualified_alias_column(dialect: &impl Dialect, table_alias: &str, column: &str) -> String {
    format!(
        "{}.{}",
        quote_table_name(dialect, table_alias),
        dialect.quote_ident(column)
    )
}

fn quote_table_ref(dialect: &impl Dialect, table: &TableRef) -> String {
    quote_table_name(dialect, table.name())
}

fn quote_table_name(dialect: &impl Dialect, table: &str) -> String {
    table
        .split('.')
        .map(|part| dialect.quote_ident(part))
        .collect::<Vec<_>>()
        .join(".")
}

fn joined_selected_column(dialect: &impl Dialect, column: &JoinedSelectColumn) -> String {
    format!(
        "{} AS {}",
        qualified_alias_column(dialect, column.table_alias(), column.column()),
        dialect.quote_ident(column.alias())
    )
}

fn column_list(dialect: &impl Dialect, columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| dialect.quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ")
}

fn placeholders(dialect: &impl Dialect, start: usize, count: usize) -> String {
    (start..start + count)
        .map(|index| dialect.placeholder(index))
        .collect::<Vec<_>>()
        .join(", ")
}
