use ormdantic_core::{OrmdanticError, OrmdanticResult};
use ormdantic_dialects::Dialect;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryOperation {
    Select,
    Insert,
    Update,
    Upsert,
    Delete,
    Count,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledQuery {
    sql: String,
    params: Vec<String>,
    operation: QueryOperation,
}

impl CompiledQuery {
    pub fn new(sql: String, params: Vec<String>, operation: QueryOperation) -> Self {
        Self {
            sql,
            params,
            operation,
        }
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn params(&self) -> &[String] {
        &self.params
    }

    pub fn operation(&self) -> &QueryOperation {
        &self.operation
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    name: String,
}

impl TableRef {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectColumn {
    name: String,
    alias: Option<String>,
}

impl SelectColumn {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            alias: None,
        }
    }

    pub fn aliased(name: impl Into<String>, alias: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            alias: Some(alias.into()),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Filter {
    Eq { column: String, param: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBy {
    column: String,
    direction: SortDirection,
}

impl OrderBy {
    pub fn new(column: impl Into<String>, direction: SortDirection) -> Self {
        Self {
            column: column.into(),
            direction,
        }
    }

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn direction(&self) -> &SortDirection {
        &self.direction
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryAst {
    Select {
        table: TableRef,
        columns: Vec<SelectColumn>,
        filters: Vec<Filter>,
        order_by: Vec<OrderBy>,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    Count {
        table: TableRef,
        filters: Vec<Filter>,
    },
    Insert {
        table: TableRef,
        columns: Vec<String>,
    },
    Update {
        table: TableRef,
        columns: Vec<String>,
        pk: String,
    },
    Upsert {
        table: TableRef,
        columns: Vec<String>,
        pk: String,
    },
    Delete {
        table: TableRef,
        pk: String,
    },
}

impl QueryAst {
    pub fn compile(&self, dialect: &impl Dialect) -> OrmdanticResult<CompiledQuery> {
        match self {
            Self::Select {
                table,
                columns,
                filters,
                order_by,
                limit,
                offset,
            } => compile_select(dialect, table, columns, filters, order_by, *limit, *offset),
            Self::Count { table, filters } => compile_count(dialect, table, filters),
            Self::Insert { table, columns } => compile_insert(dialect, table, columns),
            Self::Update { table, columns, pk } => compile_update(dialect, table, columns, pk),
            Self::Upsert { table, columns, pk } => compile_upsert(dialect, table, columns, pk),
            Self::Delete { table, pk } => compile_delete(dialect, table, pk),
        }
    }
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
    let mut sql = format!(
        "SELECT {selected} FROM {}",
        dialect.quote_ident(table.name())
    );
    let params = append_filters(dialect, &mut sql, filters, &mut bind_index);
    append_ordering(dialect, &mut sql, order_by);
    if let Some(limit) = limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    if let Some(offset) = offset {
        sql.push_str(&format!(" OFFSET {offset}"));
    }
    Ok(CompiledQuery::new(sql, params, QueryOperation::Select))
}

fn compile_count(
    dialect: &impl Dialect,
    table: &TableRef,
    filters: &[Filter],
) -> OrmdanticResult<CompiledQuery> {
    let mut bind_index = 1;
    let mut sql = format!("SELECT COUNT(*) FROM {}", dialect.quote_ident(table.name()));
    let params = append_filters(dialect, &mut sql, filters, &mut bind_index);
    Ok(CompiledQuery::new(sql, params, QueryOperation::Count))
}

fn compile_insert(
    dialect: &impl Dialect,
    table: &TableRef,
    columns: &[String],
) -> OrmdanticResult<CompiledQuery> {
    require_columns(columns, "insert")?;
    let rendered_columns = column_list(dialect, columns);
    let placeholders = placeholders(dialect, 1, columns.len());
    Ok(CompiledQuery::new(
        format!(
            "INSERT INTO {} ({rendered_columns}) VALUES ({placeholders})",
            dialect.quote_ident(table.name())
        ),
        columns.to_vec(),
        QueryOperation::Insert,
    ))
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
    Ok(CompiledQuery::new(
        format!(
            "UPDATE {} SET {assignments} WHERE {} = {pk_placeholder}",
            dialect.quote_ident(table.name()),
            dialect.quote_ident(pk)
        ),
        params,
        QueryOperation::Update,
    ))
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
    Ok(CompiledQuery::new(
        format!(
            "{} {}",
            insert.sql(),
            dialect.upsert_conflict_clause(pk, &update_columns)
        ),
        columns.to_vec(),
        QueryOperation::Upsert,
    ))
}

fn compile_delete(
    dialect: &impl Dialect,
    table: &TableRef,
    pk: &str,
) -> OrmdanticResult<CompiledQuery> {
    Ok(CompiledQuery::new(
        format!(
            "DELETE FROM {} WHERE {} = {}",
            dialect.quote_ident(table.name()),
            dialect.quote_ident(pk),
            dialect.placeholder(1)
        ),
        vec![pk.to_string()],
        QueryOperation::Delete,
    ))
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

    let mut params = Vec::with_capacity(filters.len());
    let rendered = filters
        .iter()
        .map(|filter| match filter {
            Filter::Eq { column, param } => {
                params.push(param.clone());
                let placeholder = dialect.placeholder(*bind_index);
                *bind_index += 1;
                format!("{} = {placeholder}", dialect.quote_ident(column))
            }
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
            format!("{} {direction}", dialect.quote_ident(order.column()))
        })
        .collect::<Vec<_>>()
        .join(", ");
    sql.push_str(" ORDER BY ");
    sql.push_str(&rendered);
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

fn selected_column(dialect: &impl Dialect, table: &TableRef, column: &SelectColumn) -> String {
    let rendered = qualified_column(dialect, table, column.name());
    match column.alias() {
        Some(alias) => format!("{rendered} AS {}", dialect.quote_ident(alias)),
        None => rendered,
    }
}

fn qualified_column(dialect: &impl Dialect, table: &TableRef, column: &str) -> String {
    format!(
        "{}.{}",
        dialect.quote_ident(table.name()),
        dialect.quote_ident(column)
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

#[cfg(test)]
mod tests {
    use super::{Filter, OrderBy, QueryAst, QueryOperation, SelectColumn, SortDirection, TableRef};
    use ormdantic_dialects::{PostgresDialect, SqliteDialect};

    #[test]
    fn compiles_select_for_sqlite() {
        let query = QueryAst::Select {
            table: TableRef::new("flavors"),
            columns: vec![SelectColumn::new("id"), SelectColumn::new("name")],
            filters: vec![Filter::Eq {
                column: "id".to_string(),
                param: "id".to_string(),
            }],
            order_by: vec![],
            limit: Some(10),
            offset: Some(20),
        }
        .compile(&SqliteDialect)
        .expect("query should compile");

        assert_eq!(
            query.sql(),
            "SELECT \"flavors\".\"id\", \"flavors\".\"name\" FROM \"flavors\" WHERE \"id\" = ? LIMIT 10 OFFSET 20"
        );
        assert_eq!(query.params(), &["id".to_string()]);
        assert_eq!(query.operation(), &QueryOperation::Select);
    }

    #[test]
    fn compiles_select_with_aliases_and_ordering_for_postgres() {
        let query = QueryAst::Select {
            table: TableRef::new("flavors"),
            columns: vec![
                SelectColumn::aliased("id", "flavors\\id"),
                SelectColumn::aliased("name", "flavors\\name"),
            ],
            filters: vec![Filter::Eq {
                column: "name".to_string(),
                param: "name".to_string(),
            }],
            order_by: vec![OrderBy::new("name", SortDirection::Desc)],
            limit: Some(5),
            offset: None,
        }
        .compile(&PostgresDialect)
        .expect("query should compile");

        assert_eq!(
            query.sql(),
            "SELECT \"flavors\".\"id\" AS \"flavors\\id\", \"flavors\".\"name\" AS \"flavors\\name\" FROM \"flavors\" WHERE \"name\" = $1 ORDER BY \"name\" DESC LIMIT 5"
        );
        assert_eq!(query.params(), &["name".to_string()]);
    }

    #[test]
    fn compiles_count_for_sqlite() {
        let query = QueryAst::Count {
            table: TableRef::new("flavors"),
            filters: vec![Filter::Eq {
                column: "name".to_string(),
                param: "name".to_string(),
            }],
        }
        .compile(&SqliteDialect)
        .expect("query should compile");

        assert_eq!(
            query.sql(),
            "SELECT COUNT(*) FROM \"flavors\" WHERE \"name\" = ?"
        );
        assert_eq!(query.params(), &["name".to_string()]);
        assert_eq!(query.operation(), &QueryOperation::Count);
    }

    #[test]
    fn compiles_insert_for_postgres() {
        let query = QueryAst::Insert {
            table: TableRef::new("flavors"),
            columns: vec!["id".to_string(), "name".to_string()],
        }
        .compile(&PostgresDialect)
        .expect("query should compile");

        assert_eq!(
            query.sql(),
            "INSERT INTO \"flavors\" (\"id\", \"name\") VALUES ($1, $2)"
        );
        assert_eq!(query.params(), &["id".to_string(), "name".to_string()]);
    }

    #[test]
    fn compiles_upsert_for_postgres() {
        let query = QueryAst::Upsert {
            table: TableRef::new("flavors"),
            columns: vec!["id".to_string(), "name".to_string()],
            pk: "id".to_string(),
        }
        .compile(&PostgresDialect)
        .expect("query should compile");

        assert_eq!(
            query.sql(),
            "INSERT INTO \"flavors\" (\"id\", \"name\") VALUES ($1, $2) ON CONFLICT (\"id\") DO UPDATE SET \"name\" = excluded.\"name\""
        );
    }

    #[test]
    fn compiles_pk_only_upsert_as_do_nothing() {
        let query = QueryAst::Upsert {
            table: TableRef::new("flavors"),
            columns: vec!["id".to_string()],
            pk: "id".to_string(),
        }
        .compile(&SqliteDialect)
        .expect("query should compile");

        assert_eq!(
            query.sql(),
            "INSERT INTO \"flavors\" (\"id\") VALUES (?) ON CONFLICT (\"id\") DO NOTHING"
        );
    }

    #[test]
    fn compiles_update_for_postgres() {
        let query = QueryAst::Update {
            table: TableRef::new("flavors"),
            columns: vec!["name".to_string(), "strength".to_string()],
            pk: "id".to_string(),
        }
        .compile(&PostgresDialect)
        .expect("query should compile");

        assert_eq!(
            query.sql(),
            "UPDATE \"flavors\" SET \"name\" = $1, \"strength\" = $2 WHERE \"id\" = $3"
        );
        assert_eq!(
            query.params(),
            &["name".to_string(), "strength".to_string(), "id".to_string()]
        );
    }

    #[test]
    fn compiles_delete_for_sqlite() {
        let query = QueryAst::Delete {
            table: TableRef::new("flavors"),
            pk: "id".to_string(),
        }
        .compile(&SqliteDialect)
        .expect("query should compile");

        assert_eq!(query.sql(), "DELETE FROM \"flavors\" WHERE \"id\" = ?");
        assert_eq!(query.params(), &["id".to_string()]);
    }
}
