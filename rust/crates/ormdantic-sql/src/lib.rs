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
    Ne { column: String, param: String },
    Lt { column: String, param: String },
    Le { column: String, param: String },
    Gt { column: String, param: String },
    Ge { column: String, param: String },
    Like { column: String, param: String },
    ILike { column: String, param: String },
    In { column: String, params: Vec<String> },
    NotIn { column: String, params: Vec<String> },
    IsNull { column: String },
    IsNotNull { column: String },
    And(Vec<Filter>),
    Or(Vec<Filter>),
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
pub struct JoinSpec {
    table: String,
    alias: String,
    left_alias: String,
    left_column: String,
    right_alias: String,
    right_column: String,
}

impl JoinSpec {
    pub fn left_join(
        table: impl Into<String>,
        alias: impl Into<String>,
        left_alias: impl Into<String>,
        left_column: impl Into<String>,
        right_alias: impl Into<String>,
        right_column: impl Into<String>,
    ) -> Self {
        Self {
            table: table.into(),
            alias: alias.into(),
            left_alias: left_alias.into(),
            left_column: left_column.into(),
            right_alias: right_alias.into(),
            right_column: right_column.into(),
        }
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }

    pub fn left_alias(&self) -> &str {
        &self.left_alias
    }

    pub fn left_column(&self) -> &str {
        &self.left_column
    }

    pub fn right_alias(&self) -> &str {
        &self.right_alias
    }

    pub fn right_column(&self) -> &str {
        &self.right_column
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinedSelectColumn {
    table_alias: String,
    column: String,
    alias: String,
}

impl JoinedSelectColumn {
    pub fn aliased(
        table_alias: impl Into<String>,
        column: impl Into<String>,
        alias: impl Into<String>,
    ) -> Self {
        Self {
            table_alias: table_alias.into(),
            column: column.into(),
            alias: alias.into(),
        }
    }

    pub fn table_alias(&self) -> &str {
        &self.table_alias
    }

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn alias(&self) -> &str {
        &self.alias
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
    JoinedSelect {
        table: TableRef,
        columns: Vec<JoinedSelectColumn>,
        joins: Vec<JoinSpec>,
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
            Self::JoinedSelect {
                table,
                columns,
                joins,
                filters,
                order_by,
                limit,
                offset,
            } => compile_joined_select(
                dialect, table, columns, joins, filters, order_by, *limit, *offset,
            ),
            Self::Count { table, filters } => compile_count(dialect, table, filters),
            Self::Insert { table, columns } => compile_insert(dialect, table, columns),
            Self::Update { table, columns, pk } => compile_update(dialect, table, columns, pk),
            Self::Upsert { table, columns, pk } => compile_upsert(dialect, table, columns, pk),
            Self::Delete { table, pk } => compile_delete(dialect, table, pk),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn compile_joined_select(
    dialect: &impl Dialect,
    table: &TableRef,
    columns: &[JoinedSelectColumn],
    joins: &[JoinSpec],
    filters: &[Filter],
    order_by: &[OrderBy],
    limit: Option<usize>,
    offset: Option<usize>,
) -> OrmdanticResult<CompiledQuery> {
    require_joined_select_columns(columns)?;
    let mut bind_index = 1;
    let selected = columns
        .iter()
        .map(|column| joined_selected_column(dialect, column))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!(
        "SELECT {selected} FROM {}",
        dialect.quote_ident(table.name())
    );
    for join in joins {
        sql.push_str(" LEFT JOIN ");
        sql.push_str(&dialect.quote_ident(join.table()));
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
    }
    let params =
        append_filters_with_alias(dialect, &mut sql, filters, &mut bind_index, table.name());
    append_ordering_with_alias(dialect, &mut sql, order_by, table.name());
    if let Some(limit) = limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    if let Some(offset) = offset {
        sql.push_str(&format!(" OFFSET {offset}"));
    }
    Ok(CompiledQuery::new(sql, params, QueryOperation::Select))
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
            format!("{} {direction}", dialect.quote_ident(order.column()))
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
    match filter {
        Filter::Eq { column, param } => render_binary_filter(
            dialect,
            render_column(column),
            "=",
            param,
            bind_index,
            params,
        ),
        Filter::Ne { column, param } => render_binary_filter(
            dialect,
            render_column(column),
            "!=",
            param,
            bind_index,
            params,
        ),
        Filter::Lt { column, param } => render_binary_filter(
            dialect,
            render_column(column),
            "<",
            param,
            bind_index,
            params,
        ),
        Filter::Le { column, param } => render_binary_filter(
            dialect,
            render_column(column),
            "<=",
            param,
            bind_index,
            params,
        ),
        Filter::Gt { column, param } => render_binary_filter(
            dialect,
            render_column(column),
            ">",
            param,
            bind_index,
            params,
        ),
        Filter::Ge { column, param } => render_binary_filter(
            dialect,
            render_column(column),
            ">=",
            param,
            bind_index,
            params,
        ),
        Filter::Like { column, param } => render_binary_filter(
            dialect,
            render_column(column),
            "LIKE",
            param,
            bind_index,
            params,
        ),
        Filter::ILike { column, param } => render_binary_filter(
            dialect,
            format!("LOWER({})", render_column(column)),
            "LIKE",
            param,
            bind_index,
            params,
        ),
        Filter::In {
            column,
            params: names,
        } => render_in_filter(
            dialect,
            render_column(column),
            "IN",
            names,
            bind_index,
            params,
        ),
        Filter::NotIn {
            column,
            params: names,
        } => render_in_filter(
            dialect,
            render_column(column),
            "NOT IN",
            names,
            bind_index,
            params,
        ),
        Filter::IsNull { column } => format!("{} IS NULL", render_column(column)),
        Filter::IsNotNull { column } => format!("{} IS NOT NULL", render_column(column)),
        Filter::And(filters) => {
            render_filter_group(dialect, "AND", filters, bind_index, params, render_column)
        }
        Filter::Or(filters) => {
            render_filter_group(dialect, "OR", filters, bind_index, params, render_column)
        }
    }
}

fn render_binary_filter(
    dialect: &impl Dialect,
    column: String,
    operator: &str,
    param: &str,
    bind_index: &mut usize,
    params: &mut Vec<String>,
) -> String {
    params.push(param.to_string());
    let placeholder = dialect.placeholder(*bind_index);
    *bind_index += 1;
    format!("{column} {operator} {placeholder}")
}

fn render_in_filter(
    dialect: &impl Dialect,
    column: String,
    operator: &str,
    names: &[String],
    bind_index: &mut usize,
    params: &mut Vec<String>,
) -> String {
    if names.is_empty() {
        return if operator == "IN" {
            "1 = 0".to_string()
        } else {
            "1 = 1".to_string()
        };
    }
    let placeholders = names
        .iter()
        .map(|name| {
            params.push(name.clone());
            let placeholder = dialect.placeholder(*bind_index);
            *bind_index += 1;
            placeholder
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("{column} {operator} ({placeholders})")
}

fn render_filter_group(
    dialect: &impl Dialect,
    operator: &str,
    filters: &[Filter],
    bind_index: &mut usize,
    params: &mut Vec<String>,
    render_column: impl Fn(&str) -> String + Copy,
) -> String {
    let rendered = filters
        .iter()
        .map(|filter| render_filter(dialect, filter, bind_index, params, render_column))
        .collect::<Vec<_>>()
        .join(&format!(" {operator} "));
    format!("({rendered})")
}

fn append_ordering_with_alias(
    dialect: &impl Dialect,
    sql: &mut String,
    order_by: &[OrderBy],
    table_alias: &str,
) {
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
                qualified_alias_column(dialect, table_alias, order.column())
            )
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
    format!(
        "{}.{}",
        dialect.quote_ident(table.name()),
        dialect.quote_ident(column)
    )
}

fn qualified_alias_column(dialect: &impl Dialect, table_alias: &str, column: &str) -> String {
    format!(
        "{}.{}",
        dialect.quote_ident(table_alias),
        dialect.quote_ident(column)
    )
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

#[cfg(test)]
mod tests {
    use super::{
        Filter, JoinSpec, JoinedSelectColumn, OrderBy, QueryAst, QueryOperation, SelectColumn,
        SortDirection, TableRef,
    };
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
    fn compiles_joined_select_for_relationships() {
        let query = QueryAst::JoinedSelect {
            table: TableRef::new("coffee"),
            columns: vec![
                JoinedSelectColumn::aliased("coffee", "id", "coffee\\id"),
                JoinedSelectColumn::aliased("coffee/flavor", "id", "coffee/flavor\\id"),
                JoinedSelectColumn::aliased("coffee/flavor", "name", "coffee/flavor\\name"),
            ],
            joins: vec![JoinSpec::left_join(
                "flavors",
                "coffee/flavor",
                "coffee",
                "flavor",
                "coffee/flavor",
                "id",
            )],
            filters: vec![Filter::Eq {
                column: "id".to_string(),
                param: "id".to_string(),
            }],
            order_by: Vec::new(),
            limit: None,
            offset: None,
        }
        .compile(&SqliteDialect)
        .expect("joined select should compile");

        assert_eq!(
            query.sql(),
            "SELECT \"coffee\".\"id\" AS \"coffee\\id\", \"coffee/flavor\".\"id\" AS \"coffee/flavor\\id\", \"coffee/flavor\".\"name\" AS \"coffee/flavor\\name\" FROM \"coffee\" LEFT JOIN \"flavors\" AS \"coffee/flavor\" ON \"coffee\".\"flavor\" = \"coffee/flavor\".\"id\" WHERE \"coffee\".\"id\" = ?"
        );
        assert_eq!(query.params(), &["id".to_string()]);
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
