use ormdantic_schema::{
    CheckConstraintDef, ColumnDef, ConstraintDef, ForeignKeyAction, ForeignKeyDef, IndexDef,
    UniqueConstraintDef,
};

use crate::{Dialect, DialectKind};

pub(crate) fn compile_create_table(
    dialect: &(impl Dialect + ?Sized),
    table: &ormdantic_schema::TableDef,
) -> Vec<String> {
    let mut parts = table
        .columns()
        .iter()
        .map(|column| render_column_def(dialect, column))
        .collect::<Vec<_>>();

    for constraint in table.unique_constraints() {
        parts.push(render_unique_constraint(dialect, constraint));
    }
    for constraint in table.check_constraints() {
        parts.push(render_check_constraint(dialect, constraint));
    }
    for foreign_key in table.foreign_keys() {
        parts.push(render_foreign_key(dialect, foreign_key));
    }

    let mut statements = vec![format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        dialect.quote_ident(table.name()),
        parts.join(", ")
    )];
    for index in table.indexes() {
        statements.push(compile_create_index(dialect, table.name(), index));
    }
    statements
}

pub(crate) fn render_column_def(dialect: &(impl Dialect + ?Sized), column: &ColumnDef) -> String {
    let mut sql = format!(
        "{} {}",
        dialect.quote_ident(column.name()),
        dialect.render_column_type(column)
    );
    if column.is_primary_key() {
        sql.push_str(" PRIMARY KEY");
    }
    if !column.is_nullable() || column.is_primary_key() {
        sql.push_str(" NOT NULL");
    }
    if column.is_autoincrement() {
        sql.push_str(" AUTOINCREMENT");
    }
    if let Some(default) = column.server_default() {
        sql.push_str(" DEFAULT ");
        sql.push_str(default);
    }
    if let Some(collation) = column.collation() {
        sql.push_str(" COLLATE ");
        sql.push_str(collation);
    }
    if let Some(computed) = column.computed() {
        sql.push_str(" GENERATED ALWAYS AS (");
        sql.push_str(computed.expression());
        sql.push(')');
        if computed.is_persisted() {
            sql.push_str(" STORED");
        }
    }
    sql
}

pub(crate) fn compile_create_index(
    dialect: &(impl Dialect + ?Sized),
    table: &str,
    index: &IndexDef,
) -> String {
    let uniqueness = if index.is_unique() { "UNIQUE " } else { "" };
    let method = index
        .method_name()
        .map(|method| format!(" USING {method}"))
        .unwrap_or_default();
    let columns = index
        .columns()
        .iter()
        .map(|column| dialect.quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let include = if index.include_columns_ref().is_empty() {
        String::new()
    } else {
        format!(
            " INCLUDE ({})",
            index
                .include_columns_ref()
                .iter()
                .map(|column| dialect.quote_ident(column))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let predicate = index
        .predicate()
        .map(|predicate| format!(" WHERE {predicate}"))
        .unwrap_or_default();
    format!(
        "CREATE {uniqueness}INDEX IF NOT EXISTS {} ON {}{method} ({columns}){include}{predicate}",
        dialect.quote_ident(index.name()),
        dialect.quote_ident(table)
    )
}

pub(crate) fn compile_add_column(
    dialect: &(impl Dialect + ?Sized),
    table: &str,
    column: &ColumnDef,
) -> String {
    match dialect.kind() {
        DialectKind::MsSql => format!(
            "ALTER TABLE {} ADD {}",
            dialect.quote_ident(table),
            render_column_def(dialect, column)
        ),
        DialectKind::Oracle => format!(
            "ALTER TABLE {} ADD ({})",
            dialect.quote_ident(table),
            render_column_def(dialect, column)
        ),
        _ => format!(
            "ALTER TABLE {} ADD COLUMN {}",
            dialect.quote_ident(table),
            render_column_def(dialect, column)
        ),
    }
}

pub(crate) fn compile_drop_column(
    dialect: &(impl Dialect + ?Sized),
    table: &str,
    column: &str,
) -> String {
    format!(
        "ALTER TABLE {} DROP COLUMN {}",
        dialect.quote_ident(table),
        dialect.quote_ident(column)
    )
}

pub(crate) fn compile_alter_column(
    dialect: &(impl Dialect + ?Sized),
    table: &str,
    column: &ColumnDef,
) -> String {
    match dialect.kind() {
        DialectKind::MySql | DialectKind::MariaDb => format!(
            "ALTER TABLE {} MODIFY COLUMN {}",
            dialect.quote_ident(table),
            render_column_def(dialect, column)
        ),
        DialectKind::Oracle => format!(
            "ALTER TABLE {} MODIFY ({} {})",
            dialect.quote_ident(table),
            dialect.quote_ident(column.name()),
            dialect.render_column_type(column)
        ),
        _ => format!(
            "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
            dialect.quote_ident(table),
            dialect.quote_ident(column.name()),
            dialect.render_column_type(column)
        ),
    }
}

pub(crate) fn compile_drop_index(
    dialect: &(impl Dialect + ?Sized),
    table: &str,
    name: &str,
) -> String {
    match dialect.kind() {
        DialectKind::MySql | DialectKind::MariaDb | DialectKind::MsSql => format!(
            "DROP INDEX {} ON {}",
            dialect.quote_ident(name),
            dialect.quote_ident(table)
        ),
        DialectKind::Oracle => format!("DROP INDEX {}", dialect.quote_ident(name)),
        _ => format!("DROP INDEX IF EXISTS {}", dialect.quote_ident(name)),
    }
}

pub(crate) fn render_constraint(
    dialect: &(impl Dialect + ?Sized),
    constraint: &ConstraintDef,
) -> String {
    match constraint {
        ConstraintDef::Unique(constraint) => render_unique_constraint(dialect, constraint),
        ConstraintDef::Check(constraint) => render_check_constraint(dialect, constraint),
        ConstraintDef::ForeignKey(constraint) => render_foreign_key(dialect, constraint),
    }
}

fn render_unique_constraint(
    dialect: &(impl Dialect + ?Sized),
    constraint: &UniqueConstraintDef,
) -> String {
    let columns = constraint
        .columns()
        .iter()
        .map(|column| dialect.quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CONSTRAINT {} UNIQUE ({columns})",
        dialect.quote_ident(constraint.name())
    )
}

fn render_check_constraint(
    dialect: &(impl Dialect + ?Sized),
    constraint: &CheckConstraintDef,
) -> String {
    match constraint.name() {
        Some(name) => format!(
            "CONSTRAINT {} CHECK ({})",
            dialect.quote_ident(name),
            constraint.expression()
        ),
        None => format!("CHECK ({})", constraint.expression()),
    }
}

fn render_foreign_key(dialect: &(impl Dialect + ?Sized), foreign_key: &ForeignKeyDef) -> String {
    let local_columns = foreign_key
        .local_columns()
        .iter()
        .map(|column| dialect.quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let remote_columns = foreign_key
        .remote_columns()
        .iter()
        .map(|column| dialect.quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = String::new();
    if let Some(name) = foreign_key.name() {
        sql.push_str("CONSTRAINT ");
        sql.push_str(&dialect.quote_ident(name));
        sql.push(' ');
    }
    sql.push_str(&format!(
        "FOREIGN KEY ({local_columns}) REFERENCES {} ({remote_columns})",
        dialect.quote_ident(foreign_key.remote_table())
    ));
    if let Some(action) = foreign_key.on_delete_action() {
        sql.push_str(" ON DELETE ");
        sql.push_str(render_foreign_key_action(action));
    }
    if let Some(action) = foreign_key.on_update_action() {
        sql.push_str(" ON UPDATE ");
        sql.push_str(render_foreign_key_action(action));
    }
    sql
}

fn render_foreign_key_action(action: &ForeignKeyAction) -> &'static str {
    match action {
        ForeignKeyAction::Cascade => "CASCADE",
        ForeignKeyAction::Restrict => "RESTRICT",
        ForeignKeyAction::SetNull => "SET NULL",
        ForeignKeyAction::SetDefault => "SET DEFAULT",
        ForeignKeyAction::NoAction => "NO ACTION",
    }
}
