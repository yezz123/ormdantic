use ormdantic_dialects::{AnyDialect, PostgresDialect, SqliteDialect};
use ormdantic_sql::{Filter, QueryAst, SelectColumn, TableRef};

#[test]
fn compiles_flat_select_with_aliases() {
    let query = QueryAst::Select {
        table: TableRef::new("flavors"),
        columns: vec![
            SelectColumn::aliased("id", "flavors\\id"),
            SelectColumn::aliased("name", "flavors\\name"),
        ],
        filters: vec![Filter::Eq {
            column: "id".to_string(),
            param: "id".to_string(),
        }],
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }
    .compile(&PostgresDialect)
    .expect("select should compile");

    assert_eq!(
        query.sql(),
        "SELECT \"flavors\".\"id\" AS \"flavors\\id\", \"flavors\".\"name\" AS \"flavors\\name\" FROM \"flavors\" WHERE \"id\" = $1"
    );
}

#[test]
fn compiles_mysql_insert_for_driver_sql() {
    let dialect = AnyDialect::parse("mysql+pymysql://localhost/db").unwrap();
    let query = QueryAst::Insert {
        table: TableRef::new("flavors"),
        columns: vec!["id".to_string(), "name".to_string()],
    }
    .compile(&dialect)
    .expect("insert should compile");

    assert_eq!(
        query.sql(),
        "INSERT INTO `flavors` (`id`, `name`) VALUES (?, ?)"
    );
}

#[test]
fn compiles_schema_qualified_table_refs() {
    let select = QueryAst::Select {
        table: TableRef::new("inventory.flavors"),
        columns: vec![SelectColumn::aliased("id", "flavors\\id")],
        filters: vec![Filter::Eq {
            column: "id".to_string(),
            param: "id".to_string(),
        }],
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }
    .compile(&PostgresDialect)
    .expect("schema-qualified select should compile");
    let insert = QueryAst::Insert {
        table: TableRef::new("inventory.flavors"),
        columns: vec!["id".to_string(), "name".to_string()],
    }
    .compile(&PostgresDialect)
    .expect("schema-qualified insert should compile");

    assert_eq!(
        select.sql(),
        "SELECT \"inventory\".\"flavors\".\"id\" AS \"flavors\\id\" FROM \"inventory\".\"flavors\" WHERE \"id\" = $1"
    );
    assert_eq!(
        insert.sql(),
        "INSERT INTO \"inventory\".\"flavors\" (\"id\", \"name\") VALUES ($1, $2)"
    );
}

#[test]
fn compiles_sqlite_delete() {
    let query = QueryAst::Delete {
        table: TableRef::new("flavors"),
        pk: "id".to_string(),
    }
    .compile(&SqliteDialect)
    .expect("delete should compile");

    assert_eq!(query.sql(), "DELETE FROM \"flavors\" WHERE \"id\" = ?");
}
