use ormdantic_dialects::{AnyDialect, Dialect, PostgresDialect};
use ormdantic_schema::{
    CheckConstraintDef, ColumnDef, ConstraintDef, FieldKind, ForeignKeyAction, ForeignKeyDef,
    IndexDef, SchemaOperation, TableDef, UniqueConstraintDef,
};

fn sample_table() -> TableDef {
    TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![
            ColumnDef::new("id", FieldKind::Integer)
                .primary_key(true)
                .autoincrement(true),
            ColumnDef::new("name", FieldKind::String)
                .with_server_default("'vanilla'")
                .with_collation("NOCASE"),
            ColumnDef::new("supplier_id", FieldKind::Integer).nullable(true),
            ColumnDef::new("rating", FieldKind::Decimal).numeric(5, 2),
        ],
        vec![IndexDef::new("flavor_name_idx", vec!["name".to_string()])
            .unique(true)
            .method("btree")
            .include_columns(vec!["rating".to_string()])
            .where_expr("name IS NOT NULL")],
        vec![UniqueConstraintDef::new(
            "flavor_name_unique",
            vec!["name".to_string()],
        )],
        Vec::new(),
    )
    .with_check_constraints(vec![
        CheckConstraintDef::new("rating >= 0").named("flavor_rating_check")
    ])
    .with_foreign_keys(vec![ForeignKeyDef::new(
        vec!["supplier_id".to_string()],
        "supplier",
        vec!["id".to_string()],
    )
    .named("flavor_supplier_fk")
    .on_delete(ForeignKeyAction::SetNull)
    .on_update(ForeignKeyAction::Cascade)])
}

#[test]
fn renders_create_table_with_constraints_and_indexes() {
    let statements = PostgresDialect
        .compile_schema_operation(&SchemaOperation::CreateTable(sample_table()))
        .unwrap();

    assert_eq!(
        statements,
        vec![
            r#"CREATE TABLE IF NOT EXISTS "flavor" ("id" INTEGER PRIMARY KEY NOT NULL AUTOINCREMENT, "name" TEXT NOT NULL DEFAULT 'vanilla' COLLATE NOCASE, "supplier_id" INTEGER, "rating" NUMERIC(5, 2) NOT NULL, CONSTRAINT "flavor_name_unique" UNIQUE ("name"), CONSTRAINT "flavor_rating_check" CHECK (rating >= 0), CONSTRAINT "flavor_supplier_fk" FOREIGN KEY ("supplier_id") REFERENCES "supplier" ("id") ON DELETE SET NULL ON UPDATE CASCADE)"#.to_string(),
            r#"CREATE UNIQUE INDEX IF NOT EXISTS "flavor_name_idx" ON "flavor" USING btree ("name") INCLUDE ("rating") WHERE name IS NOT NULL"#.to_string(),
        ]
    );
}

#[test]
fn renders_alter_column_by_backend() {
    let operation = SchemaOperation::AlterColumn {
        table: "flavor".to_string(),
        column: ColumnDef::new("rating", FieldKind::Integer).nullable(true),
    };
    let cases = [
        (
            "postgresql",
            r#"ALTER TABLE "flavor" ALTER COLUMN "rating" TYPE INTEGER"#,
        ),
        (
            "mysql",
            "ALTER TABLE `flavor` MODIFY COLUMN `rating` INTEGER",
        ),
        (
            "mariadb",
            "ALTER TABLE `flavor` MODIFY COLUMN `rating` INTEGER",
        ),
        (
            "mssql",
            "ALTER TABLE [flavor] ALTER COLUMN [rating] TYPE INTEGER",
        ),
        (
            "oracle",
            r#"ALTER TABLE "flavor" MODIFY ("rating" INTEGER)"#,
        ),
    ];

    for (dialect, expected) in cases {
        let sql = AnyDialect::parse(dialect)
            .unwrap()
            .compile_schema_operation(&operation)
            .unwrap();
        assert_eq!(sql, vec![expected.to_string()]);
    }
}

#[test]
fn renders_drop_index_by_backend() {
    let operation = SchemaOperation::DropIndex {
        table: "flavor".to_string(),
        name: "flavor_name_idx".to_string(),
    };
    let cases = [
        ("sqlite", r#"DROP INDEX IF EXISTS "flavor_name_idx""#),
        ("postgresql", r#"DROP INDEX IF EXISTS "flavor_name_idx""#),
        ("mysql", "DROP INDEX `flavor_name_idx` ON `flavor`"),
        ("mariadb", "DROP INDEX `flavor_name_idx` ON `flavor`"),
        ("mssql", "DROP INDEX [flavor_name_idx] ON [flavor]"),
        ("oracle", r#"DROP INDEX "flavor_name_idx""#),
    ];

    for (dialect, expected) in cases {
        let sql = AnyDialect::parse(dialect)
            .unwrap()
            .compile_schema_operation(&operation)
            .unwrap();
        assert_eq!(sql, vec![expected.to_string()]);
    }
}

#[test]
fn renders_add_and_drop_constraints() {
    let add = PostgresDialect
        .compile_schema_operation(&SchemaOperation::AddConstraint {
            table: "flavor".to_string(),
            constraint: ConstraintDef::Check(
                CheckConstraintDef::new("rating >= 0").named("rating_check"),
            ),
        })
        .unwrap();
    let drop = PostgresDialect
        .compile_schema_operation(&SchemaOperation::DropConstraint {
            table: "flavor".to_string(),
            name: "rating_check".to_string(),
        })
        .unwrap();

    assert_eq!(
        add,
        vec![
            r#"ALTER TABLE "flavor" ADD CONSTRAINT "rating_check" CHECK (rating >= 0)"#.to_string()
        ]
    );
    assert_eq!(
        drop,
        vec![r#"ALTER TABLE "flavor" DROP CONSTRAINT "rating_check""#.to_string()]
    );
}
