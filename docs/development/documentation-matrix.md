# Documentation completeness matrix

This inventory maps every public top-level export, CLI workflow, driver, and
Playground workflow to one canonical documentation location. Use it to find the
authoritative behavior and to spot documentation gaps when the public API changes.

## Python public surface

| Inventory key | Covered public names | Canonical documentation | Lifecycle and limits |
| --- | --- | --- | --- |
| `public:Ormdantic` | `Ormdantic` | [Ormdantic API](../api/ormdantic.md) | Register models before `init()`. Initialization creates registered tables; use migrations for controlled production changes. |
| `public:Table` | `Table`, `Order` | [Table API](../api/table.md) | Table handles are available after initialization. Result ordering is only stable when requested explicitly. |
| `public:metadata` | `TableColumn`, `TableExclusion`, `DatabaseNamespace`, `DatabaseSequence`, `DatabaseView`, `TableIndex`, `TableCheck`, `TableUnique`, `TableForeignKey` | [Metadata models](../api/metadata.md) | Backend support differs; review the destination driver before using storage-specific metadata. |
| `public:errors` | `OrmdanticError`, `ConfigurationError`, `QueryCompilationError`, `QueryExecutionError`, `DatabaseConnectionError`, `SchemaError`, `MigrationError`, `ReflectionError`, `RelationshipLoadingError`, `HydrationError`, `TransactionError`, `NativeExtensionError`, `UndefinedBackReferenceError`, `MismatchingBackReferenceError`, `MustUnionForeignKeyError`, `TypeConversionError` | [Error API](../api/errors.md) | Exceptions include safe structured context, but applications remain responsible for client-safe message mapping. |
| `public:events` | `EventRegistry` | [Events](../api/events.md) | Async handlers run in the operation lifecycle. A failing handler can fail the operation; keep handlers bounded. |
| `public:QueryExpression` | `QueryExpression`, `column`, `projection`, `assignment`, `select_query`, `update_query`, `case`, `cast`, `tuple_`, `cte`, `count`, `sum`, `avg`, `min`, `max`, `exists`, `not_exists`, `not_`, `group`, `over`, `subquery`, `literal`, `raw_sql_safe` | [Query expressions](../api/expressions.md) | Prefer typed builders. `raw_sql_safe` is an explicit trust boundary and does not make untrusted SQL safe. |
| `public:relations` | `RelationExpression` | [Relationships](../concepts/relationships.md) | Relation predicates depend on registered relationship metadata and backend query capabilities. |
| `public:associations` | `association_proxy`, `hybrid_property` | [Associations](../api/associations.md) | These helpers express model behavior; verify generated SQL when a hybrid expression is backend-specific. |
| `public:loaders` | `joined`, `joinedload`, `selectin`, `selectinload`, `lazy`, `lazyload`, `noload`, `load` | [Relationship loaders](../api/loaders.md) | Loading is explicit. Select-in batching and joined row growth have different performance tradeoffs. |
| `public:engine` | `runtime_capabilities` | [Native engine](../api/engine.md) | Capabilities describe the installed native extension, not whether a remote server is reachable. |

## Command-line workflows

| Inventory key | Commands | Canonical documentation | Safeguards and limits |
| --- | --- | --- | --- |
| `cli:migrations` | `snapshot`, `init`, `create`, `preview`, `autogenerate`, `apply`, `apply-dir`, `status`, `history`, `current`, `rollback`, `repair`, `check`, `squash` | [Migration API and CLI](../api/migrations.md) | Artifacts are dialect-specific and checksummed. Apply, rollback, repair, and squash require deliberate review; destructive SQL needs explicit authorization. Cancellation cannot undo statements already committed by a backend. |
| `cli:playground` | `ormdantic playground` | [Start the Playground](../playground/index.md) | Textual is optional. The TUI uses the same migration engine and cannot weaken production safety. |

## Driver coverage

| Inventory key | Backend | Canonical documentation | Important boundary |
| --- | --- | --- | --- |
| `driver:sqlite` | SQLite | [SQLite](../drivers/sqlite.md) | File paths and in-memory connection lifetime matter; some ALTER operations use table rebuilds. |
| `driver:postgresql` | PostgreSQL | [PostgreSQL](../drivers/postgresql.md) | Native enums, schemas, TLS URL options, and transactional DDL need backend-aware review. |
| `driver:mysql` | MySQL | [MySQL](../drivers/mysql.md) | Engine, charset, implicit commits, and server SQL mode affect behavior. |
| `driver:mariadb` | MariaDB | [MariaDB](../drivers/mariadb.md) | MariaDB is a distinct dialect; do not assume every MySQL feature compiles identically. |
| `driver:mssql` | SQL Server | [SQL Server](../drivers/mssql.md) | Driver URL, certificate trust, schemas, and batch restrictions are backend-specific. |
| `driver:oracle` | Oracle | [Oracle](../drivers/oracle.md) | Service names, identifier rules, sequences, and non-transactional DDL require explicit planning. |

## Playground coverage

| Inventory key | Workflow | Canonical documentation | Behavior |
| --- | --- | --- | --- |
| `playground:schema-watching` | File watching, database polling, refresh generations, pause/resume | [Schema watching](../playground/schema-watching.md) | Refreshes are debounced and stale generations are ignored. Pause before large external changes. |
| `playground:migration-workflows` | Generate, preview, apply, rollback, repair, squash, history | [Migration workflows](../playground/migration-workflows.md) | Operations run asynchronously and report their final state. Closing the UI does not promise reversal of completed SQL. |
| `playground:editor` | Full TOML and SQL documents, selection execution, drafts | [Editor](../playground/editor.md) | TOML is validated before save. SQL execution is limited to the reviewed selection and remains a privileged operation. |
| `playground:safety` | Confirmation, typed phrases, destructive review, production lock | [Safety](../playground/safety.md) | Destructive actions are available, but require destructive acknowledgement plus the active environment policy. Production cannot be downgraded. |
| `playground:configuration` | Project target, migration directory, watch paths, environments | [Configuration](../playground/configuration.md) | Paths resolve within the project root. Environment files provide values but are not written by the Playground. |
| `playground:diagnostics` | Status, logs, connection errors, troubleshooting | [Troubleshooting](../playground/troubleshooting.md) | Diagnostics redact known credentials. Treat logs as sensitive operational data anyway. |

## Maintenance rule

When a top-level export, command, driver, screen, or workflow is added, update its
canonical page and this matrix in the same change. The documentation test enforces
the stable keys used by the major surfaces.
