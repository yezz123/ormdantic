# Rust bridge internals

These modules are private implementation details. The public runtime uses Rust-owned database and table handles exposed through `ormdantic._ormdantic`; Python modules keep the user-facing facade small while delegating hydration, filter normalization, schema compilation, reflection, and migration execution to Rust.

## Pydantic introspection

::: ormdantic._introspect.FieldMetadata
::: ormdantic._introspect.model_fields
::: ormdantic._introspect.model_field
::: ormdantic._introspect.annotation_allows_none
::: ormdantic._introspect.is_union_annotation
::: ormdantic._introspect.is_list_annotation
::: ormdantic._introspect.contains_list_annotation
::: ormdantic._introspect.is_dict_annotation
::: ormdantic._introspect.first_model_arg
::: ormdantic._introspect.rebuild_model

## Schema helpers

::: ormdantic.schema.validate_table_map
::: ormdantic.schema.compile_create_table_sql
::: ormdantic.schema.compile_drop_table_sql

## Hydration bridge

::: ormdantic.hydration.hydrate_flat_payload
::: ormdantic.hydration.hydrate_joined_payload
::: ormdantic.hydration.plan_result_shape
