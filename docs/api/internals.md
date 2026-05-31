# Rust Bridge Internals

These modules are private implementation details. They are documented to make the Python/Rust boundary reviewable, not to make them public API.

## Pydantic Introspection

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

## Rust Query Bridge

::: ormdantic.generator._rust_query.CompiledQuery
::: ormdantic.generator._rust_query.RustQuery
::: ormdantic.generator._rust_query.rust_available
::: ormdantic.generator._rust_query.compile_select_pk
::: ormdantic.generator._rust_query.compile_find_many
::: ormdantic.generator._rust_query.compile_joined_find_many
::: ormdantic.generator._rust_query.compile_count
::: ormdantic.generator._rust_query.compile_insert
::: ormdantic.generator._rust_query.compile_update
::: ormdantic.generator._rust_query.compile_upsert
::: ormdantic.generator._rust_query.compile_delete_pk
::: ormdantic.generator._rust_query.bind_compiled_query

## Rust Schema Bridge

::: ormdantic.generator._rust_schema.validate_table_map
::: ormdantic.generator._rust_schema.compile_create_table_sql
::: ormdantic.generator._rust_schema.compile_drop_table_sql

## Hydration Bridge

::: ormdantic.generator._hydration.hydrate_flat_payload
::: ormdantic.generator._hydration.hydrate_joined_payload
::: ormdantic.generator._hydration.plan_result_shape
