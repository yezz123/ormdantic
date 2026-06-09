use ormdantic_hydrate::{HydratedRow, HydrationGraph, RelationshipNode};
use ormdantic_schema::{RelationshipCardinality, RelationshipDef, TableDef};

fn row(values: &[(&str, &str)]) -> HydratedRow {
    values
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

fn flavors_relationship() -> RelationshipDef {
    RelationshipDef::new("flavors", "flavor", "id", RelationshipCardinality::Many)
}

#[test]
fn relationship_nodes_expose_nested_children() {
    let child = RelationshipNode::new("coffee/flavor/roast", flavors_relationship());
    let node = RelationshipNode::new("coffee/flavor", flavors_relationship())
        .with_children(vec![child.clone()]);

    assert_eq!(node.path(), "coffee/flavor");
    assert_eq!(node.relationship().field(), "flavors");
    assert_eq!(node.children(), &[child]);
}

#[test]
fn hydration_graph_deduplicates_by_primary_key() {
    let table = TableDef::new("coffee", "id", vec!["id".to_string()]);
    let graph = HydrationGraph::new(table);

    let rows = graph.deduplicate_rows(vec![
        row(&[("id", "1"), ("name", "mocha")]),
        row(&[("id", "1"), ("name", "mocha duplicate")]),
        row(&[("id", ""), ("name", "empty")]),
        row(&[("name", "missing")]),
        row(&[("id", "2"), ("name", "latte")]),
    ]);

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("name").unwrap(), "mocha");
    assert_eq!(rows[1].get("name").unwrap(), "latte");
}

#[test]
fn hydration_graph_supports_composite_keys_and_relationships() {
    let table = TableDef::new(
        "coffee",
        "id",
        vec!["tenant_id".to_string(), "id".to_string()],
    );
    let node = RelationshipNode::new("coffee/flavor", flavors_relationship());
    let graph = HydrationGraph::new(table)
        .composite_key(vec!["tenant_id".to_string(), "id".to_string()])
        .with_relationships(vec![node]);

    assert_eq!(
        graph.primary_key_columns(),
        &["tenant_id".to_string(), "id".to_string()]
    );
    assert_eq!(graph.relationships().len(), 1);
    assert_eq!(graph.root_table().name(), "coffee");
}
