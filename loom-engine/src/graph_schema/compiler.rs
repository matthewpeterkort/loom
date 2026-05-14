use anyhow::{anyhow, Result};
use std::collections::BTreeMap;

use crate::mapping::{EdgeMapping, GraphMappingManifest, SourceMapping, VertexMapping};
use crate::source::SourceTableRef;

use super::model::{GraphEdgeSpec, GraphNodeSpec, GraphSchemaSpec};

pub fn compile_graph_schema_spec(spec: &GraphSchemaSpec) -> Result<GraphMappingManifest> {
    if spec.version != 1 {
        return Err(anyhow!(
            "unsupported graph schema spec version {}",
            spec.version
        ));
    }
    let mut alias_by_source = BTreeMap::<SourceTableRef, String>::new();
    let mut sources = BTreeMap::<String, SourceMapping>::new();
    for source in spec
        .nodes
        .iter()
        .map(|node| &node.source)
        .chain(spec.edges.iter().map(|edge| &edge.source))
    {
        if let Some(existing) = alias_by_source.get(source) {
            let _ = existing;
            continue;
        }
        let alias = alias_for_source(source, alias_by_source.len());
        alias_by_source.insert(source.clone(), alias.clone());
        sources.insert(
            alias,
            SourceMapping {
                source: Some(source.source_id.clone()),
                path: None,
                format: Default::default(),
            },
        );
    }
    let vertices = spec
        .nodes
        .iter()
        .map(|node| compile_node_spec(node, &alias_by_source))
        .collect::<Result<Vec<_>>>()?;
    let edges = spec
        .edges
        .iter()
        .map(|edge| compile_edge_spec(edge, &alias_by_source))
        .collect::<Result<Vec<_>>>()?;

    Ok(GraphMappingManifest {
        version: 1,
        graph: Some(spec.graph.clone()),
        display_name: spec.display_name.clone(),
        sources,
        vertices,
        edges,
        validation: Default::default(),
        identity: Default::default(),
        references: Default::default(),
        metadata: spec.metadata.clone(),
    })
}

fn compile_node_spec(
    spec: &GraphNodeSpec,
    alias_by_source: &BTreeMap<SourceTableRef, String>,
) -> Result<VertexMapping> {
    let source = alias_by_source
        .get(&spec.source)
        .cloned()
        .ok_or_else(|| anyhow!("missing source alias for node `{}`", spec.label))?;
    Ok(VertexMapping {
        source,
        label: spec.label.clone(),
        id: spec.id.clone(),
        predicate: spec.predicate.clone(),
        columns: spec.columns.clone(),
        props: spec.props.clone(),
        prop_types: spec.prop_types.clone(),
    })
}

fn compile_edge_spec(
    spec: &GraphEdgeSpec,
    alias_by_source: &BTreeMap<SourceTableRef, String>,
) -> Result<EdgeMapping> {
    let source = alias_by_source
        .get(&spec.source)
        .cloned()
        .ok_or_else(|| anyhow!("missing source alias for edge `{}`", spec.label))?;
    Ok(EdgeMapping {
        source,
        label: spec.label.clone(),
        from_label: spec.from_label.clone(),
        to_label: spec.to_label.clone(),
        from: spec.from.clone(),
        to: spec.to.clone(),
        id: spec.id.clone(),
        predicate: spec.predicate.clone(),
        columns: spec.columns.clone(),
        props: spec.props.clone(),
        prop_types: spec.prop_types.clone(),
    })
}

fn alias_for_source(source: &SourceTableRef, index: usize) -> String {
    format!(
        "gs_{index:04}_{}_{}",
        sanitize_identifier(&source.source_id),
        sanitize_identifier(&source.table_id)
    )
}

fn sanitize_identifier(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    while out.contains("__") {
        out = out.replace("__", "_");
    }
    out.trim_matches('_').to_string()
}
