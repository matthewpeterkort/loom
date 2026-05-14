//! Graph mapping models, SQL plan generation, and manifest validation.

use anyhow::{anyhow, Result};
use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::source::{
    infer_and_read_source, read_source_rows, ReadOptions, SourceCatalog, SourceDescriptor,
    SourceLocation, SourceRegistration, SourceRow, SourceTable,
};

mod catalog;
mod model;
mod sql;
mod validation;

pub use catalog::*;
pub use model::*;

use sql::{
    compile_edge_view_plan, compile_identity_view_plan, compile_node_view_plan,
    compile_reference_view_plan,
};
use validation::validate_mapping_manifest_inner;

pub(crate) use sql::{
    build_edge_view_dataframe, build_identity_view_dataframe, build_node_view_dataframe,
    build_reference_view_dataframe,
};

#[derive(Debug, Clone)]
struct DynamicRows {
    base_columns: &'static [&'static str],
    dynamic_columns: BTreeSet<String>,
    rows: Vec<BTreeMap<String, String>>,
}

const BASE_VERTEX_COLUMNS: &[&str] = &["id", "label", "props", "source_id", "source_row"];
const BASE_EDGE_COLUMNS: &[&str] = &[
    "id",
    "from_id",
    "to_id",
    "label",
    "props",
    "source_id",
    "source_row",
];
const RESERVED_COLUMNS: &[&str] = &[
    "id",
    "label",
    "props",
    "source_id",
    "source_row",
    "from_id",
    "to_id",
];

pub fn compile_mapping_manifest(
    manifest: &GraphMappingManifest,
    base_dir: &Path,
) -> Result<MappingCompileResult> {
    compile_mapping_manifest_inner(manifest, base_dir, None)
}

pub fn compile_mapping_manifest_with_catalog(
    manifest: &GraphMappingManifest,
    base_dir: &Path,
    catalog: &SourceCatalog,
) -> Result<MappingCompileResult> {
    compile_mapping_manifest_inner(manifest, base_dir, Some(catalog))
}

pub fn validate_mapping_manifest(
    manifest: &GraphMappingManifest,
    base_dir: &Path,
) -> MappingValidationReport {
    validate_mapping_manifest_inner(manifest, base_dir, None, None)
}

pub fn validate_mapping_manifest_with_catalog(
    manifest: &GraphMappingManifest,
    base_dir: &Path,
    catalog: &SourceCatalog,
    graph_schema: Option<&crate::schema::CompiledGraphSchema>,
) -> MappingValidationReport {
    validate_mapping_manifest_inner(manifest, base_dir, Some(catalog), graph_schema)
}

pub fn compile_virtual_graph_plan_with_catalog(
    manifest: &GraphMappingManifest,
    base_dir: &Path,
    catalog: &SourceCatalog,
    graph_schema: Option<&crate::schema::CompiledGraphSchema>,
) -> Result<CompiledVirtualGraphPlan> {
    let validation = validate_mapping_manifest_inner(manifest, base_dir, Some(catalog), graph_schema);
    if !validation.valid {
        return Err(anyhow!(
            "invalid graph mapping manifest: {}",
            validation
                .errors
                .iter()
                .map(|e| format!("{}: {}", e.path, e.message))
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }
    let graph = graph_id_for_manifest(manifest, None)?;
    let source_descriptors = resolve_source_descriptors(manifest, base_dir, Some(catalog))?;
    let mut node_views = Vec::new();
    let mut edge_views = Vec::new();
    let mut identity_views = Vec::new();
    let mut reference_views = Vec::new();
    let mut warnings = validation.warnings;
    let graph_vertex_table = format!("vertices_{graph}");
    let graph_edge_table = format!("edges_{graph}");

    for (idx, vertex) in manifest.vertices.iter().enumerate() {
        let source = source_descriptors.get(&vertex.source).ok_or_else(|| {
            anyhow!(
                "vertices[{idx}] references unknown source `{}`",
                vertex.source
            )
        })?;
        let plan = compile_node_view_plan(&graph, idx, vertex, source)?;
        node_views.push(plan);
    }
    for (idx, edge) in manifest.edges.iter().enumerate() {
        let source = source_descriptors
            .get(&edge.source)
            .ok_or_else(|| anyhow!("edges[{idx}] references unknown source `{}`", edge.source))?;
        let plan = compile_edge_view_plan(&graph, idx, edge, source)?;
        edge_views.push(plan);
    }
    if node_views.is_empty() {
        warnings.push("mapping contains no vertex plans".to_string());
    }

    let mut vertices_by_label = BTreeMap::<String, Vec<(&VertexMapping, &SourceDescriptor)>>::new();
    for vertex in &manifest.vertices {
        if let Some(source) = source_descriptors.get(&vertex.source) {
            vertices_by_label
                .entry(vertex.label.clone())
                .or_default()
                .push((vertex, source));
        }
    }
    for (label, vertices) in &vertices_by_label {
        let rule = manifest.identity.get(label);
        identity_views.push(compile_identity_view_plan(&graph, label, vertices, rule)?);
    }
    for reference in &manifest.references {
        let source = source_descriptors.get(&reference.source).ok_or_else(|| {
            anyhow!(
                "reference `{}` references unknown source `{}`",
                reference.name,
                reference.source
            )
        })?;
        reference_views.push(compile_reference_view_plan(&graph, reference, source)?);
    }

    let vertex_columns = collect_union_columns(
        &[
            "id",
            "label",
            "source_id",
            "source_row",
            "payload_codec",
            "payload_bin",
            "payload_json_bin",
        ],
        node_views
            .iter()
            .map(|plan| plan.projected_columns.as_slice())
            .collect::<Vec<_>>()
            .as_slice(),
    );
    let edge_columns = collect_union_columns(
        &[
            "id",
            "from_id",
            "to_id",
            "label",
            "source_id",
            "source_row",
            "payload_codec",
            "payload_bin",
            "payload_json_bin",
        ],
        edge_views
            .iter()
            .map(|plan| plan.projected_columns.as_slice())
            .collect::<Vec<_>>()
            .as_slice(),
    );

    Ok(CompiledVirtualGraphPlan {
        graph,
        vertex_table: graph_vertex_table,
        edge_table: graph_edge_table,
        vertex_columns,
        edge_columns,
        node_views,
        edge_views,
        identity_views,
        reference_views,
        source_dependencies: source_dependencies(manifest),
        warnings,
    })
}

fn compile_mapping_manifest_inner(
    manifest: &GraphMappingManifest,
    base_dir: &Path,
    catalog: Option<&SourceCatalog>,
) -> Result<MappingCompileResult> {
    let validation = validate_mapping_manifest_inner(manifest, base_dir, catalog, None);
    if !validation.valid {
        return Err(anyhow!(
            "invalid graph mapping manifest: {}",
            validation
                .errors
                .iter()
                .map(|e| format!("{}: {}", e.path, e.message))
                .collect::<Vec<_>>()
                .join("; ")
        ));
    }

    let sources = load_sources(manifest, base_dir, catalog)?;
    let mut vertices = DynamicRows::new(BASE_VERTEX_COLUMNS);
    let mut edges = DynamicRows::new(BASE_EDGE_COLUMNS);
    let mut seen_vertices = HashSet::<String>::new();
    let mut seen_edges = HashSet::<String>::new();
    let mut report = MappingCompileReport::default();

    for (mapping_idx, vertex) in manifest.vertices.iter().enumerate() {
        let source = sources.get(&vertex.source).ok_or_else(|| {
            anyhow!(
                "vertices[{mapping_idx}] references unknown source `{}`",
                vertex.source
            )
        })?;
        for row in &source.rows {
            if !eval_predicate_opt(vertex.predicate.as_ref(), row) {
                report.filtered_vertex_rows += 1;
                continue;
            }
            let id = eval_expr(&vertex.id, row);
            if id.is_empty() {
                report.filtered_vertex_rows += 1;
                continue;
            }
            if !seen_vertices.insert(id.clone()) {
                report.duplicate_vertices += 1;
                continue;
            }
            let mut out = BTreeMap::new();
            out.insert("id".to_string(), id);
            out.insert("label".to_string(), vertex.label.clone());
            out.insert("props".to_string(), props_json(row, &vertex.props));
            out.insert("source_id".to_string(), source.source_id.clone());
            out.insert("source_row".to_string(), row.line_number.to_string());
            for (column, mapping) in &vertex.columns {
                out.insert(column.clone(), eval_expr(mapping.expr(), row));
                vertices.dynamic_columns.insert(column.clone());
            }
            vertices.rows.push(out);
            *report
                .vertex_labels
                .entry(vertex.label.clone())
                .or_default() += 1;
        }
    }

    for (mapping_idx, edge) in manifest.edges.iter().enumerate() {
        let source = sources.get(&edge.source).ok_or_else(|| {
            anyhow!(
                "edges[{mapping_idx}] references unknown source `{}`",
                edge.source
            )
        })?;
        for row in &source.rows {
            if !eval_predicate_opt(edge.predicate.as_ref(), row) {
                report.filtered_edge_rows += 1;
                continue;
            }
            let from = eval_expr(&edge.from, row);
            let to = eval_expr(&edge.to, row);
            if from.is_empty() || to.is_empty() {
                report.filtered_edge_rows += 1;
                continue;
            }
            let id = edge
                .id
                .as_ref()
                .map(|expr| eval_expr(expr, row))
                .filter(|id| !id.is_empty())
                .unwrap_or_else(|| format!("edge/{}/{from}/{to}/{}", edge.label, row.line_number));
            if !seen_edges.insert(id.clone()) {
                report.duplicate_edges += 1;
                continue;
            }
            let mut out = BTreeMap::new();
            out.insert("id".to_string(), id);
            out.insert("from_id".to_string(), from);
            out.insert("to_id".to_string(), to);
            out.insert("label".to_string(), edge.label.clone());
            out.insert("props".to_string(), props_json(row, &edge.props));
            out.insert("source_id".to_string(), source.source_id.clone());
            out.insert("source_row".to_string(), row.line_number.to_string());
            for (column, mapping) in &edge.columns {
                out.insert(column.clone(), eval_expr(mapping.expr(), row));
                edges.dynamic_columns.insert(column.clone());
            }
            edges.rows.push(out);
            *report.edge_labels.entry(edge.label.clone()).or_default() += 1;
        }
    }

    report.vertices = vertices.rows.len();
    report.edges = edges.rows.len();
    if report.duplicate_vertices > 0 {
        report.warnings.push(format!(
            "{} duplicate vertex rows were skipped",
            report.duplicate_vertices
        ));
    }
    if report.duplicate_edges > 0 {
        report.warnings.push(format!(
            "{} duplicate edge rows were skipped",
            report.duplicate_edges
        ));
    }

    Ok(MappingCompileResult {
        report,
        vertices: vertices.into_batch()?,
        edges: edges.into_batch()?,
    })
}

pub fn load_mapping_manifest(path: &Path) -> Result<GraphMappingManifest> {
    let content = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&content)?)
}

pub fn source_dependencies(manifest: &GraphMappingManifest) -> Vec<String> {
    let mut deps = manifest
        .sources
        .values()
        .filter_map(|source| source.source.clone())
        .collect::<Vec<_>>();
    deps.sort();
    deps.dedup();
    deps
}

pub fn graph_id_for_manifest(
    manifest: &GraphMappingManifest,
    fallback: Option<&str>,
) -> Result<String> {
    manifest
        .graph
        .as_deref()
        .or(fallback)
        .map(str::to_string)
        .filter(|graph| !graph.trim().is_empty())
        .ok_or_else(|| anyhow!("graph mapping manifest must define `graph`"))
}

pub fn new_descriptor(
    graph: String,
    manifest: GraphMappingManifest,
    existing: Option<GraphMappingDescriptor>,
) -> GraphMappingDescriptor {
    let now = unix_seconds();
    GraphMappingDescriptor {
        graph,
        display_name: manifest.display_name.clone(),
        bound_schema: existing.as_ref().and_then(|d| d.bound_schema.clone()),
        source_dependencies: source_dependencies(&manifest),
        manifest,
        status: GraphMappingStatus::Registered,
        virtual_binding: existing.as_ref().and_then(|d| d.virtual_binding.clone()),
        published_graph: existing.as_ref().and_then(|d| d.published_graph.clone()),
        identity_summary: existing.as_ref().and_then(|d| d.identity_summary.clone()),
        last_report: None,
        last_error: None,
        created_unix_seconds: existing
            .as_ref()
            .map(|d| d.created_unix_seconds)
            .unwrap_or(now),
        updated_unix_seconds: now,
    }
}

fn collect_union_columns(base: &[&str], projected_lists: &[&[String]]) -> Vec<String> {
    let mut columns = base.iter().map(|value| value.to_string()).collect::<Vec<_>>();
    let mut seen = columns.iter().cloned().collect::<HashSet<_>>();
    for projected in projected_lists {
        for column in *projected {
            if seen.insert(column.clone()) {
                columns.push(column.clone());
            }
        }
    }
    columns
}

fn load_sources(
    manifest: &GraphMappingManifest,
    base_dir: &Path,
    catalog: Option<&SourceCatalog>,
) -> Result<HashMap<String, SourceTable>> {
    manifest
        .sources
        .iter()
        .map(|(id, source)| {
            let table = if let Some(source_id) = &source.source {
                let catalog = catalog.ok_or_else(|| {
                    anyhow!("mapping source `{id}` references catalog source `{source_id}` but no source catalog was provided")
                })?;
                let descriptor = catalog.require(source_id)?;
                read_source_rows(&descriptor, None)?
            } else {
                let path = source
                    .path
                    .as_deref()
                    .ok_or_else(|| anyhow!("mapping source `{id}` missing path"))?;
                let registration = SourceRegistration {
                    id: id.clone(),
                    display_name: None,
                    format: source.format.clone(),
                    location: SourceLocation::Local {
                        path: resolve_path(base_dir, path).to_string_lossy().to_string(),
                    },
                    read_options: ReadOptions::default(),
                };
                infer_and_read_source(registration)?
                    .1
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("mapping source `{id}` did not expose a default logical table"))?
            };
            Ok((id.clone(), table))
        })
        .collect()
}

fn resolve_source_descriptors(
    manifest: &GraphMappingManifest,
    base_dir: &Path,
    catalog: Option<&SourceCatalog>,
) -> Result<HashMap<String, SourceDescriptor>> {
    manifest
        .sources
        .iter()
        .map(|(alias, source)| {
            let descriptor = if let Some(source_id) = &source.source {
                let catalog = catalog.ok_or_else(|| {
                    anyhow!("mapping source `{alias}` references catalog source `{source_id}` but no source catalog was provided")
                })?;
                catalog.require(source_id)?
            } else {
                let path = source
                    .path
                    .as_deref()
                    .ok_or_else(|| anyhow!("mapping source `{alias}` missing path"))?;
                let registration = SourceRegistration {
                    id: alias.clone(),
                    display_name: None,
                    format: source.format.clone(),
                    location: SourceLocation::Local {
                        path: resolve_path(base_dir, path).to_string_lossy().to_string(),
                    },
                    read_options: ReadOptions::default(),
                };
                infer_and_read_source(registration)?.0
            };
            Ok((alias.clone(), descriptor))
        })
        .collect()
}

fn eval_expr(expr: &Expr, row: &SourceRow) -> String {
    match expr {
        Expr::Column { column } => row.values.get(column).cloned().unwrap_or_default(),
        Expr::Literal { literal } => literal.clone(),
        Expr::Concat { concat } => concat.iter().map(|expr| eval_expr(expr, row)).collect(),
        Expr::Coalesce { coalesce } => coalesce
            .iter()
            .map(|expr| eval_expr(expr, row))
            .find(|value| !value.is_empty())
            .unwrap_or_default(),
        Expr::RowNumber { row_number } => {
            if *row_number {
                row.line_number.to_string()
            } else {
                String::new()
            }
        }
        Expr::Lower { lower } => eval_expr(lower, row).to_ascii_lowercase(),
        Expr::Upper { upper } => eval_expr(upper, row).to_ascii_uppercase(),
        Expr::Trim { trim } => eval_expr(trim, row).trim().to_string(),
        Expr::Replace { replace } => {
            eval_expr(&replace.value, row).replace(&replace.from, &replace.to)
        }
        Expr::NullIf { null_if } => {
            if null_if.len() != 2 {
                return String::new();
            }
            let value = eval_expr(&null_if[0], row);
            if value == eval_expr(&null_if[1], row) {
                String::new()
            } else {
                value
            }
        }
        Expr::Sha256 { sha256 } => {
            let mut hasher = Sha256::new();
            hasher.update(eval_expr(sha256, row).as_bytes());
            format!("{:x}", hasher.finalize())
        }
        Expr::Text(value) => value.clone(),
    }
}

fn eval_predicate_opt(predicate: Option<&Predicate>, row: &SourceRow) -> bool {
    predicate
        .map(|predicate| eval_predicate(predicate, row))
        .unwrap_or(true)
}

fn eval_predicate(predicate: &Predicate, row: &SourceRow) -> bool {
    match predicate {
        Predicate::Eq { eq } => eval_expr(&eq.left, row) == eval_expr(&eq.right, row),
        Predicate::Neq { neq } => eval_expr(&neq.left, row) != eval_expr(&neq.right, row),
        Predicate::IsEmpty { is_empty } => eval_expr(is_empty, row).is_empty(),
        Predicate::IsNotEmpty { is_not_empty } => !eval_expr(is_not_empty, row).is_empty(),
        Predicate::And { and } => and.iter().all(|predicate| eval_predicate(predicate, row)),
        Predicate::Or { or } => or.iter().any(|predicate| eval_predicate(predicate, row)),
        Predicate::Not { not } => !eval_predicate(not, row),
    }
}

fn props_json(row: &SourceRow, mapping: &PropsMapping) -> String {
    let selected = match mapping {
        PropsMapping::None => return "{}".to_string(),
        PropsMapping::All => row.values.iter().collect::<Vec<_>>(),
        PropsMapping::Except(exclude) => {
            let exclude = exclude.iter().collect::<HashSet<_>>();
            row.values
                .iter()
                .filter(|(key, _)| !exclude.contains(*key))
                .collect::<Vec<_>>()
        }
        PropsMapping::Only(only) => {
            let only = only.iter().collect::<HashSet<_>>();
            row.values
                .iter()
                .filter(|(key, _)| only.contains(*key))
                .collect::<Vec<_>>()
        }
    };
    let mut obj = Map::new();
    for (key, value) in selected {
        if !value.is_empty() {
            obj.insert(key.clone(), Value::String(value.clone()));
        }
    }
    Value::Object(obj).to_string()
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

impl Default for PropsMapping {
    fn default() -> Self {
        PropsMapping::None
    }
}

impl DynamicRows {
    fn new(base_columns: &'static [&'static str]) -> Self {
        Self {
            base_columns,
            dynamic_columns: BTreeSet::new(),
            rows: Vec::new(),
        }
    }

    fn schema(&self) -> Arc<Schema> {
        let mut fields = self
            .base_columns
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, true))
            .collect::<Vec<_>>();
        for column in &self.dynamic_columns {
            if !self.base_columns.iter().any(|base| base == column) {
                fields.push(Field::new(column, DataType::Utf8, true));
            }
        }
        Arc::new(Schema::new(fields))
    }

    fn into_batch(self) -> Result<RecordBatch> {
        let schema = self.schema();
        let columns = schema
            .fields()
            .iter()
            .map(|field| {
                Arc::new(StringArray::from(
                    self.rows
                        .iter()
                        .map(|row| row.get(field.name()).cloned().unwrap_or_default())
                        .collect::<Vec<_>>(),
                )) as ArrayRef
            })
            .collect::<Vec<_>>();
        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

pub(crate) fn resolve_path(base_dir: &Path, path: &str) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        path
    } else {
        base_dir.join(path)
    }
}
