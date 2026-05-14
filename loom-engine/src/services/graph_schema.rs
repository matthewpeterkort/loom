use crate::*;
use anyhow::{anyhow, Result};
use std::collections::BTreeSet;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

impl Engine {
    pub async fn register_graph_schema(
        &self,
        spec: graph_schema::GraphSchemaSpec,
    ) -> Result<graph_schema::GraphSchemaDescriptor> {
        let id = graph_schema_id_for_spec(&spec)?;
        let existing = self.graph_schema_catalog.get(&id)?;
        let descriptor = new_graph_schema_descriptor(id, spec, existing);
        self.graph_schema_catalog.upsert(descriptor.clone())?;
        Ok(descriptor)
    }

    pub fn list_graph_schemas(&self) -> Result<Vec<graph_schema::GraphSchemaDescriptor>> {
        self.graph_schema_catalog.list()
    }

    pub fn get_graph_schema(&self, id: &str) -> Result<graph_schema::GraphSchemaDescriptor> {
        self.graph_schema_catalog.require(id)
    }

    pub async fn validate_graph_schema_with_schema(
        &self,
        id: &str,
        base_dir: &Path,
        graph_schema: Option<&crate::schema::CompiledGraphSchema>,
        bound_schema: Option<String>,
    ) -> Result<graph_schema::GraphSchemaValidationReport> {
        let mut descriptor = self.graph_schema_catalog.require(id)?;
        let (manifest, mut report) =
            self.compile_and_validate_graph_schema_spec(&descriptor.spec, base_dir, graph_schema)
                .await?;
        descriptor.bound_schema = bound_schema;
        descriptor.compiled_manifest = Some(manifest.clone());
        descriptor.last_validation = Some(report.clone());
        descriptor.updated_unix_seconds = unix_seconds();
        descriptor.status = if report.valid {
            graph_schema::GraphSchemaStatus::Draft
        } else {
            graph_schema::GraphSchemaStatus::Error
        };
        descriptor.last_error = if report.valid {
            None
        } else {
            Some(join_graph_schema_errors(&report.errors))
        };
        self.graph_schema_catalog.upsert(descriptor)?;
        report.manifest = Some(manifest);
        Ok(report)
    }

    pub async fn preview_graph_schema_with_schema(
        &self,
        id: &str,
        base_dir: &Path,
        graph_schema: Option<&crate::schema::CompiledGraphSchema>,
        bound_schema: Option<String>,
    ) -> Result<graph_schema::GraphSchemaPreview> {
        let mut descriptor = self.graph_schema_catalog.require(id)?;
        let (manifest, report) =
            self.compile_and_validate_graph_schema_spec(&descriptor.spec, base_dir, graph_schema)
                .await?;
        if !report.valid {
            return Err(anyhow!(
                "invalid graph schema: {}",
                join_graph_schema_errors(&report.errors)
            ));
        }
        let mapping_validation = report
            .mapping_validation
            .clone()
            .ok_or_else(|| anyhow!("missing mapping validation in graph schema preview"))?;
        let plan = mapping_validation
            .plan_preview
            .clone()
            .ok_or_else(|| anyhow!("missing plan preview in graph schema preview"))?;
        let mut preview = graph_schema::GraphSchemaPreview {
            graph: descriptor.graph.clone(),
            warnings: report.warnings.clone(),
            source_hints: report.source_hints.clone(),
            manifest: Some(manifest.clone()),
            mapping_validation: Some(mapping_validation.clone()),
            ..Default::default()
        };
        for node_view in &plan.node_views {
            let count = self.count_rows_for_view(node_view.view_name.as_str()).await?;
            *preview.node_counts.entry(node_view.label.clone()).or_default() += count;
            let sample_rows = self.sample_rows_for_view(node_view.view_name.as_str(), 3).await?;
            preview
                .sample_node_rows
                .entry(node_view.label.clone())
                .or_default()
                .extend(sample_rows);
        }
        for edge_view in &plan.edge_views {
            let count = self.count_rows_for_view(edge_view.view_name.as_str()).await?;
            *preview.edge_counts.entry(edge_view.label.clone()).or_default() += count;
            let sample_rows = self.sample_rows_for_view(edge_view.view_name.as_str(), 3).await?;
            preview
                .sample_edge_rows
                .entry(edge_view.label.clone())
                .or_default()
                .extend(sample_rows);
        }
        preview.sample_node_rows.values_mut().for_each(|rows| rows.truncate(3));
        preview.sample_edge_rows.values_mut().for_each(|rows| rows.truncate(3));

        descriptor.bound_schema = bound_schema;
        descriptor.compiled_manifest = Some(manifest);
        descriptor.last_validation = Some(report);
        descriptor.last_preview = Some(preview.clone());
        descriptor.status = graph_schema::GraphSchemaStatus::Draft;
        descriptor.updated_unix_seconds = unix_seconds();
        descriptor.last_error = None;
        self.graph_schema_catalog.upsert(descriptor)?;
        Ok(preview)
    }

    pub async fn register_graph_schema_runtime_with_schema(
        &self,
        id: &str,
        base_dir: &Path,
        compile: bool,
        graph_schema: Option<&crate::schema::CompiledGraphSchema>,
        bound_schema: Option<String>,
    ) -> Result<graph_schema::GraphSchemaDescriptor> {
        let mut descriptor = self.graph_schema_catalog.require(id)?;
        let manifest = graph_schema::compile_graph_schema_spec(&descriptor.spec)?;
        let graph = mapping::graph_id_for_manifest(&manifest, Some(&descriptor.graph))?;
        let mapping_descriptor = self
            .register_graph_mapping_with_schema(
                manifest.clone(),
                base_dir,
                compile,
                graph_schema,
                bound_schema.clone(),
            )
            .await?;
        descriptor.bound_schema = bound_schema;
        descriptor.compiled_manifest = Some(manifest);
        descriptor.registered_mapping_graph = Some(graph.clone());
        descriptor.published_graph = if compile { Some(graph.clone()) } else { None };
        descriptor.last_error = mapping_descriptor.last_error.clone();
        descriptor.status = match mapping_descriptor.status {
            mapping::GraphMappingStatus::Registered => graph_schema::GraphSchemaStatus::Registered,
            mapping::GraphMappingStatus::Compiled => graph_schema::GraphSchemaStatus::Published,
            mapping::GraphMappingStatus::Error => graph_schema::GraphSchemaStatus::Error,
        };
        descriptor.updated_unix_seconds = unix_seconds();
        self.graph_schema_catalog.upsert(descriptor.clone())?;
        Ok(descriptor)
    }

    pub fn load_graph_schema_catalog_snapshot(
        &self,
        snapshot: graph_schema::GraphSchemaCatalogSnapshot,
    ) -> Result<()> {
        self.graph_schema_catalog.replace_all(snapshot)
    }

    async fn compile_and_validate_graph_schema_spec(
        &self,
        spec: &graph_schema::GraphSchemaSpec,
        base_dir: &Path,
        graph_schema: Option<&crate::schema::CompiledGraphSchema>,
    ) -> Result<(
        mapping::GraphMappingManifest,
        graph_schema::GraphSchemaValidationReport,
    )> {
        let mut report = graph_schema::GraphSchemaValidationReport::default();
        if spec.version != 1 {
            report.errors.push(graph_schema::GraphSchemaValidationError::new(
                "version",
                format!("unsupported graph schema spec version {}", spec.version),
            ));
        }
        if spec.graph.trim().is_empty() {
            report.errors.push(graph_schema::GraphSchemaValidationError::new(
                "graph",
                "graph schema spec must define `graph`",
            ));
        }
        if spec.nodes.is_empty() {
            report.errors.push(graph_schema::GraphSchemaValidationError::new(
                "nodes",
                "graph schema spec must define at least one node",
            ));
        }

        let unique_sources = collect_spec_sources(spec);
        for source_ref in &unique_sources {
            match self.source_catalog.require(&source_ref.source_id) {
                Ok(descriptor) => {
                    let table = descriptor
                        .tables
                        .iter()
                        .find(|table| table.id == source_ref.table_id)
                        .cloned();
                    match table {
                        Some(table) => {
                            if descriptor.tables.len() > 1 && descriptor.default_table().map(|t| &t.id) != Some(&source_ref.table_id) {
                                report.errors.push(graph_schema::GraphSchemaValidationError::new(
                                    format!(
                                        "sources.{}.{}",
                                        source_ref.source_id, source_ref.table_id
                                    ),
                                    "graph schema authoring currently supports only default tables from multi-table sources",
                                ));
                            }
                            report.source_hints.push(
                                self.build_source_hint(source_ref, &table, graph_schema)
                                    .await?,
                            );
                        }
                        None => report.errors.push(graph_schema::GraphSchemaValidationError::new(
                            format!("sources.{}.{}", source_ref.source_id, source_ref.table_id),
                            "selected source table does not exist",
                        )),
                    }
                }
                Err(err) => report.errors.push(graph_schema::GraphSchemaValidationError::new(
                    format!("sources.{}.{}", source_ref.source_id, source_ref.table_id),
                    err.to_string(),
                )),
            }
        }
        if !report.errors.is_empty() {
            report.valid = false;
            return Ok((
                graph_schema::compile_graph_schema_spec(spec)?,
                report,
            ));
        }

        let manifest = graph_schema::compile_graph_schema_spec(spec)?;
        let mapping_validation = self
            .validate_graph_mapping_full_with_schema(&manifest, base_dir, graph_schema)
            .await;
        report.warnings.extend(mapping_validation.warnings.clone());
        report.errors.extend(
            mapping_validation
                .errors
                .iter()
                .cloned()
                .map(Into::into),
        );
        report.valid = mapping_validation.valid && report.errors.is_empty();
        report.mapping_validation = Some(mapping_validation);
        Ok((manifest, report))
    }

    async fn build_source_hint(
        &self,
        source_ref: &source::SourceTableRef,
        table: &source::SourceTableDescriptor,
        graph_schema: Option<&crate::schema::CompiledGraphSchema>,
    ) -> Result<graph_schema::GraphSchemaSourceHint> {
        let profile = self
            .profile_source_table_with_schema(
                &source_ref.source_id,
                &source_ref.table_id,
                graph_schema,
            )
            .await?;
        Ok(graph_schema::GraphSchemaSourceHint {
            source: source_ref.clone(),
            table_name: table.table_name.clone(),
            columns: table
                .schema
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
            possible_id_columns: table.quality.possible_id_columns.clone(),
            semantic_suggestions: profile.suggestions,
            likely_id_columns: profile
                .column_profiles
                .iter()
                .filter(|profile| profile.likely_id)
                .map(|profile| profile.column.clone())
                .collect(),
            likely_reference_columns: profile
                .column_profiles
                .iter()
                .filter(|profile| profile.likely_foreign_key)
                .map(|profile| profile.column.clone())
                .collect(),
            column_profiles: profile.column_profiles,
            clusters: profile.clusters,
            entity_candidates: profile.entity_candidates,
            edge_candidates: profile.edge_candidates,
            schema_derived: profile.schema_derived,
        })
    }

    async fn count_rows_for_view(&self, view_name: &str) -> Result<usize> {
        let sql = format!("SELECT COUNT(*) AS count FROM {view_name}");
        let rows = self.query_sql_json_rows(&sql).await?;
        let row = rows
            .first()
            .ok_or_else(|| anyhow!("count query returned no rows"))?;
        let value: serde_json::Value = serde_json::from_str(row)?;
        Ok(value["count"].as_u64().unwrap_or_default() as usize)
    }

    async fn sample_rows_for_view(
        &self,
        view_name: &str,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let sql = format!("SELECT * FROM {view_name} LIMIT {limit}");
        let rows = self.query_sql_json_rows(&sql).await?;
        rows.into_iter()
            .map(|row| serde_json::from_str(&row).map_err(Into::into))
            .collect()
    }
}

fn graph_schema_id_for_spec(spec: &graph_schema::GraphSchemaSpec) -> Result<String> {
    let id = spec.id.as_deref().unwrap_or(spec.graph.as_str()).trim();
    if id.is_empty() {
        return Err(anyhow!("graph schema spec must define `id` or non-empty `graph`"));
    }
    Ok(id.to_string())
}

fn new_graph_schema_descriptor(
    id: String,
    spec: graph_schema::GraphSchemaSpec,
    existing: Option<graph_schema::GraphSchemaDescriptor>,
) -> graph_schema::GraphSchemaDescriptor {
    let now = unix_seconds();
    graph_schema::GraphSchemaDescriptor {
        id,
        graph: spec.graph.clone(),
        display_name: spec.display_name.clone(),
        bound_schema: spec.bound_schema.clone().or_else(|| existing.as_ref().and_then(|d| d.bound_schema.clone())),
        spec,
        compiled_manifest: existing.as_ref().and_then(|d| d.compiled_manifest.clone()),
        status: existing
            .as_ref()
            .map(|d| d.status.clone())
            .unwrap_or(graph_schema::GraphSchemaStatus::Draft),
        last_validation: existing.as_ref().and_then(|d| d.last_validation.clone()),
        last_preview: existing.as_ref().and_then(|d| d.last_preview.clone()),
        registered_mapping_graph: existing
            .as_ref()
            .and_then(|d| d.registered_mapping_graph.clone()),
        published_graph: existing.as_ref().and_then(|d| d.published_graph.clone()),
        last_error: existing.as_ref().and_then(|d| d.last_error.clone()),
        created_unix_seconds: existing
            .as_ref()
            .map(|d| d.created_unix_seconds)
            .unwrap_or(now),
        updated_unix_seconds: now,
    }
}

fn collect_spec_sources(spec: &graph_schema::GraphSchemaSpec) -> Vec<source::SourceTableRef> {
    let mut seen = BTreeSet::<(String, String)>::new();
    let mut refs = Vec::new();
    for source in spec
        .nodes
        .iter()
        .map(|node| &node.source)
        .chain(spec.edges.iter().map(|edge| &edge.source))
    {
        let key = (source.source_id.clone(), source.table_id.clone());
        if seen.insert(key) {
            refs.push(source.clone());
        }
    }
    refs
}

fn join_graph_schema_errors(errors: &[graph_schema::GraphSchemaValidationError]) -> String {
    errors
        .iter()
        .map(|error| format!("{}: {}", error.path, error.message))
        .collect::<Vec<_>>()
        .join("; ")
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
