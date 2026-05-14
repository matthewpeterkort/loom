use crate::*;
use datafusion::functions_aggregate::expr_fn::{count, count_distinct};

impl Engine {
    pub async fn register_mapped_graph(
        &self,
        name: &str,
        manifest: &mapping::GraphMappingManifest,
        base_dir: &std::path::Path,
    ) -> Result<mapping::MappingCompileReport> {
        let graph = mapping::compile_mapping_manifest_with_catalog(
            manifest,
            base_dir,
            &self.source_catalog,
        )?;
        let report = graph.report.clone();
        self.register_graph_batches(name, graph.vertices, graph.edges)
            .await?;
        Ok(report)
    }

    pub fn validate_graph_mapping(
        &self,
        manifest: &mapping::GraphMappingManifest,
        base_dir: &std::path::Path,
    ) -> mapping::MappingValidationReport {
        self.validate_graph_mapping_with_schema(manifest, base_dir, None)
    }

    pub fn validate_graph_mapping_with_schema(
        &self,
        manifest: &mapping::GraphMappingManifest,
        base_dir: &std::path::Path,
        graph_schema: Option<&crate::schema::CompiledGraphSchema>,
    ) -> mapping::MappingValidationReport {
        mapping::validate_mapping_manifest_with_catalog(
            manifest,
            base_dir,
            &self.source_catalog,
            graph_schema,
        )
    }

    pub async fn validate_graph_mapping_full(
        &self,
        manifest: &mapping::GraphMappingManifest,
        base_dir: &std::path::Path,
    ) -> mapping::MappingValidationReport {
        self.validate_graph_mapping_full_with_schema(manifest, base_dir, None)
            .await
    }

    pub async fn validate_graph_mapping_full_with_schema(
        &self,
        manifest: &mapping::GraphMappingManifest,
        base_dir: &std::path::Path,
        graph_schema: Option<&crate::schema::CompiledGraphSchema>,
    ) -> mapping::MappingValidationReport {
        let mut report = self.validate_graph_mapping_with_schema(manifest, base_dir, graph_schema);
        if !report.valid {
            return report;
        }
        let mut plan = match mapping::compile_virtual_graph_plan_with_catalog(
            manifest,
            base_dir,
            &self.source_catalog,
            graph_schema,
        ) {
            Ok(plan) => plan,
            Err(err) => {
                report.valid = false;
                report.errors.push(mapping::MappingValidationError {
                    path: "plan".to_string(),
                    message: err.to_string(),
                });
                return report;
            }
        };
        if let Err(err) = self
            .ensure_mapping_sources_registered(manifest, base_dir, &plan)
            .await
        {
            report.valid = false;
            report.errors.push(mapping::MappingValidationError {
                path: "sources".to_string(),
                message: err.to_string(),
            });
            report.plan_preview = Some(plan);
            return report;
        }
        if let Err(err) = self
            .register_virtual_graph_views_from_plan(manifest, base_dir, &plan)
            .await
        {
            report.valid = false;
            report.errors.push(mapping::MappingValidationError {
                path: "views".to_string(),
                message: format!("could not register virtual graph views for validation: {err}"),
            });
            report.plan_preview = Some(plan);
            return report;
        }
        for node_view in &mut plan.node_views {
            match self
                .build_node_dataframe_for_plan(manifest, node_view.mapping_index, base_dir)
                .await
            {
                Ok(dataframe) => match self.explain_mapping_view(dataframe).await {
                    Ok(explain) => node_view.explain = Some(explain),
                    Err(err) => {
                        report.valid = false;
                        report.errors.push(mapping::MappingValidationError {
                            path: format!("{}.view", node_view.mapping_path),
                            message: format!(
                                "DataFusion could not plan generated mapping view: {err}"
                            ),
                        });
                    }
                },
                Err(err) => {
                    report.valid = false;
                    report.errors.push(mapping::MappingValidationError {
                        path: format!("{}.view", node_view.mapping_path),
                        message: format!("could not build mapping view dataframe: {err}"),
                    });
                }
            }
        }
        for edge_view in &mut plan.edge_views {
            match self
                .build_edge_dataframe_for_plan(manifest, edge_view.mapping_index, base_dir)
                .await
            {
                Ok(dataframe) => match self.explain_mapping_view(dataframe).await {
                    Ok(explain) => edge_view.explain = Some(explain),
                    Err(err) => {
                        report.valid = false;
                        report.errors.push(mapping::MappingValidationError {
                            path: format!("{}.view", edge_view.mapping_path),
                            message: format!(
                                "DataFusion could not plan generated mapping view: {err}"
                            ),
                        });
                    }
                },
                Err(err) => {
                    report.valid = false;
                    report.errors.push(mapping::MappingValidationError {
                        path: format!("{}.view", edge_view.mapping_path),
                        message: format!("could not build mapping view dataframe: {err}"),
                    });
                }
            }
        }
        for identity_view in &mut plan.identity_views {
            match self
                .build_identity_dataframe_for_plan(
                    manifest,
                    &plan.graph,
                    identity_view.label.as_str(),
                    base_dir,
                )
                .await
            {
                Ok(dataframe) => match self.explain_mapping_view(dataframe).await {
                    Ok(explain) => identity_view.explain = Some(explain),
                    Err(err) => {
                        report.valid = false;
                        report.errors.push(mapping::MappingValidationError {
                            path: format!("identity.{}.view", identity_view.label),
                            message: format!(
                                "DataFusion could not plan generated identity view: {err}"
                            ),
                        });
                    }
                },
                Err(err) => {
                    report.valid = false;
                    report.errors.push(mapping::MappingValidationError {
                        path: format!("identity.{}.view", identity_view.label),
                        message: format!("could not build identity view dataframe: {err}"),
                    });
                }
            }
        }
        for reference_view in &mut plan.reference_views {
            match self
                .build_reference_dataframe_for_plan(
                    manifest,
                    &plan.graph,
                    reference_view.name.as_str(),
                    base_dir,
                )
                .await
            {
                Ok(dataframe) => match self.explain_mapping_view(dataframe).await {
                    Ok(explain) => reference_view.explain = Some(explain),
                    Err(err) => {
                        report.valid = false;
                        report.errors.push(mapping::MappingValidationError {
                            path: format!("references.{}.view", reference_view.name),
                            message: format!(
                                "DataFusion could not plan generated reference view: {err}"
                            ),
                        });
                    }
                },
                Err(err) => {
                    report.valid = false;
                    report.errors.push(mapping::MappingValidationError {
                        path: format!("references.{}.view", reference_view.name),
                        message: format!("could not build reference view dataframe: {err}"),
                    });
                }
            }
        }
        if !report.valid {
            report.plan_preview = Some(plan);
            return report;
        }

        match self
            .validate_mapping_semantics(manifest, &plan, &mut report)
            .await
        {
            Ok(()) => {}
            Err(err) => {
                report.valid = false;
                report.errors.push(mapping::MappingValidationError {
                    path: "validation".to_string(),
                    message: err.to_string(),
                });
            }
        }
        report.plan_preview = Some(plan);
        report.valid = report.errors.is_empty();
        report
    }

    pub async fn register_graph_mapping(
        &self,
        manifest: mapping::GraphMappingManifest,
        base_dir: &std::path::Path,
        compile: bool,
    ) -> Result<mapping::GraphMappingDescriptor> {
        self.register_graph_mapping_with_schema(manifest, base_dir, compile, None, None)
            .await
    }

    pub async fn register_graph_mapping_with_schema(
        &self,
        manifest: mapping::GraphMappingManifest,
        base_dir: &std::path::Path,
        compile: bool,
        graph_schema: Option<&crate::schema::CompiledGraphSchema>,
        bound_schema: Option<String>,
    ) -> Result<mapping::GraphMappingDescriptor> {
        let graph = mapping::graph_id_for_manifest(&manifest, None)?;
        let existing = self.graph_mapping_catalog.get(&graph)?;
        let mut descriptor = mapping::new_descriptor(graph.clone(), manifest, existing);
        descriptor.bound_schema = bound_schema;
        let validation = self
            .validate_graph_mapping_full_with_schema(&descriptor.manifest, base_dir, graph_schema)
            .await;
        if !validation.valid {
            descriptor.status = mapping::GraphMappingStatus::Error;
            descriptor.last_error = Some(format!(
                "invalid graph mapping manifest: {}",
                validation
                    .errors
                    .iter()
                    .map(|e| format!("{}: {}", e.path, e.message))
                    .collect::<Vec<_>>()
                    .join("; ")
            ));
            self.graph_mapping_catalog.upsert(descriptor.clone())?;
            return Ok(descriptor);
        }
        let plan = mapping::compile_virtual_graph_plan_with_catalog(
            &descriptor.manifest,
            base_dir,
            &self.source_catalog,
            graph_schema,
        )?;
        if let Err(err) = self
            .register_virtual_graph_descriptor(&mut descriptor, base_dir, &plan)
            .await
        {
            descriptor.status = mapping::GraphMappingStatus::Error;
            descriptor.last_error = Some(err.to_string());
            self.graph_mapping_catalog.upsert(descriptor.clone())?;
            return Err(err);
        }
        if compile {
            if let Err(err) = self
                .publish_graph_mapping_descriptor(&mut descriptor, base_dir, &plan)
                .await
            {
                descriptor.status = mapping::GraphMappingStatus::Error;
                descriptor.last_error = Some(err.to_string());
                self.graph_mapping_catalog.upsert(descriptor.clone())?;
                return Err(err);
            }
        }
        self.graph_mapping_catalog.upsert(descriptor.clone())?;
        Ok(descriptor)
    }

    pub fn list_graph_mappings(&self) -> Result<Vec<mapping::GraphMappingDescriptor>> {
        self.graph_mapping_catalog.list()
    }

    pub fn get_graph_mapping(&self, graph: &str) -> Result<mapping::GraphMappingDescriptor> {
        self.graph_mapping_catalog.require(graph)
    }

    pub async fn compile_graph_mapping(
        &self,
        graph: &str,
        base_dir: &std::path::Path,
    ) -> Result<mapping::GraphMappingDescriptor> {
        self.compile_graph_mapping_with_schema(graph, base_dir, None)
            .await
    }

    pub async fn compile_graph_mapping_with_schema(
        &self,
        graph: &str,
        base_dir: &std::path::Path,
        graph_schema: Option<&crate::schema::CompiledGraphSchema>,
    ) -> Result<mapping::GraphMappingDescriptor> {
        let mut descriptor = self.graph_mapping_catalog.require(graph)?;
        let plan = mapping::compile_virtual_graph_plan_with_catalog(
            &descriptor.manifest,
            base_dir,
            &self.source_catalog,
            graph_schema,
        )?;
        if let Err(err) = self
            .register_virtual_graph_descriptor(&mut descriptor, base_dir, &plan)
            .await
        {
            descriptor.status = mapping::GraphMappingStatus::Error;
            descriptor.last_error = Some(err.to_string());
            descriptor.updated_unix_seconds = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or_default();
            self.graph_mapping_catalog.upsert(descriptor)?;
            return Err(err);
        }
        if let Err(err) = self
            .publish_graph_mapping_descriptor(&mut descriptor, base_dir, &plan)
            .await
        {
            descriptor.status = mapping::GraphMappingStatus::Error;
            descriptor.last_error = Some(err.to_string());
            descriptor.updated_unix_seconds = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or_default();
            self.graph_mapping_catalog.upsert(descriptor)?;
            return Err(err);
        }
        self.graph_mapping_catalog.upsert(descriptor.clone())?;
        Ok(descriptor)
    }

    pub async fn refresh_graph_mapping(
        &self,
        graph: &str,
        base_dir: &std::path::Path,
    ) -> Result<mapping::GraphMappingDescriptor> {
        self.refresh_graph_mapping_with_schema(graph, base_dir, None)
            .await
    }

    pub async fn refresh_graph_mapping_with_schema(
        &self,
        graph: &str,
        base_dir: &std::path::Path,
        graph_schema: Option<&crate::schema::CompiledGraphSchema>,
    ) -> Result<mapping::GraphMappingDescriptor> {
        let descriptor = self.graph_mapping_catalog.require(graph)?;
        for source_id in &descriptor.source_dependencies {
            self.refresh_source(source_id).await?;
        }
        let mut refreshed = mapping::new_descriptor(
            descriptor.graph.clone(),
            descriptor.manifest.clone(),
            Some(descriptor.clone()),
        );
        let plan = mapping::compile_virtual_graph_plan_with_catalog(
            &refreshed.manifest,
            base_dir,
            &self.source_catalog,
            graph_schema,
        )?;
        self.register_virtual_graph_descriptor(&mut refreshed, base_dir, &plan)
            .await?;
        if descriptor.published_graph.is_some()
            || matches!(descriptor.status, mapping::GraphMappingStatus::Compiled)
        {
            self.publish_graph_mapping_descriptor(&mut refreshed, base_dir, &plan)
                .await?;
        }
        self.graph_mapping_catalog.upsert(refreshed.clone())?;
        Ok(refreshed)
    }

    pub async fn load_graph_mapping_catalog_snapshot(
        &self,
        snapshot: mapping::GraphMappingCatalogSnapshot,
        base_dir: &std::path::Path,
    ) -> Result<Vec<String>> {
        self.graph_mapping_catalog.replace_all(snapshot)?;
        let mut errors = Vec::new();
        for mut descriptor in self.graph_mapping_catalog.list()? {
            let plan = match mapping::compile_virtual_graph_plan_with_catalog(
                &descriptor.manifest,
                base_dir,
                &self.source_catalog,
                None,
            ) {
                Ok(plan) => plan,
                Err(err) => {
                    descriptor.status = mapping::GraphMappingStatus::Error;
                    descriptor.last_error = Some(err.to_string());
                    self.graph_mapping_catalog.upsert(descriptor.clone())?;
                    errors.push(format!("{}: {}", descriptor.graph, err));
                    continue;
                }
            };
            match self
                .register_virtual_graph_descriptor(&mut descriptor, base_dir, &plan)
                .await
            {
                Ok(()) => {
                    if descriptor.published_graph.is_some()
                        || matches!(descriptor.status, mapping::GraphMappingStatus::Compiled)
                    {
                        if let Err(err) = self
                            .publish_graph_mapping_descriptor(&mut descriptor, base_dir, &plan)
                            .await
                        {
                            descriptor.status = mapping::GraphMappingStatus::Error;
                            descriptor.last_error = Some(err.to_string());
                            self.graph_mapping_catalog.upsert(descriptor.clone())?;
                            errors.push(format!("{}: {}", descriptor.graph, err));
                            continue;
                        }
                    }
                    self.graph_mapping_catalog.upsert(descriptor)?;
                }
                Err(err) => {
                    descriptor.status = mapping::GraphMappingStatus::Error;
                    descriptor.last_error = Some(err.to_string());
                    self.graph_mapping_catalog.upsert(descriptor.clone())?;
                    errors.push(format!("{}: {}", descriptor.graph, err));
                }
            }
        }
        Ok(errors)
    }

    async fn register_virtual_graph_descriptor(
        &self,
        descriptor: &mut mapping::GraphMappingDescriptor,
        base_dir: &std::path::Path,
        plan: &mapping::CompiledVirtualGraphPlan,
    ) -> Result<()> {
        self.ensure_mapping_sources_registered(&descriptor.manifest, base_dir, plan)
            .await?;
        self.register_virtual_graph_views_from_plan(&descriptor.manifest, base_dir, plan)
            .await?;
        descriptor.virtual_binding = Some(mapping::VirtualGraphBinding {
            graph: descriptor.graph.clone(),
            vertex_table: plan.vertex_table.clone(),
            edge_table: plan.edge_table.clone(),
            node_view_names: plan.node_views.iter().map(|plan| plan.view_name.clone()).collect(),
            edge_view_names: plan.edge_views.iter().map(|plan| plan.view_name.clone()).collect(),
            identity_view_names: plan
                .identity_views
                .iter()
                .map(|plan| plan.view_name.clone())
                .collect(),
            reference_view_names: plan
                .reference_views
                .iter()
                .map(|plan| plan.view_name.clone())
                .collect(),
            source_dependencies: plan.source_dependencies.clone(),
            source_fingerprints: self.source_fingerprints(&plan.source_dependencies)?,
            registered_unix_seconds: unix_seconds_local(),
        });
        descriptor.identity_summary = Some(self.build_identity_summary(plan));
        descriptor.source_dependencies = plan.source_dependencies.clone();
        descriptor.status = mapping::GraphMappingStatus::Registered;
        descriptor.last_error = None;
        descriptor.updated_unix_seconds = unix_seconds_local();
        if descriptor.last_report.is_none() {
            descriptor.last_report = Some(mapping::MappingCompileReport {
                vertex_plans: plan.node_views.len(),
                edge_plans: plan.edge_views.len(),
                warnings: plan.warnings.clone(),
                ..Default::default()
            });
        }
        Ok(())
    }

    async fn publish_graph_mapping_descriptor(
        &self,
        descriptor: &mut mapping::GraphMappingDescriptor,
        base_dir: &std::path::Path,
        plan: &mapping::CompiledVirtualGraphPlan,
    ) -> Result<()> {
        let compile_started = std::time::Instant::now();
        let vertices = self.collect_view_batches(&plan.vertex_table).await?;
        let edges = self.collect_view_batches(&plan.edge_table).await?;
        let graph_root = join_uri(
            &join_uri(&self.config.work_dir, "graph_mappings"),
            &descriptor.graph,
        );
        let vertices_uri = join_uri(&graph_root, "vertices");
        let edges_uri = join_uri(&graph_root, "edges");
        let adjacency_uri = join_uri(&graph_root, "adjacency");
        std::fs::create_dir_all(&vertices_uri)?;
        std::fs::create_dir_all(&edges_uri)?;
        std::fs::create_dir_all(&adjacency_uri)?;
        write_batches_as_graph_storage(&vertices_uri, "vertices", &vertices).await?;
        write_batches_as_graph_storage(&edges_uri, "edges", &edges).await?;
        let csr = build_csr_from_graph_batches(&vertices, &edges);
        write_csr_to_vortex(&csr, &adjacency_uri).await?;
        self.register_graph_vortex(&descriptor.graph, &vertices_uri, &edges_uri)
            .await?;
        let compile_duration_ms = compile_started.elapsed().as_millis();
        let source_fingerprints = self.source_fingerprints(&plan.source_dependencies)?;
        self.upsert_graph_catalog_from_batches(GraphCatalogInput {
            name: descriptor.graph.clone(),
            source_kind: graph::GraphSourceKind::MappingManifest,
            vertices_uri: vertices_uri.clone(),
            edges_uri: edges_uri.clone(),
            adjacency_uri: Some(adjacency_uri),
            vertex_batches: vertices.clone(),
            edge_batches: edges.clone(),
            source_dependencies: plan.source_dependencies.clone(),
            source_fingerprints,
            mapping_graph: descriptor.manifest.graph.clone(),
            mapping_version: Some(descriptor.manifest.version),
            compile_duration_ms: Some(compile_duration_ms),
        })?;
        let compatibility_report = mapping::compile_mapping_manifest_with_catalog(
            &descriptor.manifest,
            base_dir,
            &self.source_catalog,
        )?
        .report;
        descriptor.status = mapping::GraphMappingStatus::Compiled;
        descriptor.published_graph = Some(descriptor.graph.clone());
        descriptor.last_report = Some(mapping::MappingCompileReport {
            vertices: vertices.iter().map(RecordBatch::num_rows).sum(),
            edges: edges.iter().map(RecordBatch::num_rows).sum(),
            vertex_plans: plan.node_views.len(),
            edge_plans: plan.edge_views.len(),
            vertex_labels: label_counts_from_batches(&vertices),
            edge_labels: label_counts_from_batches(&edges),
            filtered_vertex_rows: compatibility_report.filtered_vertex_rows,
            filtered_edge_rows: compatibility_report.filtered_edge_rows,
            duplicate_vertices: compatibility_report.duplicate_vertices,
            duplicate_edges: compatibility_report.duplicate_edges,
            unresolved_edge_endpoints: compatibility_report.unresolved_edge_endpoints,
            coercion_failures: compatibility_report.coercion_failures,
            provenance_missing: compatibility_report.provenance_missing,
            vertices_uri: Some(vertices_uri),
            edges_uri: Some(edges_uri),
            warnings: plan.warnings.clone(),
            ..Default::default()
        });
        descriptor.last_error = None;
        descriptor.updated_unix_seconds = unix_seconds_local();
        Ok(())
    }

    pub fn compile_graph_mapping_sql_preview(
        &self,
        manifest: &mapping::GraphMappingManifest,
        base_dir: &std::path::Path,
    ) -> Result<mapping::CompiledVirtualGraphPlan> {
        mapping::compile_virtual_graph_plan_with_catalog(
            manifest,
            base_dir,
            &self.source_catalog,
            None,
        )
    }

    pub fn get_virtual_graph_descriptor(
        &self,
        graph: &str,
    ) -> Result<mapping::GraphMappingDescriptor> {
        self.graph_mapping_catalog.require(graph)
    }

    pub async fn list_virtual_graph_views(
        &self,
        graph: &str,
    ) -> Result<Vec<(String, Vec<String>)>> {
        let descriptor = self.graph_mapping_catalog.require(graph)?;
        let binding = descriptor
            .virtual_binding
            .ok_or_else(|| anyhow!("graph `{graph}` has no registered virtual views"))?;
        let mut names = vec![binding.vertex_table, binding.edge_table];
        names.extend(binding.node_view_names);
        names.extend(binding.edge_view_names);
        names.extend(binding.identity_view_names);
        names.extend(binding.reference_view_names);
        let mut out = Vec::new();
        for name in names {
            let dataframe = self.session.table(&name).await?;
            let schema = dataframe.schema();
            let columns = schema
                .fields()
                .iter()
                .map(|field| field.name().to_string())
                .collect::<Vec<_>>();
            out.push((name, columns));
        }
        Ok(out)
    }

    pub async fn get_virtual_graph_identity_summary(
        &self,
        graph: &str,
    ) -> Result<mapping::VirtualGraphIdentitySummary> {
        let descriptor = self.graph_mapping_catalog.require(graph)?;
        let binding = descriptor
            .virtual_binding
            .clone()
            .ok_or_else(|| anyhow!("graph `{graph}` has no registered virtual views"))?;
        let mut summary = descriptor.identity_summary.unwrap_or_default();
        for label in &summary.labels {
            let duplicate_keys = self
                .session
                .table(label.view_name.as_str())
                .await?
                .filter(
                    datafusion::prelude::col("alias_key")
                        .is_not_null()
                        .and(datafusion::prelude::col("alias_key").not_eq(datafusion::prelude::lit(""))),
                )?
                .aggregate(
                    vec![
                        datafusion::prelude::col("alias_name"),
                        datafusion::prelude::col("alias_key"),
                    ],
                    vec![count_distinct(datafusion::prelude::col("canonical_id"))
                        .alias("canonical_count")],
                )?
                .filter(datafusion::prelude::col("canonical_count").gt(datafusion::prelude::lit(1_i64)))?;
            let count = self.dataframe_count(duplicate_keys).await?;
            if count > 0 {
                summary
                    .duplicate_alias_keys
                    .insert(label.label.clone(), count);
            }
        }
        for reference in &summary.references {
            let unresolved = self
                .dataframe_count(
                    self.session
                        .table(reference.view_name.as_str())
                        .await?
                        .filter(datafusion::prelude::col("resolution_status").eq(datafusion::prelude::lit("unresolved")))?,
                )
                .await?;
            let ambiguous = self
                .dataframe_count(
                    self.session
                        .table(reference.view_name.as_str())
                        .await?
                        .filter(datafusion::prelude::col("resolution_status").eq(datafusion::prelude::lit("ambiguous")))?,
                )
                .await?;
            if unresolved > 0 {
                summary
                    .unresolved_references
                    .insert(reference.name.clone(), unresolved);
            }
            if ambiguous > 0 {
                summary
                    .ambiguous_references
                    .insert(reference.name.clone(), ambiguous);
            }
        }
        let _ = binding;
        Ok(summary)
    }

    pub async fn query_virtual_graph_sql_batches(
        &self,
        graph: &str,
        sql: &str,
    ) -> Result<Vec<RecordBatch>> {
        let descriptor = self.graph_mapping_catalog.require(graph)?;
        self.validate_virtual_graph_sql_scope(&descriptor, sql)?;
        self.query_sql_batches(sql).await
    }

    pub async fn explain_virtual_graph_sql(
        &self,
        graph: &str,
        sql: &str,
    ) -> Result<String> {
        let descriptor = self.graph_mapping_catalog.require(graph)?;
        self.validate_virtual_graph_sql_scope(&descriptor, sql)?;
        self.explain_sql(sql).await
    }

    pub async fn preview_virtual_reference_rows(
        &self,
        graph: &str,
        reference_name: &str,
        limit: usize,
    ) -> Result<Vec<String>> {
        let descriptor = self.graph_mapping_catalog.require(graph)?;
        let binding = descriptor
            .virtual_binding
            .ok_or_else(|| anyhow!("graph `{graph}` has no registered virtual views"))?;
        let prefix = format!(
            "graph_{}_reference_",
            graph
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '_' })
                .collect::<String>()
                .trim_matches('_')
        );
        let view_name = binding
            .reference_view_names
            .into_iter()
            .find(|name| name == &format!("{prefix}{}", sanitize_output_name(reference_name)))
            .ok_or_else(|| anyhow!("reference view `{reference_name}` not found for graph `{graph}`"))?;
        let dataframe = self
            .session
            .table(view_name.as_str())
            .await?
            .limit(0, Some(limit))?;
        let batches = dataframe.collect().await?;
        json_rows::batches_to_json_rows(&batches)
    }

    fn validate_virtual_graph_sql_scope(
        &self,
        descriptor: &mapping::GraphMappingDescriptor,
        sql: &str,
    ) -> Result<()> {
        let binding = descriptor
            .virtual_binding
            .as_ref()
            .ok_or_else(|| anyhow!("graph `{}` has no registered virtual views", descriptor.graph))?;
        let mut allowed = HashSet::new();
        allowed.insert(binding.vertex_table.clone());
        allowed.insert(binding.edge_table.clone());
        allowed.extend(binding.node_view_names.iter().cloned());
        allowed.extend(binding.edge_view_names.iter().cloned());
        allowed.extend(binding.identity_view_names.iter().cloned());
        allowed.extend(binding.reference_view_names.iter().cloned());
        for relation in extract_relation_names(sql) {
            let graph_scoped = relation.starts_with("vertices_")
                || relation.starts_with("edges_")
                || relation.starts_with("graph_")
                || relation.starts_with("source_");
            if graph_scoped && !allowed.contains(&relation) {
                return Err(anyhow!(
                    "sql references relation `{relation}` which is not registered for graph `{}`",
                    descriptor.graph
                ));
            }
        }
        Ok(())
    }

    pub fn get_graph_descriptor(&self, graph: &str) -> Result<graph::GraphDescriptor> {
        self.graph_catalog.require(graph)
    }

    pub fn get_graph_columns(
        &self,
        graph: &str,
    ) -> Result<(Vec<graph::GraphColumn>, Vec<graph::GraphColumn>)> {
        let descriptor = self.graph_catalog.require(graph)?;
        Ok((
            descriptor.active_snapshot.vertex_columns,
            descriptor.active_snapshot.edge_columns,
        ))
    }

    pub fn get_graph_stats(&self, graph: &str) -> Result<graph::GraphStats> {
        Ok(self.graph_catalog.require(graph)?.active_snapshot.stats)
    }

    pub async fn load_graph_catalog_snapshot(
        &self,
        snapshot: graph::GraphCatalogSnapshot,
    ) -> Result<Vec<String>> {
        self.graph_catalog.replace_all(snapshot)?;
        let mut errors = Vec::new();
        for mut descriptor in self.graph_catalog.list()? {
            let vertices_uri = descriptor.active_snapshot.vertices_uri.clone();
            let edges_uri = descriptor.active_snapshot.edges_uri.clone();
            if let Err(err) = self
                .register_graph_vortex(&descriptor.name, &vertices_uri, &edges_uri)
                .await
            {
                descriptor.status = graph::GraphStatus::Error;
                descriptor.last_error = Some(err.to_string());
                self.graph_catalog.upsert(descriptor.clone())?;
                errors.push(format!("{}: {}", descriptor.name, err));
            }
        }
        Ok(errors)
    }

    pub(crate) fn source_fingerprints(
        &self,
        source_ids: &[String],
    ) -> Result<BTreeMap<String, String>> {
        let mut fingerprints = BTreeMap::new();
        for source_id in source_ids {
            if let Some(source) = self.source_catalog.get(source_id)? {
                if let Some(fingerprint) = source.stats.fingerprint {
                    fingerprints.insert(source_id.clone(), fingerprint);
                }
            }
        }
        Ok(fingerprints)
    }

    pub(crate) fn upsert_graph_catalog_from_batches(&self, input: GraphCatalogInput) -> Result<()> {
        let existing = self.graph_catalog.get(&input.name)?;
        let now = unix_seconds_local();
        let version = existing
            .as_ref()
            .map(|descriptor| descriptor.active_version + 1)
            .unwrap_or(1);
        let vertex_columns = graph_columns_from_batches(&input.vertex_batches, true);
        let edge_columns = graph_columns_from_batches(&input.edge_batches, false);
        let vertex_labels = label_counts_from_batches(&input.vertex_batches);
        let edge_labels = label_counts_from_batches(&input.edge_batches);
        let vertex_count = input.vertex_batches.iter().map(RecordBatch::num_rows).sum();
        let edge_count = input.edge_batches.iter().map(RecordBatch::num_rows).sum();
        let vertex_file_bytes = directory_file_size(&input.vertices_uri).ok();
        let edge_file_bytes = directory_file_size(&input.edges_uri).ok();
        let storage_paths = vec![input.vertices_uri.clone(), input.edges_uri.clone()]
            .into_iter()
            .chain(input.adjacency_uri.clone())
            .collect::<Vec<_>>();
        let descriptor = graph::GraphDescriptor {
            name: input.name.clone(),
            active_version: version,
            status: graph::GraphStatus::Active,
            source_kind: input.source_kind.clone(),
            vertex_table: format!("vertices_{}", input.name),
            edge_table: format!("edges_{}", input.name),
            active_snapshot: graph::GraphSnapshot {
                version,
                vertices_uri: input.vertices_uri.clone(),
                edges_uri: input.edges_uri.clone(),
                adjacency_uri: input.adjacency_uri.clone(),
                vertex_table: format!("vertices_{}", input.name),
                edge_table: format!("edges_{}", input.name),
                mapping_graph: input.mapping_graph.clone(),
                mapping_version: input.mapping_version,
                source_dependencies: input.source_dependencies.clone(),
                source_fingerprints: input.source_fingerprints,
                vertex_columns,
                edge_columns,
                stats: graph::GraphStats {
                    vertices: vertex_count,
                    edges: edge_count,
                    vertex_labels,
                    edge_labels,
                    vertex_file_bytes,
                    edge_file_bytes,
                    compile_duration_ms: input.compile_duration_ms,
                },
                provenance: graph::GraphProvenance {
                    source_kind: Some(input.source_kind.clone()),
                    mapping_graph: input.mapping_graph,
                    source_ids: input.source_dependencies,
                    storage_paths,
                },
                created_unix_seconds: now,
            },
            created_unix_seconds: existing
                .as_ref()
                .map(|descriptor| descriptor.created_unix_seconds)
                .unwrap_or(now),
            updated_unix_seconds: now,
            last_error: None,
        };
        self.graph_catalog.upsert(descriptor)
    }

    async fn explain_sql(&self, sql: &str) -> Result<String> {
        let explain_sql = format!("EXPLAIN VERBOSE {sql}");
        let batches = self.query_sql_batches(&explain_sql).await?;
        let mut lines = Vec::new();
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let mut values = Vec::new();
                for field in batch.schema().fields() {
                    if let Some(array) = batch.column_by_name(field.name()) {
                        values.push(arrow_value_to_string(array.as_ref(), row_idx));
                    }
                }
                lines.push(values.join(" | "));
            }
        }
        Ok(lines.join("\n"))
    }

    async fn dataframe_count(&self, dataframe: datafusion::prelude::DataFrame) -> Result<usize> {
        let batches = dataframe
            .aggregate(
                vec![],
                vec![count(datafusion::prelude::lit(1)).alias("count")],
            )?
            .collect()
            .await?;
        let Some(batch) = batches.first() else {
            return Ok(0);
        };
        let Some(array) = batch.column_by_name("count") else {
            return Ok(0);
        };
        Ok(arrow_value_to_string(array.as_ref(), 0)
            .parse::<usize>()
            .unwrap_or_default())
    }

    async fn explain_mapping_view(
        &self,
        dataframe: datafusion::prelude::DataFrame,
    ) -> Result<String> {
        let plan = dataframe.into_unoptimized_plan();
        Ok(format!("{plan:?}"))
    }

    async fn dataframe_rows(
        &self,
        dataframe: datafusion::prelude::DataFrame,
    ) -> Result<Vec<HashMap<String, String>>> {
        let batches = dataframe.collect().await?;
        let mut rows = Vec::new();
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let mut row = HashMap::new();
                for field in batch.schema().fields() {
                    if let Some(array) = batch.column_by_name(field.name()) {
                        row.insert(
                            field.name().clone(),
                            arrow_value_to_string(array.as_ref(), row_idx),
                        );
                    }
                }
                rows.push(row);
            }
        }
        Ok(rows)
    }

    fn resolve_mapping_source_descriptor(
        &self,
        source: &mapping::SourceMapping,
        alias: &str,
        base_dir: &std::path::Path,
    ) -> Result<source::SourceDescriptor> {
        if let Some(source_id) = &source.source {
            return self.source_catalog.require(source_id);
        }
        let path = source
            .path
            .as_deref()
            .ok_or_else(|| anyhow!("mapping source `{alias}` missing path"))?;
        let registration = source::SourceRegistration {
            id: alias.to_string(),
            display_name: None,
            format: source.format.clone(),
            location: source::SourceLocation::Local {
                path: mapping::resolve_path(base_dir, path).to_string_lossy().to_string(),
            },
            read_options: source::ReadOptions::default(),
        };
        let (descriptor, _) = source::infer_and_read_source(registration)?;
        Ok(descriptor)
    }

    async fn build_node_dataframe_for_plan(
        &self,
        manifest: &mapping::GraphMappingManifest,
        mapping_index: usize,
        base_dir: &std::path::Path,
    ) -> Result<datafusion::prelude::DataFrame> {
        let mapping = &manifest.vertices[mapping_index];
        let source = self.resolve_mapping_source_descriptor(
            &manifest.sources[&mapping.source],
            &mapping.source,
            base_dir,
        )?;
        mapping::build_node_view_dataframe(&self.session, mapping, &source).await
    }

    async fn build_edge_dataframe_for_plan(
        &self,
        manifest: &mapping::GraphMappingManifest,
        mapping_index: usize,
        base_dir: &std::path::Path,
    ) -> Result<datafusion::prelude::DataFrame> {
        let mapping = &manifest.edges[mapping_index];
        let source = self.resolve_mapping_source_descriptor(
            &manifest.sources[&mapping.source],
            &mapping.source,
            base_dir,
        )?;
        mapping::build_edge_view_dataframe(&self.session, mapping, &source).await
    }

    async fn build_identity_dataframe_for_plan(
        &self,
        manifest: &mapping::GraphMappingManifest,
        _graph: &str,
        label: &str,
        base_dir: &std::path::Path,
    ) -> Result<datafusion::prelude::DataFrame> {
        let rule = manifest.identity.get(label);
        let mut vertices = Vec::new();
        for vertex in &manifest.vertices {
            if vertex.label != label {
                continue;
            }
            let source = self.resolve_mapping_source_descriptor(
                &manifest.sources[&vertex.source],
                &vertex.source,
                base_dir,
            )?;
            vertices.push((vertex, source));
        }
        let refs = vertices
            .iter()
            .map(|(vertex, source)| (*vertex, source))
            .collect::<Vec<_>>();
        mapping::build_identity_view_dataframe(&self.session, label, &refs, rule).await
    }

    async fn build_reference_dataframe_for_plan(
        &self,
        manifest: &mapping::GraphMappingManifest,
        graph: &str,
        name: &str,
        base_dir: &std::path::Path,
    ) -> Result<datafusion::prelude::DataFrame> {
        let reference = manifest
            .references
            .iter()
            .find(|reference| reference.name == name)
            .ok_or_else(|| anyhow!("reference `{name}` not found in manifest"))?;
        let source = self.resolve_mapping_source_descriptor(
            &manifest.sources[&reference.source],
            &reference.source,
            base_dir,
        )?;
        mapping::build_reference_view_dataframe(&self.session, graph, reference, &source)
            .await
    }

    async fn ensure_mapping_sources_registered(
        &self,
        manifest: &mapping::GraphMappingManifest,
        base_dir: &std::path::Path,
        plan: &mapping::CompiledVirtualGraphPlan,
    ) -> Result<()> {
        for node_view in &plan.node_views {
            self.ensure_mapping_source_registered(
                &manifest.sources[&node_view.source_alias],
                &node_view.source_alias,
                &node_view.source_table_name,
                base_dir,
                node_view.source_table_ref.as_ref(),
            )
            .await?;
        }
        for edge_view in &plan.edge_views {
            self.ensure_mapping_source_registered(
                &manifest.sources[&edge_view.source_alias],
                &edge_view.source_alias,
                &edge_view.source_table_name,
                base_dir,
                edge_view.source_table_ref.as_ref(),
            )
            .await?;
        }
        for reference_view in &plan.reference_views {
            self.ensure_mapping_source_registered(
                &manifest.sources[&reference_view.source_alias],
                &reference_view.source_alias,
                &reference_view.source_table_name,
                base_dir,
                reference_view.source_table_ref.as_ref(),
            )
            .await?;
        }
        Ok(())
    }

    async fn ensure_mapping_source_registered(
        &self,
        source: &mapping::SourceMapping,
        alias: &str,
        table_name: &str,
        base_dir: &std::path::Path,
        source_table_ref: Option<&source::SourceTableRef>,
    ) -> Result<()> {
        if let Some(source_id) = &source.source {
            let table_ref = source_table_ref.cloned().ok_or_else(|| {
                anyhow!("mapping source `{alias}` missing source table reference for `{source_id}`")
            })?;
            self.ensure_source_table_registered(&table_ref).await?;
            return Ok(());
        }
        let path = source
            .path
            .as_deref()
            .ok_or_else(|| anyhow!("mapping source `{alias}` missing path"))?;
        let registration = source::SourceRegistration {
            id: alias.to_string(),
            display_name: None,
            format: source.format.clone(),
            location: source::SourceLocation::Local {
                path: mapping::resolve_path(base_dir, path).to_string_lossy().to_string(),
            },
            read_options: source::ReadOptions::default(),
        };
        let (descriptor, tables) = source::infer_and_read_source(registration)?;
        let table = descriptor.require_single_table()?.clone();
        let table_rows = tables
            .into_iter()
            .find(|candidate| candidate.table_id == table.id)
            .ok_or_else(|| anyhow!("inline mapping source `{alias}` did not preload a table"))?;
        let batch = source::source_rows_to_batch_with_provenance(&table_rows)?;
        let _ = self.session.deregister_table(table_name);
        let provider = datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        self.session
            .register_table(table_name.to_string(), Arc::new(provider))?;
        Ok(())
    }

    async fn register_virtual_graph_views_from_plan(
        &self,
        manifest: &mapping::GraphMappingManifest,
        base_dir: &std::path::Path,
        plan: &mapping::CompiledVirtualGraphPlan,
    ) -> Result<()> {
        for node_view in &plan.node_views {
            let dataframe = self
                .build_node_dataframe_for_plan(manifest, node_view.mapping_index, base_dir)
                .await?;
            let _ = self.session.deregister_table(node_view.view_name.as_str());
            self.session
                .register_table(node_view.view_name.clone(), dataframe.into_view())?;
        }
        for edge_view in &plan.edge_views {
            let dataframe = self
                .build_edge_dataframe_for_plan(manifest, edge_view.mapping_index, base_dir)
                .await?;
            let _ = self.session.deregister_table(edge_view.view_name.as_str());
            self.session
                .register_table(edge_view.view_name.clone(), dataframe.into_view())?;
        }
        for identity_view in &plan.identity_views {
            let dataframe = self
                .build_identity_dataframe_for_plan(
                    manifest,
                    &plan.graph,
                    identity_view.label.as_str(),
                    base_dir,
                )
                .await?;
            let _ = self.session.deregister_table(identity_view.view_name.as_str());
            self.session
                .register_table(identity_view.view_name.clone(), dataframe.into_view())?;
        }
        for reference_view in &plan.reference_views {
            let dataframe = self
                .build_reference_dataframe_for_plan(
                    manifest,
                    &plan.graph,
                    reference_view.name.as_str(),
                    base_dir,
                )
                .await?;
            let _ = self.session.deregister_table(reference_view.view_name.as_str());
            self.session
                .register_table(reference_view.view_name.clone(), dataframe.into_view())?;
        }
        self.register_union_view(
            &plan.vertex_table,
            &plan.vertex_columns,
            &plan
                .node_views
                .iter()
                .map(|view| view.view_name.as_str())
                .collect::<Vec<_>>(),
        )
        .await?;
        self.register_union_view(
            &plan.edge_table,
            &plan.edge_columns,
            &plan
                .edge_views
                .iter()
                .map(|view| view.view_name.as_str())
                .collect::<Vec<_>>(),
        )
        .await?;
        Ok(())
    }

    async fn register_union_view(
        &self,
        table_name: &str,
        columns: &[String],
        component_view_names: &[&str],
    ) -> Result<()> {
        let dataframe = if component_view_names.is_empty() {
            self.empty_dataframe(columns)?
        } else {
            let mut union_df: Option<datafusion::prelude::DataFrame> = None;
            for view_name in component_view_names {
                let dataframe = self.normalize_view_dataframe(view_name, columns).await?;
                union_df = Some(match union_df {
                    Some(existing) => existing.union(dataframe)?,
                    None => dataframe,
                });
            }
            let select_exprs = columns
                .iter()
                .map(|column| datafusion::prelude::ident(column))
                .collect::<Vec<_>>();
            union_df
                .ok_or_else(|| anyhow!("no component views for `{table_name}`"))?
                .distinct_on(
                    vec![datafusion::prelude::ident("id")],
                    select_exprs,
                    Some(vec![
                        datafusion::prelude::ident("id").sort(true, true),
                        datafusion::prelude::ident("source_row").sort(true, true),
                    ]),
                )?
        };
        let _ = self.session.deregister_table(table_name);
        self.session
            .register_table(table_name.to_string(), dataframe.into_view())?;
        Ok(())
    }

    async fn normalize_view_dataframe(
        &self,
        view_name: &str,
        columns: &[String],
    ) -> Result<datafusion::prelude::DataFrame> {
        let mut dataframe = self.session.table(view_name).await?;
        let existing = dataframe
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect::<HashSet<_>>();
        for column in columns {
            if !existing.contains(column) {
                dataframe = dataframe.with_column(column, Self::default_virtual_graph_expr(column))?;
            }
        }
        let projections = columns
            .iter()
            .map(|column| datafusion::prelude::ident(column))
            .collect::<Vec<_>>();
        dataframe.select(projections).map_err(Into::into)
    }

    fn default_virtual_graph_expr(column: &str) -> datafusion::prelude::Expr {
        match column {
            "payload_bin" | "payload_json_bin" => datafusion::prelude::Expr::Literal(
                datafusion::scalar::ScalarValue::Binary(None),
                None,
            ),
            _ => datafusion::prelude::lit(""),
        }
    }

    fn empty_dataframe(&self, columns: &[String]) -> Result<datafusion::prelude::DataFrame> {
        let schema = Arc::new(arrow::datatypes::Schema::new(
            columns
                .iter()
                .map(|column| arrow::datatypes::Field::new(column, arrow::datatypes::DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ));
        let arrays = columns
            .iter()
            .map(|_| Arc::new(arrow::array::StringArray::from(Vec::<String>::new())) as arrow::array::ArrayRef)
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(schema, arrays)?;
        self.session.read_batch(batch).map_err(Into::into)
    }

    async fn collect_view_batches(&self, table_name: &str) -> Result<Vec<RecordBatch>> {
        self.session.table(table_name).await?.collect().await.map_err(Into::into)
    }

    fn build_identity_summary(
        &self,
        plan: &mapping::CompiledVirtualGraphPlan,
    ) -> mapping::VirtualGraphIdentitySummary {
        mapping::VirtualGraphIdentitySummary {
            labels: plan
                .identity_views
                .iter()
                .map(|view| mapping::IdentityLabelSummary {
                    label: view.label.clone(),
                    view_name: view.view_name.clone(),
                    alias_names: view.alias_names.clone(),
                })
                .collect(),
            references: plan
                .reference_views
                .iter()
                .map(|view| mapping::ReferenceSummary {
                    name: view.name.clone(),
                    view_name: view.view_name.clone(),
                    from_label: view.from_label.clone(),
                    to_label: view.to_label.clone(),
                })
                .collect(),
            ..Default::default()
        }
    }

    async fn validate_mapping_semantics(
        &self,
        manifest: &mapping::GraphMappingManifest,
        plan: &mapping::CompiledVirtualGraphPlan,
        report: &mut mapping::MappingValidationReport,
    ) -> Result<()> {
        let mut vertices = Vec::<HashMap<String, String>>::new();
        let mut edges_by_plan = Vec::<Vec<HashMap<String, String>>>::new();
        let mut vertex_ids = HashSet::<String>::new();
        let mut vertex_label_ids = HashSet::<(String, String)>::new();
        let mut seen_edges = HashSet::<String>::new();

        for vertex_plan in &plan.node_views {
            let input_count = self
                .dataframe_count(self.session.table(vertex_plan.source_table_name.as_str()).await?)
                .await?;
            report
                .metrics
                .input_rows
                .insert(vertex_plan.mapping_path.clone(), input_count);
            let rows = self
                .dataframe_rows(self.session.table(vertex_plan.view_name.as_str()).await?)
                .await?;
            report
                .metrics
                .emitted_rows
                .insert(vertex_plan.mapping_path.clone(), rows.len());
            for row in rows {
                let id = row.get("id").cloned().unwrap_or_default();
                if id.trim().is_empty() {
                    report.metrics.filtered_empty_ids += 1;
                    if manifest.validation.empty_ids == mapping::EmptyIdPolicy::Error {
                        report.errors.push(mapping::MappingValidationError {
                            path: format!("{}.id", vertex_plan.mapping_path),
                            message: "vertex id evaluated to an empty value".to_string(),
                        });
                    }
                    continue;
                }
                if !vertex_ids.insert(id.clone()) {
                    report.metrics.duplicate_vertex_ids += 1;
                }
                let label = row.get("label").cloned().unwrap_or_default();
                vertex_label_ids.insert((label, id));
                validate_row_provenance(&row, &vertex_plan.mapping_path, report);
                self.validate_row_coercions(manifest, &vertex_plan.mapping_path, &row, report);
                vertices.push(row);
            }
        }

        for edge_plan in &plan.edge_views {
            let input_count = self
                .dataframe_count(self.session.table(edge_plan.source_table_name.as_str()).await?)
                .await?;
            report
                .metrics
                .input_rows
                .insert(edge_plan.mapping_path.clone(), input_count);
            let rows = self
                .dataframe_rows(self.session.table(edge_plan.view_name.as_str()).await?)
                .await?;
            report
                .metrics
                .emitted_rows
                .insert(edge_plan.mapping_path.clone(), rows.len());
            for row in &rows {
                let id = row.get("id").cloned().unwrap_or_default();
                let from = row.get("from_id").cloned().unwrap_or_default();
                let to = row.get("to_id").cloned().unwrap_or_default();
                if id.trim().is_empty() || from.trim().is_empty() || to.trim().is_empty() {
                    report.metrics.filtered_empty_ids += 1;
                    if manifest.validation.empty_ids == mapping::EmptyIdPolicy::Error {
                        report.errors.push(mapping::MappingValidationError {
                            path: edge_plan.mapping_path.clone(),
                            message: "edge id or endpoint evaluated to an empty value".to_string(),
                        });
                    }
                }
                if !seen_edges.insert(id) {
                    report.metrics.duplicate_edge_ids += 1;
                }
                validate_row_provenance(row, &edge_plan.mapping_path, report);
                self.validate_row_coercions(manifest, &edge_plan.mapping_path, row, report);
            }
            edges_by_plan.push(rows);
        }

        if report.metrics.duplicate_vertex_ids > 0 {
            match manifest.validation.duplicate_vertex_ids {
                mapping::DuplicateIdPolicy::Error => {
                    report.errors.push(mapping::MappingValidationError {
                        path: "validation.duplicate_vertex_ids".to_string(),
                        message: format!(
                            "{} duplicate vertex ids found",
                            report.metrics.duplicate_vertex_ids
                        ),
                    })
                }
                mapping::DuplicateIdPolicy::First => report.warnings.push(format!(
                    "{} duplicate vertex rows will be skipped by first occurrence policy",
                    report.metrics.duplicate_vertex_ids
                )),
            }
        }
        if report.metrics.duplicate_edge_ids > 0 {
            match manifest.validation.duplicate_edge_ids {
                mapping::DuplicateIdPolicy::Error => {
                    report.errors.push(mapping::MappingValidationError {
                        path: "validation.duplicate_edge_ids".to_string(),
                        message: format!(
                            "{} duplicate edge ids found",
                            report.metrics.duplicate_edge_ids
                        ),
                    })
                }
                mapping::DuplicateIdPolicy::First => report.warnings.push(format!(
                    "{} duplicate edge rows will be skipped by first occurrence policy",
                    report.metrics.duplicate_edge_ids
                )),
            }
        }

        for (idx, rows) in edges_by_plan.iter().enumerate() {
            let Some(edge) = manifest.edges.get(idx) else {
                continue;
            };
            let from_label = edge.from_label.as_deref().unwrap_or_default().to_string();
            let to_label = edge.to_label.as_deref().unwrap_or_default().to_string();
            for row in rows {
                let from = row.get("from_id").cloned().unwrap_or_default();
                let to = row.get("to_id").cloned().unwrap_or_default();
                let mut missing = Vec::new();
                if !vertex_label_ids.contains(&(from_label.clone(), from.clone())) {
                    missing.push(("from", from_label.clone(), from));
                }
                if !vertex_label_ids.contains(&(to_label.clone(), to.clone())) {
                    missing.push(("to", to_label.clone(), to));
                }
                for (side, label, id) in missing {
                    report.metrics.unresolved_edge_endpoints += 1;
                    *report
                        .metrics
                        .unresolved_edge_endpoint_counts
                        .entry(format!("edges[{idx}].{side}:{label}"))
                        .or_default() += 1;
                    match manifest.validation.missing_edge_endpoints {
                        mapping::MissingEndpointPolicy::Error => {
                            report.errors.push(mapping::MappingValidationError {
                                path: format!("edges[{idx}].{side}"),
                                message: format!(
                                    "edge endpoint `{id}` does not resolve to vertex label `{label}`"
                                ),
                            });
                        }
                        mapping::MissingEndpointPolicy::Warn => {
                            report.warnings.push(format!(
                                "edge endpoint `{id}` does not resolve to vertex label `{label}`"
                            ));
                        }
                        mapping::MissingEndpointPolicy::Skip => {}
                    }
                }
            }
        }
        for identity_view in &plan.identity_views {
            let duplicate_count = self
                .dataframe_count(
                    self.session
                        .table(identity_view.view_name.as_str())
                        .await?
                        .filter(
                            datafusion::prelude::col("alias_key")
                                .is_not_null()
                                .and(datafusion::prelude::col("alias_key").not_eq(datafusion::prelude::lit(""))),
                        )?
                        .aggregate(
                            vec![
                                datafusion::prelude::col("alias_name"),
                                datafusion::prelude::col("alias_key"),
                            ],
                            vec![count_distinct(datafusion::prelude::col("canonical_id"))
                                .alias("canonical_count")],
                        )?
                        .filter(
                            datafusion::prelude::col("canonical_count")
                                .gt(datafusion::prelude::lit(1_i64)),
                        )?,
                )
                .await?;
            if duplicate_count > 0 {
                report
                    .metrics
                    .duplicate_alias_keys
                    .insert(identity_view.label.clone(), duplicate_count);
                match manifest.validation.duplicate_alias_keys {
                    mapping::AliasConflictPolicy::Error => {
                        report.errors.push(mapping::MappingValidationError {
                            path: format!("identity.{}", identity_view.label),
                            message: format!(
                                "{} duplicate alias keys found for label `{}`",
                                duplicate_count, identity_view.label
                            ),
                        });
                    }
                    mapping::AliasConflictPolicy::Warn => report.warnings.push(format!(
                        "{} duplicate alias keys found for label `{}`",
                        duplicate_count, identity_view.label
                    )),
                    mapping::AliasConflictPolicy::First => report.warnings.push(format!(
                        "{} duplicate alias keys found for label `{}`; first match policy will apply",
                        duplicate_count, identity_view.label
                    )),
                }
            }
        }
        for reference_view in &plan.reference_views {
            let unresolved_count = self
                .dataframe_count(
                    self.session
                        .table(reference_view.view_name.as_str())
                        .await?
                        .filter(
                            datafusion::prelude::col("resolution_status")
                                .eq(datafusion::prelude::lit("unresolved")),
                        )?,
                )
                .await?;
            if unresolved_count > 0 {
                report.metrics.unresolved_references.insert(
                    reference_view.name.clone(),
                    unresolved_count,
                );
                match manifest.validation.unresolved_references {
                    mapping::ReferenceResolutionPolicy::Error => {
                        report.errors.push(mapping::MappingValidationError {
                            path: format!("references.{}", reference_view.name),
                            message: format!(
                                "{} unresolved references found for `{}`",
                                unresolved_count, reference_view.name
                            ),
                        });
                    }
                    mapping::ReferenceResolutionPolicy::Warn => report.warnings.push(format!(
                        "{} unresolved references found for `{}`",
                        unresolved_count, reference_view.name
                    )),
                    mapping::ReferenceResolutionPolicy::Skip => {}
                }
            }
            let ambiguous_count = self
                .dataframe_count(
                    self.session
                        .table(reference_view.view_name.as_str())
                        .await?
                        .filter(
                            datafusion::prelude::col("resolution_status")
                                .eq(datafusion::prelude::lit("ambiguous")),
                        )?,
                )
                .await?;
            if ambiguous_count > 0 {
                report.metrics.ambiguous_references.insert(
                    reference_view.name.clone(),
                    ambiguous_count,
                );
                match manifest.validation.ambiguous_references {
                    mapping::AmbiguousReferencePolicy::Error => {
                        report.errors.push(mapping::MappingValidationError {
                            path: format!("references.{}", reference_view.name),
                            message: format!(
                                "{} ambiguous references found for `{}`",
                                ambiguous_count, reference_view.name
                            ),
                        });
                    }
                    mapping::AmbiguousReferencePolicy::Warn => report.warnings.push(format!(
                        "{} ambiguous references found for `{}`",
                        ambiguous_count, reference_view.name
                    )),
                    mapping::AmbiguousReferencePolicy::First => report.warnings.push(format!(
                        "{} ambiguous references found for `{}`; first match policy will apply",
                        ambiguous_count, reference_view.name
                    )),
                }
            }
        }
        let _ = vertices;
        Ok(())
    }

    fn validate_row_coercions(
        &self,
        manifest: &mapping::GraphMappingManifest,
        mapping_path: &str,
        row: &HashMap<String, String>,
        report: &mut mapping::MappingValidationReport,
    ) {
        let typed_columns = typed_columns_for_path(manifest, mapping_path);
        for (column, output_type) in typed_columns {
            let value = row.get(&column).cloned().unwrap_or_default();
            if value.is_empty() || value_coerces_to_type(&value, &output_type) {
                continue;
            }
            *report
                .metrics
                .coercion_failures
                .entry(column.clone())
                .or_default() += 1;
            if manifest.validation.type_coercion == mapping::TypeCoercionPolicy::Strict {
                report.errors.push(mapping::MappingValidationError {
                    path: format!("{mapping_path}.columns.{column}.type"),
                    message: format!("value `{value}` cannot be coerced to {output_type:?}"),
                });
            } else {
                report.warnings.push(format!(
                    "{mapping_path}.{column} value `{value}` cannot be coerced to {output_type:?}"
                ));
            }
        }
    }
}

fn sanitize_output_name(value: &str) -> String {
    let out = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string();
    if out.is_empty() {
        "view".to_string()
    } else {
        out
    }
}

fn extract_relation_names(sql: &str) -> Vec<String> {
    let normalized = sql.replace(',', " , ").replace('\n', " ");
    let tokens = normalized
        .split_whitespace()
        .map(|token| token.trim().trim_matches(|ch| ch == '"' || ch == ';' || ch == ','))
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();
    let mut names = Vec::new();
    let mut idx = 0;
    while idx + 1 < tokens.len() {
        let token = tokens[idx].to_ascii_lowercase();
        if token == "from" || token == "join" {
            let candidate = tokens[idx + 1];
            if !candidate.starts_with('(') && !candidate.eq_ignore_ascii_case("select") {
                let relation = candidate
                    .split('.')
                    .next_back()
                    .unwrap_or(candidate)
                    .trim_matches('"')
                    .to_string();
                names.push(relation);
            }
        }
        idx += 1;
    }
    names
}
