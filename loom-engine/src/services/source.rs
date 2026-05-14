use crate::*;
use std::collections::HashMap;

impl Engine {
    pub async fn register_source(
        &self,
        registration: source::SourceRegistration,
    ) -> Result<source::SourceDescriptor> {
        let (mut descriptor, preloaded_tables) = source::infer_and_read_source(registration)?;
        if let Some(existing) = self.source_catalog.get(&descriptor.id)? {
            descriptor.created_unix_seconds = existing.created_unix_seconds;
        }
        if descriptor.tables.len() == 1 {
            let table_ref = source::SourceTableRef {
                source_id: descriptor.id.clone(),
                table_id: descriptor.tables[0].id.clone(),
            };
            let preloaded = preloaded_tables.first();
            self.register_source_table_from_descriptor(&descriptor, &table_ref, preloaded)
                .await?;
            if let Some(table) = descriptor.tables.get_mut(0) {
                table.registered = true;
                descriptor.registered = Some(true);
                descriptor.table_name = Some(table.table_name.clone());
                descriptor.schema = Some(table.schema.clone());
            }
        }
        self.source_catalog.upsert(descriptor.clone())?;
        Ok(descriptor)
    }

    pub async fn refresh_source(&self, id: &str) -> Result<source::SourceDescriptor> {
        let existing = self.source_catalog.require(id)?;
        let registration = source::SourceRegistration {
            id: existing.id.clone(),
            display_name: existing.display_name.clone(),
            format: existing.format.clone(),
            location: existing.location.clone(),
            read_options: existing.read_options.clone(),
        };
        self.register_source(registration).await
    }

    pub fn list_sources(&self) -> Result<Vec<source::SourceDescriptor>> {
        self.source_catalog.list()
    }

    pub fn get_source(&self, id: &str) -> Result<source::SourceDescriptor> {
        self.source_catalog.require(id)
    }

    pub fn list_source_tables(&self, source_id: &str) -> Result<Vec<source::SourceTableDescriptor>> {
        self.source_catalog.list_tables(source_id)
    }

    pub fn get_source_table(
        &self,
        source_id: &str,
        table_id: &str,
    ) -> Result<source::SourceTableDescriptor> {
        self.source_catalog.require_table(source_id, table_id)
    }

    pub async fn ensure_source_table_registered(
        &self,
        table_ref: &source::SourceTableRef,
    ) -> Result<source::SourceTableDescriptor> {
        let table = self
            .source_catalog
            .require_table(&table_ref.source_id, &table_ref.table_id)?;
        if table.registered {
            return Ok(table);
        }
        if matches!(table.kind, source::SourceTableKind::Derived) {
            return Box::pin(self.ensure_transform_table_registered(table_ref)).await;
        }
        let descriptor = self.source_catalog.require(&table_ref.source_id)?;
        self.register_source_table_from_descriptor(&descriptor, table_ref, None)
            .await?;
        self.source_catalog.set_table_registered(table_ref, true)?;
        self.source_catalog
            .require_table(&table_ref.source_id, &table_ref.table_id)
    }

    pub fn sample_source(&self, id: &str, limit: usize) -> Result<Vec<HashMap<String, String>>> {
        let descriptor = self.source_catalog.require(id)?;
        let table = source::read_source_rows(&descriptor, Some(limit))?;
        Ok(table.rows.into_iter().map(|row| row.values).collect())
    }

    pub async fn sample_source_table(
        &self,
        source_id: &str,
        table_id: &str,
        limit: usize,
    ) -> Result<Vec<HashMap<String, String>>> {
        let table_ref = source::SourceTableRef {
            source_id: source_id.to_string(),
            table_id: table_id.to_string(),
        };
        self.ensure_source_table_registered(&table_ref).await?;
        let table = self.resolve_source_table_rows(&table_ref, Some(limit)).await?;
        Ok(table.rows.into_iter().map(|row| row.values).collect())
    }

    pub fn profile_source(&self, id: &str) -> Result<source::SourceProfile> {
        self.profile_source_with_schema(id, None)
    }

    pub fn profile_source_with_schema(
        &self,
        id: &str,
        graph_schema: Option<&crate::schema::CompiledGraphSchema>,
    ) -> Result<source::SourceProfile> {
        let descriptor = self.source_catalog.require(id)?;
        let table = source::read_source_rows(&descriptor, Some(1000))?;
        Ok(source::profile_source_table_with_schema(
            &table,
            &descriptor.format,
            graph_schema,
        ))
    }

    pub async fn profile_source_table(
        &self,
        source_id: &str,
        table_id: &str,
    ) -> Result<source::SourceProfile> {
        self.profile_source_table_with_schema(source_id, table_id, None)
            .await
    }

    pub async fn profile_source_table_with_schema(
        &self,
        source_id: &str,
        table_id: &str,
        graph_schema: Option<&crate::schema::CompiledGraphSchema>,
    ) -> Result<source::SourceProfile> {
        let descriptor = self.source_catalog.require(source_id)?;
        let table_ref = source::SourceTableRef {
            source_id: source_id.to_string(),
            table_id: table_id.to_string(),
        };
        self.ensure_source_table_registered(&table_ref).await?;
        let table = self.resolve_source_table_rows(&table_ref, Some(1000)).await?;
        Ok(source::profile_source_table_with_schema(
            &table,
            &descriptor.format,
            graph_schema,
        ))
    }

    pub async fn suggest_graph_mapping(
        &self,
        graph: String,
        display_name: Option<String>,
        source_ids: Vec<String>,
        target_vocabulary: Option<String>,
        max_sample_rows: Option<usize>,
    ) -> Result<suggest::GraphSuggestionReport> {
        self.suggest_graph_mapping_with_schema(
            graph,
            display_name,
            source_ids,
            target_vocabulary,
            max_sample_rows,
            None,
        )
        .await
    }

    pub async fn suggest_graph_mapping_with_schema(
        &self,
        graph: String,
        display_name: Option<String>,
        source_ids: Vec<String>,
        target_vocabulary: Option<String>,
        max_sample_rows: Option<usize>,
        graph_schema: Option<&crate::schema::CompiledGraphSchema>,
    ) -> Result<suggest::GraphSuggestionReport> {
        let limit = max_sample_rows.unwrap_or(1000);
        let mut sources = Vec::new();
        let mut profiles = Vec::new();
        for source_id in source_ids {
            let descriptor = self.source_catalog.require(&source_id)?;
            let table = source::read_source_rows(&descriptor, Some(limit))?;
            let profile =
                source::profile_source_table_with_schema(&table, &descriptor.format, graph_schema);
            profiles.push(profile.clone());
            sources.push((descriptor, table, profile));
        }
        let (manifest, candidates, warnings) = if let Some(graph_schema) = graph_schema {
            suggest::build_schema_manifest_suggestion(
                suggest::SuggestionInput {
                    graph: graph.clone(),
                    display_name: display_name.clone(),
                    target_vocabulary: target_vocabulary.clone(),
                    sources,
                },
                graph_schema,
            )?
        } else {
            suggest::build_manifest_suggestion(suggest::SuggestionInput {
                graph: graph.clone(),
                display_name: display_name.clone(),
                target_vocabulary: target_vocabulary.clone(),
                sources,
            })?
        };
        let validation = self
            .validate_graph_mapping_full(&manifest, std::path::Path::new("."))
            .await;
        Ok(suggest::GraphSuggestionReport {
            graph,
            display_name,
            target_vocabulary: target_vocabulary.unwrap_or_else(|| "fhir_lite".to_string()),
            source_profiles: profiles,
            candidates,
            manifest,
            validation,
            warnings,
        })
    }

    pub async fn load_source_catalog_snapshot(
        &self,
        snapshot: source::SourceCatalogSnapshot,
    ) -> Result<Vec<String>> {
        self.source_catalog.replace_all(snapshot)?;
        let mut errors = Vec::new();
        for descriptor in self.source_catalog.list()? {
            if descriptor.tables.len() == 1 {
                let table_ref = source::SourceTableRef {
                    source_id: descriptor.id.clone(),
                    table_id: descriptor.tables[0].id.clone(),
                };
                if let Err(err) = self
                    .register_source_table_from_descriptor(&descriptor, &table_ref, None)
                    .await
                {
                    errors.push(format!("{}: {}", descriptor.id, err));
                } else {
                    self.source_catalog.set_table_registered(&table_ref, true)?;
                }
            }
        }
        Ok(errors)
    }

    async fn register_source_table_from_descriptor(
        &self,
        descriptor: &source::SourceDescriptor,
        table_ref: &source::SourceTableRef,
        preloaded: Option<&source::SourceTable>,
    ) -> Result<()> {
        let table = self
            .source_catalog
            .get_table(&table_ref.source_id, &table_ref.table_id)?
            .or_else(|| {
                descriptor
                    .tables
                    .iter()
                    .find(|candidate| candidate.id == table_ref.table_id)
                    .cloned()
            })
            .ok_or_else(|| {
                anyhow!(
                    "source `{}` does not contain table `{}`",
                    table_ref.source_id,
                    table_ref.table_id
                )
            })?;
        let table_name = &table.table_name;
        let _ = self.session.deregister_table(table_name.as_str());
        match descriptor.format {
            source::SourceFormat::Parquet => {
                let source::SourceLocation::Local { path } = &descriptor.location;
                self.session
                    .register_parquet(table_name, path, ParquetReadOptions::default())
                    .await?;
            }
            _ => {
                let table_rows = match preloaded {
                    Some(table_rows) => table_rows.clone(),
                    None => source::read_source_table_rows(descriptor, table_ref, None)?,
                };
                let batch = source::source_rows_to_batch_with_provenance(&table_rows)?;
                let provider = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
                self.session
                    .register_table(table_name.clone(), Arc::new(provider))?;
            }
        }
        Ok(())
    }

    pub(crate) async fn resolve_source_table_rows(
        &self,
        table_ref: &source::SourceTableRef,
        limit: Option<usize>,
    ) -> Result<source::SourceTable> {
        let descriptor = self.source_catalog.require(&table_ref.source_id)?;
        let table = self
            .source_catalog
            .require_table(&table_ref.source_id, &table_ref.table_id)?;
        match table.kind {
            source::SourceTableKind::Derived => self
                .resolve_registered_table_rows(&table, limit)
                .await,
            _ => source::read_source_table_rows(&descriptor, table_ref, limit),
        }
    }

    async fn resolve_registered_table_rows(
        &self,
        table: &source::SourceTableDescriptor,
        limit: Option<usize>,
    ) -> Result<source::SourceTable> {
        let mut dataframe = self.session.table(table.table_name.as_str()).await?;
        let mut selected = table
            .inferred_column_names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        selected.push("__source_row");
        dataframe = dataframe.select_columns(&selected)?;
        if let Some(limit) = limit {
            dataframe = dataframe.limit(0, Some(limit))?;
        }
        let batches = dataframe.collect().await?;
        source::record_batches_to_source_table(
            &batches,
            table
                .metadata
                .get("input_source_id")
                .cloned()
                .unwrap_or_default()
                .as_str(),
            &table.id,
            &table.inferred_column_names,
        )
    }
}
