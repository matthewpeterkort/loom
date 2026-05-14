use super::model::{SourceDescriptor, SourceTableDescriptor, SourceTableRef};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Default)]
pub struct SourceCatalog {
    sources: RwLock<HashMap<String, SourceDescriptor>>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceCatalogSnapshot {
    pub sources: Vec<SourceDescriptor>,
}

impl SourceCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert(&self, descriptor: SourceDescriptor) -> Result<()> {
        let mut guard = self
            .sources
            .write()
            .map_err(|_| anyhow!("source catalog lock poisoned"))?;
        guard.insert(descriptor.id.clone(), descriptor);
        Ok(())
    }

    pub fn get(&self, id: &str) -> Result<Option<SourceDescriptor>> {
        let guard = self
            .sources
            .read()
            .map_err(|_| anyhow!("source catalog lock poisoned"))?;
        Ok(guard.get(id).cloned())
    }

    pub fn require(&self, id: &str) -> Result<SourceDescriptor> {
        self.get(id)?
            .ok_or_else(|| anyhow!("source `{id}` not found"))
    }

    pub fn list(&self) -> Result<Vec<SourceDescriptor>> {
        let guard = self
            .sources
            .read()
            .map_err(|_| anyhow!("source catalog lock poisoned"))?;
        let mut sources = guard.values().cloned().collect::<Vec<_>>();
        sources.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(sources)
    }

    pub fn list_tables(&self, source_id: &str) -> Result<Vec<SourceTableDescriptor>> {
        let source = self.require(source_id)?;
        let mut tables = source.tables;
        tables.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(tables)
    }

    pub fn get_table(&self, source_id: &str, table_id: &str) -> Result<Option<SourceTableDescriptor>> {
        let Some(source) = self.get(source_id)? else {
            return Ok(None);
        };
        Ok(source.tables.into_iter().find(|table| table.id == table_id))
    }

    pub fn require_table(&self, source_id: &str, table_id: &str) -> Result<SourceTableDescriptor> {
        self.get_table(source_id, table_id)?
            .ok_or_else(|| anyhow!("source `{source_id}` does not contain table `{table_id}`"))
    }

    pub fn set_table_registered(&self, table_ref: &SourceTableRef, registered: bool) -> Result<()> {
        let mut guard = self
            .sources
            .write()
            .map_err(|_| anyhow!("source catalog lock poisoned"))?;
        let source = guard
            .get_mut(&table_ref.source_id)
            .ok_or_else(|| anyhow!("source `{}` not found", table_ref.source_id))?;
        let table = source
            .tables
            .iter_mut()
            .find(|table| table.id == table_ref.table_id)
            .ok_or_else(|| {
                anyhow!(
                    "source `{}` does not contain table `{}`",
                    table_ref.source_id,
                    table_ref.table_id
                )
            })?;
        table.registered = registered;
        if source.tables.len() == 1 {
            source.registered = Some(registered);
        }
        Ok(())
    }

    pub fn upsert_table(
        &self,
        source_id: &str,
        table: SourceTableDescriptor,
    ) -> Result<SourceDescriptor> {
        let mut guard = self
            .sources
            .write()
            .map_err(|_| anyhow!("source catalog lock poisoned"))?;
        let source = guard
            .get_mut(source_id)
            .ok_or_else(|| anyhow!("source `{source_id}` not found"))?;
        if let Some(existing) = source.tables.iter_mut().find(|existing| existing.id == table.id) {
            *existing = table;
        } else {
            source.tables.push(table);
            source.tables.sort_by(|a, b| a.id.cmp(&b.id));
        }
        Ok(source.clone())
    }

    pub fn snapshot(&self) -> Result<SourceCatalogSnapshot> {
        Ok(SourceCatalogSnapshot {
            sources: self.list()?,
        })
    }

    pub fn replace_all(&self, snapshot: SourceCatalogSnapshot) -> Result<()> {
        let mut guard = self
            .sources
            .write()
            .map_err(|_| anyhow!("source catalog lock poisoned"))?;
        guard.clear();
        for mut source in snapshot.sources {
            if source.tables.is_empty() {
                if let (Some(table_name), Some(schema)) = (source.table_name.clone(), source.schema.clone())
                {
                    source.tables.push(SourceTableDescriptor {
                        id: "primary".to_string(),
                        display_name: source.display_name.clone(),
                        table_name,
                        schema,
                        stats: source.stats.clone(),
                        created_unix_seconds: source.created_unix_seconds,
                        updated_unix_seconds: source.updated_unix_seconds,
                        registered: false,
                        kind: Default::default(),
                        header_row_index: Some(0),
                        data_start_row_index: Some(1),
                        original_column_names: Vec::new(),
                        inferred_column_names: Vec::new(),
                        quality: Default::default(),
                        metadata: Default::default(),
                    });
                }
            }
            for table in &mut source.tables {
                table.registered = false;
            }
            source.registered = Some(false);
            guard.insert(source.id.clone(), source);
        }
        Ok(())
    }
}
