use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;

use super::model::GraphSchemaDescriptor;

#[derive(Debug, Default)]
pub struct GraphSchemaCatalog {
    schemas: RwLock<HashMap<String, GraphSchemaDescriptor>>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphSchemaCatalogSnapshot {
    pub schemas: Vec<GraphSchemaDescriptor>,
}

impl GraphSchemaCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert(&self, descriptor: GraphSchemaDescriptor) -> Result<()> {
        let mut guard = self
            .schemas
            .write()
            .map_err(|_| anyhow!("graph schema catalog lock poisoned"))?;
        guard.insert(descriptor.id.clone(), descriptor);
        Ok(())
    }

    pub fn get(&self, id: &str) -> Result<Option<GraphSchemaDescriptor>> {
        let guard = self
            .schemas
            .read()
            .map_err(|_| anyhow!("graph schema catalog lock poisoned"))?;
        Ok(guard.get(id).cloned())
    }

    pub fn require(&self, id: &str) -> Result<GraphSchemaDescriptor> {
        self.get(id)?
            .ok_or_else(|| anyhow!("graph schema `{id}` not found"))
    }

    pub fn list(&self) -> Result<Vec<GraphSchemaDescriptor>> {
        let guard = self
            .schemas
            .read()
            .map_err(|_| anyhow!("graph schema catalog lock poisoned"))?;
        let mut descriptors = guard.values().cloned().collect::<Vec<_>>();
        descriptors.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(descriptors)
    }

    pub fn snapshot(&self) -> Result<GraphSchemaCatalogSnapshot> {
        Ok(GraphSchemaCatalogSnapshot {
            schemas: self.list()?,
        })
    }

    pub fn replace_all(&self, snapshot: GraphSchemaCatalogSnapshot) -> Result<()> {
        let mut guard = self
            .schemas
            .write()
            .map_err(|_| anyhow!("graph schema catalog lock poisoned"))?;
        guard.clear();
        for descriptor in snapshot.schemas {
            guard.insert(descriptor.id.clone(), descriptor);
        }
        Ok(())
    }
}
