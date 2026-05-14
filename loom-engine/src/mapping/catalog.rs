use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use super::model::GraphMappingDescriptor;

#[derive(Debug, Default)]
pub struct GraphMappingCatalog {
    mappings: RwLock<HashMap<String, GraphMappingDescriptor>>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphMappingCatalogSnapshot {
    pub mappings: Vec<GraphMappingDescriptor>,
}

impl GraphMappingCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert(&self, descriptor: GraphMappingDescriptor) -> Result<()> {
        let mut guard = self
            .mappings
            .write()
            .map_err(|_| anyhow!("graph mapping catalog lock poisoned"))?;
        guard.insert(descriptor.graph.clone(), descriptor);
        Ok(())
    }

    pub fn get(&self, graph: &str) -> Result<Option<GraphMappingDescriptor>> {
        let guard = self
            .mappings
            .read()
            .map_err(|_| anyhow!("graph mapping catalog lock poisoned"))?;
        Ok(guard.get(graph).cloned())
    }

    pub fn require(&self, graph: &str) -> Result<GraphMappingDescriptor> {
        self.get(graph)?
            .ok_or_else(|| anyhow!("graph mapping `{graph}` not found"))
    }

    pub fn list(&self) -> Result<Vec<GraphMappingDescriptor>> {
        let guard = self
            .mappings
            .read()
            .map_err(|_| anyhow!("graph mapping catalog lock poisoned"))?;
        let mut mappings = guard.values().cloned().collect::<Vec<_>>();
        mappings.sort_by(|a, b| a.graph.cmp(&b.graph));
        Ok(mappings)
    }

    pub fn snapshot(&self) -> Result<GraphMappingCatalogSnapshot> {
        Ok(GraphMappingCatalogSnapshot {
            mappings: self.list()?,
        })
    }

    pub fn replace_all(&self, snapshot: GraphMappingCatalogSnapshot) -> Result<()> {
        let mut guard = self
            .mappings
            .write()
            .map_err(|_| anyhow!("graph mapping catalog lock poisoned"))?;
        guard.clear();
        for mapping in snapshot.mappings {
            guard.insert(mapping.graph.clone(), mapping);
        }
        Ok(())
    }
}
