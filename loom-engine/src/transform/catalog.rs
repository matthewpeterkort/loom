use super::model::TransformDescriptor;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Debug, Default)]
pub struct TransformCatalog {
    transforms: RwLock<HashMap<String, TransformDescriptor>>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct TransformCatalogSnapshot {
    pub transforms: Vec<TransformDescriptor>,
}

impl TransformCatalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert(&self, descriptor: TransformDescriptor) -> Result<()> {
        let mut guard = self
            .transforms
            .write()
            .map_err(|_| anyhow!("transform catalog lock poisoned"))?;
        guard.insert(descriptor.id.clone(), descriptor);
        Ok(())
    }

    pub fn get(&self, id: &str) -> Result<Option<TransformDescriptor>> {
        let guard = self
            .transforms
            .read()
            .map_err(|_| anyhow!("transform catalog lock poisoned"))?;
        Ok(guard.get(id).cloned())
    }

    pub fn require(&self, id: &str) -> Result<TransformDescriptor> {
        self.get(id)?
            .ok_or_else(|| anyhow!("transform `{id}` not found"))
    }

    pub fn list(&self) -> Result<Vec<TransformDescriptor>> {
        let guard = self
            .transforms
            .read()
            .map_err(|_| anyhow!("transform catalog lock poisoned"))?;
        let mut transforms = guard.values().cloned().collect::<Vec<_>>();
        transforms.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(transforms)
    }

    pub fn snapshot(&self) -> Result<TransformCatalogSnapshot> {
        Ok(TransformCatalogSnapshot {
            transforms: self.list()?,
        })
    }

    pub fn replace_all(&self, snapshot: TransformCatalogSnapshot) -> Result<()> {
        let mut guard = self
            .transforms
            .write()
            .map_err(|_| anyhow!("transform catalog lock poisoned"))?;
        guard.clear();
        for transform in snapshot.transforms {
            guard.insert(transform.id.clone(), transform);
        }
        Ok(())
    }
}
