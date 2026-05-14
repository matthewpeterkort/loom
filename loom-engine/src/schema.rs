use anyhow::{anyhow, Result};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, UnionFields, UnionMode};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

/// Maximum recursion depth when expanding nested `$ref`s / object properties.
/// Keeps schema size manageable while covering realistic FHIR nesting.
const MAX_DEPTH: usize = 6;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GraphSchemaDescriptor {
    pub schema_id: Option<String>,
    pub dialect: Option<String>,
    pub entity_type_count: usize,
    pub property_count: usize,
    pub link_count: usize,
    pub wildcard_link_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompiledGraphSchema {
    pub descriptor: GraphSchemaDescriptor,
    #[serde(default)]
    pub entities: BTreeMap<String, EntityType>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EntityType {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub required: Vec<String>,
    #[serde(default)]
    pub properties: BTreeMap<String, PropertyDefinition>,
    #[serde(default)]
    pub links: Vec<LinkDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PropertyKind {
    Scalar,
    Array,
    Object,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PropertyDefinition {
    pub name: String,
    pub kind: PropertyKind,
    pub required: bool,
    #[serde(default)]
    pub json_types: Vec<String>,
    pub schema: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LinkDefinition {
    pub rel: String,
    pub target_entity: String,
    pub wildcard_target: bool,
    #[serde(default)]
    pub template: LinkTemplateBinding,
    #[serde(default)]
    pub target_hints: TargetHintSet,
    #[serde(default)]
    pub extensions: BTreeMap<String, Value>,
    pub schema: Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct LinkTemplateBinding {
    #[serde(default)]
    pub pointers: BTreeMap<String, String>,
    #[serde(default)]
    pub required: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TargetHintSet {
    #[serde(default)]
    pub backref: Vec<String>,
    #[serde(default)]
    pub direction: Vec<String>,
    #[serde(default)]
    pub multiplicity: Vec<String>,
    #[serde(default)]
    pub regex_match: Vec<String>,
    #[serde(default)]
    pub extensions: BTreeMap<String, Value>,
}

impl CompiledGraphSchema {
    pub fn entity(&self, name: &str) -> Option<&EntityType> {
        self.entities.get(name)
    }

    pub fn has_entity(&self, name: &str) -> bool {
        self.entities.contains_key(name)
    }

    pub fn links_for_rel<'a>(&'a self, rel: &str) -> Vec<(&'a EntityType, &'a LinkDefinition)> {
        self.entities
            .values()
            .flat_map(|entity| {
                entity
                    .links
                    .iter()
                    .filter(move |link| link.rel == rel)
                    .map(move |link| (entity, link))
            })
            .collect()
    }

    pub fn link_allows(&self, from_label: &str, rel: &str, to_label: &str) -> bool {
        self.entity(from_label)
            .map(|entity| {
                entity.links.iter().any(|link| {
                    link.rel == rel
                        && (link.wildcard_target
                            || link.target_entity == to_label
                            || link.target_entity == "Resource")
                })
            })
            .unwrap_or(false)
    }

    pub fn entity_names(&self) -> Vec<String> {
        self.entities.keys().cloned().collect()
    }

    pub fn outgoing_links(&self, entity: &str) -> Vec<&LinkDefinition> {
        self.entity(entity)
            .map(|entity| entity.links.iter().collect())
            .unwrap_or_default()
    }

    pub fn incoming_links<'a>(
        &'a self,
        entity: &str,
    ) -> Vec<(&'a EntityType, &'a LinkDefinition)> {
        self.entities
            .values()
            .flat_map(|source| {
                source
                    .links
                    .iter()
                    .filter(move |link| link.wildcard_target || link.target_entity == entity)
                    .map(move |link| (source, link))
            })
            .collect()
    }

    pub fn allowed_targets_for_rel(&self, from_label: &str, rel: &str) -> Vec<String> {
        let mut targets = self
            .entity(from_label)
            .map(|entity| {
                entity
                    .links
                    .iter()
                    .filter(|link| link.rel == rel)
                    .map(|link| link.target_entity.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        targets.sort();
        targets.dedup();
        targets
    }

    pub fn reverse_sources_for_rel(&self, to_label: &str, rel: &str) -> Vec<String> {
        let mut labels = self
            .incoming_links(to_label)
            .into_iter()
            .filter(|(_, link)| link.rel == rel)
            .map(|(source, _)| source.name.clone())
            .collect::<Vec<_>>();
        labels.sort();
        labels.dedup();
        labels
    }
}

pub fn compile_graph_schema(schema: &Value) -> Result<CompiledGraphSchema> {
    let obj = schema
        .as_object()
        .ok_or_else(|| anyhow!("schema payload must be a JSON object"))?;
    let defs = obj
        .get("$defs")
        .or_else(|| obj.get("definitions"))
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("schema missing `$defs` object"))?;

    let mut entities = BTreeMap::new();
    let mut warnings = Vec::new();
    let mut property_count = 0usize;
    let mut link_count = 0usize;
    let mut wildcard_link_count = 0usize;

    for (name, def) in defs {
        if !is_object_schema(def) {
            continue;
        }
        let required = def
            .get("required")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let mut properties = BTreeMap::new();
        if let Some(props) = def.get("properties").and_then(Value::as_object) {
            for (prop_name, prop_schema) in props {
                ensure_property_refs_resolve(prop_schema, defs)?;
                property_count += 1;
                properties.insert(
                    prop_name.clone(),
                    PropertyDefinition {
                        name: prop_name.clone(),
                        kind: property_kind(prop_schema, defs)?,
                        required: required.iter().any(|item| item == prop_name),
                        json_types: normalized_types(prop_schema),
                        schema: prop_schema.clone(),
                    },
                );
            }
        }

        let mut links = Vec::new();
        if let Some(raw_links) = def.get("links").and_then(Value::as_array) {
            for (idx, raw_link) in raw_links.iter().enumerate() {
                let link = raw_link.as_object().ok_or_else(|| {
                    anyhow!("entity `{name}` link at index {idx} must be an object")
                })?;
                let rel = link
                    .get("rel")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("entity `{name}` link at index {idx} missing `rel`"))?
                    .to_string();
                let target_schema = link
                    .get("targetSchema")
                    .and_then(Value::as_object)
                    .ok_or_else(|| {
                        anyhow!(
                            "entity `{name}` link `{rel}` missing object `targetSchema`"
                        )
                    })?;
                let target_ref = target_schema
                    .get("$ref")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        anyhow!(
                            "entity `{name}` link `{rel}` missing `targetSchema.$ref`"
                        )
                    })?;
                let target_entity =
                    resolve_ref_name(target_ref).ok_or_else(|| anyhow!("bad targetSchema ref `{target_ref}`"))?;
                if !defs.contains_key(&target_entity) {
                    return Err(anyhow!(
                        "entity `{name}` link `{rel}` targets unknown schema `{target_entity}`"
                    ));
                }
                let wildcard_target = target_entity == "Resource";
                if wildcard_target {
                    wildcard_link_count += 1;
                }
                let template = LinkTemplateBinding {
                    pointers: link
                        .get("templatePointers")
                        .and_then(Value::as_object)
                        .map(|obj| {
                            obj.iter()
                                .filter_map(|(key, value)| {
                                    value.as_str().map(|value| (key.clone(), value.to_string()))
                                })
                                .collect::<BTreeMap<_, _>>()
                        })
                        .unwrap_or_default(),
                    required: link
                        .get("templateRequired")
                        .and_then(Value::as_array)
                        .map(|values| {
                            values
                                .iter()
                                .filter_map(Value::as_str)
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default(),
                };
                let target_hints = compile_target_hints(link.get("targetHints"));
                let known_keys = [
                    "href",
                    "rel",
                    "targetSchema",
                    "templatePointers",
                    "templateRequired",
                    "targetHints",
                ];
                let extensions = link
                    .iter()
                    .filter(|(key, _)| !known_keys.contains(&key.as_str()))
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect::<BTreeMap<_, _>>();
                link_count += 1;
                links.push(LinkDefinition {
                    rel,
                    target_entity,
                    wildcard_target,
                    template,
                    target_hints,
                    extensions,
                    schema: raw_link.clone(),
                });
            }
        } else if def.get("links").is_some() {
            warnings.push(format!("entity `{name}` has non-array `links`; ignoring"));
        }

        entities.insert(
            name.clone(),
            EntityType {
                name: name.clone(),
                title: def.get("title").and_then(Value::as_str).map(ToString::to_string),
                description: def
                    .get("description")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                required,
                properties,
                links,
            },
        );
    }

    Ok(CompiledGraphSchema {
        descriptor: GraphSchemaDescriptor {
            schema_id: obj.get("$id").and_then(Value::as_str).map(ToString::to_string),
            dialect: obj
                .get("$schema")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            entity_type_count: entities.len(),
            property_count,
            link_count,
            wildcard_link_count,
        },
        entities,
        warnings,
    })
}

fn compile_target_hints(node: Option<&Value>) -> TargetHintSet {
    let Some(Value::Object(obj)) = node else {
        return TargetHintSet::default();
    };
    let backref = obj
        .get("backref")
        .and_then(Value::as_array)
        .map(string_list)
        .unwrap_or_default();
    let direction = obj
        .get("direction")
        .or_else(|| obj.get("directionality"))
        .and_then(Value::as_array)
        .map(string_list)
        .unwrap_or_default();
    let multiplicity = obj
        .get("multiplicity")
        .and_then(Value::as_array)
        .map(string_list)
        .unwrap_or_default();
    let regex_match = obj
        .get("regex_match")
        .and_then(Value::as_array)
        .map(string_list)
        .unwrap_or_default();
    let extensions = obj
        .iter()
        .filter(|(key, _)| {
            !matches!(
                key.as_str(),
                "backref" | "direction" | "directionality" | "multiplicity" | "regex_match"
            )
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    TargetHintSet {
        backref,
        direction,
        multiplicity,
        regex_match,
        extensions,
    }
}

fn string_list(values: &Vec<Value>) -> Vec<String> {
    values
        .iter()
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect()
}

fn resolve_ref_name(reference: &str) -> Option<String> {
    if let Some(name) = reference.strip_prefix("#/$defs/") {
        return Some(name.to_string());
    }
    if let Some(name) = reference.strip_prefix("#/definitions/") {
        return Some(name.to_string());
    }
    if reference.ends_with(".yaml") {
        return Path::new(reference)
            .file_stem()
            .map(|stem| stem.to_string_lossy().to_string());
    }
    reference
        .rsplit('/')
        .next()
        .map(|tail| tail.trim_end_matches(".json").trim_end_matches(".yaml").to_string())
}

fn ensure_property_refs_resolve(
    node: &Value,
    defs: &Map<String, Value>,
) -> Result<()> {
    if let Some(reference) = node.get("$ref").and_then(Value::as_str) {
        let name = resolve_ref_name(reference)
            .ok_or_else(|| anyhow!("invalid property $ref `{reference}`"))?;
        if !defs.contains_key(&name) {
            return Err(anyhow!("unresolved property $ref `{reference}`"));
        }
    }
    if let Some(items) = node.get("items") {
        ensure_property_refs_resolve(items, defs)?;
    }
    for keyword in ["anyOf", "oneOf", "allOf"] {
        if let Some(values) = node.get(keyword).and_then(Value::as_array) {
            for value in values {
                ensure_property_refs_resolve(value, defs)?;
            }
        }
    }
    if let Some(props) = node.get("properties").and_then(Value::as_object) {
        for value in props.values() {
            ensure_property_refs_resolve(value, defs)?;
        }
    }
    Ok(())
}

fn property_kind(node: &Value, defs: &Map<String, Value>) -> Result<PropertyKind> {
    if let Some(reference) = node.get("$ref").and_then(Value::as_str) {
        let name = resolve_ref_name(reference)
            .ok_or_else(|| anyhow!("invalid property $ref `{reference}`"))?;
        let target = defs
            .get(&name)
            .ok_or_else(|| anyhow!("unresolved property $ref `{reference}`"))?;
        return property_kind(target, defs);
    }
    if node.get("items").is_some() || normalized_types(node).iter().any(|kind| kind == "array") {
        return Ok(PropertyKind::Array);
    }
    if node.get("properties").is_some()
        || normalized_types(node).iter().any(|kind| kind == "object")
    {
        return Ok(PropertyKind::Object);
    }
    let types = normalized_types(node);
    if types
        .iter()
        .any(|kind| matches!(kind.as_str(), "string" | "integer" | "number" | "boolean"))
    {
        return Ok(PropertyKind::Scalar);
    }
    Ok(PropertyKind::Unknown)
}

pub struct CalyprSchemaRegistry {
    root_schema: Value,
    // Cache: built on first access per resource type.
    resource_schemas: HashMap<String, SchemaRef>,
}

impl CalyprSchemaRegistry {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let root_schema: Value = serde_json::from_str(&content)?;
        Ok(Self {
            root_schema,
            resource_schemas: HashMap::new(),
        })
    }

    /// Returns the Arrow schema for `resource_type`, building it lazily on
    /// first call.
    pub fn get_schema(&mut self, resource_type: &str) -> Option<SchemaRef> {
        if self.resource_schemas.contains_key(resource_type) {
            return self.resource_schemas.get(resource_type).cloned();
        }

        let defs = self.root_schema.get("$defs").and_then(|d| d.as_object())?;
        let def = defs.get(resource_type)?.clone();
        let mut visited: HashSet<String> = HashSet::new();
        visited.insert(resource_type.to_string());

        let schema = self
            .build_schema(resource_type, &def, &mut visited, 0)
            .ok()?;
        let arc = Arc::new(schema);
        self.resource_schemas
            .insert(resource_type.to_string(), arc.clone());
        Some(arc)
    }

    fn build_schema(
        &self,
        _name: &str,
        def: &Value,
        visited: &mut HashSet<String>,
        depth: usize,
    ) -> Result<Schema> {
        let mut fields: Vec<Field> = Vec::new();
        let mut choice_groups: HashMap<String, Vec<(String, DataType)>> = HashMap::new();

        if let Some(props) = def.get("properties").and_then(|p| p.as_object()) {
            for (prop_name, prop_def) in props {
                if let Some(choice_key) = prop_def.get("one_of_many").and_then(|v| v.as_str()) {
                    let dt = self.derive_data_type(prop_def, visited, depth + 1)?;
                    choice_groups
                        .entry(choice_key.to_string())
                        .or_default()
                        .push((prop_name.to_string(), dt));
                } else {
                    let dt = self.derive_data_type(prop_def, visited, depth + 1)?;
                    fields.push(Field::new(prop_name, dt, true));
                }
            }
        }

        for (choice_name, variants) in choice_groups {
            let uf: Vec<(i8, Arc<Field>)> = variants
                .iter()
                .enumerate()
                .map(|(i, (var_name, dt))| {
                    (i as i8, Arc::new(Field::new(var_name, dt.clone(), true)))
                })
                .collect();
            let union_dt = DataType::Union(UnionFields::from_iter(uf), UnionMode::Sparse);
            fields.push(Field::new(&choice_name, union_dt, true));
        }

        Ok(Schema::new(fields))
    }

    fn derive_data_type(
        &self,
        def: &Value,
        visited: &mut HashSet<String>,
        depth: usize,
    ) -> Result<DataType> {
        if depth > MAX_DEPTH {
            return Ok(DataType::Utf8);
        }

        if let Some(ref_path) = def.get("$ref").and_then(|r| r.as_str()) {
            let ref_name = ref_path.split('/').last().unwrap_or(ref_path);
            if visited.contains(ref_name) {
                return Ok(DataType::Utf8);
            }
            visited.insert(ref_name.to_string());
            let result = match self.resolve_ref(ref_path) {
                Ok(resolved) => self.derive_data_type(&resolved, visited, depth),
                Err(_) => Ok(DataType::Utf8),
            };
            visited.remove(ref_name);
            return result;
        }

        for kw in &["anyOf", "oneOf"] {
            if let Some(variants) = def.get(kw).and_then(|a| a.as_array()) {
                for variant in variants {
                    let skip = variant.get("type").and_then(|t| t.as_str()) == Some("null");
                    if !skip {
                        return self.derive_data_type(variant, visited, depth);
                    }
                }
                return Ok(DataType::Utf8);
            }
        }

        match def.get("type").and_then(|t| t.as_str()) {
            Some("string") => return Ok(DataType::Utf8),
            Some("number") => return Ok(DataType::Float64),
            Some("integer") => return Ok(DataType::Int64),
            Some("boolean") => return Ok(DataType::Boolean),
            Some("array") => {
                let inner = if let Some(items) = def.get("items") {
                    self.derive_data_type(items, visited, depth + 1)?
                } else {
                    DataType::Utf8
                };
                return Ok(DataType::List(Arc::new(Field::new("item", inner, true))));
            }
            Some("object") => {
                if depth >= MAX_DEPTH {
                    return Ok(DataType::Utf8);
                }
                let mut sub_fields: Vec<Field> = Vec::new();
                if let Some(props) = def.get("properties").and_then(|p| p.as_object()) {
                    for (k, v) in props {
                        let dt = self.derive_data_type(v, visited, depth + 1)?;
                        sub_fields.push(Field::new(k, dt, true));
                    }
                }
                return if sub_fields.is_empty() {
                    Ok(DataType::Utf8)
                } else {
                    Ok(DataType::Struct(sub_fields.into()))
                };
            }
            _ => {}
        }

        Ok(DataType::Utf8)
    }

    fn resolve_ref(&self, ref_path: &str) -> Result<Value> {
        if ref_path.starts_with('#') {
            let mut curr = &self.root_schema;
            for part in ref_path.split('/').skip(1) {
                curr = curr
                    .get(part)
                    .ok_or_else(|| anyhow!("bad ref: {ref_path}"))?;
            }
            Ok(curr.clone())
        } else {
            let last = ref_path
                .split('/')
                .last()
                .ok_or_else(|| anyhow!("invalid ref: {ref_path}"))?;
            self.root_schema
                .get("$defs")
                .and_then(|d| d.get(last))
                .cloned()
                .ok_or_else(|| anyhow!("unresolved ref: {ref_path}"))
        }
    }
}

fn is_object_schema(v: &Value) -> bool {
    matches!(v.get("type").and_then(Value::as_str), Some("object")) || v.get("properties").is_some()
}

fn normalized_types(schema_node: &Value) -> Vec<String> {
    match schema_node.get("type") {
        Some(Value::String(s)) => vec![s.clone()],
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(Value::as_str)
            .map(ToString::to_string)
            .collect(),
        _ => Vec::new(),
    }
}
