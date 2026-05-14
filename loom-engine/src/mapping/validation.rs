use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use crate::source::{
    infer_and_read_source, ReadOptions, SourceCatalog, SourceFormat, SourceLocation,
    SourceRegistration, SourceSchema,
};
use crate::schema::CompiledGraphSchema;

use super::model::{
    ColumnMapping, Expr, GraphMappingManifest, MappingValidationError, MappingValidationMetrics,
    MappingValidationReport, OutputType, Predicate, PropsMapping, ReferenceRule,
};
use super::{resolve_path, RESERVED_COLUMNS};

pub(super) fn validate_mapping_manifest_inner(
    manifest: &GraphMappingManifest,
    base_dir: &Path,
    catalog: Option<&SourceCatalog>,
    graph_schema: Option<&CompiledGraphSchema>,
) -> MappingValidationReport {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    let mut metrics = MappingValidationMetrics::default();
    let mut schemas = HashMap::<String, SourceSchema>::new();
    let vertex_labels = manifest
        .vertices
        .iter()
        .map(|vertex| vertex.label.clone())
        .collect::<HashSet<_>>();

    if manifest.version != 1 {
        errors.push(validation_error(
            "version",
            format!(
                "unsupported graph mapping manifest version {}",
                manifest.version
            ),
        ));
    }
    if let Some(graph) = &manifest.graph {
        if !is_valid_identifier(graph) {
            errors.push(validation_error(
                "graph",
                "graph id must contain only ASCII letters, digits, and underscores",
            ));
        }
    }
    if manifest.sources.is_empty() {
        errors.push(validation_error(
            "sources",
            "mapping manifest must define at least one source",
        ));
    }
    if manifest.vertices.is_empty() {
        errors.push(validation_error(
            "vertices",
            "mapping manifest must define at least one vertex mapping",
        ));
    }
    for (alias, source) in &manifest.sources {
        if !is_valid_identifier(alias) {
            errors.push(validation_error(
                format!("sources.{alias}"),
                "source alias must contain only ASCII letters, digits, and underscores",
            ));
        }
        match (&source.source, &source.path) {
            (Some(source_id), _) => match catalog {
                Some(catalog) => match catalog.require(source_id) {
                    Ok(descriptor) => {
                        if descriptor.format == SourceFormat::Parquet {
                            errors.push(validation_error(
                                format!("sources.{alias}.source"),
                                "parquet mapping sources must expose a stable row provenance column; configure one before compiling mappings",
                            ));
                        }
                        if let Some(table) = descriptor.default_table() {
                            schemas.insert(alias.clone(), table.schema.clone());
                        } else {
                            errors.push(validation_error(
                                format!("sources.{alias}.source"),
                                format!("source `{}` does not expose a default logical table", descriptor.id),
                            ));
                        }
                    }
                    Err(err) => errors.push(validation_error(
                        format!("sources.{alias}.source"),
                        err.to_string(),
                    )),
                },
                None => errors.push(validation_error(
                    format!("sources.{alias}.source"),
                    "catalog source references require a source catalog",
                )),
            },
            (None, Some(path)) => {
                warnings.push(format!(
                    "sources.{alias}.path uses deprecated inline source path"
                ));
                let registration = SourceRegistration {
                    id: alias.clone(),
                    display_name: None,
                    format: source.format.clone(),
                    location: SourceLocation::Local {
                        path: resolve_path(base_dir, path).to_string_lossy().to_string(),
                    },
                    read_options: ReadOptions::default(),
                };
                match infer_and_read_source(registration) {
                    Ok((descriptor, _)) => {
                        if let Some(table) = descriptor.default_table() {
                            schemas.insert(alias.clone(), table.schema.clone());
                        } else {
                            errors.push(validation_error(
                                format!("sources.{alias}.path"),
                                "inline source path did not expose a default logical table",
                            ));
                        }
                    }
                    Err(err) => errors.push(validation_error(
                        format!("sources.{alias}.path"),
                        err.to_string(),
                    )),
                }
            }
            (None, None) => errors.push(validation_error(
                format!("sources.{alias}"),
                "mapping source must define `source` catalog id or deprecated `path`",
            )),
        }
    }

    for (idx, vertex) in manifest.vertices.iter().enumerate() {
        validate_label(
            &vertex.label,
            &format!("vertices[{idx}].label"),
            &mut errors,
        );
        let Some(schema) = schemas.get(&vertex.source) else {
            errors.push(validation_error(
                format!("vertices[{idx}].source"),
                format!("unknown source `{}`", vertex.source),
            ));
            continue;
        };
        if let Some(graph_schema) = graph_schema {
            validate_schema_vertex_mapping(
                vertex,
                idx,
                graph_schema,
                schema,
                &mut errors,
                &mut metrics,
            );
        }
        validate_expr(
            &vertex.id,
            schema,
            &format!("vertices[{idx}].id"),
            &mut errors,
        );
        validate_predicate_opt(
            vertex.predicate.as_ref(),
            schema,
            &format!("vertices[{idx}].where"),
            &mut errors,
        );
        validate_output_columns(
            &vertex.columns,
            schema,
            &format!("vertices[{idx}].columns"),
            &mut errors,
        );
        validate_props(
            &vertex.props,
            schema,
            &format!("vertices[{idx}].props"),
            &mut errors,
        );
        validate_prop_types(
            &vertex.prop_types,
            schema,
            &format!("vertices[{idx}].prop_types"),
            &mut errors,
        );
    }

    for (idx, edge) in manifest.edges.iter().enumerate() {
        validate_label(&edge.label, &format!("edges[{idx}].label"), &mut errors);
        if let Some(graph_schema) = graph_schema {
            validate_schema_edge_mapping(edge, idx, graph_schema, &mut errors);
        }
        validate_edge_endpoint_label(
            edge.from_label.as_deref(),
            &vertex_labels,
            &format!("edges[{idx}].from_label"),
            &mut errors,
        );
        validate_edge_endpoint_label(
            edge.to_label.as_deref(),
            &vertex_labels,
            &format!("edges[{idx}].to_label"),
            &mut errors,
        );
        let Some(schema) = schemas.get(&edge.source) else {
            errors.push(validation_error(
                format!("edges[{idx}].source"),
                format!("unknown source `{}`", edge.source),
            ));
            continue;
        };
        validate_expr(
            &edge.from,
            schema,
            &format!("edges[{idx}].from"),
            &mut errors,
        );
        validate_expr(&edge.to, schema, &format!("edges[{idx}].to"), &mut errors);
        if let Some(id) = &edge.id {
            validate_expr(id, schema, &format!("edges[{idx}].id"), &mut errors);
        }
        validate_predicate_opt(
            edge.predicate.as_ref(),
            schema,
            &format!("edges[{idx}].where"),
            &mut errors,
        );
        validate_output_columns(
            &edge.columns,
            schema,
            &format!("edges[{idx}].columns"),
            &mut errors,
        );
        validate_props(
            &edge.props,
            schema,
            &format!("edges[{idx}].props"),
            &mut errors,
        );
        validate_prop_types(
            &edge.prop_types,
            schema,
            &format!("edges[{idx}].prop_types"),
            &mut errors,
        );
    }

    for (label, rule) in &manifest.identity {
        if !vertex_labels.contains(label) {
            errors.push(validation_error(
                format!("identity.{label}"),
                format!("identity rule references unknown vertex label `{label}`"),
            ));
        }
        for (alias, expr) in &rule.aliases {
            if !is_valid_identifier(alias) {
                errors.push(validation_error(
                    format!("identity.{label}.aliases.{alias}"),
                    "alias name must contain only ASCII letters, digits, and underscores",
                ));
            }
            for (idx, vertex) in manifest
                .vertices
                .iter()
                .enumerate()
                .filter(|(_, vertex)| vertex.label == *label)
            {
                if let Some(schema) = schemas.get(&vertex.source) {
                    validate_expr(
                        expr,
                        schema,
                        &format!("identity.{label}.aliases.{alias}.vertices[{idx}]"),
                        &mut errors,
                    );
                }
            }
        }
        if let Some(expr) = &rule.canonical_id {
            for (idx, vertex) in manifest
                .vertices
                .iter()
                .enumerate()
                .filter(|(_, vertex)| vertex.label == *label)
            {
                if let Some(schema) = schemas.get(&vertex.source) {
                    validate_expr(
                        expr,
                        schema,
                        &format!("identity.{label}.canonical_id.vertices[{idx}]"),
                        &mut errors,
                    );
                }
            }
        }
    }

    for (idx, reference) in manifest.references.iter().enumerate() {
        validate_reference_rule(reference, idx, &vertex_labels, &schemas, &mut errors);
    }

    MappingValidationReport {
        valid: errors.is_empty(),
        errors,
        warnings,
        policy: manifest.validation.clone(),
        metrics,
        plan_preview: None,
    }
}

fn validate_schema_vertex_mapping(
    vertex: &super::model::VertexMapping,
    idx: usize,
    graph_schema: &CompiledGraphSchema,
    source_schema: &SourceSchema,
    errors: &mut Vec<MappingValidationError>,
    metrics: &mut MappingValidationMetrics,
) {
    let Some(entity) = graph_schema.entity(&vertex.label) else {
        errors.push(validation_error(
            format!("vertices[{idx}].label"),
            format!("vertex label `{}` is not defined in bound graph schema", vertex.label),
        ));
        return;
    };
    for property in vertex.columns.keys() {
        if !entity.properties.contains_key(property) {
            errors.push(validation_error(
                format!("vertices[{idx}].columns.{property}"),
                format!(
                    "property `{property}` is not defined on schema entity `{}`",
                    vertex.label
                ),
            ));
        }
    }
    for property in vertex.prop_types.keys() {
        if !entity.properties.contains_key(property) {
            errors.push(validation_error(
                format!("vertices[{idx}].prop_types.{property}"),
                format!(
                    "property `{property}` is not defined on schema entity `{}`",
                    vertex.label
                ),
            ));
        }
    }

    let satisfied = satisfied_schema_fields(vertex, source_schema);
    let missing = entity
        .required
        .iter()
        .filter(|field| !satisfied.contains(*field))
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        metrics
            .missing_required_fields
            .entry(vertex.label.clone())
            .or_default()
            .extend(missing.clone());
        errors.push(validation_error(
            format!("vertices[{idx}]"),
            format!(
                "mapping for schema entity `{}` does not satisfy required fields: {}",
                vertex.label,
                missing.join(", ")
            ),
        ));
    }
}

fn satisfied_schema_fields(
    vertex: &super::model::VertexMapping,
    source_schema: &SourceSchema,
) -> HashSet<String> {
    let mut satisfied = HashSet::new();
    satisfied.insert("id".to_string());
    satisfied.insert("resourceType".to_string());
    for property in vertex.columns.keys() {
        satisfied.insert(property.clone());
    }

    let passthrough = match &vertex.props {
        PropsMapping::None => Vec::new(),
        PropsMapping::All => source_schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>(),
        PropsMapping::Except(exclude) => {
            let exclude = exclude.iter().cloned().collect::<HashSet<_>>();
            source_schema
                .columns
                .iter()
                .filter(|column| !exclude.contains(&column.name))
                .map(|column| column.name.clone())
                .collect::<Vec<_>>()
        }
        PropsMapping::Only(only) => only.clone(),
    };
    for column in passthrough {
        satisfied.insert(column);
    }
    satisfied
}

fn validate_schema_edge_mapping(
    edge: &super::model::EdgeMapping,
    idx: usize,
    graph_schema: &CompiledGraphSchema,
    errors: &mut Vec<MappingValidationError>,
) {
    let matches = graph_schema.links_for_rel(&edge.label);
    if matches.is_empty() {
        errors.push(validation_error(
            format!("edges[{idx}].label"),
            format!("edge label `{}` is not defined in bound graph schema", edge.label),
        ));
        return;
    }
    if let Some(from_label) = edge.from_label.as_deref() {
        if !graph_schema.has_entity(from_label) {
            errors.push(validation_error(
                format!("edges[{idx}].from_label"),
                format!("edge source label `{from_label}` is not defined in bound graph schema"),
            ));
        }
    }
    if let Some(to_label) = edge.to_label.as_deref() {
        if !graph_schema.has_entity(to_label) {
            errors.push(validation_error(
                format!("edges[{idx}].to_label"),
                format!("edge target label `{to_label}` is not defined in bound graph schema"),
            ));
        }
    }
    if let (Some(from_label), Some(to_label)) = (edge.from_label.as_deref(), edge.to_label.as_deref()) {
        if !graph_schema.link_allows(from_label, &edge.label, to_label) {
            errors.push(validation_error(
                format!("edges[{idx}]"),
                format!(
                    "bound graph schema does not allow relation `{}` from `{}` to `{}`",
                    edge.label, from_label, to_label
                ),
            ));
        }
    }
    for property in edge.columns.keys() {
        let property_allowed = edge
            .from_label
            .as_deref()
            .and_then(|label| graph_schema.entity(label))
            .map(|entity| entity.properties.contains_key(property))
            .unwrap_or(true);
        if !property_allowed {
            errors.push(validation_error(
                format!("edges[{idx}].columns.{property}"),
                format!(
                    "property `{property}` is not defined on schema entity `{}`",
                    edge.from_label.as_deref().unwrap_or("")
                ),
            ));
        }
    }
}

fn validate_reference_rule(
    reference: &ReferenceRule,
    idx: usize,
    vertex_labels: &HashSet<String>,
    schemas: &HashMap<String, SourceSchema>,
    errors: &mut Vec<MappingValidationError>,
) {
    if !is_valid_identifier(&reference.name) {
        errors.push(validation_error(
            format!("references[{idx}].name"),
            "reference rule name must contain only ASCII letters, digits, and underscores",
        ));
    }
    if !vertex_labels.contains(&reference.from_label) {
        errors.push(validation_error(
            format!("references[{idx}].from_label"),
            format!(
                "reference rule references unknown vertex label `{}`",
                reference.from_label
            ),
        ));
    }
    if !vertex_labels.contains(&reference.to_label) {
        errors.push(validation_error(
            format!("references[{idx}].to_label"),
            format!(
                "reference rule references unknown vertex label `{}`",
                reference.to_label
            ),
        ));
    }
    let Some(schema) = schemas.get(&reference.source) else {
        errors.push(validation_error(
            format!("references[{idx}].source"),
            format!("unknown source `{}`", reference.source),
        ));
        return;
    };
    validate_expr(
        &reference.from_key,
        schema,
        &format!("references[{idx}].from_key"),
        errors,
    );
    validate_expr(
        &reference.to_key,
        schema,
        &format!("references[{idx}].to_key"),
        errors,
    );
    validate_predicate_opt(
        reference.predicate.as_ref(),
        schema,
        &format!("references[{idx}].where"),
        errors,
    );
}

fn validate_expr(
    expr: &Expr,
    schema: &SourceSchema,
    path: &str,
    errors: &mut Vec<MappingValidationError>,
) {
    match expr {
        Expr::Column { column } => {
            if !schema.columns.iter().any(|col| col.name == *column) {
                errors.push(validation_error(
                    format!("{path}.column"),
                    format!("unknown source column `{column}`"),
                ));
            }
        }
        Expr::Literal { .. } | Expr::RowNumber { .. } | Expr::Text(_) => {}
        Expr::Concat { concat } => {
            if concat.is_empty() {
                errors.push(validation_error(
                    path,
                    "`concat` must contain at least one expression",
                ));
            }
            for (idx, expr) in concat.iter().enumerate() {
                validate_expr(expr, schema, &format!("{path}.concat[{idx}]"), errors);
            }
        }
        Expr::Coalesce { coalesce } => {
            if coalesce.is_empty() {
                errors.push(validation_error(
                    path,
                    "`coalesce` must contain at least one expression",
                ));
            }
            for (idx, expr) in coalesce.iter().enumerate() {
                validate_expr(expr, schema, &format!("{path}.coalesce[{idx}]"), errors);
            }
        }
        Expr::Lower { lower } => validate_expr(lower, schema, &format!("{path}.lower"), errors),
        Expr::Upper { upper } => validate_expr(upper, schema, &format!("{path}.upper"), errors),
        Expr::Trim { trim } => validate_expr(trim, schema, &format!("{path}.trim"), errors),
        Expr::Replace { replace } => validate_expr(
            &replace.value,
            schema,
            &format!("{path}.replace.value"),
            errors,
        ),
        Expr::NullIf { null_if } => {
            if null_if.len() != 2 {
                errors.push(validation_error(
                    path,
                    "`null_if` must contain exactly two expressions",
                ));
            }
            for (idx, expr) in null_if.iter().enumerate() {
                validate_expr(expr, schema, &format!("{path}.null_if[{idx}]"), errors);
            }
        }
        Expr::Sha256 { sha256 } => validate_expr(sha256, schema, &format!("{path}.sha256"), errors),
    }
}

fn validate_predicate_opt(
    predicate: Option<&Predicate>,
    schema: &SourceSchema,
    path: &str,
    errors: &mut Vec<MappingValidationError>,
) {
    if let Some(predicate) = predicate {
        validate_predicate(predicate, schema, path, errors);
    }
}

fn validate_predicate(
    predicate: &Predicate,
    schema: &SourceSchema,
    path: &str,
    errors: &mut Vec<MappingValidationError>,
) {
    match predicate {
        Predicate::Eq { eq } => {
            validate_expr(&eq.left, schema, &format!("{path}.eq.left"), errors);
            validate_expr(&eq.right, schema, &format!("{path}.eq.right"), errors);
        }
        Predicate::Neq { neq } => {
            validate_expr(&neq.left, schema, &format!("{path}.neq.left"), errors);
            validate_expr(&neq.right, schema, &format!("{path}.neq.right"), errors);
        }
        Predicate::IsEmpty { is_empty } => {
            validate_expr(is_empty, schema, &format!("{path}.is_empty"), errors)
        }
        Predicate::IsNotEmpty { is_not_empty } => validate_expr(
            is_not_empty,
            schema,
            &format!("{path}.is_not_empty"),
            errors,
        ),
        Predicate::And { and } => {
            if and.is_empty() {
                errors.push(validation_error(
                    path,
                    "`and` must contain at least one predicate",
                ));
            }
            for (idx, predicate) in and.iter().enumerate() {
                validate_predicate(predicate, schema, &format!("{path}.and[{idx}]"), errors);
            }
        }
        Predicate::Or { or } => {
            if or.is_empty() {
                errors.push(validation_error(
                    path,
                    "`or` must contain at least one predicate",
                ));
            }
            for (idx, predicate) in or.iter().enumerate() {
                validate_predicate(predicate, schema, &format!("{path}.or[{idx}]"), errors);
            }
        }
        Predicate::Not { not } => validate_predicate(not, schema, &format!("{path}.not"), errors),
    }
}

fn validate_output_columns(
    columns: &BTreeMap<String, ColumnMapping>,
    schema: &SourceSchema,
    path: &str,
    errors: &mut Vec<MappingValidationError>,
) {
    for (name, mapping) in columns {
        if RESERVED_COLUMNS.contains(&name.as_str()) {
            errors.push(validation_error(
                format!("{path}.{name}"),
                format!("`{name}` is a reserved graph table column"),
            ));
        }
        validate_expr(mapping.expr(), schema, &format!("{path}.{name}"), errors);
        if mapping.coerce() && mapping.output_type() == OutputType::String {
            errors.push(validation_error(
                format!("{path}.{name}.type"),
                "`coerce` requires a non-string output type",
            ));
        }
    }
}

fn validate_edge_endpoint_label(
    label: Option<&str>,
    vertex_labels: &HashSet<String>,
    path: &str,
    errors: &mut Vec<MappingValidationError>,
) {
    let Some(label) = label else {
        errors.push(validation_error(
            path,
            "edge mappings must declare explicit endpoint labels with `from_label` and `to_label`",
        ));
        return;
    };
    validate_label(label, path, errors);
    if !vertex_labels.contains(label) {
        errors.push(validation_error(
            path,
            format!("endpoint label `{label}` does not match any declared vertex label"),
        ));
    }
}

fn validate_props(
    props: &PropsMapping,
    schema: &SourceSchema,
    path: &str,
    errors: &mut Vec<MappingValidationError>,
) {
    match props {
        PropsMapping::None | PropsMapping::All => {}
        PropsMapping::Except(columns) | PropsMapping::Only(columns) => {
            for (idx, column) in columns.iter().enumerate() {
                if !schema.columns.iter().any(|col| col.name == *column) {
                    errors.push(validation_error(
                        format!("{path}[{idx}]"),
                        format!("unknown source column `{column}`"),
                    ));
                }
            }
        }
    }
}

fn validate_prop_types(
    prop_types: &BTreeMap<String, OutputType>,
    schema: &SourceSchema,
    path: &str,
    errors: &mut Vec<MappingValidationError>,
) {
    for source_column in prop_types.keys() {
        if !schema.columns.iter().any(|col| col.name == *source_column) {
            errors.push(validation_error(
                format!("{path}.{source_column}"),
                format!("unknown source column `{source_column}`"),
            ));
        }
    }
}

fn validate_label(label: &str, path: &str, errors: &mut Vec<MappingValidationError>) {
    if !is_valid_label(label) {
        errors.push(validation_error(
            path,
            "label must start with an ASCII letter and contain only ASCII letters, digits, underscores, hyphens, or periods",
        ));
    }
}

fn is_valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn is_valid_label(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(ch) if ch.is_ascii_alphabetic())
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' || ch == '.')
}

fn validation_error(path: impl Into<String>, message: impl Into<String>) -> MappingValidationError {
    MappingValidationError {
        path: path.into(),
        message: message.into(),
    }
}
