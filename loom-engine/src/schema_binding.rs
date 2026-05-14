use crate::schema::{CompiledGraphSchema, EntityType, LinkDefinition, PropertyDefinition};
use crate::source::{ColumnType, SourceTable};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaTypeFamily {
    String,
    Code,
    Integer,
    Decimal,
    Boolean,
    Date,
    DateTime,
    Uri,
    Identifier,
    Reference,
    Coding,
    CodeableConcept,
    Quantity,
    Period,
    Unknown,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ObservedColumnShape {
    #[serde(default)]
    pub null_ratio: f64,
    #[serde(default)]
    pub parseable_integer_ratio: f64,
    #[serde(default)]
    pub parseable_decimal_ratio: f64,
    #[serde(default)]
    pub parseable_boolean_ratio: f64,
    #[serde(default)]
    pub parseable_date_ratio: f64,
    #[serde(default)]
    pub parseable_datetime_ratio: f64,
    #[serde(default)]
    pub identifier_like: bool,
    #[serde(default)]
    pub short_code_like: bool,
    #[serde(default)]
    pub long_text_like: bool,
    #[serde(default)]
    pub normalized_sample_values: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ObservedDatatypeCandidate {
    pub family: SchemaTypeFamily,
    pub confidence: f64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ColumnClusterKind {
    Coding,
    CodeableConcept,
    Quantity,
    Period,
    HumanName,
    Address,
    ContactPoint,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ObservedColumnCluster {
    pub kind: ColumnClusterKind,
    pub columns: Vec<String>,
    pub confidence: f64,
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaPropertyMatch {
    pub entity: String,
    pub property: String,
    pub family: SchemaTypeFamily,
    pub confidence: f64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaEntityCandidate {
    pub entity: String,
    pub confidence: f64,
    pub reason: String,
    #[serde(default)]
    pub matched_properties: Vec<String>,
    #[serde(default)]
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaEdgeCandidate {
    pub rel: String,
    pub from_entity: String,
    pub to_entity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_column: Option<String>,
    pub confidence: f64,
    pub reason: String,
    #[serde(default)]
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct TableBindingAnalysis {
    pub column_shapes: BTreeMap<String, ObservedColumnShape>,
    pub datatype_candidates: BTreeMap<String, Vec<ObservedDatatypeCandidate>>,
    pub property_matches: BTreeMap<String, Vec<SchemaPropertyMatch>>,
    pub clusters: Vec<ObservedColumnCluster>,
    pub entity_candidates: Vec<SchemaEntityCandidate>,
    pub edge_candidates: Vec<SchemaEdgeCandidate>,
}

#[derive(Debug, Clone)]
pub struct TableBindingContext<'a> {
    pub source_id: &'a str,
    pub table: &'a SourceTable,
    pub analysis: &'a TableBindingAnalysis,
}

#[derive(Debug, Clone)]
struct SchemaPropertySignature {
    entity: String,
    property: String,
    family: SchemaTypeFamily,
    required: bool,
    tokens: BTreeSet<String>,
}

#[derive(Debug, Clone)]
struct SchemaLinkSignature {
    from_entity: String,
    rel: String,
    to_entity: String,
    wildcard_target: bool,
    tokens: BTreeSet<String>,
}

#[derive(Debug, Clone, Default)]
struct SchemaEntityBindingVocabulary {
    properties: Vec<SchemaPropertySignature>,
    links: Vec<SchemaLinkSignature>,
}

pub fn analyze_table_bindings(
    table: &SourceTable,
    inferred_types: &HashMap<String, ColumnType>,
    graph_schema: &CompiledGraphSchema,
) -> TableBindingAnalysis {
    let vocab = compile_vocabulary(graph_schema);
    let mut analysis = TableBindingAnalysis::default();
    let row_count = table.rows.len().max(1);
    for column in &table.headers {
        let shape = observe_column_shape(table, column);
        let inferred_type = inferred_types
            .get(column)
            .cloned()
            .unwrap_or(ColumnType::String);
        let datatype_candidates = datatype_candidates_for_shape(column, &shape, inferred_type);
        let matches = property_matches_for_column(column, &shape, &datatype_candidates, &vocab);
        analysis.column_shapes.insert(column.clone(), shape);
        analysis
            .datatype_candidates
            .insert(column.clone(), datatype_candidates);
        analysis.property_matches.insert(column.clone(), matches);
    }
    analysis.clusters = detect_column_clusters(table, &analysis.column_shapes);
    analysis.entity_candidates =
        entity_candidates_for_table(row_count, &vocab, &analysis.property_matches, &analysis.clusters);
    analysis.edge_candidates = local_edge_candidates_for_table(&analysis, &vocab);
    analysis
}

pub fn cross_table_edge_candidates(
    contexts: &[TableBindingContext<'_>],
    graph_schema: &CompiledGraphSchema,
) -> Vec<SchemaEdgeCandidate> {
    let vocab = compile_vocabulary(graph_schema);
    let id_columns = contexts
        .iter()
        .flat_map(|context| {
            context
                .analysis
                .entity_candidates
                .iter()
                .filter_map(|candidate| {
                    best_id_column_for_entity(context.analysis, &candidate.entity).map(|column| {
                        (
                            candidate.entity.clone(),
                            context.source_id.to_string(),
                            column.to_string(),
                            normalized_value_set(context.table, column),
                        )
                    })
                })
        })
        .collect::<Vec<_>>();

    let mut candidates = Vec::new();
    let mut seen = HashSet::new();
    for context in contexts {
        for entity in &context.analysis.entity_candidates {
            for link in vocab.links.iter().filter(|link| link.from_entity == entity.entity) {
                let local = context
                    .analysis
                    .edge_candidates
                    .iter()
                    .filter(|candidate| {
                        candidate.from_entity == entity.entity
                            && candidate.rel == link.rel
                            && candidate.to_entity == link.to_entity
                    })
                    .max_by(|a, b| a.confidence.total_cmp(&b.confidence));
                let local_boost = local.map(|candidate| candidate.confidence).unwrap_or(0.0);
                for (target_entity, _target_source, target_column, target_values) in &id_columns {
                    if !link.wildcard_target && target_entity != &link.to_entity {
                        continue;
                    }
                    for column in reference_candidate_columns(
                        context.table,
                        &context.analysis.column_shapes,
                        &link.to_entity,
                        Some(target_column),
                    ) {
                        let overlap = overlap_ratio(
                            &normalized_value_set(context.table, column),
                            target_values,
                        );
                        if overlap < 0.25 && local_boost < 0.7 {
                            continue;
                        }
                        let key = format!(
                            "{}:{}:{}:{}",
                            context.source_id, entity.entity, link.rel, column
                        );
                        if !seen.insert(key) {
                            continue;
                        }
                        let confidence =
                            (entity.confidence * 0.35) + (overlap * 0.5) + (local_boost * 0.15);
                        candidates.push(SchemaEdgeCandidate {
                            rel: link.rel.clone(),
                            from_entity: entity.entity.clone(),
                            to_entity: link.to_entity.clone(),
                            source_column: Some(column.to_string()),
                            confidence: confidence.min(0.99),
                            reason: format!(
                                "column `{}` overlaps candidate {} identifiers and matches schema link `{}`",
                                column, link.to_entity, link.rel
                            ),
                            evidence: vec![
                                format!("overlap_ratio={overlap:.2}"),
                                format!("target_id_column={target_column}"),
                            ],
                        });
                    }
                }
            }
        }
    }
    candidates.sort_by(|a, b| b.confidence.total_cmp(&a.confidence));
    candidates
}

pub fn best_id_column_for_entity<'a>(
    analysis: &'a TableBindingAnalysis,
    entity: &str,
) -> Option<&'a str> {
    let mut best: Option<(&str, f64)> = None;
    for (column, matches) in &analysis.property_matches {
        for candidate in matches {
            let id_like = candidate.entity == entity
                && (candidate.property.eq_ignore_ascii_case("id")
                    || candidate.family == SchemaTypeFamily::Identifier);
            if id_like && best.map(|(_, score)| score).unwrap_or_default() < candidate.confidence {
                best = Some((column.as_str(), candidate.confidence));
            }
        }
    }
    best.map(|(column, _)| column)
}

fn compile_vocabulary(schema: &CompiledGraphSchema) -> SchemaEntityBindingVocabulary {
    let mut vocab = SchemaEntityBindingVocabulary::default();
    for entity in schema.entities.values() {
        for property in entity.properties.values() {
            vocab.properties.push(SchemaPropertySignature {
                entity: entity.name.clone(),
                property: property.name.clone(),
                family: property_family(property),
                required: property.required,
                tokens: property_tokens(entity, property),
            });
        }
        for link in &entity.links {
            vocab.links.push(SchemaLinkSignature {
                from_entity: entity.name.clone(),
                rel: link.rel.clone(),
                to_entity: link.target_entity.clone(),
                wildcard_target: link.wildcard_target,
                tokens: link_tokens(entity, link),
            });
        }
    }
    vocab
}

fn observe_column_shape(table: &SourceTable, column: &str) -> ObservedColumnShape {
    let mut values = Vec::new();
    let mut non_empty = 0usize;
    let mut integer = 0usize;
    let mut decimal = 0usize;
    let mut boolean = 0usize;
    let mut date = 0usize;
    let mut datetime = 0usize;
    let mut samples = Vec::new();
    for row in &table.rows {
        let value = row.values.get(column).cloned().unwrap_or_default();
        if value.trim().is_empty() {
            continue;
        }
        let trimmed = value.trim().to_string();
        non_empty += 1;
        if trimmed.parse::<i64>().is_ok() {
            integer += 1;
        }
        if trimmed.parse::<f64>().is_ok() {
            decimal += 1;
        }
        if is_boolean_like(&trimmed) {
            boolean += 1;
        }
        if is_datetime_like(&trimmed) {
            datetime += 1;
        }
        if is_date_like(&trimmed) {
            date += 1;
        }
        if samples.len() < 5
            && !samples
                .iter()
                .any(|sample: &String| sample.eq_ignore_ascii_case(&trimmed))
        {
            samples.push(trimmed.clone());
        }
        values.push(trimmed);
    }
    let total = table.rows.len().max(1) as f64;
    let non_empty_total = non_empty.max(1) as f64;
    let avg_len = if values.is_empty() {
        0.0
    } else {
        values.iter().map(|value| value.len() as f64).sum::<f64>() / values.len() as f64
    };
    let distinct = values.iter().cloned().collect::<HashSet<_>>().len();
    let uniqueness_ratio = if non_empty == 0 {
        0.0
    } else {
        distinct as f64 / non_empty as f64
    };
    ObservedColumnShape {
        null_ratio: ((table.rows.len().saturating_sub(non_empty)) as f64 / total).min(1.0),
        parseable_integer_ratio: integer as f64 / non_empty_total,
        parseable_decimal_ratio: decimal as f64 / non_empty_total,
        parseable_boolean_ratio: boolean as f64 / non_empty_total,
        parseable_date_ratio: date as f64 / non_empty_total,
        parseable_datetime_ratio: datetime as f64 / non_empty_total,
        identifier_like: uniqueness_ratio >= 0.75
            && avg_len <= 32.0
            && values.iter().all(|value| is_identifier_token(value)),
        short_code_like: avg_len <= 16.0 && uniqueness_ratio < 0.5,
        long_text_like: avg_len >= 32.0,
        normalized_sample_values: samples
            .into_iter()
            .map(|value| normalize_token(&value))
            .collect(),
    }
}

fn datatype_candidates_for_shape(
    column: &str,
    shape: &ObservedColumnShape,
    inferred_type: ColumnType,
) -> Vec<ObservedDatatypeCandidate> {
    let mut candidates = Vec::new();
    let normalized = normalize_token(column);
    if shape.identifier_like && (normalized.contains("id") || normalized.contains("identifier")) {
        candidates.push(datatype_candidate(
            SchemaTypeFamily::Identifier,
            0.93,
            "high uniqueness and identifier-like token shape",
        ));
    }
    if normalized.contains("reference")
        || normalized.contains("subject")
        || normalized.ends_with("_id")
    {
        candidates.push(datatype_candidate(
            SchemaTypeFamily::Reference,
            0.78,
            "column name suggests a row-to-row reference",
        ));
    }
    if shape.parseable_datetime_ratio > 0.9 {
        candidates.push(datatype_candidate(
            SchemaTypeFamily::DateTime,
            0.95,
            "values consistently parse like datetime strings",
        ));
    } else if shape.parseable_date_ratio > 0.9 {
        candidates.push(datatype_candidate(
            SchemaTypeFamily::Date,
            0.92,
            "values consistently parse like dates",
        ));
    }
    if shape.parseable_boolean_ratio > 0.95 {
        candidates.push(datatype_candidate(
            SchemaTypeFamily::Boolean,
            0.97,
            "values consistently parse like booleans",
        ));
    }
    if shape.parseable_integer_ratio > 0.95 {
        candidates.push(datatype_candidate(
            SchemaTypeFamily::Integer,
            0.95,
            "values consistently parse like integers",
        ));
    } else if shape.parseable_decimal_ratio > 0.95 {
        candidates.push(datatype_candidate(
            SchemaTypeFamily::Decimal,
            0.94,
            "values consistently parse like decimals",
        ));
    }
    if normalized.contains("code") && shape.short_code_like {
        candidates.push(datatype_candidate(
            SchemaTypeFamily::Code,
            0.84,
            "short low-cardinality code-like values",
        ));
    }
    if normalized.contains("system") || normalized.contains("url") || normalized.contains("uri") {
        candidates.push(datatype_candidate(
            SchemaTypeFamily::Uri,
            0.8,
            "column name suggests URI/system values",
        ));
    }
    if candidates.is_empty() {
        let family = match inferred_type {
            ColumnType::Boolean => SchemaTypeFamily::Boolean,
            ColumnType::Integer => SchemaTypeFamily::Integer,
            ColumnType::Float => SchemaTypeFamily::Decimal,
            ColumnType::String => SchemaTypeFamily::String,
        };
        candidates.push(datatype_candidate(
            family,
            0.6,
            "fallback inferred from primitive source type",
        ));
    }
    candidates.sort_by(|a, b| b.confidence.total_cmp(&a.confidence));
    candidates
}

fn detect_column_clusters(
    table: &SourceTable,
    shapes: &BTreeMap<String, ObservedColumnShape>,
) -> Vec<ObservedColumnCluster> {
    let headers = table
        .headers
        .iter()
        .map(|column| (normalize_token(column), column.clone()))
        .collect::<Vec<_>>();
    let mut clusters = Vec::new();
    let find = |token: &str| {
        headers
            .iter()
            .find(|(normalized, _)| normalized.contains(token))
            .map(|(_, original)| original.clone())
    };
    if let Some(code) = find("code") {
        let mut cols = vec![code.clone()];
        if let Some(display) = find("display") {
            cols.push(display);
        }
        if let Some(system) = find("system") {
            cols.push(system);
        }
        if cols.len() >= 2 {
            clusters.push(ObservedColumnCluster {
                kind: if cols.iter().any(|column| normalize_token(column).contains("display")) {
                    ColumnClusterKind::CodeableConcept
                } else {
                    ColumnClusterKind::Coding
                },
                columns: cols,
                confidence: 0.88,
                evidence: vec!["code/display/system-style sibling columns detected".to_string()],
            });
        }
    }
    if let Some(value) = find("value") {
        let mut cols = vec![value.clone()];
        if let Some(unit) = find("unit") {
            cols.push(unit);
        }
        if let Some(code) = find("code") {
            cols.push(code);
        }
        if cols.len() >= 2
            && shapes
                .get(&value)
                .map(|shape| shape.parseable_decimal_ratio > 0.8 || shape.parseable_integer_ratio > 0.8)
                .unwrap_or(false)
        {
            clusters.push(ObservedColumnCluster {
                kind: ColumnClusterKind::Quantity,
                columns: cols,
                confidence: 0.9,
                evidence: vec!["numeric value plus unit/code sibling columns detected".to_string()],
            });
        }
    }
    if let (Some(start), Some(end)) = (find("start"), find("end")) {
        clusters.push(ObservedColumnCluster {
            kind: ColumnClusterKind::Period,
            columns: vec![start, end],
            confidence: 0.86,
            evidence: vec!["start/end sibling columns detected".to_string()],
        });
    }
    clusters
}

fn property_matches_for_column(
    column: &str,
    shape: &ObservedColumnShape,
    datatype_candidates: &[ObservedDatatypeCandidate],
    vocab: &SchemaEntityBindingVocabulary,
) -> Vec<SchemaPropertyMatch> {
    let column_tokens = token_set(column);
    let mut matches = Vec::new();
    for property in &vocab.properties {
        let type_score = family_match_score(&property.family, shape, datatype_candidates);
        let name_score = token_similarity(&column_tokens, &property.tokens);
        let exact_name = property
            .tokens
            .iter()
            .any(|token| column_tokens.contains(token));
        let mut confidence = (type_score * 0.55) + (name_score * 0.45);
        if exact_name {
            confidence += 0.1;
        }
        if property.required {
            confidence += 0.03;
        }
        if confidence < 0.45 {
            continue;
        }
        matches.push(SchemaPropertyMatch {
            entity: property.entity.clone(),
            property: property.property.clone(),
            family: property.family.clone(),
            confidence: confidence.min(0.99),
            reason: format!(
                "column shape and name are compatible with {}.{} ({:?})",
                property.entity, property.property, property.family
            ),
        });
    }
    matches.sort_by(|a, b| b.confidence.total_cmp(&a.confidence));
    matches.truncate(6);
    matches
}

fn entity_candidates_for_table(
    row_count: usize,
    vocab: &SchemaEntityBindingVocabulary,
    property_matches: &BTreeMap<String, Vec<SchemaPropertyMatch>>,
    clusters: &[ObservedColumnCluster],
) -> Vec<SchemaEntityCandidate> {
    let mut by_entity: BTreeMap<String, Vec<&SchemaPropertyMatch>> = BTreeMap::new();
    for matches in property_matches.values() {
        for candidate in matches {
            by_entity
                .entry(candidate.entity.clone())
                .or_default()
                .push(candidate);
        }
    }
    let mut candidates = Vec::new();
    for entity in entities(vocab) {
        let matches = by_entity.get(&entity).cloned().unwrap_or_default();
        if matches.is_empty() {
            continue;
        }
        let mut best_by_property = BTreeMap::<String, f64>::new();
        for candidate in matches {
            let score = best_by_property
                .entry(candidate.property.clone())
                .or_insert(candidate.confidence);
            if candidate.confidence > *score {
                *score = candidate.confidence;
            }
        }
        let coverage = best_by_property.len() as f64 / 6.0;
        let avg_score = best_by_property.values().sum::<f64>() / best_by_property.len() as f64;
        let cluster_boost = clusters
            .iter()
            .map(|cluster| match cluster.kind {
                ColumnClusterKind::Coding | ColumnClusterKind::CodeableConcept => 0.03,
                ColumnClusterKind::Quantity | ColumnClusterKind::Period => 0.04,
                _ => 0.0,
            })
            .sum::<f64>();
        let confidence = (avg_score * 0.65)
            + (coverage.min(1.0) * 0.25)
            + cluster_boost
            + if row_count > 1 { 0.02 } else { 0.0 };
        if confidence < 0.5 {
            continue;
        }
        candidates.push(SchemaEntityCandidate {
            entity: entity.clone(),
            confidence: confidence.min(0.99),
            reason: format!(
                "table columns cover {} schema properties for entity `{}`",
                best_by_property.len(),
                entity
            ),
            matched_properties: best_by_property.keys().cloned().collect(),
            evidence: best_by_property
                .iter()
                .map(|(property, score)| format!("{property}:{score:.2}"))
                .collect(),
        });
    }
    candidates.sort_by(|a, b| b.confidence.total_cmp(&a.confidence));
    candidates.truncate(8);
    candidates
}

fn local_edge_candidates_for_table(
    analysis: &TableBindingAnalysis,
    vocab: &SchemaEntityBindingVocabulary,
) -> Vec<SchemaEdgeCandidate> {
    let mut candidates = Vec::new();
    for entity in &analysis.entity_candidates {
        for link in vocab.links.iter().filter(|link| link.from_entity == entity.entity) {
            if let Some(column) = reference_candidate_columns_from_analysis(
                analysis,
                &link.to_entity,
                None,
                Some(&link.tokens),
            )
            .into_iter()
            .next()
            {
                let confidence = (entity.confidence * 0.6) + 0.22;
                candidates.push(SchemaEdgeCandidate {
                    rel: link.rel.clone(),
                    from_entity: entity.entity.clone(),
                    to_entity: link.to_entity.clone(),
                    source_column: Some(column.to_string()),
                    confidence: confidence.min(0.99),
                    reason: format!(
                        "column `{}` looks like a `{}` reference compatible with schema link `{}`",
                        column, link.to_entity, link.rel
                    ),
                    evidence: vec![format!("link_tokens={}", join_tokens(&link.tokens))],
                });
            }
        }
    }
    candidates.sort_by(|a, b| b.confidence.total_cmp(&a.confidence));
    candidates
}

fn property_family(property: &PropertyDefinition) -> SchemaTypeFamily {
    if property.name.eq_ignore_ascii_case("id") {
        return SchemaTypeFamily::Identifier;
    }
    if property.name.eq_ignore_ascii_case("identifier")
        || property.name.to_ascii_lowercase().ends_with("identifier")
    {
        return SchemaTypeFamily::Identifier;
    }
    if property.name.ends_with("DateTime") {
        return SchemaTypeFamily::DateTime;
    }
    if property.name.ends_with("Date") {
        return SchemaTypeFamily::Date;
    }
    if property.name.eq_ignore_ascii_case("code") {
        return SchemaTypeFamily::CodeableConcept;
    }
    if property.name.to_ascii_lowercase().contains("value")
        && property.name.to_ascii_lowercase().contains("quantity")
    {
        return SchemaTypeFamily::Quantity;
    }
    let refs = referenced_type_names(&property.schema);
    for name in refs {
        match name.as_str() {
            "Identifier" => return SchemaTypeFamily::Identifier,
            "Reference" => return SchemaTypeFamily::Reference,
            "Coding" => return SchemaTypeFamily::Coding,
            "CodeableConcept" => return SchemaTypeFamily::CodeableConcept,
            "Quantity" => return SchemaTypeFamily::Quantity,
            "Period" => return SchemaTypeFamily::Period,
            _ => {}
        }
    }
    if property
        .json_types
        .iter()
        .any(|kind| kind.eq_ignore_ascii_case("integer"))
    {
        return SchemaTypeFamily::Integer;
    }
    if property
        .json_types
        .iter()
        .any(|kind| kind.eq_ignore_ascii_case("number"))
    {
        return SchemaTypeFamily::Decimal;
    }
    if property
        .json_types
        .iter()
        .any(|kind| kind.eq_ignore_ascii_case("boolean"))
    {
        return SchemaTypeFamily::Boolean;
    }
    if property
        .json_types
        .iter()
        .any(|kind| kind.eq_ignore_ascii_case("string"))
    {
        let name = property.name.to_ascii_lowercase();
        if name.contains("date_time") || name.contains("datetime") {
            return SchemaTypeFamily::DateTime;
        }
        if name.ends_with("date") {
            return SchemaTypeFamily::Date;
        }
        if name.contains("uri") || name.contains("system") || name.contains("url") {
            return SchemaTypeFamily::Uri;
        }
        if name == "status" || name.ends_with("_status") || name.contains("type") {
            return SchemaTypeFamily::Code;
        }
        return SchemaTypeFamily::String;
    }
    SchemaTypeFamily::Unknown
}

fn property_tokens(entity: &EntityType, property: &PropertyDefinition) -> BTreeSet<String> {
    let mut tokens = token_set(&property.name);
    if property.name.eq_ignore_ascii_case("id") {
        tokens.insert("id".to_string());
        tokens.extend(token_set(&entity.name));
    }
    if property.name.eq_ignore_ascii_case("identifier") {
        tokens.insert("identifier".to_string());
        tokens.insert("id".to_string());
    }
    tokens
}

fn link_tokens(entity: &EntityType, link: &LinkDefinition) -> BTreeSet<String> {
    let mut tokens = token_set(&link.rel);
    tokens.extend(token_set(&link.target_entity));
    tokens.extend(token_set(&entity.name));
    tokens
}

fn entities(vocab: &SchemaEntityBindingVocabulary) -> Vec<String> {
    let mut entities = BTreeSet::new();
    for property in &vocab.properties {
        entities.insert(property.entity.clone());
    }
    entities.into_iter().collect()
}

fn family_match_score(
    family: &SchemaTypeFamily,
    shape: &ObservedColumnShape,
    datatype_candidates: &[ObservedDatatypeCandidate],
) -> f64 {
    if let Some(candidate) = datatype_candidates.iter().find(|candidate| &candidate.family == family) {
        return candidate.confidence;
    }
    match family {
        SchemaTypeFamily::Identifier => {
            if shape.identifier_like {
                0.88
            } else {
                0.15
            }
        }
        SchemaTypeFamily::Reference => {
            if shape.identifier_like {
                0.76
            } else {
                0.1
            }
        }
        SchemaTypeFamily::Code | SchemaTypeFamily::Coding | SchemaTypeFamily::CodeableConcept => {
            if shape.short_code_like {
                0.74
            } else {
                0.18
            }
        }
        SchemaTypeFamily::Quantity | SchemaTypeFamily::Decimal => {
            if shape.parseable_decimal_ratio > 0.8 || shape.parseable_integer_ratio > 0.8 {
                0.8
            } else {
                0.12
            }
        }
        SchemaTypeFamily::Integer => {
            if shape.parseable_integer_ratio > 0.8 {
                0.82
            } else {
                0.1
            }
        }
        SchemaTypeFamily::Boolean => {
            if shape.parseable_boolean_ratio > 0.8 {
                0.85
            } else {
                0.05
            }
        }
        SchemaTypeFamily::Date => {
            if shape.parseable_date_ratio > 0.8 {
                0.84
            } else {
                0.05
            }
        }
        SchemaTypeFamily::DateTime => {
            if shape.parseable_datetime_ratio > 0.8 {
                0.86
            } else {
                0.05
            }
        }
        SchemaTypeFamily::Uri => {
            if !shape.long_text_like {
                0.55
            } else {
                0.2
            }
        }
        SchemaTypeFamily::String | SchemaTypeFamily::Unknown => 0.4,
        SchemaTypeFamily::Period => 0.22,
    }
}

fn reference_candidate_columns<'a>(
    table: &'a SourceTable,
    shapes: &BTreeMap<String, ObservedColumnShape>,
    target_entity: &str,
    target_id_column: Option<&str>,
) -> Vec<&'a str> {
    let target_tokens = token_set(target_entity);
    table.headers
        .iter()
        .filter(|column| {
            let tokens = token_set(column);
            let name_match = !tokens.is_disjoint(&target_tokens);
            let id_match = target_id_column
                .map(|id_column| token_set(id_column).intersection(&tokens).next().is_some())
                .unwrap_or(false);
            let shape = shapes.get(*column);
            (name_match || id_match || normalize_token(column).ends_with("id"))
                && shape.map(|shape| shape.identifier_like || shape.short_code_like).unwrap_or(false)
        })
        .map(|column| column.as_str())
        .collect()
}

fn reference_candidate_columns_from_analysis<'a>(
    analysis: &'a TableBindingAnalysis,
    target_entity: &str,
    target_id_column: Option<&str>,
    extra_tokens: Option<&BTreeSet<String>>,
) -> Vec<&'a str> {
    let mut target_tokens = token_set(target_entity);
    if let Some(tokens) = extra_tokens {
        target_tokens.extend(tokens.iter().cloned());
    }
    let mut columns = analysis
        .column_shapes
        .iter()
        .filter(|(column, shape)| {
            let tokens = token_set(column);
            let name_match = !tokens.is_disjoint(&target_tokens);
            let id_match = target_id_column
                .map(|id_column| token_set(id_column).intersection(&tokens).next().is_some())
                .unwrap_or(false);
            (name_match || id_match || normalize_token(column).ends_with("id"))
                && (shape.identifier_like || shape.short_code_like)
        })
        .map(|(column, _)| column.as_str())
        .collect::<Vec<_>>();
    columns.sort();
    columns
}

fn overlap_ratio(left: &HashSet<String>, right: &HashSet<String>) -> f64 {
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }
    let overlap = left.intersection(right).count() as f64;
    let denom = left.len().min(right.len()) as f64;
    overlap / denom
}

fn normalized_value_set(table: &SourceTable, column: &str) -> HashSet<String> {
    table.rows
        .iter()
        .filter_map(|row| row.values.get(column))
        .map(|value| normalize_token(value))
        .filter(|value| !value.is_empty())
        .collect()
}

fn referenced_type_names(schema: &Value) -> Vec<String> {
    let mut refs = Vec::new();
    collect_ref_names(schema, &mut refs);
    refs
}

fn collect_ref_names(value: &Value, refs: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            if let Some(reference) = map.get("$ref").and_then(Value::as_str) {
                if let Some(name) = reference.rsplit('/').next() {
                    refs.push(name.to_string());
                }
            }
            for value in map.values() {
                collect_ref_names(value, refs);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_ref_names(value, refs);
            }
        }
        _ => {}
    }
}

fn datatype_candidate(
    family: SchemaTypeFamily,
    confidence: f64,
    reason: &str,
) -> ObservedDatatypeCandidate {
    ObservedDatatypeCandidate {
        family,
        confidence,
        reason: reason.to_string(),
    }
}

fn token_similarity(left: &BTreeSet<String>, right: &BTreeSet<String>) -> f64 {
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }
    let overlap = left.intersection(right).count() as f64;
    overlap / left.len().max(right.len()) as f64
}

fn normalize_token(value: &str) -> String {
    let mut normalized = String::with_capacity(value.len());
    let mut last_was_sep = false;
    let mut prev_lower_or_digit = false;
    for ch in value.chars() {
        if ch.is_ascii_uppercase() && prev_lower_or_digit && !last_was_sep {
            normalized.push('_');
        }
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
            last_was_sep = false;
            prev_lower_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
        } else if !last_was_sep {
            normalized.push('_');
            last_was_sep = true;
            prev_lower_or_digit = false;
        }
    }
    normalized.trim_matches('_').to_string()
}

fn token_set(value: &str) -> BTreeSet<String> {
    normalize_token(value)
        .split('_')
        .filter(|token| !token.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn join_tokens(tokens: &BTreeSet<String>) -> String {
    tokens.iter().cloned().collect::<Vec<_>>().join(",")
}

fn is_identifier_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 48
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/'))
}

fn is_boolean_like(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "true" | "false" | "yes" | "no" | "1" | "0"
    )
}

fn is_date_like(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(idx, ch)| idx == 4 || idx == 7 || ch.is_ascii_digit())
}

fn is_datetime_like(value: &str) -> bool {
    value.contains('T') && value.contains(':') && is_date_like(&value[..10.min(value.len())])
}
