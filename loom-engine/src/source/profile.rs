use super::model::{ColumnType, SourceFormat, SourceTable};
use crate::schema::CompiledGraphSchema;
use crate::schema_binding::{
    analyze_table_bindings, ObservedColumnCluster, ObservedColumnShape, ObservedDatatypeCandidate,
    SchemaEdgeCandidate, SchemaEntityCandidate, SchemaPropertyMatch,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceProfile {
    pub source_id: String,
    pub columns: Vec<String>,
    pub skipped_comment_rows: usize,
    #[serde(default)]
    pub row_count: usize,
    #[serde(default)]
    pub column_profiles: Vec<SourceColumnProfile>,
    #[serde(default)]
    pub suggestions: Vec<SourceProfileSuggestion>,
    #[serde(default)]
    pub clusters: Vec<ObservedColumnCluster>,
    #[serde(default)]
    pub entity_candidates: Vec<SchemaEntityCandidate>,
    #[serde(default)]
    pub edge_candidates: Vec<SchemaEdgeCandidate>,
    #[serde(default)]
    pub schema_derived: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceColumnProfile {
    pub column: String,
    pub non_empty_count: usize,
    pub distinct_count: usize,
    pub uniqueness_ratio: f64,
    pub inferred_type: ColumnType,
    pub sample_values: Vec<String>,
    pub likely_id: bool,
    pub likely_foreign_key: bool,
    #[serde(default)]
    pub semantic_roles: Vec<String>,
    #[serde(default)]
    pub observed_shape: ObservedColumnShape,
    #[serde(default)]
    pub datatype_candidates: Vec<ObservedDatatypeCandidate>,
    #[serde(default)]
    pub property_matches: Vec<SchemaPropertyMatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceProfileSuggestion {
    pub column: String,
    pub suggested_mapping: String,
    pub confidence: f64,
    pub reason: String,
}

pub fn profile_source_table(table: &SourceTable, format: &SourceFormat) -> SourceProfile {
    profile_source_table_with_schema(table, format, None)
}

pub fn profile_source_table_with_schema(
    table: &SourceTable,
    format: &SourceFormat,
    graph_schema: Option<&CompiledGraphSchema>,
) -> SourceProfile {
    let inferred_types = infer_column_types(table);
    let mut suggestions = Vec::new();
    let mut column_profiles = Vec::new();
    let analysis = graph_schema.map(|schema| analyze_table_bindings(table, &inferred_types, schema));

    for column in &table.headers {
        let mut profile = profile_column(table, column, &inferred_types);
        if let Some(analysis) = &analysis {
            profile.observed_shape = analysis
                .column_shapes
                .get(column)
                .cloned()
                .unwrap_or_default();
            profile.datatype_candidates = analysis
                .datatype_candidates
                .get(column)
                .cloned()
                .unwrap_or_default();
            profile.property_matches = analysis
                .property_matches
                .get(column)
                .cloned()
                .unwrap_or_default();
            profile.semantic_roles = profile
                .property_matches
                .iter()
                .map(|candidate| format!("{}.{}", candidate.entity, candidate.property))
                .collect();
            profile.likely_id = profile.likely_id
                || profile.property_matches.iter().any(|candidate| {
                    candidate.property.eq_ignore_ascii_case("id")
                        || matches!(candidate.family, crate::schema_binding::SchemaTypeFamily::Identifier)
                });
            profile.likely_foreign_key = profile.likely_foreign_key
                || profile.property_matches.iter().any(|candidate| {
                    matches!(
                        candidate.family,
                        crate::schema_binding::SchemaTypeFamily::Reference
                    )
                });
            suggestions.extend(profile.property_matches.iter().take(2).map(|candidate| {
                SourceProfileSuggestion {
                    column: column.clone(),
                    suggested_mapping: format!("{}.{}", candidate.entity, candidate.property),
                    confidence: candidate.confidence,
                    reason: candidate.reason.clone(),
                }
            }));
        } else {
            if let Some((mapping, confidence, reason)) = suggestion_for_column(column, format) {
                suggestions.push(SourceProfileSuggestion {
                    column: column.clone(),
                    suggested_mapping: mapping.to_string(),
                    confidence,
                    reason: reason.to_string(),
                });
            }
        }
        column_profiles.push(profile);
    }
    suggestions.sort_by(|a, b| b.confidence.total_cmp(&a.confidence));
    dedupe_suggestions(&mut suggestions);
    SourceProfile {
        source_id: table.source_id.clone(),
        columns: table.headers.clone(),
        skipped_comment_rows: table.skipped_comment_rows,
        row_count: table.rows.len(),
        column_profiles,
        suggestions,
        clusters: analysis
            .as_ref()
            .map(|analysis| analysis.clusters.clone())
            .unwrap_or_default(),
        entity_candidates: analysis
            .as_ref()
            .map(|analysis| analysis.entity_candidates.clone())
            .unwrap_or_default(),
        edge_candidates: analysis
            .as_ref()
            .map(|analysis| analysis.edge_candidates.clone())
            .unwrap_or_default(),
        schema_derived: graph_schema.is_some(),
    }
}

fn profile_column(
    table: &SourceTable,
    column: &str,
    inferred_types: &HashMap<String, ColumnType>,
) -> SourceColumnProfile {
    let mut distinct = HashSet::<String>::new();
    let mut samples = Vec::<String>::new();
    let mut non_empty_count = 0;
    for row in &table.rows {
        let value = row.values.get(column).cloned().unwrap_or_default();
        if value.is_empty() {
            continue;
        }
        non_empty_count += 1;
        distinct.insert(value.clone());
        if samples.len() < 5 && !samples.iter().any(|sample| sample == &value) {
            samples.push(value);
        }
    }
    let uniqueness_ratio = if non_empty_count == 0 {
        0.0
    } else {
        distinct.len() as f64 / non_empty_count as f64
    };
    let roles = semantic_roles_for_column(column, uniqueness_ratio);
    SourceColumnProfile {
        column: column.to_string(),
        non_empty_count,
        distinct_count: distinct.len(),
        uniqueness_ratio,
        inferred_type: inferred_types
            .get(column)
            .cloned()
            .unwrap_or(ColumnType::String),
        sample_values: samples,
        likely_id: roles.iter().any(|role| role.ends_with(".id")) || uniqueness_ratio >= 0.98,
        likely_foreign_key: roles.iter().any(|role| role.ends_with(".reference"))
            || normalized(column).ends_with("_id"),
        semantic_roles: roles,
        observed_shape: ObservedColumnShape::default(),
        datatype_candidates: Vec::new(),
        property_matches: Vec::new(),
    }
}

fn infer_column_types(table: &SourceTable) -> HashMap<String, ColumnType> {
    table.headers
        .iter()
        .map(|column| {
            let values = table
                .rows
                .iter()
                .filter_map(|row| row.values.get(column).cloned())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>();
            (column.clone(), infer_type(&values))
        })
        .collect()
}

fn infer_type(values: &[String]) -> ColumnType {
    if values.is_empty() {
        return ColumnType::String;
    }
    if values.iter().all(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "true" | "false" | "yes" | "no" | "1" | "0"
        )
    }) {
        return ColumnType::Boolean;
    }
    if values.iter().all(|value| value.parse::<i64>().is_ok()) {
        return ColumnType::Integer;
    }
    if values.iter().all(|value| value.parse::<f64>().is_ok()) {
        return ColumnType::Float;
    }
    ColumnType::String
}

fn semantic_roles_for_column(column: &str, uniqueness_ratio: f64) -> Vec<String> {
    let mut roles = Vec::new();
    let normalized = normalized(column);
    match normalized.as_str() {
        "patient_id" | "person_id" | "subject_id" => roles.push("Patient.id".to_string()),
        "sample_id" | "specimen_id" => roles.push("Specimen.id".to_string()),
        "tumor_sample_barcode" => roles.push("Specimen.reference".to_string()),
        "hugo_symbol" | "gene" | "gene_symbol" => roles.push("Gene.symbol".to_string()),
        "chromosome" | "chrom" => roles.push("Variant.chromosome".to_string()),
        "start_position" | "pos" | "position" => roles.push("Variant.start".to_string()),
        "end_position" => roles.push("Variant.end".to_string()),
        "reference_allele" | "ref" => roles.push("Variant.reference_allele".to_string()),
        "tumor_seq_allele2" | "alt" | "alternate_allele" => {
            roles.push("Variant.alternate_allele".to_string())
        }
        "stable_id" => roles.push("CaseList.id".to_string()),
        "sample_id_list" | "case_list_ids" => roles.push("CaseList.samples".to_string()),
        _ => {}
    }
    if roles.is_empty() && normalized.ends_with("_id") {
        if uniqueness_ratio >= 0.98 {
            roles.push("Generic.id".to_string());
        } else {
            roles.push("Generic.reference".to_string());
        }
    }
    roles
}

fn suggestion_for_column(
    column: &str,
    _format: &SourceFormat,
) -> Option<(&'static str, f64, &'static str)> {
    let normalized = normalized(column);
    match normalized.as_str() {
        "patient_id" => Some(("Patient.id", 0.99, "common biomedical patient identifier")),
        "sample_id" => Some(("Specimen.id", 0.99, "common biomedical sample identifier")),
        "tumor_sample_barcode" => Some((
            "GenomicFinding.sample",
            0.99,
            "MAF/cBioPortal mutation row joins to sample by tumor barcode",
        )),
        "hugo_symbol" | "gene" | "gene_symbol" => {
            Some(("Gene.symbol", 0.95, "gene symbol column"))
        }
        "chromosome" | "chrom" => Some(("Variant.chromosome", 0.94, "genomic coordinate column")),
        "start_position" | "pos" | "position" => {
            Some(("Variant.start", 0.9, "genomic coordinate column"))
        }
        "end_position" => Some(("Variant.end", 0.9, "genomic coordinate column")),
        "reference_allele" | "ref" => {
            Some(("Variant.reference_allele", 0.9, "variant allele column"))
        }
        "tumor_seq_allele2" | "alt" | "alternate_allele" => {
            Some(("Variant.alternate_allele", 0.9, "variant allele column"))
        }
        "stable_id" => Some(("CaseList.id", 0.85, "case list stable id")),
        "sample_id_list" | "case_list_ids" => {
            Some(("CaseList.samples", 0.85, "case-list sample ids"))
        }
        _ => None,
    }
}

fn dedupe_suggestions(suggestions: &mut Vec<SourceProfileSuggestion>) {
    let mut seen = HashSet::new();
    suggestions.retain(|suggestion| seen.insert((suggestion.column.clone(), suggestion.suggested_mapping.clone())));
}

fn normalized(column: &str) -> String {
    column.trim().to_ascii_lowercase()
}
