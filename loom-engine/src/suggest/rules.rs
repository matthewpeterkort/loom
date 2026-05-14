use anyhow::{anyhow, Result};
use std::collections::{BTreeMap, HashSet};

use crate::mapping::{
    DuplicateIdPolicy, EmptyIdPolicy, GraphMappingManifest, MappingValidationPolicy,
    MissingEndpointPolicy, SourceMapping, TypeCoercionPolicy, VertexMapping,
};

use super::builders::{
    case_list_vertex, case_specimen_edge, finding_variant_edge, gene_vertex,
    genomic_finding_vertex, has_any, patient_specimen_edge, patient_vertex, specimen_finding_edge,
    specimen_vertex, variant_gene_edge, variant_vertex,
};
use super::model::{MappingSuggestionCandidate, SuggestionInput, SuggestionKind};

#[derive(Debug, Clone)]
pub(super) struct SourceFacts {
    pub(super) alias: String,
    pub(super) id: String,
    pub(super) columns: HashSet<String>,
}

pub fn build_manifest_suggestion(
    input: SuggestionInput,
) -> Result<(
    GraphMappingManifest,
    Vec<MappingSuggestionCandidate>,
    Vec<String>,
)> {
    if input.graph.trim().is_empty() {
        return Err(anyhow!("graph id is required"));
    }
    if input.sources.is_empty() {
        return Err(anyhow!(
            "at least one source is required for graph suggestion"
        ));
    }
    let facts = input
        .sources
        .iter()
        .map(|(descriptor, table, _)| SourceFacts {
            alias: descriptor.id.clone(),
            id: descriptor.id.clone(),
            columns: table.headers.iter().cloned().collect(),
        })
        .collect::<Vec<_>>();

    let mut candidates = Vec::new();
    let mut warnings = Vec::new();
    let patient_source =
        first_with(&facts, &["PATIENT_ID"]).or_else(|| first_with(&facts, &["person_id"]));
    let sample_source =
        first_with(&facts, &["SAMPLE_ID"]).or_else(|| first_with(&facts, &["sample_id"]));
    let mutation_source = first_with(&facts, &["Hugo_Symbol", "Tumor_Sample_Barcode"])
        .or_else(|| first_with(&facts, &["gene", "sample_id"]));
    let case_source = first_with(&facts, &["stable_id", "sample_id"]);

    let mut vertices = Vec::new();
    if let Some(source) = patient_source {
        vertices.push(patient_vertex(&source.alias, source));
        candidates.push(candidate(
            "vertex.patient_id",
            SuggestionKind::Vertex,
            "Patient",
            &source.id,
            0.96,
            "patient/person identifier column detected",
            vec!["PATIENT_ID/person_id-like column".to_string()],
        ));
    }
    if let Some(source) = sample_source {
        vertices.push(specimen_vertex(&source.alias, source));
        candidates.push(candidate(
            "vertex.sample_id",
            SuggestionKind::Vertex,
            "Specimen",
            &source.id,
            0.96,
            "sample/specimen identifier column detected",
            vec!["SAMPLE_ID/sample_id-like column".to_string()],
        ));
    }
    if let Some(source) = mutation_source {
        vertices.push(gene_vertex(&source.alias, source));
        vertices.push(variant_vertex(&source.alias, source));
        vertices.push(genomic_finding_vertex(&source.alias, source));
        candidates.push(candidate(
            "vertex.gene_symbol",
            SuggestionKind::Vertex,
            "Gene",
            &source.id,
            0.94,
            "gene symbol column detected",
            vec!["Hugo_Symbol/gene-like column".to_string()],
        ));
        candidates.push(candidate(
            "vertex.variant_coordinates",
            SuggestionKind::Vertex,
            "Variant",
            &source.id,
            0.91,
            "genomic coordinate and allele columns detected",
            vec!["chromosome/start/end/ref/alt-like columns".to_string()],
        ));
        candidates.push(candidate(
            "vertex.genomic_finding",
            SuggestionKind::Vertex,
            "GenomicFinding",
            &source.id,
            0.88,
            "sample-level mutation rows detected",
            vec!["sample barcode plus variant columns".to_string()],
        ));
    }
    if let Some(source) = case_source {
        vertices.push(case_list_vertex(&source.alias));
        candidates.push(candidate(
            "vertex.case_list",
            SuggestionKind::Vertex,
            "CaseList",
            &source.id,
            0.86,
            "case-list stable id and sample id columns detected",
            vec!["stable_id and sample_id columns".to_string()],
        ));
    }

    let mut edges = Vec::new();
    if let Some(source) = sample_source {
        if has_vertex(&vertices, "Patient")
            && has_vertex(&vertices, "Specimen")
            && has_any(source, &["PATIENT_ID", "person_id"])
        {
            edges.push(patient_specimen_edge(&source.alias, source));
            candidates.push(candidate(
                "edge.patient_specimen",
                SuggestionKind::Edge,
                "HAS_SPECIMEN",
                &source.id,
                0.94,
                "patient id and sample id co-occur in the same table",
                vec!["PATIENT_ID/person_id -> SAMPLE_ID/sample_id".to_string()],
            ));
        }
    }
    if let Some(source) = mutation_source {
        if has_vertex(&vertices, "Specimen") && has_vertex(&vertices, "GenomicFinding") {
            edges.push(specimen_finding_edge(&source.alias, source));
        }
        if has_vertex(&vertices, "GenomicFinding") && has_vertex(&vertices, "Variant") {
            edges.push(finding_variant_edge(&source.alias, source));
        }
        if has_vertex(&vertices, "Variant") && has_vertex(&vertices, "Gene") {
            edges.push(variant_gene_edge(&source.alias, source));
        }
        candidates.push(candidate(
            "edge.sample_variant_gene",
            SuggestionKind::Edge,
            "sample-variant-gene path",
            &source.id,
            0.89,
            "mutation-like table can connect sample, finding, variant, and gene vertices",
            vec!["sample barcode, gene symbol, and coordinate columns".to_string()],
        ));
    }
    if let Some(source) = case_source {
        if has_vertex(&vertices, "CaseList") && has_vertex(&vertices, "Specimen") {
            edges.push(case_specimen_edge(&source.alias));
            candidates.push(candidate(
                "edge.case_specimen",
                SuggestionKind::Edge,
                "HAS_CASE",
                &source.id,
                0.84,
                "case-list rows reference sample ids",
                vec!["stable_id -> sample_id".to_string()],
            ));
        }
    }

    if vertices.is_empty() {
        warnings.push("no vertex suggestions reached selection threshold".to_string());
    }
    let mut sources = BTreeMap::new();
    for (descriptor, _, _) in &input.sources {
        sources.insert(
            descriptor.id.clone(),
            SourceMapping {
                source: Some(descriptor.id.clone()),
                path: None,
                format: descriptor.format.clone(),
            },
        );
    }
    let manifest = GraphMappingManifest {
        version: 1,
        graph: Some(input.graph),
        display_name: input.display_name,
        sources,
        vertices,
        edges,
        validation: MappingValidationPolicy {
            duplicate_vertex_ids: DuplicateIdPolicy::First,
            duplicate_edge_ids: DuplicateIdPolicy::Error,
            missing_edge_endpoints: MissingEndpointPolicy::Error,
            empty_ids: EmptyIdPolicy::Error,
            type_coercion: TypeCoercionPolicy::Strict,
            ..Default::default()
        },
        identity: BTreeMap::new(),
        references: Vec::new(),
        metadata: BTreeMap::from([(
            "target_vocabulary".to_string(),
            input
                .target_vocabulary
                .unwrap_or_else(|| "fhir_lite".to_string()),
        )]),
    };
    Ok((manifest, candidates, warnings))
}

fn first_with<'a>(facts: &'a [SourceFacts], columns: &[&str]) -> Option<&'a SourceFacts> {
    facts.iter().find(|source| {
        columns.iter().all(|column| {
            source.columns.contains(*column)
                || source.columns.contains(&column.to_ascii_lowercase())
                || source.columns.contains(&column.to_ascii_uppercase())
        })
    })
}

fn has_vertex(vertices: &[VertexMapping], label: &str) -> bool {
    vertices.iter().any(|vertex| vertex.label == label)
}

fn candidate(
    rule_id: &str,
    kind: SuggestionKind,
    label: &str,
    source: &str,
    confidence: f64,
    reason: &str,
    evidence: Vec<String>,
) -> MappingSuggestionCandidate {
    MappingSuggestionCandidate {
        rule_id: rule_id.to_string(),
        kind,
        selected: true,
        confidence,
        label: label.to_string(),
        source: source.to_string(),
        reason: reason.to_string(),
        evidence,
    }
}
