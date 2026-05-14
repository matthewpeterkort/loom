use anyhow::{anyhow, Result};
use std::collections::{BTreeMap, HashSet};

use crate::mapping::{
    ColumnMapping, EdgeMapping, Expr, GraphMappingManifest, PropsMapping, SourceMapping,
    VertexMapping,
};
use crate::schema::CompiledGraphSchema;
use crate::schema_binding::{best_id_column_for_entity, cross_table_edge_candidates, TableBindingAnalysis, TableBindingContext};
use crate::source::{SourceDescriptor, SourceProfile, SourceTable};

use super::model::{MappingSuggestionCandidate, SuggestionInput, SuggestionKind};

#[derive(Debug, Clone)]
struct SelectedEntity {
    source: String,
    entity: String,
    id_column: Option<String>,
    confidence: f64,
}

pub fn build_schema_manifest_suggestion(
    input: SuggestionInput,
    graph_schema: &CompiledGraphSchema,
) -> Result<(
    GraphMappingManifest,
    Vec<MappingSuggestionCandidate>,
    Vec<String>,
)> {
    if input.graph.trim().is_empty() {
        return Err(anyhow!("graph id is required"));
    }
    if input.sources.is_empty() {
        return Err(anyhow!("at least one source is required for graph suggestion"));
    }
    let analyses = input
        .sources
        .iter()
        .map(|(_, table, profile)| build_analysis_from_profile(table, profile))
        .collect::<Vec<_>>();
    let contexts = input
        .sources
        .iter()
        .zip(analyses.iter())
        .map(|((descriptor, table, _), analysis)| TableBindingContext {
            source_id: descriptor.id.as_str(),
            table,
            analysis,
        })
        .collect::<Vec<_>>();
    let cross_edges = cross_table_edge_candidates(&contexts, graph_schema);
    let mut warnings = Vec::new();
    let mut candidates = Vec::new();
    let mut vertices = Vec::new();
    let mut selected_entities = Vec::new();
    for ((descriptor, _table, profile), analysis) in input.sources.iter().zip(analyses.iter()) {
        let picked = pick_entities_for_source(profile, analysis);
        if picked.is_empty() {
            warnings.push(format!(
                "source `{}` produced no schema-derived entity candidates above threshold",
                descriptor.id
            ));
        }
        for entity in picked {
            let vertex = build_vertex_mapping(descriptor, profile, &entity);
            candidates.push(MappingSuggestionCandidate {
                rule_id: format!("schema.vertex.{}.{}", descriptor.id, entity.entity),
                kind: SuggestionKind::Vertex,
                selected: true,
                confidence: entity.confidence,
                label: entity.entity.clone(),
                source: descriptor.id.clone(),
                reason: "schema-derived entity candidate".to_string(),
                evidence: vec![
                    format!("source={}", descriptor.id),
                    format!("id_column={}", entity.id_column.clone().unwrap_or_else(|| "<row_number>".to_string())),
                ],
            });
            selected_entities.push(entity);
            vertices.push(vertex);
        }
    }
    let selected_set = selected_entities
        .iter()
        .map(|entity| (entity.source.clone(), entity.entity.clone()))
        .collect::<HashSet<_>>();
    let mut edges = Vec::new();
    let mut seen_edges = HashSet::new();
    for ((descriptor, _table, profile), _analysis) in input.sources.iter().zip(analyses.iter()) {
        let edge_candidates = profile
            .edge_candidates
            .iter()
            .cloned()
            .chain(
                cross_edges
                    .iter()
                    .filter(|candidate| candidate.source_column.is_some())
                    .cloned(),
            )
            .filter(|candidate| {
                selected_set.contains(&(descriptor.id.clone(), candidate.from_entity.clone()))
                    && selected_entities
                        .iter()
                        .any(|entity| entity.entity == candidate.to_entity)
            })
            .collect::<Vec<_>>();
        for candidate in edge_candidates {
            let Some(from_entity) = selected_entities.iter().find(|entity| {
                entity.source == descriptor.id && entity.entity == candidate.from_entity
            }) else {
                continue;
            };
            let edge_key = format!(
                "{}:{}:{}:{}",
                descriptor.id, candidate.from_entity, candidate.rel, candidate.to_entity
            );
            if !seen_edges.insert(edge_key) {
                continue;
            }
            let Some(source_column) = candidate.source_column.clone() else {
                continue;
            };
            edges.push(build_edge_mapping(descriptor, from_entity, &candidate, &source_column));
            candidates.push(MappingSuggestionCandidate {
                rule_id: format!(
                    "schema.edge.{}.{}.{}",
                    descriptor.id, candidate.from_entity, candidate.rel
                ),
                kind: SuggestionKind::Edge,
                selected: true,
                confidence: candidate.confidence,
                label: candidate.rel.clone(),
                source: descriptor.id.clone(),
                reason: candidate.reason.clone(),
                evidence: candidate.evidence.clone(),
            });
        }
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
        validation: Default::default(),
        identity: Default::default(),
        references: Vec::new(),
        metadata: BTreeMap::from([(
            "target_vocabulary".to_string(),
            input
                .target_vocabulary
                .unwrap_or_else(|| "schema_bound".to_string()),
        )]),
    };
    Ok((manifest, candidates, warnings))
}

fn pick_entities_for_source(
    profile: &SourceProfile,
    analysis: &TableBindingAnalysis,
) -> Vec<SelectedEntity> {
    let mut candidates = profile
        .entity_candidates
        .iter()
        .filter(|candidate| candidate.confidence >= 0.58)
        .take(3)
        .map(|candidate| SelectedEntity {
            source: profile.source_id.clone(),
            entity: candidate.entity.clone(),
            id_column: best_id_column_for_entity(analysis, &candidate.entity).map(ToString::to_string),
            confidence: candidate.confidence,
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        if let Some(candidate) = profile.entity_candidates.first() {
            candidates.push(SelectedEntity {
                source: profile.source_id.clone(),
                entity: candidate.entity.clone(),
                id_column: best_id_column_for_entity(analysis, &candidate.entity).map(ToString::to_string),
                confidence: candidate.confidence,
            });
        }
    }
    candidates
}

fn build_vertex_mapping(
    descriptor: &SourceDescriptor,
    profile: &SourceProfile,
    entity: &SelectedEntity,
) -> VertexMapping {
    let id_column = entity.id_column.as_deref();
    let mut columns = BTreeMap::new();
    let mut seen_properties = HashSet::new();
    for column in &profile.column_profiles {
        let best = column
            .property_matches
            .iter()
            .filter(|candidate| candidate.entity == entity.entity)
            .max_by(|a, b| a.confidence.total_cmp(&b.confidence));
        if let Some(best) = best {
            if best.property.eq_ignore_ascii_case("id") {
                continue;
            }
            if seen_properties.insert(best.property.clone()) {
                columns.insert(
                    best.property.clone(),
                    ColumnMapping::Expr(Expr::Column {
                        column: column.column.clone(),
                    }),
                );
            }
        }
    }
    VertexMapping {
        source: descriptor.id.clone(),
        label: entity.entity.clone(),
        id: entity_id_expr(&entity.entity, id_column),
        predicate: None,
        columns,
        props: PropsMapping::None,
        prop_types: BTreeMap::new(),
    }
}

fn build_edge_mapping(
    descriptor: &SourceDescriptor,
    from_entity: &SelectedEntity,
    candidate: &crate::schema_binding::SchemaEdgeCandidate,
    source_column: &str,
) -> EdgeMapping {
    EdgeMapping {
        source: descriptor.id.clone(),
        label: candidate.rel.clone(),
        from_label: Some(candidate.from_entity.clone()),
        to_label: Some(candidate.to_entity.clone()),
        from: entity_id_expr(&from_entity.entity, from_entity.id_column.as_deref()),
        to: Expr::Concat {
            concat: vec![
                Expr::Text(format!("{}/", candidate.to_entity)),
                Expr::Column {
                    column: source_column.to_string(),
                },
            ],
        },
        id: Some(Expr::Concat {
            concat: vec![
                Expr::Text("edge/".to_string()),
                entity_id_expr(&from_entity.entity, from_entity.id_column.as_deref()),
                Expr::Text(format!("/{}/", candidate.rel)),
                Expr::Column {
                    column: source_column.to_string(),
                },
            ],
        }),
        predicate: None,
        columns: BTreeMap::new(),
        props: PropsMapping::None,
        prop_types: BTreeMap::new(),
    }
}

fn entity_id_expr(entity: &str, id_column: Option<&str>) -> Expr {
    Expr::Concat {
        concat: vec![
            Expr::Text(format!("{entity}/")),
            match id_column {
                Some(column) => Expr::Column {
                    column: column.to_string(),
                },
                None => Expr::RowNumber { row_number: true },
            },
        ],
    }
}

fn build_analysis_from_profile(_table: &SourceTable, profile: &SourceProfile) -> TableBindingAnalysis {
    let mut analysis = TableBindingAnalysis::default();
    analysis.clusters = profile.clusters.clone();
    analysis.entity_candidates = profile.entity_candidates.clone();
    analysis.edge_candidates = profile.edge_candidates.clone();
    for column in &profile.column_profiles {
        analysis
            .column_shapes
            .insert(column.column.clone(), column.observed_shape.clone());
        analysis
            .datatype_candidates
            .insert(column.column.clone(), column.datatype_candidates.clone());
        analysis
            .property_matches
            .insert(column.column.clone(), column.property_matches.clone());
    }
    analysis
}
