use loom_engine::mapping::GraphMappingStatus;
use loom_engine::schema::compile_graph_schema;
use loom_engine::source::{SourceFormat, SourceLocation, SourceRegistration};
use loom_engine::{Engine, EngineConfig};
use serde_json::Value;
use std::path::Path;
use tempfile::tempdir;

#[path = "support/cbio_small.rs"]
mod cbio_small;

fn local_path_source(id: &str, path: &Path, format: SourceFormat) -> SourceRegistration {
    SourceRegistration {
        id: id.to_string(),
        display_name: Some(id.to_string()),
        format,
        location: SourceLocation::Local {
            path: path.to_string_lossy().to_string(),
        },
        read_options: Default::default(),
    }
}

#[tokio::test]
async fn suggests_cbioportal_graph_manifest_from_catalog_sources() -> anyhow::Result<()> {
    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    cbio_small::register_fixture_sources(&engine).await?;
    let suggestion = engine
        .suggest_graph_mapping(
            "suggested_cbio".to_string(),
            Some("Suggested cBio Graph".to_string()),
            vec![
                "patients".to_string(),
                "samples".to_string(),
                "mutations".to_string(),
                "sequenced_cases".to_string(),
            ],
            Some("fhir_lite".to_string()),
            Some(100),
        )
        .await?;

    assert_eq!(suggestion.manifest.graph.as_deref(), Some("suggested_cbio"));
    assert!(suggestion.validation.valid);
    assert!(suggestion
        .manifest
        .sources
        .values()
        .all(|source| source.source.is_some() && source.path.is_none()));
    for label in [
        "Patient",
        "Specimen",
        "Gene",
        "Variant",
        "GenomicFinding",
        "CaseList",
    ] {
        assert!(
            suggestion
                .manifest
                .vertices
                .iter()
                .any(|vertex| vertex.label == label),
            "missing vertex {label}"
        );
    }
    for label in [
        "HAS_SPECIMEN",
        "HAS_OBSERVATION",
        "OBSERVES_VARIANT",
        "IN_GENE",
        "HAS_CASE",
    ] {
        assert!(
            suggestion
                .manifest
                .edges
                .iter()
                .any(|edge| edge.label == label
                    && edge.from_label.is_some()
                    && edge.to_label.is_some()),
            "missing edge {label}"
        );
    }
    assert!(suggestion
        .candidates
        .iter()
        .any(|candidate| candidate.rule_id == "edge.patient_specimen"));
    let descriptor = engine
        .register_graph_mapping(suggestion.manifest, &cbio_small::fixture_dir(), true)
        .await?;
    assert_eq!(descriptor.status, GraphMappingStatus::Compiled);
    let rows = engine
        .query_sql_json_rows(
            "SELECT COUNT(*) AS count FROM vertices_suggested_cbio WHERE label = 'Patient'",
        )
        .await?;
    let row: Value = serde_json::from_str(&rows[0])?;
    assert_eq!(row["count"], 2);
    Ok(())
}

#[tokio::test]
async fn suggests_graph_manifest_from_generic_source_names() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let samples_path = tmp.path().join("measurements.tsv");
    std::fs::write(
        &samples_path,
        "person_id\tsample_id\tassay\nP1\tS1\tpanel\nP1\tS2\tpanel\n",
    )?;
    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    engine
        .register_source(local_path_source(
            "measurements",
            &samples_path,
            SourceFormat::Tsv,
        ))
        .await?;
    let suggestion = engine
        .suggest_graph_mapping(
            "generic_graph".to_string(),
            None,
            vec!["measurements".to_string()],
            Some("fhir_lite".to_string()),
            Some(100),
        )
        .await?;
    assert!(suggestion
        .manifest
        .vertices
        .iter()
        .any(|vertex| vertex.label == "Patient"));
    assert!(suggestion
        .manifest
        .vertices
        .iter()
        .any(|vertex| vertex.label == "Specimen"));
    assert!(suggestion
        .candidates
        .iter()
        .any(|candidate| candidate.rule_id == "edge.patient_specimen"));
    Ok(())
}

#[tokio::test]
async fn schema_bound_suggestion_uses_schema_derived_candidates() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let patients_path = tmp.path().join("people.tsv");
    let samples_path = tmp.path().join("biospecimens.tsv");
    std::fs::write(&patients_path, "id\tmrn\nP1\tM1\nP2\tM2\n")?;
    std::fs::write(
        &samples_path,
        "identifier\tpatient_id\ttype\nS1\tP1\tblood\nS2\tP2\ttissue\n",
    )?;
    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    engine
        .register_source(local_path_source("people", &patients_path, SourceFormat::Tsv))
        .await?;
    engine
        .register_source(local_path_source(
            "biospecimens",
            &samples_path,
            SourceFormat::Tsv,
        ))
        .await?;
    let schema_doc = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": {
            "Patient": {
                "type":"object",
                "properties":{"id":{"type":"string"},"mrn":{"type":"string"}}
            },
            "Specimen": {
                "type":"object",
                "properties":{"id":{"type":"string"},"identifier":{"type":"string"},"type":{"type":"string"}},
                "links":[{"rel":"subject_Patient","targetSchema":{"$ref":"#/$defs/Patient"}}]
            }
        }
    });
    let compiled = compile_graph_schema(&schema_doc)?;
    let suggestion = engine
        .suggest_graph_mapping_with_schema(
            "schema_bound".to_string(),
            None,
            vec!["people".to_string(), "biospecimens".to_string()],
            Some("master".to_string()),
            Some(100),
            Some(&compiled),
        )
        .await?;
    assert!(suggestion.source_profiles.iter().all(|profile| profile.schema_derived));
    assert!(suggestion
        .manifest
        .vertices
        .iter()
        .any(|vertex| vertex.label == "Patient"));
    assert!(suggestion
        .manifest
        .vertices
        .iter()
        .any(|vertex| vertex.label == "Specimen"));
    assert!(suggestion
        .manifest
        .edges
        .iter()
        .any(|edge| edge.label == "subject_Patient"));
    assert!(suggestion
        .candidates
        .iter()
        .any(|candidate| candidate.rule_id.starts_with("schema.vertex")));
    Ok(())
}
