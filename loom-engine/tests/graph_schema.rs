use loom_engine::graph_schema::{GraphEdgeSpec, GraphNodeSpec, GraphSchemaSpec, GraphSchemaStatus};
use loom_engine::mapping::{ColumnMapping, Expr, PropsMapping};
use loom_engine::schema::compile_graph_schema;
use loom_engine::source::SourceTableRef;
use loom_engine::{Engine, EngineConfig};
use loomql_ast::{Query, Step};
use std::collections::BTreeMap;
use std::path::Path;
use tempfile::tempdir;

#[path = "support/cbio_small.rs"]
mod cbio_small;
#[path = "support/cbio_demo.rs"]
mod cbio_demo;

fn default_table_ref(engine: &Engine, source_id: &str) -> anyhow::Result<SourceTableRef> {
    let table = engine
        .list_source_tables(source_id)?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("source `{source_id}` has no tables"))?;
    Ok(SourceTableRef {
        source_id: source_id.to_string(),
        table_id: table.id,
    })
}

fn patient_specimen_spec(engine: &Engine) -> anyhow::Result<GraphSchemaSpec> {
    let patient_ref = default_table_ref(engine, "patients")?;
    let sample_ref = default_table_ref(engine, "samples")?;
    Ok(GraphSchemaSpec {
        version: 1,
        id: Some("cbio_builder".to_string()),
        graph: "cbio_builder".to_string(),
        display_name: Some("cBio Builder".to_string()),
        bound_schema: None,
        nodes: vec![
            GraphNodeSpec {
                source: patient_ref,
                label: "Patient".to_string(),
                id: Expr::Concat {
                    concat: vec![
                        Expr::Text("Patient/".to_string()),
                        Expr::Column {
                            column: "PATIENT_ID".to_string(),
                        },
                    ],
                },
                predicate: None,
                columns: BTreeMap::from([(
                    "patient_id".to_string(),
                    ColumnMapping::Expr(Expr::Column {
                        column: "PATIENT_ID".to_string(),
                    }),
                )]),
                props: PropsMapping::None,
                prop_types: BTreeMap::new(),
                schema_entity: None,
            },
            GraphNodeSpec {
                source: sample_ref.clone(),
                label: "Specimen".to_string(),
                id: Expr::Concat {
                    concat: vec![
                        Expr::Text("Specimen/".to_string()),
                        Expr::Column {
                            column: "SAMPLE_ID".to_string(),
                        },
                    ],
                },
                predicate: None,
                columns: BTreeMap::from([
                    (
                        "identifier".to_string(),
                        ColumnMapping::Expr(Expr::Column {
                            column: "SAMPLE_ID".to_string(),
                        }),
                    ),
                    (
                        "type".to_string(),
                        ColumnMapping::Expr(Expr::Column {
                            column: "SAMPLE_TYPE".to_string(),
                        }),
                    ),
                ]),
                props: PropsMapping::None,
                prop_types: BTreeMap::new(),
                schema_entity: None,
            },
        ],
        edges: vec![GraphEdgeSpec {
            source: sample_ref,
            label: "subject_Patient".to_string(),
            from_label: Some("Specimen".to_string()),
            to_label: Some("Patient".to_string()),
            from: Expr::Concat {
                concat: vec![
                    Expr::Text("Specimen/".to_string()),
                    Expr::Column {
                        column: "SAMPLE_ID".to_string(),
                    },
                ],
            },
            to: Expr::Concat {
                concat: vec![
                    Expr::Text("Patient/".to_string()),
                    Expr::Column {
                        column: "PATIENT_ID".to_string(),
                    },
                ],
            },
            id: None,
            predicate: None,
            columns: BTreeMap::new(),
            props: PropsMapping::None,
            prop_types: BTreeMap::new(),
            schema_relation: None,
        }],
        metadata: BTreeMap::new(),
    })
}

#[tokio::test]
async fn graph_schema_spec_registers_and_queries_virtual_graph() -> anyhow::Result<()> {
    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    cbio_small::register_fixture_sources(&engine).await?;
    let spec = patient_specimen_spec(&engine)?;

    let descriptor = engine.register_graph_schema(spec).await?;
    assert_eq!(descriptor.status, GraphSchemaStatus::Draft);

    let preview = engine
        .preview_graph_schema_with_schema("cbio_builder", Path::new("."), None, None)
        .await?;
    assert_eq!(preview.node_counts.get("Patient"), Some(&2));
    assert_eq!(preview.node_counts.get("Specimen"), Some(&3));
    assert_eq!(preview.edge_counts.get("subject_Patient"), Some(&3));
    assert!(!preview
        .sample_node_rows
        .get("Patient")
        .cloned()
        .unwrap_or_default()
        .is_empty());

    let registered = engine
        .register_graph_schema_runtime_with_schema("cbio_builder", Path::new("."), false, None, None)
        .await?;
    assert_eq!(registered.status, GraphSchemaStatus::Registered);
    assert_eq!(registered.registered_mapping_graph.as_deref(), Some("cbio_builder"));
    let binding = engine
        .get_virtual_graph_descriptor("cbio_builder")?
        .virtual_binding
        .expect("virtual binding");
    let patient_view = &binding.node_view_names[0];
    let specimen_view = &binding.node_view_names[1];
    let subject_edge = &binding.edge_view_names[0];

    let rows = engine
        .query_sql_json_rows(
            format!(
                r#"
                SELECT p.patient_id, s.identifier
                FROM {patient_view} p
                JOIN {subject_edge} e ON e.to_id = p.id
                JOIN {specimen_view} s ON s.id = e.from_id
                ORDER BY p.patient_id, s.identifier
                "#
            )
            .as_str(),
        )
        .await?;
    assert_eq!(rows.len(), 3);

    let query = Query {
        graph: "cbio_builder".to_string(),
        steps: vec![
            Step::V {
                ids: vec!["Patient/TCGA-A1-A0SB".to_string()],
            },
            Step::In {
                labels: vec!["subject_Patient".to_string()],
            },
            Step::Render {
                fields: vec!["id".to_string(), "identifier".to_string()],
            },
        ],
    };
    let loomql_rows = engine.query_json_rows(&query).await?;
    assert_eq!(loomql_rows.len(), 2);
    Ok(())
}

#[tokio::test]
async fn graph_schema_validation_rejects_invalid_columns_and_schema_relations() -> anyhow::Result<()>
{
    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    cbio_small::register_fixture_sources(&engine).await?;
    let patient_ref = default_table_ref(&engine, "patients")?;
    let sample_ref = default_table_ref(&engine, "samples")?;
    let schema_doc = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": {
            "Patient": {"type":"object","properties":{"id":{"type":"string"}}},
            "Specimen": {
                "type":"object",
                "properties":{"id":{"type":"string"}, "identifier":{"type":"string"}},
                "links": [{
                    "rel": "subject_Patient",
                    "targetSchema": {"$ref": "#/$defs/Patient"}
                }]
            }
        }
    });
    let compiled = compile_graph_schema(&schema_doc)?;
    let spec = GraphSchemaSpec {
        version: 1,
        id: Some("broken_schema".to_string()),
        graph: "broken_schema".to_string(),
        display_name: None,
        bound_schema: Some("master".to_string()),
        nodes: vec![GraphNodeSpec {
            source: patient_ref.clone(),
            label: "Patient".to_string(),
            id: Expr::Column {
                column: "PATIENT_ID".to_string(),
            },
            predicate: None,
            columns: BTreeMap::from([(
                "unknown_prop".to_string(),
                ColumnMapping::Expr(Expr::Column {
                    column: "DOES_NOT_EXIST".to_string(),
                }),
            )]),
            props: PropsMapping::None,
            prop_types: BTreeMap::new(),
            schema_entity: Some("Patient".to_string()),
        }],
        edges: vec![GraphEdgeSpec {
            source: sample_ref,
            label: "bad_edge".to_string(),
            from_label: Some("Specimen".to_string()),
            to_label: Some("Patient".to_string()),
            from: Expr::Column {
                column: "SAMPLE_ID".to_string(),
            },
            to: Expr::Column {
                column: "PATIENT_ID".to_string(),
            },
            id: None,
            predicate: None,
            columns: BTreeMap::new(),
            props: PropsMapping::None,
            prop_types: BTreeMap::new(),
            schema_relation: Some("subject_Patient".to_string()),
        }],
        metadata: BTreeMap::new(),
    };
    let _ = engine.register_graph_schema(spec).await?;
    let report = engine
        .validate_graph_schema_with_schema(
            "broken_schema",
            Path::new("."),
            Some(&compiled),
            Some("master".to_string()),
        )
        .await?;
    assert!(!report.valid);
    assert!(report
        .errors
        .iter()
        .any(|error| error.path.contains("columns") || error.path.contains("edges")));
    Ok(())
}

#[tokio::test]
async fn graph_schema_preview_includes_schema_derived_source_candidates() -> anyhow::Result<()> {
    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    cbio_small::register_fixture_sources(&engine).await?;
    let schema_doc = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": {
            "Patient": {
                "type":"object",
                "properties":{"id":{"type":"string"},"patient_id":{"type":"string"}}
            },
            "Specimen": {
                "type":"object",
                "properties":{"id":{"type":"string"},"identifier":{"type":"string"},"type":{"type":"string"}},
                "links":[{"rel":"subject_Patient","targetSchema":{"$ref":"#/$defs/Patient"}}]
            }
        }
    });
    let compiled = compile_graph_schema(&schema_doc)?;
    let spec = patient_specimen_spec(&engine)?;
    engine.register_graph_schema(spec).await?;
    let preview = engine
        .preview_graph_schema_with_schema(
            "cbio_builder",
            Path::new("."),
            Some(&compiled),
            Some("master".to_string()),
        )
        .await?;
    assert!(!preview.source_hints.is_empty());
    assert!(preview.source_hints.iter().any(|hint| hint.schema_derived));
    assert!(preview.source_hints.iter().any(|hint| {
        hint.entity_candidates
            .iter()
            .any(|candidate| candidate.entity == "Specimen")
    }));
    Ok(())
}

#[tokio::test]
async fn graph_schema_preview_for_cbio_demo_returns_meaningful_counts() -> anyhow::Result<()> {
    let demo_dir = cbio_demo::cbio_demo_dir();
    anyhow::ensure!(demo_dir.exists(), "cbio-demo fixture missing");
    let tmp = tempdir()?;
    let fixture_dir = tmp.path().join("cbio-demo-tsv");
    cbio_demo::materialize_cbio_demo_fixture(&fixture_dir)?;
    let engine = Engine::new(EngineConfig {
        work_dir: tmp.path().to_string_lossy().to_string(),
    })?;
    cbio_demo::register_cbio_demo_sources(&engine, &fixture_dir).await?;
    let graph_schema = compile_graph_schema(&cbio_demo::load_sanitized_master_graph_schema()?)?;
    let spec = cbio_demo::build_cbio_demo_graph_schema_spec(&engine)?;
    let descriptor = engine.register_graph_schema(spec).await?;
    let preview = engine
        .preview_graph_schema_with_schema(
            &descriptor.id,
            &fixture_dir,
            Some(&graph_schema),
            Some("master".to_string()),
        )
        .await?;
    assert!(preview.node_counts.get("Patient").copied().unwrap_or_default() > 1000);
    assert!(preview.edge_counts.get("focus").copied().unwrap_or_default() > 1000);
    assert!(preview
        .source_hints
        .iter()
        .any(|hint| hint.source.source_id == "mutations"));
    Ok(())
}
