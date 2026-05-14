use loom_engine::graph::{GraphColumnRole, GraphSourceKind};
use loom_engine::mapping::{load_mapping_manifest, GraphMappingManifest, GraphMappingStatus};
use loom_engine::{Engine, EngineConfig};
use loomql_ast::{Query, Step};
use serde_json::Value;
use tempfile::tempdir;

#[path = "support/cbio_small.rs"]
mod cbio_small;

#[tokio::test]
async fn manifest_maps_catalog_sources_to_fhir_lite_graph() -> anyhow::Result<()> {
    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    cbio_small::register_fixture_sources(&engine).await?;
    let manifest = load_mapping_manifest(&cbio_small::fixture_dir().join("mapping.json"))?;
    let descriptor = engine
        .register_graph_mapping(manifest, &cbio_small::fixture_dir(), true)
        .await?;
    assert_eq!(descriptor.status, GraphMappingStatus::Compiled);
    let report = descriptor.last_report.as_ref().expect("compile report");
    assert_eq!(report.vertex_plans, 6);
    assert_eq!(report.edge_plans, 5);
    assert!(report
        .vertices_uri
        .as_deref()
        .unwrap_or_default()
        .contains("/graph_mappings/cbio/vertices"));
    assert_eq!(report.vertex_labels.get("Patient"), Some(&2));
    assert_eq!(report.vertex_labels.get("Specimen"), Some(&3));
    assert_eq!(report.vertex_labels.get("Gene"), Some(&2));
    assert_eq!(report.vertex_labels.get("Variant"), Some(&3));
    assert_eq!(report.vertex_labels.get("GenomicFinding"), Some(&3));
    assert_eq!(report.vertex_labels.get("CaseList"), Some(&1));
    let graph = engine.get_graph_descriptor("cbio")?;
    assert_eq!(graph.active_version, 1);
    assert_eq!(graph.source_kind, GraphSourceKind::MappingManifest);
    assert_eq!(graph.active_snapshot.stats.vertices, 14);
    assert_eq!(graph.active_snapshot.stats.edges, 14);
    assert_eq!(graph.active_snapshot.stats.vertex_labels.get("Patient"), Some(&2));
    assert!(graph
        .active_snapshot
        .source_dependencies
        .iter()
        .any(|source| source == "patients"));
    assert!(graph
        .active_snapshot
        .source_fingerprints
        .contains_key("patients"));
    assert!(graph
        .active_snapshot
        .vertex_columns
        .iter()
        .any(|column| column.name == "prop_os_status" && column.role == GraphColumnRole::Property));
    assert!(graph
        .active_snapshot
        .edge_columns
        .iter()
        .any(|column| column.name == "from_id" && column.role == GraphColumnRole::Endpoint));
    assert!(graph.active_snapshot.stats.vertex_file_bytes.unwrap_or_default() > 0);

    let tp53_sql = r#"
        SELECT DISTINCT
            p.patient_id,
            s.sample_id,
            g.gene_symbol,
            v.variant_id,
            v.chromosome,
            v.start_position,
            v.end_position,
            v.reference_allele,
            v.tumor_seq_allele2,
            f.source_id,
            f.source_row
        FROM vertices_cbio p
        JOIN edges_cbio ps ON ps.from_id = p.id AND ps.label = 'HAS_SPECIMEN'
        JOIN vertices_cbio s ON s.id = ps.to_id
        JOIN edges_cbio sf ON sf.from_id = s.id AND sf.label = 'HAS_OBSERVATION'
        JOIN vertices_cbio f ON f.id = sf.to_id
        JOIN edges_cbio fv ON fv.from_id = f.id AND fv.label = 'OBSERVES_VARIANT'
        JOIN vertices_cbio v ON v.id = fv.to_id
        JOIN edges_cbio vg ON vg.from_id = v.id AND vg.label = 'IN_GENE'
        JOIN vertices_cbio g ON g.id = vg.to_id
        WHERE p.label = 'Patient' AND g.gene_symbol = 'TP53'
        ORDER BY p.patient_id, s.sample_id
    "#;
    let rows = engine.query_sql_json_rows(tp53_sql).await?;
    assert_eq!(rows.len(), 2);
    let parsed = rows
        .iter()
        .map(|row| serde_json::from_str::<Value>(row))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(parsed[0]["patient_id"], "TCGA-A1-A0SB");
    assert_eq!(parsed[0]["source_id"], "mutations");
    assert_eq!(parsed[1]["patient_id"], "TCGA-A2-A04P");

    let prop_rows = engine
        .query_sql_json_rows(
            "SELECT prop_os_status, prop_os_months FROM vertices_cbio WHERE label = 'Patient' ORDER BY patient_id LIMIT 1",
        )
        .await?;
    let props: Value = serde_json::from_str(&prop_rows[0])?;
    assert_eq!(props["prop_os_status"], "0:LIVING");

    let query = Query {
        graph: "cbio".to_string(),
        steps: vec![
            Step::V {
                ids: vec!["Specimen/TCGA-A1-A0SB-01".to_string()],
            },
            Step::OutE {
                labels: vec!["HAS_OBSERVATION".to_string()],
            },
            Step::Render {
                fields: vec![
                    "from_id".to_string(),
                    "to_id".to_string(),
                    "label".to_string(),
                ],
            },
        ],
    };
    let loomql_rows = engine.query_json_rows(&query).await?;
    assert_eq!(loomql_rows.len(), 1);
    Ok(())
}

#[tokio::test]
async fn graph_mapping_registers_virtual_views_by_default() -> anyhow::Result<()> {
    let temp = tempdir()?;
    let engine = Engine::new(EngineConfig {
        work_dir: temp.path().to_string_lossy().to_string(),
    })?;
    cbio_small::register_fixture_sources(&engine).await?;
    let manifest = load_mapping_manifest(&cbio_small::fixture_dir().join("mapping.json"))?;

    let descriptor = engine
        .register_graph_mapping(manifest, &cbio_small::fixture_dir(), false)
        .await?;

    assert_eq!(descriptor.status, GraphMappingStatus::Registered);
    assert!(descriptor.virtual_binding.is_some());
    assert_eq!(descriptor.published_graph, None);

    let binding = descriptor.virtual_binding.as_ref().expect("virtual binding");
    assert_eq!(binding.graph, "cbio");
    assert_eq!(binding.vertex_table, "vertices_cbio");
    assert_eq!(binding.edge_table, "edges_cbio");
    assert_eq!(binding.node_view_names.len(), 6);
    assert_eq!(binding.edge_view_names.len(), 5);
    assert!(binding.source_dependencies.iter().any(|source| source == "patients"));
    assert!(binding.source_fingerprints.contains_key("patients"));

    let vertex_rows = engine
        .query_sql_json_rows(
            "SELECT label, source_id, source_row FROM vertices_cbio ORDER BY label, id LIMIT 3",
        )
        .await?;
    assert!(!vertex_rows.is_empty());
    let first_vertex: Value = serde_json::from_str(&vertex_rows[0])?;
    assert!(first_vertex["label"].as_str().unwrap_or_default().len() > 0);
    assert!(first_vertex["source_id"].as_str().unwrap_or_default().len() > 0);
    assert!(first_vertex["source_row"].as_str().unwrap_or_default().len() > 0);

    let edge_rows = engine
        .query_sql_json_rows(
            "SELECT from_id, to_id, label, source_id, source_row FROM edges_cbio ORDER BY label, id LIMIT 3",
        )
        .await?;
    assert!(!edge_rows.is_empty());

    assert!(engine.get_graph_descriptor("cbio").is_err());
    assert!(!temp.path().join("graph_mappings").join("cbio").exists());
    Ok(())
}

#[tokio::test]
async fn virtual_graph_loomql_vertex_traversal_matches_published_graph() -> anyhow::Result<()> {
    let virtual_tmp = tempdir()?;
    let virtual_engine = Engine::new(EngineConfig {
        work_dir: virtual_tmp.path().to_string_lossy().to_string(),
    })?;
    cbio_small::register_fixture_sources(&virtual_engine).await?;
    let manifest = load_mapping_manifest(&cbio_small::fixture_dir().join("mapping.json"))?;
    let virtual_descriptor = virtual_engine
        .register_graph_mapping(manifest.clone(), &cbio_small::fixture_dir(), false)
        .await?;
    assert_eq!(
        virtual_descriptor.status,
        GraphMappingStatus::Registered,
        "{}",
        virtual_descriptor
            .last_error
            .clone()
            .unwrap_or_else(|| "missing registration error".to_string())
    );
    assert!(virtual_descriptor.virtual_binding.is_some());
    assert!(virtual_engine.get_graph_mapping("cbio")?.virtual_binding.is_some());

    let published_tmp = tempdir()?;
    let published_engine = Engine::new(EngineConfig {
        work_dir: published_tmp.path().to_string_lossy().to_string(),
    })?;
    cbio_small::register_fixture_sources(&published_engine).await?;
    published_engine
        .register_graph_mapping(manifest, &cbio_small::fixture_dir(), true)
        .await?;

    let virtual_query = Query {
        graph: "cbio".to_string(),
        steps: vec![
            Step::V {
                ids: vec!["Patient/TCGA-A1-A0SB".to_string()],
            },
            Step::Out {
                labels: vec!["HAS_SPECIMEN".to_string()],
            },
            Step::Render {
                fields: vec![
                    "id".to_string(),
                    "label".to_string(),
                    "sample_id".to_string(),
                ],
            },
        ],
    };

    let mut virtual_rows = virtual_engine.query_json_rows(&virtual_query).await?;
    let mut published_rows = published_engine
        .query_sql_json_rows(
            r#"
            SELECT s.id, s.label, s.sample_id
            FROM vertices_cbio p
            JOIN edges_cbio e ON e.from_id = p.id AND e.label = 'HAS_SPECIMEN'
            JOIN vertices_cbio s ON s.id = e.to_id
            WHERE p.id = 'Patient/TCGA-A1-A0SB'
            ORDER BY s.id
            "#,
        )
        .await?;
    virtual_rows.sort();
    published_rows.sort();
    assert_eq!(virtual_rows, published_rows);
    Ok(())
}

#[tokio::test]
async fn virtual_graph_loomql_supports_late_materialization_edge_steps_and_both() -> anyhow::Result<()>
{
    let temp = tempdir()?;
    let engine = Engine::new(EngineConfig {
        work_dir: temp.path().to_string_lossy().to_string(),
    })?;
    cbio_small::register_fixture_sources(&engine).await?;
    let manifest = load_mapping_manifest(&cbio_small::fixture_dir().join("mapping.json"))?;
    let descriptor = engine
        .register_graph_mapping(manifest, &cbio_small::fixture_dir(), false)
        .await?;
    assert_eq!(
        descriptor.status,
        GraphMappingStatus::Registered,
        "{}",
        descriptor
            .last_error
            .clone()
            .unwrap_or_else(|| "missing registration error".to_string())
    );
    assert!(descriptor.virtual_binding.is_some());

    let late_materialization = Query {
        graph: "cbio".to_string(),
        steps: vec![
            Step::V {
                ids: vec!["Specimen/TCGA-A1-A0SB-01".to_string()],
            },
            Step::Out {
                labels: vec!["HAS_OBSERVATION".to_string()],
            },
            Step::Out {
                labels: vec!["OBSERVES_VARIANT".to_string()],
            },
            Step::Out {
                labels: vec!["IN_GENE".to_string()],
            },
            Step::Has {
                field: "gene_symbol".to_string(),
                eq: Value::String("TP53".to_string()),
            },
            Step::Render {
                fields: vec![
                    "id".to_string(),
                    "label".to_string(),
                    "gene_symbol".to_string(),
                ],
            },
        ],
    };
    let late_rows = engine.query_json_rows(&late_materialization).await?;
    assert_eq!(late_rows.len(), 1);
    let late_row: Value = serde_json::from_str(&late_rows[0])?;
    assert_eq!(late_row["label"], "Gene");
    assert_eq!(late_row["gene_symbol"], "TP53");

    let out_edge_query = Query {
        graph: "cbio".to_string(),
        steps: vec![
            Step::V {
                ids: vec!["Specimen/TCGA-A1-A0SB-01".to_string()],
            },
            Step::OutE {
                labels: vec!["HAS_OBSERVATION".to_string()],
            },
            Step::Render {
                fields: vec![
                    "from_id".to_string(),
                    "to_id".to_string(),
                    "label".to_string(),
                ],
            },
        ],
    };
    let out_edge_rows = engine.query_json_rows(&out_edge_query).await?;
    assert_eq!(out_edge_rows.len(), 1);

    let both_query = Query {
        graph: "cbio".to_string(),
        steps: vec![
            Step::V {
                ids: vec!["Specimen/TCGA-A1-A0SB-01".to_string()],
            },
            Step::Both { labels: vec![] },
            Step::Render {
                fields: vec!["id".to_string(), "label".to_string()],
            },
        ],
    };
    let both_rows = engine.query_json_rows(&both_query).await?;
    assert!(!both_rows.is_empty());
    let mut ids = both_rows
        .iter()
        .map(|row| serde_json::from_str::<Value>(row))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|row| row["id"].as_str().unwrap_or_default().to_string())
        .collect::<Vec<_>>();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), both_rows.len());
    assert!(ids.iter().any(|id| id == "Patient/TCGA-A1-A0SB"));
    Ok(())
}

#[tokio::test]
async fn graph_mapping_catalog_snapshot_reloads_and_reregisters_graph_tables() -> anyhow::Result<()>
{
    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    cbio_small::register_fixture_sources(&engine).await?;
    let source_snapshot = engine.source_catalog.snapshot()?;
    let manifest = load_mapping_manifest(&cbio_small::fixture_dir().join("mapping.json"))?;
    engine
        .register_graph_mapping(manifest, &cbio_small::fixture_dir(), true)
        .await?;
    engine
        .compile_graph_mapping("cbio", &cbio_small::fixture_dir())
        .await?;
    assert_eq!(engine.get_graph_descriptor("cbio")?.active_version, 2);
    let mapping_snapshot = engine.graph_mapping_catalog.snapshot()?;
    let graph_snapshot = engine.graph_catalog.snapshot()?;
    assert_eq!(mapping_snapshot.mappings.len(), 1);

    let restored = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    let source_errors = restored
        .load_source_catalog_snapshot(source_snapshot)
        .await?;
    assert!(source_errors.is_empty());
    let mapping_errors = restored
        .load_graph_mapping_catalog_snapshot(mapping_snapshot, &cbio_small::fixture_dir())
        .await?;
    assert!(mapping_errors.is_empty());
    let graph_errors = restored.load_graph_catalog_snapshot(graph_snapshot).await?;
    assert!(graph_errors.is_empty());
    let rows = restored
        .query_sql_json_rows("SELECT COUNT(*) AS count FROM vertices_cbio")
        .await?;
    let row: Value = serde_json::from_str(&rows[0])?;
    assert_eq!(row["count"], 14);
    Ok(())
}

#[test]
fn graph_mapping_manifest_accepts_deprecated_nodes_alias() -> anyhow::Result<()> {
    let manifest: GraphMappingManifest = serde_json::from_value(serde_json::json!({
        "sources": {
            "patients": { "path": "data_clinical_patient.txt", "format": "cbio_tsv" }
        },
        "nodes": [{
            "source": "patients",
            "label": "Patient",
            "id": { "column": "PATIENT_ID" }
        }]
    }))?;
    assert_eq!(manifest.version, 1);
    assert_eq!(manifest.vertices.len(), 1);
    Ok(())
}
