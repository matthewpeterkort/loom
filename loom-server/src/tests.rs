use super::*;
use arrow::array::StringArray;
use loom_engine::graph_schema::{GraphEdgeSpec, GraphNodeSpec, GraphSchemaSpec, GraphSchemaStatus};
use loom_engine::IngestMode;
use loom_engine::schema::compile_graph_schema;
use loomql_ast::{Query, Step};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use routes::catalog::suggest_graph_profile;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::io::{BufRead, BufReader};
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tempfile::tempdir;
use loom_engine::transform;

#[test]
fn persists_schema_state_roundtrip() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let state_file = tmp.path().join("schema_state.json");

    let stored = StoredSchema {
        doc: serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$defs": {}
        }),
        summary: SchemaSummary {
            name: "fhir".to_string(),
            schema_dialect: Some("https://json-schema.org/draft/2020-12/schema".to_string()),
            schema_id: Some("urn:test:fhir".to_string()),
            defs_count: 0,
            resource_types: 0,
            link_relations: 0,
            has_hypermedia_links: false,
            compiled_resource_shapes: 0,
            compiled_entity_types: 0,
            compiled_properties: 0,
            compiled_links: 0,
            wildcard_links: 0,
            created_unix_seconds: 1,
        },
        arrow_shapes: vec![],
        promoted_columns: vec![],
        graph_schema: loom_engine::schema::CompiledGraphSchema {
            descriptor: loom_engine::schema::GraphSchemaDescriptor {
                schema_id: Some("urn:test:fhir".to_string()),
                dialect: Some("https://json-schema.org/draft/2020-12/schema".to_string()),
                entity_type_count: 0,
                property_count: 0,
                link_count: 0,
                wildcard_link_count: 0,
            },
            entities: HashMap::new().into_iter().collect(),
            warnings: vec![],
        },
        compile_warnings: vec![],
    };

    let mut schemas = HashMap::new();
    schemas.insert("fhir".to_string(), stored);
    let mut graph_schema_bindings = HashMap::new();
    graph_schema_bindings.insert("demo".to_string(), "fhir".to_string());

    let snapshot = PersistedSchemaState {
        schemas,
        graph_schema_bindings,
    };
    save_schema_state_to_path(&state_file, &snapshot)?;
    let loaded = load_schema_state_from_path(&state_file)?;

    assert_eq!(loaded.schemas.len(), 1);
    assert!(loaded.schemas.contains_key("fhir"));
    assert_eq!(
        loaded.graph_schema_bindings.get("demo").map(String::as_str),
        Some("fhir")
    );
    Ok(())
}

#[tokio::test]
async fn registers_compiled_graph_schema_and_exposes_graph_introspection() -> anyhow::Result<()> {
    let engine = Arc::new(Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?);
    let state = AppState {
        engine,
        schemas: Arc::new(RwLock::new(HashMap::new())),
        graph_schema_bindings: Arc::new(RwLock::new(HashMap::new())),
    };
    let schema_doc = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "urn:test:master",
        "$defs": {
            "Resource": {
                "type": "object",
                "properties": {"id": {"type": "string"}}
            },
            "Patient": {
                "type": "object",
                "properties": {"id": {"type": "string"}, "mrn": {"type": "string"}}
            },
            "Gene": {
                "type": "object",
                "properties": {"id": {"type": "string"}, "symbol": {"type": "string"}}
            },
            "Directory": {
                "type": "object",
                "properties": {"id": {"type": "string"}, "path": {"type": "string"}},
                "links": [{
                    "rel": "child",
                    "targetSchema": {"$ref": "#/$defs/Resource"},
                    "templatePointers": {"id": "/child/reference"},
                    "targetHints": {"direction": ["outbound"], "multiplicity": ["has_many"]}
                }]
            },
            "Observation": {
                "type": "object",
                "properties": {"id": {"type": "string"}, "focus": {"type": "object"}},
                "links": [{
                    "rel": "focus",
                    "targetSchema": {"$ref": "#/$defs/Resource"},
                    "templatePointers": {"id": "/focus/reference"},
                    "templateRequired": ["id"],
                    "targetHints": {"direction": ["outbound"], "multiplicity": ["has_many"]}
                }]
            }
        }
    });

    let upsert = routes::schema::upsert_schema(
        State(state.clone()),
        axum::extract::Path("master".to_string()),
        Json(schema_doc),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert_eq!(upsert.summary.compiled_entity_types, 5);
    assert_eq!(upsert.summary.wildcard_links, 2);

    let graph_schema = routes::schema::get_schema_graph(
        State(state.clone()),
        axum::extract::Path("master".to_string()),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert!(graph_schema.graph_schema.entities.contains_key("Directory"));
    assert!(graph_schema.graph_schema.entities.contains_key("Observation"));
    assert!(graph_schema.graph_schema.entities.contains_key("Gene"));

    let _ = routes::catalog::bind_graph_schema(
        State(state.clone()),
        axum::extract::Path(("demo_graph".to_string(), "master".to_string())),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;

    let bound = routes::catalog::get_graph_bound_schema(
        State(state.clone()),
        axum::extract::Path("demo_graph".to_string()),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert_eq!(bound.schema_name, "master");

    let entities = routes::catalog::list_graph_schema_entities(
        State(state.clone()),
        axum::extract::Path("demo_graph".to_string()),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert!(entities.entities.iter().any(|entity| entity.name == "Patient"));

    let entity = routes::catalog::get_graph_schema_entity(
        State(state.clone()),
        axum::extract::Path(("demo_graph".to_string(), "Observation".to_string())),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert_eq!(entity.entity.links[0].rel, "focus");

    let links = routes::catalog::list_graph_schema_links(
        State(state),
        axum::extract::Path("demo_graph".to_string()),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert!(links.links.iter().any(|link| link.rel == "child"));
    Ok(())
}

#[test]
fn persists_graph_mapping_catalog_roundtrip() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let mapping_file = tmp.path().join("graph_mapping_catalog.json");
    let manifest: mapping::GraphMappingManifest = serde_json::from_value(serde_json::json!({
        "version": 1,
        "graph": "demo",
        "sources": {
            "patients": { "source": "patients" }
        },
        "vertices": [{
            "source": "patients",
            "label": "Patient",
            "id": { "column": "PATIENT_ID" }
        }]
    }))?;
    let descriptor = mapping::new_descriptor("demo".to_string(), manifest, None);
    let snapshot = mapping::GraphMappingCatalogSnapshot {
        mappings: vec![descriptor],
    };

    save_graph_mapping_catalog_state_to_path(&mapping_file, &snapshot)?;
    let loaded = load_graph_mapping_catalog_state_from_path(&mapping_file)?;

    assert_eq!(loaded.mappings.len(), 1);
    assert_eq!(loaded.mappings[0].graph, "demo");
    assert_eq!(loaded.mappings[0].source_dependencies, vec!["patients"]);
    assert_eq!(
        loaded.mappings[0].status,
        mapping::GraphMappingStatus::Registered
    );
    Ok(())
}

#[test]
fn persists_graph_schema_catalog_roundtrip() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let graph_schema_file = tmp.path().join("graph_schema_catalog.json");
    let snapshot = loom_engine::graph_schema::GraphSchemaCatalogSnapshot {
        schemas: vec![loom_engine::graph_schema::GraphSchemaDescriptor {
            id: "demo_schema".to_string(),
            graph: "demo_graph".to_string(),
            display_name: Some("demo".to_string()),
            bound_schema: Some("master".to_string()),
            spec: GraphSchemaSpec {
                version: 1,
                id: Some("demo_schema".to_string()),
                graph: "demo_graph".to_string(),
                display_name: Some("demo".to_string()),
                bound_schema: Some("master".to_string()),
                nodes: vec![],
                edges: vec![],
                metadata: Default::default(),
            },
            compiled_manifest: None,
            status: GraphSchemaStatus::Draft,
            last_validation: None,
            last_preview: None,
            registered_mapping_graph: None,
            published_graph: None,
            last_error: None,
            created_unix_seconds: 1,
            updated_unix_seconds: 1,
        }],
    };
    save_graph_schema_catalog_state_to_path(&graph_schema_file, &snapshot)?;
    let loaded = load_graph_schema_catalog_state_from_path(&graph_schema_file)?;
    assert_eq!(loaded.schemas.len(), 1);
    assert_eq!(loaded.schemas[0].id, "demo_schema");
    Ok(())
}

#[test]
fn persists_transform_catalog_roundtrip() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let transform_file = tmp.path().join("transform_catalog.json");
    let descriptor = transform::TransformDescriptor {
        id: "transform_demo_clean".to_string(),
        spec: transform::TransformSpec {
            input: source::SourceTableRef {
                source_id: "demo".to_string(),
                table_id: "primary".to_string(),
            },
            output_table_id: "clean".to_string(),
            display_name: Some("clean".to_string()),
            operations: vec![transform::TransformOperation::Trim {
                columns: vec!["sample_id".to_string()],
            }],
            metadata: Default::default(),
        },
        output_table: source::SourceTableDescriptor {
            id: "clean".to_string(),
            display_name: Some("clean".to_string()),
            table_name: "source_demo_clean".to_string(),
            schema: source::SourceSchema::default(),
            stats: source::SourceStats::default(),
            created_unix_seconds: 1,
            updated_unix_seconds: 1,
            registered: false,
            kind: source::SourceTableKind::Derived,
            header_row_index: Some(0),
            data_start_row_index: Some(1),
            original_column_names: vec![],
            inferred_column_names: vec![],
            quality: source::SourceTableQualitySummary::default(),
            metadata: [
                ("transform_id".to_string(), "transform_demo_clean".to_string()),
                ("input_source_id".to_string(), "demo".to_string()),
                ("input_table_id".to_string(), "primary".to_string()),
            ]
            .into(),
        },
        status: transform::TransformDescriptorStatus::Valid,
        last_error: None,
        created_unix_seconds: 1,
        updated_unix_seconds: 1,
    };
    let snapshot = transform::TransformCatalogSnapshot {
        transforms: vec![descriptor],
    };
    save_transform_catalog_state_to_path(&transform_file, &snapshot)?;
    let loaded = load_transform_catalog_state_from_path(&transform_file)?;
    assert_eq!(loaded.transforms.len(), 1);
    assert_eq!(loaded.transforms[0].id, "transform_demo_clean");
    Ok(())
}

#[test]
fn persists_graph_catalog_roundtrip() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let graph_file = tmp.path().join("graph_catalog.json");
    let descriptor = graph::GraphDescriptor {
        name: "demo".to_string(),
        active_version: 1,
        status: graph::GraphStatus::Active,
        source_kind: graph::GraphSourceKind::MappingManifest,
        vertex_table: "vertices_demo".to_string(),
        edge_table: "edges_demo".to_string(),
        active_snapshot: graph::GraphSnapshot {
            version: 1,
            vertices_uri: "/tmp/demo/vertices".to_string(),
            edges_uri: "/tmp/demo/edges".to_string(),
            adjacency_uri: Some("/tmp/demo/adjacency".to_string()),
            vertex_table: "vertices_demo".to_string(),
            edge_table: "edges_demo".to_string(),
            mapping_graph: Some("demo".to_string()),
            mapping_version: Some(1),
            source_dependencies: vec!["patients".to_string()],
            source_fingerprints: [("patients".to_string(), "abc".to_string())].into(),
            vertex_columns: vec![graph::GraphColumn {
                name: "prop_status".to_string(),
                data_type: "Utf8".to_string(),
                role: graph::GraphColumnRole::Property,
            }],
            edge_columns: vec![graph::GraphColumn {
                name: "from_id".to_string(),
                data_type: "Utf8".to_string(),
                role: graph::GraphColumnRole::Endpoint,
            }],
            stats: graph::GraphStats {
                vertices: 2,
                edges: 1,
                ..Default::default()
            },
            provenance: graph::GraphProvenance {
                source_kind: Some(graph::GraphSourceKind::MappingManifest),
                mapping_graph: Some("demo".to_string()),
                source_ids: vec!["patients".to_string()],
                storage_paths: vec!["/tmp/demo/vertices".to_string()],
            },
            created_unix_seconds: 1,
        },
        created_unix_seconds: 1,
        updated_unix_seconds: 1,
        last_error: None,
    };
    let snapshot = graph::GraphCatalogSnapshot {
        graphs: vec![descriptor],
    };

    save_graph_catalog_state_to_path(&graph_file, &snapshot)?;
    let loaded = load_graph_catalog_state_from_path(&graph_file)?;

    assert_eq!(loaded.graphs.len(), 1);
    assert_eq!(loaded.graphs[0].name, "demo");
    assert_eq!(loaded.graphs[0].active_snapshot.stats.vertices, 2);
    assert_eq!(
        loaded.graphs[0].active_snapshot.vertex_columns[0].role,
        graph::GraphColumnRole::Property
    );
    Ok(())
}

#[tokio::test]
async fn suggest_graph_profile_endpoint_returns_draft_manifest() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let source_path = tmp.path().join("samples.tsv");
    std::fs::write(
        &source_path,
        "person_id\tsample_id\tassay\nP1\tS1\tpanel\nP1\tS2\tpanel\n",
    )?;
    let engine = Arc::new(Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?);
    engine
        .register_source(source::SourceRegistration {
            id: "samples".to_string(),
            display_name: Some("samples".to_string()),
            format: source::SourceFormat::Tsv,
            location: source::SourceLocation::Local {
                path: source_path.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;
    let state = AppState {
        engine,
        schemas: Arc::new(RwLock::new(HashMap::new())),
        graph_schema_bindings: Arc::new(RwLock::new(HashMap::new())),
    };
    let response = suggest_graph_profile(
        State(state),
        Json(GraphSuggestionRequest {
            graph: "suggested".to_string(),
            display_name: None,
            source_ids: vec!["samples".to_string()],
            target_vocabulary: Some("fhir_lite".to_string()),
            max_sample_rows: Some(100),
        }),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert_eq!(
        response.suggestion.manifest.graph.as_deref(),
        Some("suggested")
    );
    assert!(response
        .suggestion
        .manifest
        .vertices
        .iter()
        .any(|vertex| vertex.label == "Specimen"));
    assert!(response
        .suggestion
        .candidates
        .iter()
        .any(|candidate| candidate.rule_id == "edge.patient_specimen"));
    Ok(())
}

#[tokio::test]
async fn suggest_graph_profile_endpoint_uses_bound_schema_candidates() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let patients_path = tmp.path().join("people.tsv");
    let samples_path = tmp.path().join("biospecimens.tsv");
    std::fs::write(&patients_path, "id\tmrn\nP1\tM1\nP2\tM2\n")?;
    std::fs::write(
        &samples_path,
        "identifier\tpatient_id\ttype\nS1\tP1\tblood\nS2\tP2\ttissue\n",
    )?;
    let engine = Arc::new(Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?);
    engine
        .register_source(source::SourceRegistration {
            id: "people".to_string(),
            display_name: None,
            format: source::SourceFormat::Tsv,
            location: source::SourceLocation::Local {
                path: patients_path.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;
    engine
        .register_source(source::SourceRegistration {
            id: "biospecimens".to_string(),
            display_name: None,
            format: source::SourceFormat::Tsv,
            location: source::SourceLocation::Local {
                path: samples_path.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
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
    let graph_schema = compile_graph_schema(&schema_doc)?;
    let summary = summarize_schema("master", &schema_doc, compile_arrow_shapes(&schema_doc)?.len(), &graph_schema);
    let state = AppState {
        engine,
        schemas: Arc::new(RwLock::new(HashMap::from([(
            "master".to_string(),
            StoredSchema {
                doc: schema_doc,
                summary,
                arrow_shapes: vec![],
                promoted_columns: vec![],
                graph_schema,
                compile_warnings: vec![],
            },
        )]))),
        graph_schema_bindings: Arc::new(RwLock::new(HashMap::from([(
            "schema_bound".to_string(),
            "master".to_string(),
        )]))),
    };
    let response = suggest_graph_profile(
        State(state),
        Json(GraphSuggestionRequest {
            graph: "schema_bound".to_string(),
            display_name: None,
            source_ids: vec!["people".to_string(), "biospecimens".to_string()],
            target_vocabulary: Some("master".to_string()),
            max_sample_rows: Some(100),
        }),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert!(response
        .suggestion
        .source_profiles
        .iter()
        .all(|profile| profile.schema_derived));
    assert!(response
        .suggestion
        .manifest
        .edges
        .iter()
        .any(|edge| edge.label == "subject_Patient"));
    Ok(())
}

#[tokio::test]
async fn graph_schema_routes_validate_preview_and_register() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let patients_path = tmp.path().join("patients.tsv");
    let samples_path = tmp.path().join("samples.tsv");
    std::fs::write(&patients_path, "PATIENT_ID\tMRN\np1\tM1\np2\tM2\n")?;
    std::fs::write(&samples_path, "SAMPLE_ID\tPATIENT_ID\ns1\tp1\ns2\tp2\n")?;

    let engine = Arc::new(Engine::new(EngineConfig {
        work_dir: tmp.path().to_string_lossy().to_string(),
    })?);
    engine
        .register_source(source::SourceRegistration {
            id: "patients".to_string(),
            display_name: None,
            format: source::SourceFormat::Tsv,
            location: source::SourceLocation::Local {
                path: patients_path.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;
    engine
        .register_source(source::SourceRegistration {
            id: "samples".to_string(),
            display_name: None,
            format: source::SourceFormat::Tsv,
            location: source::SourceLocation::Local {
                path: samples_path.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;
    let patient_table = engine.list_source_tables("patients")?[0].id.clone();
    let sample_table = engine.list_source_tables("samples")?[0].id.clone();
    let state = AppState {
        engine,
        schemas: Arc::new(RwLock::new(HashMap::new())),
        graph_schema_bindings: Arc::new(RwLock::new(HashMap::new())),
    };
    let schema_doc = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": {
            "Patient": {"type":"object","properties":{"id":{"type":"string"}}},
            "Specimen": {
                "type":"object",
                "properties":{"id":{"type":"string"}, "identifier":{"type":"string"}},
                "links": [{"rel":"subject_Patient","targetSchema":{"$ref":"#/$defs/Patient"}}]
            }
        }
    });
    let _ = routes::schema::upsert_schema(
        State(state.clone()),
        axum::extract::Path("master".to_string()),
        Json(schema_doc),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    let _ = routes::catalog::bind_graph_schema(
        State(state.clone()),
        axum::extract::Path(("demo_graph".to_string(), "master".to_string())),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;

    let spec = GraphSchemaSpec {
        version: 1,
        id: Some("demo_schema".to_string()),
        graph: "demo_graph".to_string(),
        display_name: Some("demo graph".to_string()),
        bound_schema: Some("master".to_string()),
        nodes: vec![
            GraphNodeSpec {
                source: source::SourceTableRef {
                    source_id: "patients".to_string(),
                    table_id: patient_table,
                },
                label: "Patient".to_string(),
                id: mapping::Expr::Concat {
                    concat: vec![
                        mapping::Expr::Text("Patient/".to_string()),
                        mapping::Expr::Column {
                            column: "PATIENT_ID".to_string(),
                        },
                    ],
                },
                predicate: None,
                columns: Default::default(),
                props: mapping::PropsMapping::None,
                prop_types: Default::default(),
                schema_entity: Some("Patient".to_string()),
            },
            GraphNodeSpec {
                source: source::SourceTableRef {
                    source_id: "samples".to_string(),
                    table_id: sample_table.clone(),
                },
                label: "Specimen".to_string(),
                id: mapping::Expr::Concat {
                    concat: vec![
                        mapping::Expr::Text("Specimen/".to_string()),
                        mapping::Expr::Column {
                            column: "SAMPLE_ID".to_string(),
                        },
                    ],
                },
                predicate: None,
                columns: [(
                    "identifier".to_string(),
                    mapping::ColumnMapping::Expr(mapping::Expr::Column {
                        column: "SAMPLE_ID".to_string(),
                    }),
                )]
                .into(),
                props: mapping::PropsMapping::None,
                prop_types: Default::default(),
                schema_entity: Some("Specimen".to_string()),
            },
        ],
        edges: vec![GraphEdgeSpec {
            source: source::SourceTableRef {
                source_id: "samples".to_string(),
                table_id: sample_table,
            },
            label: "subject_Patient".to_string(),
            from_label: Some("Specimen".to_string()),
            to_label: Some("Patient".to_string()),
            from: mapping::Expr::Concat {
                concat: vec![
                    mapping::Expr::Text("Specimen/".to_string()),
                    mapping::Expr::Column {
                        column: "SAMPLE_ID".to_string(),
                    },
                ],
            },
            to: mapping::Expr::Concat {
                concat: vec![
                    mapping::Expr::Text("Patient/".to_string()),
                    mapping::Expr::Column {
                        column: "PATIENT_ID".to_string(),
                    },
                ],
            },
            id: None,
            predicate: None,
            columns: Default::default(),
            props: mapping::PropsMapping::None,
            prop_types: Default::default(),
            schema_relation: Some("subject_Patient".to_string()),
        }],
        metadata: Default::default(),
    };

    let created = routes::catalog::register_graph_schema(
        State(state.clone()),
        Json(GraphSchemaSpecRequest { spec }),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert_eq!(created.graph_schema.id, "demo_schema");

    let validation = routes::catalog::validate_graph_schema(
        State(state.clone()),
        axum::extract::Path("demo_schema".to_string()),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert!(validation.validation.valid);

    let preview = routes::catalog::preview_graph_schema(
        State(state.clone()),
        axum::extract::Path("demo_schema".to_string()),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert_eq!(preview.preview.node_counts.get("Patient"), Some(&2));
    assert_eq!(preview.preview.edge_counts.get("subject_Patient"), Some(&2));

    let registered = routes::catalog::register_graph_schema_runtime(
        State(state.clone()),
        axum::extract::Path("demo_schema".to_string()),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert_eq!(registered.graph_schema.status, GraphSchemaStatus::Registered);
    assert_eq!(
        registered.graph_schema.registered_mapping_graph.as_deref(),
        Some("demo_graph")
    );

    let rows = state
        .engine
        .query_sql_json_rows("SELECT COUNT(*) AS count FROM edges_demo_graph")
        .await?;
    let row: Value = serde_json::from_str(&rows[0])?;
    assert_eq!(row["count"], 2);
    Ok(())
}

#[test]
fn builds_non_empty_edges_from_fhir_ndjson() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let ndjson_dir = tmp.path().join("ndjson");
    std::fs::create_dir_all(&ndjson_dir)?;

    std::fs::write(
        ndjson_dir.join("Observation.ndjson"),
        "{\"id\":\"obs1\",\"subject\":{\"reference\":\"Patient/p1\"}}\n",
    )?;
    std::fs::write(ndjson_dir.join("Patient.ndjson"), "{\"id\":\"p1\"}\n")?;

    let edges_path = tmp.path().join("edges.parquet");
    let edge_count = build_edges_parquet_from_ndjson(&ndjson_dir, &edges_path)?;
    assert!(edge_count > 0);

    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&edges_path)?)?.build()?;
    let mut rows = 0usize;
    let mut found_subject_edge = false;

    for maybe_batch in reader {
        let batch = maybe_batch?;
        rows += batch.num_rows();
        let from = batch
            .column_by_name("from_id")
            .expect("from_id column must exist")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("from_id should be utf8");
        let to = batch
            .column_by_name("to_id")
            .expect("to_id column must exist")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("to_id should be utf8");
        let labels = batch
            .column_by_name("label")
            .expect("label column must exist")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("label should be utf8");

        for i in 0..batch.num_rows() {
            if from.value(i) == "Observation/obs1"
                && to.value(i) == "Patient/p1"
                && labels.value(i).contains("subject")
            {
                found_subject_edge = true;
            }
        }
    }

    assert!(rows > 0);
    assert!(found_subject_edge);
    Ok(())
}

#[test]
fn promotes_nested_schema_fields_and_extracts_values() -> anyhow::Result<()> {
    let schema_doc = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": {
            "Observation": {
                "type": "object",
                "properties": {
                    "id": {"type":"string"},
                    "subject": {
                        "type":"object",
                        "properties": {
                            "display": {"type":"string"},
                            "age": {"type":"integer"}
                        }
                    }
                }
            }
        }
    });
    let shapes = compile_arrow_shapes(&schema_doc)?;
    let promoted = promoted_columns_from_shapes(&shapes);

    assert!(promoted.iter().any(|c| c.field_name == "subject.display"));
    assert!(promoted.iter().any(|c| c.field_name == "subject.age"));
    assert!(promoted.iter().any(|c| c.field_name == "subject"));

    let row = serde_json::json!({
        "subject": {"display":"Jane", "age": 42}
    });
    assert_eq!(
        get_value_at_dotted_path(&row, "subject.display").and_then(Value::as_str),
        Some("Jane")
    );
    assert_eq!(
        get_value_at_dotted_path(&row, "subject.age").and_then(Value::as_i64),
        Some(42)
    );
    Ok(())
}

#[tokio::test]
async fn virtual_graph_sql_and_identity_routes_work() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let patients = tmp.path().join("patients.tsv");
    let samples = tmp.path().join("samples.tsv");
    std::fs::write(&patients, "PATIENT_ID\tMRN\np1\tMRN-1\np2\tMRN-2\n")?;
    std::fs::write(
        &samples,
        "SAMPLE_ID\tPATIENT_REF\ns1\tPatient/p1\ns2\tp2\ns3\tPatient/missing\n",
    )?;

    let engine = Arc::new(Engine::new(EngineConfig {
        work_dir: tmp.path().to_string_lossy().to_string(),
    })?);
    engine
        .register_source(source::SourceRegistration {
            id: "patients".to_string(),
            display_name: None,
            format: source::SourceFormat::Tsv,
            location: source::SourceLocation::Local {
                path: patients.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;
    engine
        .register_source(source::SourceRegistration {
            id: "samples".to_string(),
            display_name: None,
            format: source::SourceFormat::Tsv,
            location: source::SourceLocation::Local {
                path: samples.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;
    let manifest: mapping::GraphMappingManifest = serde_json::from_value(serde_json::json!({
        "version": 1,
        "graph": "identity_demo",
        "sources": {
            "patients": { "source": "patients" },
            "samples": { "source": "samples" }
        },
        "vertices": [
            {
                "source": "patients",
                "label": "Patient",
                "id": { "column": "PATIENT_ID" }
            },
            {
                "source": "samples",
                "label": "Sample",
                "id": { "column": "SAMPLE_ID" }
            }
        ],
        "edges": [],
        "identity": {
            "Patient": {
                "aliases": {
                    "fhir_ref": { "concat": ["Patient/", { "column": "PATIENT_ID" }] },
                    "mrn": { "column": "MRN" }
                },
                "normalizer": "fhir_canonical"
            }
        },
        "references": [
            {
                "name": "sample_patient",
                "source": "samples",
                "from_label": "Sample",
                "to_label": "Patient",
                "from_key": { "column": "SAMPLE_ID" },
                "to_key": { "column": "PATIENT_REF" },
                "normalizer": "fhir_canonical"
            }
        ]
    }))?;
    engine
        .register_graph_mapping(manifest, tmp.path(), false)
        .await?;

    let state = AppState {
        engine,
        schemas: Arc::new(RwLock::new(HashMap::new())),
        graph_schema_bindings: Arc::new(RwLock::new(HashMap::new())),
    };

    let graph = routes::catalog::get_virtual_graph(
        State(state.clone()),
        axum::extract::Path("identity_demo".to_string()),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert_eq!(graph.mode, "virtual");
    assert!(graph.mapping.virtual_binding.is_some());

    let identity = routes::catalog::get_virtual_graph_identity(
        State(state.clone()),
        axum::extract::Path("identity_demo".to_string()),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert_eq!(
        identity.identity.unresolved_references.get("sample_patient"),
        Some(&1)
    );

    let query = routes::query::query_virtual_graph_sql(
        State(state.clone()),
        axum::extract::Path("identity_demo".to_string()),
        Json(SqlQueryRequest {
            sql: "SELECT from_key, resolved_to_id, resolution_status FROM graph_identity_demo_reference_sample_patient ORDER BY from_key".to_string(),
        }),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert_eq!(query.row_count, 3);

    let explain = routes::query::explain_virtual_graph_sql(
        State(state.clone()),
        axum::extract::Path("identity_demo".to_string()),
        Json(SqlQueryRequest {
            sql: "SELECT * FROM graph_identity_demo_reference_sample_patient".to_string(),
        }),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert!(explain.explain.contains("graph_identity_demo_reference_sample_patient"));

    let preview = routes::query::preview_virtual_reference(
        State(state),
        axum::extract::Path("identity_demo".to_string()),
        Json(ReferencePreviewRequest {
            name: "sample_patient".to_string(),
            limit: Some(2),
        }),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert_eq!(preview.row_count, 2);
    Ok(())
}

#[tokio::test]
async fn existing_loomql_routes_work_for_virtual_graphs() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let patients = tmp.path().join("patients.tsv");
    let samples = tmp.path().join("samples.tsv");
    std::fs::write(&patients, "PATIENT_ID\np1\np2\n")?;
    std::fs::write(&samples, "SAMPLE_ID\tPATIENT_ID\ns1\tp1\ns2\tp2\n")?;

    let engine = Arc::new(Engine::new(EngineConfig {
        work_dir: tmp.path().to_string_lossy().to_string(),
    })?);
    engine
        .register_source(source::SourceRegistration {
            id: "patients".to_string(),
            display_name: None,
            format: source::SourceFormat::Tsv,
            location: source::SourceLocation::Local {
                path: patients.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;
    engine
        .register_source(source::SourceRegistration {
            id: "samples".to_string(),
            display_name: None,
            format: source::SourceFormat::Tsv,
            location: source::SourceLocation::Local {
                path: samples.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;

    let manifest: mapping::GraphMappingManifest = serde_json::from_value(serde_json::json!({
        "version": 1,
        "graph": "mini_virtual",
        "sources": {
            "patients": { "source": "patients" },
            "samples": { "source": "samples" }
        },
        "vertices": [
            {
                "source": "patients",
                "label": "Patient",
                "id": { "concat": ["Patient/", { "column": "PATIENT_ID" }] }
            },
            {
                "source": "samples",
                "label": "Sample",
                "id": { "concat": ["Sample/", { "column": "SAMPLE_ID" }] }
            }
        ],
        "edges": [
            {
                "source": "samples",
                "label": "HAS_SAMPLE",
                "from_label": "Patient",
                "to_label": "Sample",
                "from": { "concat": ["Patient/", { "column": "PATIENT_ID" }] },
                "to": { "concat": ["Sample/", { "column": "SAMPLE_ID" }] }
            }
        ]
    }))?;
    engine
        .register_graph_mapping(manifest, tmp.path(), false)
        .await?;

    let state = AppState {
        engine,
        schemas: Arc::new(RwLock::new(HashMap::new())),
        graph_schema_bindings: Arc::new(RwLock::new(HashMap::new())),
    };

    let query_payload = serde_json::to_value(Query {
        graph: "mini_virtual".to_string(),
        steps: vec![
            Step::V {
                ids: vec!["Patient/p1".to_string()],
            },
            Step::Out {
                labels: vec!["HAS_SAMPLE".to_string()],
            },
            Step::Render {
                fields: vec!["id".to_string(), "label".to_string()],
            },
        ],
    })?;

    let response = routes::query::query_graph(
        State(state.clone()),
        axum::extract::Path("mini_virtual".to_string()),
        Json(query_payload.clone()),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert_eq!(response.row_count, 1);
    assert_eq!(response.rows[0]["id"], "Sample/s1");

    let full = routes::query::query_graph_full(
        State(state),
        axum::extract::Path("mini_virtual".to_string()),
        Json(query_payload),
    )
    .await;
    assert!(full.is_err());
    let (_, body) = full.err().expect("full-mode error");
    assert!(body.detail.contains("not schema-bound"));
    Ok(())
}

#[tokio::test]
async fn schema_bound_query_routes_validate_and_explain_relations() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let patients = tmp.path().join("patients.tsv");
    let samples = tmp.path().join("samples.tsv");
    std::fs::write(&patients, "PATIENT_ID\np1\n")?;
    std::fs::write(&samples, "SAMPLE_ID\tPATIENT_ID\ns1\tp1\n")?;

    let engine = Arc::new(Engine::new(EngineConfig {
        work_dir: tmp.path().to_string_lossy().to_string(),
    })?);
    engine
        .register_source(source::SourceRegistration {
            id: "patients".to_string(),
            display_name: None,
            format: source::SourceFormat::Tsv,
            location: source::SourceLocation::Local {
                path: patients.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;
    engine
        .register_source(source::SourceRegistration {
            id: "samples".to_string(),
            display_name: None,
            format: source::SourceFormat::Tsv,
            location: source::SourceLocation::Local {
                path: samples.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;

    let manifest: mapping::GraphMappingManifest = serde_json::from_value(serde_json::json!({
        "version": 1,
        "graph": "schema_query_demo",
        "sources": {
            "patients": { "source": "patients" },
            "samples": { "source": "samples" }
        },
        "vertices": [
            { "source": "patients", "label": "Patient", "id": { "concat": ["Patient/", { "column": "PATIENT_ID" }] } },
            { "source": "samples", "label": "Specimen", "id": { "concat": ["Specimen/", { "column": "SAMPLE_ID" }] } }
        ],
        "edges": [
            {
                "source": "samples",
                "label": "HAS_SPECIMEN",
                "from_label": "Patient",
                "to_label": "Specimen",
                "from": { "concat": ["Patient/", { "column": "PATIENT_ID" }] },
                "to": { "concat": ["Specimen/", { "column": "SAMPLE_ID" }] }
            }
        ]
    }))?;
    engine
        .register_graph_mapping(manifest, tmp.path(), false)
        .await?;

    let schema_doc = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": {
            "Resource": {"type": "object", "properties": {"id": {"type": "string"}}},
            "Patient": {
                "type": "object",
                "properties": {"id": {"type": "string"}},
                "links": [
                    {
                        "rel": "HAS_SPECIMEN",
                        "targetSchema": {"$ref": "#/$defs/Specimen"},
                        "templatePointers": {"id": "/specimen/reference"},
                        "targetHints": {"direction": ["outbound"], "multiplicity": ["has_many"]}
                    },
                    {
                        "rel": "focus",
                        "targetSchema": {"$ref": "#/$defs/Resource"},
                        "templatePointers": {"id": "/focus/reference"},
                        "targetHints": {"direction": ["outbound"], "multiplicity": ["has_many"]}
                    }
                ]
            },
            "Specimen": {"type": "object", "properties": {"id": {"type": "string"}}}
        }
    });

    let graph_schema = compile_graph_schema(&schema_doc)?;
    let summary = summarize_schema("master", &schema_doc, compile_arrow_shapes(&schema_doc)?.len(), &graph_schema);
    let state = AppState {
        engine,
        schemas: Arc::new(RwLock::new(HashMap::from([(
            "master".to_string(),
            StoredSchema {
                doc: schema_doc,
                summary,
                arrow_shapes: vec![],
                promoted_columns: vec![],
                graph_schema,
                compile_warnings: vec![],
            },
        )]))),
        graph_schema_bindings: Arc::new(RwLock::new(HashMap::from([(
            "schema_query_demo".to_string(),
            "master".to_string(),
        )]))),
    };

    let invalid_query = serde_json::to_value(Query {
        graph: "schema_query_demo".to_string(),
        steps: vec![
            Step::V {
                ids: vec!["Patient/p1".to_string()],
            },
            Step::Out {
                labels: vec!["UNKNOWN_REL".to_string()],
            },
        ],
    })?;
    let err = routes::query::query_graph(
        State(state.clone()),
        axum::extract::Path("schema_query_demo".to_string()),
        Json(invalid_query),
    )
    .await
    .err()
    .expect("invalid relation should fail");
    assert!(err.1.detail.contains("schema-bound query invalid"));

    let valid_query = serde_json::to_value(Query {
        graph: "schema_query_demo".to_string(),
        steps: vec![
            Step::V {
                ids: vec!["Patient/p1".to_string()],
            },
            Step::Out {
                labels: vec!["HAS_SPECIMEN".to_string()],
            },
            Step::Render {
                fields: vec!["id".to_string(), "label".to_string()],
            },
        ],
    })?;
    let response = routes::query::query_graph(
        State(state.clone()),
        axum::extract::Path("schema_query_demo".to_string()),
        Json(valid_query.clone()),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert_eq!(response.row_count, 1);

    let vocabulary = routes::query::get_query_schema_vocabulary(
        State(state.clone()),
        axum::extract::Path("schema_query_demo".to_string()),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert!(vocabulary
        .entities
        .iter()
        .any(|entity| entity.entity == "Patient"
            && entity.outgoing.iter().any(|rel| rel.rel == "focus" && rel.wildcard_target)));

    let explain = routes::query::explain_query_schema(
        State(state.clone()),
        axum::extract::Path("schema_query_demo".to_string()),
        Json(valid_query),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert!(explain.valid);
    assert!(explain
        .steps
        .iter()
        .any(|step| step.op == "out" && step.allowed_targets.contains(&"Specimen".to_string())));

    let autocomplete = routes::query::autocomplete_query_schema(
        State(state),
        axum::extract::Path("schema_query_demo".to_string()),
        axum::extract::Query(QueryAutocompleteRequest {
            labels: Some("Patient".to_string()),
            direction: Some("out".to_string()),
            prefix: Some("fo".to_string()),
        }),
    )
    .await
    .map_err(|(status, body)| anyhow::anyhow!("{status}: {}", body.detail))?;
    assert_eq!(autocomplete.relations.len(), 1);
    assert_eq!(autocomplete.relations[0].rel, "focus");
    assert!(autocomplete.relations[0].wildcard_target);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark; run explicitly"]
async fn schema_aware_serialization_benchmark() -> anyhow::Result<()> {
    let meta_dir = std::env::var("CALYPR_META_DIR")
        .unwrap_or_else(|_| "/Users/peterkor/Desktop/BMEG/grip-benchmark/calypr/META".to_string());
    let meta_path = FsPath::new(&meta_dir);
    assert!(meta_path.exists(), "META dir not found: {meta_dir}");

    let schema_doc = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "urn:example:fhir-min",
        "$defs": {
            "Observation": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "status": {"type": "string"},
                    "resourceType": {"type": "string"},
                    "valueInteger": {"type": "integer"},
                    "valueQuantity": {"type": "number"}
                }
            },
            "Patient": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "resourceType": {"type": "string"},
                    "active": {"type": "boolean"},
                    "gender": {"type": "string"}
                }
            }
        }
    });

    let arrow_shapes = compile_arrow_shapes(&schema_doc)?;
    let promoted_columns = promoted_columns_from_shapes(&arrow_shapes);

    let tmp = tempdir()?;
    let vertices_parquet = tmp.path().join("typed_vertices.parquet");
    let edges_parquet = tmp.path().join("typed_edges.parquet");
    let vortex_root = tmp.path().join("vortex");
    std::fs::create_dir_all(&vortex_root)?;

    let loaded_vertices =
        build_typed_vertex_parquet(meta_path, &vertices_parquet, &promoted_columns)?;
    let loaded_edges = build_edges_parquet_from_ndjson(meta_path, &edges_parquet)?;

    let mut vertex_columns = vec![
        VertexColumn {
            name: "id".to_string(),
            sql_type: "VARCHAR NOT NULL".to_string(),
        },
        VertexColumn {
            name: "label".to_string(),
            sql_type: "VARCHAR NOT NULL".to_string(),
        },
        VertexColumn {
            name: "payload_codec".to_string(),
            sql_type: "VARCHAR NOT NULL".to_string(),
        },
        VertexColumn {
            name: "payload_bin".to_string(),
            sql_type: "BLOB NOT NULL".to_string(),
        },
        VertexColumn {
            name: "payload_json_bin".to_string(),
            sql_type: "BLOB NOT NULL".to_string(),
        },
    ];
    for col in &promoted_columns {
        vertex_columns.push(VertexColumn {
            name: col.column_name.clone(),
            sql_type: promoted_type_to_sql(&col.kind).to_string(),
        });
    }

    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    let graph = "schema_bench";

    let t_load = Instant::now();
    engine
        .ingest_parquet_graph_to_vortex_with_vertex_columns(
            graph,
            vertices_parquet.to_str().unwrap_or_default(),
            edges_parquet.to_str().unwrap_or_default(),
            vortex_root.to_str().unwrap_or_default(),
            IngestMode::Overwrite,
            &vertex_columns,
        )
        .await?;
    let load_secs = t_load.elapsed().as_secs_f64();

    let query = Query {
        graph: graph.to_string(),
        steps: vec![
            Step::V { ids: vec![] },
            Step::HasLabel {
                labels: vec!["Observation".to_string()],
            },
            Step::Render {
                fields: vec!["id".to_string(), "label".to_string()],
            },
        ],
    };

    let t_batches = Instant::now();
    let batches = engine.query_batches(&query).await?;
    let query_batches_secs = t_batches.elapsed().as_secs_f64();
    let rows_from_batches = batches.iter().map(|b| b.num_rows()).sum::<usize>();

    let t_serde = Instant::now();
    let rows_serde = engine.query_json_rows(&query).await?;
    let query_serde_secs = t_serde.elapsed().as_secs_f64();
    let serde_bytes = serde_json::to_vec(&rows_serde)?.len() as u64;

    let t_sonic = Instant::now();
    let rows_sonic = engine.query_json_rows_sonic(&query).await?;
    let query_sonic_secs = t_sonic.elapsed().as_secs_f64();
    let sonic_bytes = serde_json::to_vec(&rows_sonic)?.len() as u64;

    let t_ndjson = Instant::now();
    let ndjson_chunks = engine.query_ndjson_chunks(&query).await?;
    let query_ndjson_secs = t_ndjson.elapsed().as_secs_f64();
    let ndjson_bytes = ndjson_chunks.iter().map(|c| c.len() as u64).sum::<u64>();
    let ndjson_rows = ndjson_chunks
        .iter()
        .map(|c| c.iter().filter(|b| **b == b'\n').count())
        .sum::<usize>();

    eprintln!(
        "\nSchema-Aware Serialization Benchmark\n\
             - meta_dir: {meta}\n\
             - loaded_vertices: {loaded_vertices}\n\
             - loaded_edges: {loaded_edges}\n\
             - promoted_columns: {promoted}\n\
             - load_seconds: {load_secs:.4}\n\
             - query_rows_batches: {rows_from_batches}\n\
             - query_seconds_batches_collect: {query_batches_secs:.4}\n\
             - query_seconds_json_serde_path: {query_serde_secs:.4}\n\
             - query_seconds_json_sonic_path: {query_sonic_secs:.4}\n\
             - query_seconds_ndjson_chunks: {query_ndjson_secs:.4}\n\
             - serde_rows: {serde_rows}\n\
             - sonic_rows: {sonic_rows}\n\
             - ndjson_rows: {ndjson_rows}\n\
             - serde_output_bytes: {serde_bytes}\n\
             - sonic_output_bytes: {sonic_bytes}\n\
             - ndjson_output_bytes: {ndjson_bytes}\n\
             - serde_output_mbps: {serde_mbps:.2}\n\
             - sonic_output_mbps: {sonic_mbps:.2}\n\
             - ndjson_output_mbps: {ndjson_mbps:.2}\n",
        meta = meta_dir,
        loaded_vertices = loaded_vertices,
        loaded_edges = loaded_edges,
        promoted = promoted_columns.len(),
        load_secs = load_secs,
        rows_from_batches = rows_from_batches,
        query_batches_secs = query_batches_secs,
        query_serde_secs = query_serde_secs,
        query_sonic_secs = query_sonic_secs,
        query_ndjson_secs = query_ndjson_secs,
        serde_rows = rows_serde.len(),
        sonic_rows = rows_sonic.len(),
        ndjson_rows = ndjson_rows,
        serde_bytes = serde_bytes,
        sonic_bytes = sonic_bytes,
        ndjson_bytes = ndjson_bytes,
        serde_mbps = (serde_bytes as f64 * 8.0) / query_serde_secs.max(1e-9) / 1_000_000.0,
        sonic_mbps = (sonic_bytes as f64 * 8.0) / query_sonic_secs.max(1e-9) / 1_000_000.0,
        ndjson_mbps = (ndjson_bytes as f64 * 8.0) / query_ndjson_secs.max(1e-9) / 1_000_000.0,
    );

    assert_eq!(rows_serde.len(), rows_sonic.len());
    assert_eq!(rows_serde.len(), rows_from_batches);
    assert_eq!(rows_serde.len(), ndjson_rows);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark; run explicitly"]
async fn full_fast_reconstruction_benchmark() -> anyhow::Result<()> {
    let meta_dir = std::env::var("CALYPR_META_DIR")
        .unwrap_or_else(|_| "/Users/peterkor/Desktop/BMEG/grip-benchmark/calypr/META".to_string());
    let meta_path = FsPath::new(&meta_dir);
    assert!(meta_path.exists(), "META dir not found: {meta_dir}");

    let schema_doc = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": {
            "Observation": {
                "type": "object",
                "properties": {
                    "id": {"type":"string"},
                    "status": {"type":"string"},
                    "subject": {
                        "type":"object",
                        "properties": {"reference": {"type":"string"}}
                    }
                }
            }
        }
    });
    let arrow_shapes = compile_arrow_shapes(&schema_doc)?;
    let graph_schema = loom_engine::schema::compile_graph_schema(&schema_doc)?;
    let promoted_columns = promoted_columns_from_shapes(&arrow_shapes);
    let stored = StoredSchema {
        doc: schema_doc.clone(),
        summary: summarize_schema("bench", &schema_doc, arrow_shapes.len(), &graph_schema),
        arrow_shapes,
        promoted_columns: promoted_columns.clone(),
        graph_schema,
        compile_warnings: vec![],
    };

    let tmp = tempdir()?;
    let vertices_parquet = tmp.path().join("typed_vertices.parquet");
    let edges_parquet = tmp.path().join("typed_edges.parquet");
    let vortex_root = tmp.path().join("vortex");
    std::fs::create_dir_all(&vortex_root)?;

    let max_rows = std::env::var("BENCH_MAX_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(20_000);

    build_typed_vertex_parquet_limited(
        meta_path,
        &vertices_parquet,
        &promoted_columns,
        Some(max_rows),
    )?;
    build_edges_parquet_from_ndjson_limited(meta_path, &edges_parquet, Some(max_rows))?;

    let mut vertex_columns = vec![
        VertexColumn {
            name: "id".to_string(),
            sql_type: "VARCHAR NOT NULL".to_string(),
        },
        VertexColumn {
            name: "label".to_string(),
            sql_type: "VARCHAR NOT NULL".to_string(),
        },
        VertexColumn {
            name: "payload_codec".to_string(),
            sql_type: "VARCHAR NOT NULL".to_string(),
        },
        VertexColumn {
            name: "payload_bin".to_string(),
            sql_type: "BLOB NOT NULL".to_string(),
        },
        VertexColumn {
            name: "payload_json_bin".to_string(),
            sql_type: "BLOB NOT NULL".to_string(),
        },
    ];
    for col in &promoted_columns {
        vertex_columns.push(VertexColumn {
            name: col.column_name.clone(),
            sql_type: promoted_type_to_sql(&col.kind).to_string(),
        });
    }

    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    let graph = "full_fast_bench";
    engine
        .ingest_parquet_graph_to_vortex_with_vertex_columns(
            graph,
            vertices_parquet.to_str().unwrap_or_default(),
            edges_parquet.to_str().unwrap_or_default(),
            vortex_root.to_str().unwrap_or_default(),
            IngestMode::Overwrite,
            &vertex_columns,
        )
        .await?;

    let query = Query {
        graph: graph.to_string(),
        steps: vec![
            Step::V { ids: vec![] },
            Step::Render {
                fields: vec!["*".to_string()],
            },
        ],
    };

    let batches = engine.query_batches(&query).await?;
    let rows = batches_to_json_rows(&batches)?;

    let t_fast = Instant::now();
    let fast_rows = reconstruct_full_rows_fast(rows, Some(&stored))?;
    let fast_secs = t_fast.elapsed().as_secs_f64();

    let t_fast_batches = Instant::now();
    let fast_bytes = reconstruct_full_rows_fast_from_batches(&batches, Some(&stored))?;
    let fast_batches_secs = t_fast_batches.elapsed().as_secs_f64();
    let fast_batch_rows: Vec<Value> = serde_json::from_slice(&fast_bytes)?;

    eprintln!(
        "\nFull Reconstruction Benchmark\n\
             - max_rows: {max_rows}\n\
             - rows: {rows}\n\
             - full_fast_reconstruct_seconds: {fast_secs:.4}\n\
             - full_fast_batch_direct_seconds: {fast_batches_secs:.4}\n\
             - speedup_x_batch_direct_vs_fast: {speedup_batch:.2}\n",
        max_rows = max_rows,
        rows = fast_rows.len(),
        fast_secs = fast_secs,
        fast_batches_secs = fast_batches_secs,
        speedup_batch = fast_secs / fast_batches_secs.max(1e-9),
    );

    assert_eq!(fast_rows.len(), fast_batch_rows.len());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark; run explicitly"]
async fn legacy_compat_calypr_benchmark() -> anyhow::Result<()> {
    let meta_dir = std::env::var("CALYPR_META_DIR")
        .unwrap_or_else(|_| "/Users/peterkor/Desktop/BMEG/grip-benchmark/calypr/META".to_string());
    let meta_path = FsPath::new(&meta_dir);
    assert!(meta_path.exists(), "META dir not found: {meta_dir}");

    let tmp = tempdir()?;
    let filtered_dir = tmp.path().join("filtered_ndjson");
    std::fs::create_dir_all(&filtered_dir)?;
    let vertices_parquet = tmp.path().join("typed_vertices.parquet");
    let edges_parquet = tmp.path().join("typed_edges.parquet");
    let vortex_root = tmp.path().join("vortex");
    std::fs::create_dir_all(&vortex_root)?;

    let (loaded_records, source_records) = build_filtered_auth_ndjson(meta_path, &filtered_dir)?;

    let schema_doc = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "urn:example:legacy-compat",
        "$defs": {
            "Observation": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "status": {"type": "string"},
                    "auth_resource_path": {"type": "string"},
                    "resourceType": {"type": "string"}
                }
            }
        }
    });

    let promoted_columns = promoted_columns_from_shapes(&compile_arrow_shapes(&schema_doc)?);
    let loaded_vertices =
        build_typed_vertex_parquet(&filtered_dir, &vertices_parquet, &promoted_columns)?;
    let loaded_edges = build_edges_parquet_from_ndjson(&filtered_dir, &edges_parquet)?;

    let mut vertex_columns = vec![
        VertexColumn {
            name: "id".to_string(),
            sql_type: "VARCHAR NOT NULL".to_string(),
        },
        VertexColumn {
            name: "label".to_string(),
            sql_type: "VARCHAR NOT NULL".to_string(),
        },
        VertexColumn {
            name: "payload_codec".to_string(),
            sql_type: "VARCHAR NOT NULL".to_string(),
        },
        VertexColumn {
            name: "payload_bin".to_string(),
            sql_type: "BLOB NOT NULL".to_string(),
        },
        VertexColumn {
            name: "payload_json_bin".to_string(),
            sql_type: "BLOB NOT NULL".to_string(),
        },
    ];
    for col in &promoted_columns {
        vertex_columns.push(VertexColumn {
            name: col.column_name.clone(),
            sql_type: promoted_type_to_sql(&col.kind).to_string(),
        });
    }

    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    let graph = "legacy_compat";
    engine
        .ingest_parquet_graph_to_vortex_with_vertex_columns(
            graph,
            vertices_parquet.to_str().unwrap_or_default(),
            edges_parquet.to_str().unwrap_or_default(),
            vortex_root.to_str().unwrap_or_default(),
            IngestMode::Overwrite,
            &vertex_columns,
        )
        .await?;

    let q0 = Query {
        graph: graph.to_string(),
        steps: vec![Step::V { ids: vec![] }, Step::Count],
    };
    let t0 = Instant::now();
    let q0_rows = engine.query_json_rows(&q0).await?;
    let q0_secs = t0.elapsed().as_secs_f64();
    let total_count = q0_rows
        .first()
        .and_then(|row| serde_json::from_str::<Value>(row).ok())
        .and_then(|row| row.get("count").and_then(Value::as_u64))
        .unwrap_or(0);

    let auth_field = "t_observation_auth_resource_path".to_string();
    let status_field = "t_observation_status".to_string();
    let auth_target = "/programs/calypr/projects/test".to_string();

    let q1 = Query {
        graph: graph.to_string(),
        steps: vec![
            Step::V { ids: vec![] },
            Step::Has {
                field: auth_field.clone(),
                eq: Value::String(auth_target.clone()),
            },
        ],
    };
    let t1 = Instant::now();
    let q1_rows = engine.query_json_rows(&q1).await?;
    let q1_secs = t1.elapsed().as_secs_f64();

    let q2 = Query {
        graph: graph.to_string(),
        steps: vec![
            Step::V { ids: vec![] },
            Step::HasLabel {
                labels: vec!["Observation".to_string()],
            },
            Step::Has {
                field: auth_field.clone(),
                eq: Value::String(auth_target.clone()),
            },
        ],
    };
    let t2 = Instant::now();
    let q2_rows = engine.query_json_rows(&q2).await?;
    let q2_secs = t2.elapsed().as_secs_f64();

    let q3b = Query {
        graph: graph.to_string(),
        steps: vec![
            Step::V { ids: vec![] },
            Step::HasLabel {
                labels: vec!["Observation".to_string()],
            },
            Step::Render {
                fields: vec!["id".to_string(), status_field, auth_field.clone()],
            },
        ],
    };
    let t3b = Instant::now();
    let q3b_rows = engine.query_json_rows(&q3b).await?;
    let q3b_secs = t3b.elapsed().as_secs_f64();

    let q3 = Query {
        graph: graph.to_string(),
        steps: vec![
            Step::V { ids: vec![] },
            Step::HasLabel {
                labels: vec!["Observation".to_string()],
            },
            Step::Render {
                fields: vec!["*".to_string()],
            },
        ],
    };
    let t3 = Instant::now();
    let q3_rows = engine.query_json_rows(&q3).await?;
    let q3_secs = t3.elapsed().as_secs_f64();

    eprintln!(
        "\nLegacy-Compat CALYPR Benchmark (Rust)\n\
             - meta_dir: {meta}\n\
             - source_records: {source}\n\
             - loaded_records_after_filter: {loaded}\n\
             - loaded_vertices: {loaded_vertices}\n\
             - loaded_edges: {loaded_edges}\n\
             - promoted_columns: {promoted}\n\
             - query0_v_count_rows: {q0count} in {q0secs:.4}s\n\
             - query1_v_has_auth_rows: {q1count} in {q1secs:.4}s\n\
             - query2_v_haslabel_observation_has_auth_rows: {q2count} in {q2secs:.4}s\n\
             - query3b_observation_projected_rows: {q3bcount} in {q3bsecs:.4}s\n\
             - query3_observation_full_rows: {q3count} in {q3secs:.4}s\n",
        meta = meta_dir,
        source = source_records,
        loaded = loaded_records,
        loaded_vertices = loaded_vertices,
        loaded_edges = loaded_edges,
        promoted = promoted_columns.len(),
        q0count = total_count,
        q0secs = q0_secs,
        q1count = q1_rows.len(),
        q1secs = q1_secs,
        q2count = q2_rows.len(),
        q2secs = q2_secs,
        q3bcount = q3b_rows.len(),
        q3bsecs = q3b_secs,
        q3count = q3_rows.len(),
        q3secs = q3_secs
    );

    Ok(())
}

fn build_filtered_auth_ndjson(
    input_dir: &FsPath,
    output_dir: &FsPath,
) -> Result<(usize, usize), anyhow::Error> {
    let mut files = std::fs::read_dir(input_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("ndjson"))
        .collect::<Vec<PathBuf>>();
    files.sort();

    let mut loaded = 0usize;
    let mut total = 0usize;

    for file in files {
        let mut out = File::create(output_dir.join(file.file_name().unwrap_or_default()))?;
        let rdr = BufReader::new(File::open(&file)?);
        let mut count = 0usize;
        for line in rdr.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            count += 1;
            total += 1;
            let auth_path = if count % 5 == 0 {
                Some("/programs/calypr/projects/test")
            } else if count % 9 == 0 {
                Some("/programs/calypr/projects/testtwo")
            } else {
                None
            };
            let Some(auth_path) = auth_path else {
                continue;
            };
            let mut parsed: Value = serde_json::from_str(&line)?;
            if let Some(obj) = parsed.as_object_mut() {
                obj.insert(
                    "auth_resource_path".to_string(),
                    Value::String(auth_path.to_string()),
                );
            } else {
                continue;
            }
            let mut bytes = serde_json::to_vec(&parsed)?;
            bytes.push(b'\n');
            out.write_all(&bytes)?;
            loaded += 1;
        }
    }
    Ok((loaded, total))
}
