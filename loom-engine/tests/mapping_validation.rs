use loom_engine::mapping::{GraphMappingManifest, GraphMappingStatus, MappingValidationReport};
use loom_engine::schema::compile_graph_schema;
use loom_engine::source::{SourceFormat, SourceLocation, SourceRegistration};
use loom_engine::{Engine, EngineConfig};
use serde_json::Value;
use tempfile::tempdir;

#[path = "support/cbio_small.rs"]
mod cbio_small;

#[tokio::test]
async fn graph_mapping_manifest_validates_missing_columns_and_reserved_outputs() -> anyhow::Result<()> {
    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    engine
        .register_source(cbio_small::local_source(
            "samples",
            "data_clinical_sample.txt",
            SourceFormat::CbioTsv,
        ))
        .await?;
    let manifest: GraphMappingManifest = serde_json::from_value(serde_json::json!({
        "version": 1,
        "graph": "bad_mapping",
        "sources": {
            "samples": { "source": "samples" }
        },
        "vertices": [{
            "source": "samples",
            "label": "Specimen",
            "id": { "column": "DOES_NOT_EXIST" },
            "columns": {
                "id": { "column": "SAMPLE_ID" }
            }
        }]
    }))?;
    let report: MappingValidationReport =
        engine.validate_graph_mapping(&manifest, &cbio_small::fixture_dir());
    assert!(!report.valid);
    assert!(report
        .errors
        .iter()
        .any(|err| err.path == "vertices[0].id.column"));
    assert!(report
        .errors
        .iter()
        .any(|err| err.path == "vertices[0].columns.id"));
    Ok(())
}

#[tokio::test]
async fn graph_mapping_manifest_filters_and_expression_ops_materialize() -> anyhow::Result<()> {
    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    engine
        .register_source(cbio_small::local_source(
            "samples",
            "data_clinical_sample.txt",
            SourceFormat::CbioTsv,
        ))
        .await?;
    let manifest: GraphMappingManifest = serde_json::from_value(serde_json::json!({
        "version": 1,
        "graph": "expr_graph",
        "sources": {
            "samples": { "source": "samples" }
        },
        "vertices": [{
            "source": "samples",
            "label": "Specimen",
            "where": {
                "and": [
                    { "eq": { "left": { "column": "CUSTOM_BATCH" }, "right": "B1" } },
                    { "is_not_empty": { "column": "SAMPLE_ID" } }
                ]
            },
            "id": {
                "concat": [
                    "Specimen/",
                    { "trim": { "column": "SAMPLE_ID" } }
                ]
            },
            "columns": {
                "batch_lower": { "lower": { "column": "CUSTOM_BATCH" } },
                "sample_upper": { "upper": { "column": "SAMPLE_ID" } },
                "sample_replaced": {
                    "replace": {
                        "value": { "column": "SAMPLE_ID" },
                        "from": "TCGA",
                        "to": "T"
                    }
                },
                "empty_if_b1": {
                    "null_if": [
                        { "column": "CUSTOM_BATCH" },
                        "B1"
                    ]
                },
                "stable_hash": {
                    "sha256": {
                        "concat": [
                            { "column": "PATIENT_ID" },
                            "/",
                            { "column": "SAMPLE_ID" }
                        ]
                    }
                }
            }
        }],
        "edges": []
    }))?;
    let plan = engine.compile_graph_mapping_sql_preview(&manifest, &cbio_small::fixture_dir())?;
    assert_eq!(plan.node_views.len(), 1);
    assert_eq!(plan.node_views[0].source_table_name, "source_samples");
    assert_eq!(plan.node_views[0].label, "Specimen");
    assert!(plan.node_views[0]
        .projected_columns
        .contains(&"source_row".to_string()));
    assert!(
        plan.node_views[0]
            .projected_columns
            .contains(&"prop_patient_id".to_string())
            == false
    );
    let descriptor = engine
        .register_graph_mapping(manifest, &cbio_small::fixture_dir(), true)
        .await?;
    let report = descriptor.last_report.as_ref().expect("compile report");
    assert_eq!(report.vertices, 2);
    assert_eq!(report.filtered_vertex_rows, 1);

    let rows = engine
        .query_sql_json_rows(
            "SELECT batch_lower, empty_if_b1, stable_hash FROM vertices_expr_graph ORDER BY id",
        )
        .await?;
    assert_eq!(rows.len(), 2);
    let first: Value = serde_json::from_str(&rows[0])?;
    assert_eq!(first["batch_lower"], "b1");
    assert_eq!(first["empty_if_b1"], Value::Null);
    assert_eq!(first["stable_hash"].as_str().unwrap_or_default().len(), 64);
    Ok(())
}

#[tokio::test]
async fn graph_mapping_validation_rejects_missing_endpoint_labels() -> anyhow::Result<()> {
    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    engine
        .register_source(cbio_small::local_source(
            "samples",
            "data_clinical_sample.txt",
            SourceFormat::CbioTsv,
        ))
        .await?;
    let manifest: GraphMappingManifest = serde_json::from_value(serde_json::json!({
        "version": 1,
        "graph": "bad_edges",
        "sources": {
            "samples": { "source": "samples" }
        },
        "vertices": [{
            "source": "samples",
            "label": "Specimen",
            "id": { "concat": ["Specimen/", { "column": "SAMPLE_ID" }] }
        }],
        "edges": [{
            "source": "samples",
            "label": "SELF",
            "from": { "concat": ["Specimen/", { "column": "SAMPLE_ID" }] },
            "to": { "concat": ["Specimen/", { "column": "SAMPLE_ID" }] }
        }]
    }))?;
    let report = engine.validate_graph_mapping(&manifest, &cbio_small::fixture_dir());
    assert!(!report.valid);
    assert!(report.errors.iter().any(|err| err.path == "edges[0].from_label"));
    assert!(report.errors.iter().any(|err| err.path == "edges[0].to_label"));
    Ok(())
}

#[tokio::test]
async fn graph_mapping_semantic_validation_enforces_policies() -> anyhow::Result<()> {
    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    engine
        .register_source(cbio_small::local_source(
            "samples",
            "data_clinical_sample.txt",
            SourceFormat::CbioTsv,
        ))
        .await?;
    let duplicate_manifest: GraphMappingManifest = serde_json::from_value(serde_json::json!({
        "version": 1,
        "graph": "duplicates",
        "sources": {
            "samples": { "source": "samples" }
        },
        "vertices": [{
            "source": "samples",
            "label": "Patient",
            "id": { "concat": ["Patient/", { "column": "PATIENT_ID" }] }
        }]
    }))?;
    let duplicate_report = engine
        .validate_graph_mapping_full(&duplicate_manifest, &cbio_small::fixture_dir())
        .await;
    assert!(!duplicate_report.valid);
    assert_eq!(duplicate_report.metrics.duplicate_vertex_ids, 1);
    assert!(duplicate_report
        .errors
        .iter()
        .any(|err| err.path == "validation.duplicate_vertex_ids"));

    let missing_endpoint_manifest: GraphMappingManifest =
        serde_json::from_value(serde_json::json!({
            "version": 1,
            "graph": "missing_endpoint",
            "sources": {
                "samples": { "source": "samples" }
            },
            "vertices": [{
                "source": "samples",
                "label": "Patient",
                "id": { "concat": ["Patient/", { "column": "PATIENT_ID" }] }
            }, {
                "source": "samples",
                "label": "Specimen",
                "id": { "concat": ["Specimen/", { "column": "SAMPLE_ID" }] }
            }],
            "edges": [{
                "source": "samples",
                "label": "BROKEN",
                "from_label": "Patient",
                "to_label": "Specimen",
                "from": { "concat": ["Patient/", { "column": "PATIENT_ID" }] },
                "to": "Specimen/DOES_NOT_EXIST"
            }]
        }))?;
    let missing_report = engine
        .validate_graph_mapping_full(&missing_endpoint_manifest, &cbio_small::fixture_dir())
        .await;
    assert!(!missing_report.valid);
    assert_eq!(missing_report.metrics.unresolved_edge_endpoints, 3);
    assert!(missing_report.errors.iter().any(|err| err.path == "edges[0].to"));
    Ok(())
}

#[tokio::test]
async fn graph_mapping_validation_checks_typed_column_coercion() -> anyhow::Result<()> {
    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    engine
        .register_source(cbio_small::local_source(
            "patients",
            "data_clinical_patient.txt",
            SourceFormat::CbioTsv,
        ))
        .await?;
    let manifest: GraphMappingManifest = serde_json::from_value(serde_json::json!({
        "version": 1,
        "graph": "typed_columns",
        "sources": {
            "patients": { "source": "patients" }
        },
        "vertices": [{
            "source": "patients",
            "label": "Patient",
            "id": { "concat": ["Patient/", { "column": "PATIENT_ID" }] },
            "props": "all",
            "prop_types": {
                "OS_STATUS": "integer"
            },
            "columns": {
                "os_months": {
                    "expr": { "column": "OS_MONTHS" },
                    "type": "float",
                    "coerce": true
                },
                "bad_integer": {
                    "expr": { "column": "OS_STATUS" },
                    "type": "integer",
                    "coerce": true
                }
            }
        }]
    }))?;
    let report = engine
        .validate_graph_mapping_full(&manifest, &cbio_small::fixture_dir())
        .await;
    assert!(!report.valid);
    assert_eq!(report.metrics.coercion_failures.get("bad_integer"), Some(&2));
    assert_eq!(
        report.metrics.coercion_failures.get("prop_os_status"),
        Some(&2)
    );
    assert!(report
        .errors
        .iter()
        .any(|err| err.path == "vertices[0].columns.bad_integer.type"));
    assert!(report.plan_preview.is_some());
    Ok(())
}

#[tokio::test]
async fn identity_and_reference_views_resolve_fhir_references() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let patients = tmp.path().join("patients.tsv");
    let samples = tmp.path().join("samples.tsv");
    std::fs::write(&patients, "PATIENT_ID\tMRN\np1\tMRN-1\np2\tMRN-2\n")?;
    std::fs::write(
        &samples,
        "SAMPLE_ID\tPATIENT_REF\ns1\tPatient/p1\ns2\tp2\ns3\tPatient/missing\n",
    )?;

    let engine = Engine::new(EngineConfig {
        work_dir: tmp.path().to_string_lossy().to_string(),
    })?;
    engine
        .register_source(SourceRegistration {
            id: "patients".to_string(),
            display_name: None,
            format: SourceFormat::Tsv,
            location: SourceLocation::Local {
                path: patients.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;
    engine
        .register_source(SourceRegistration {
            id: "samples".to_string(),
            display_name: None,
            format: SourceFormat::Tsv,
            location: SourceLocation::Local {
                path: samples.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;

    let manifest: GraphMappingManifest = serde_json::from_value(serde_json::json!({
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
    let descriptor = engine.register_graph_mapping(manifest, tmp.path(), false).await?;
    assert_eq!(
        descriptor.status,
        GraphMappingStatus::Registered,
        "last_error={:?}",
        descriptor.last_error
    );

    let identity_json = engine
        .query_sql_json_rows(
            "SELECT canonical_id, alias_name, alias_key FROM graph_identity_demo_identity_patient ORDER BY canonical_id, alias_name",
        )
        .await?;
    assert!(identity_json.len() >= 4);

    let reference_json = engine
        .query_sql_json_rows(
            "SELECT from_key, to_key, resolved_from_id, resolved_to_id, resolution_status FROM graph_identity_demo_reference_sample_patient ORDER BY from_key",
        )
        .await?;
    let parsed = reference_json
        .iter()
        .map(|row| serde_json::from_str::<Value>(row))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(parsed.len(), 3);
    assert_eq!(parsed[0]["resolved_from_id"], "s1");
    assert_eq!(parsed[0]["resolved_to_id"], "p1");
    assert_eq!(parsed[0]["resolution_status"], "resolved");
    assert_eq!(parsed[1]["resolved_to_id"], "p2");
    assert_eq!(parsed[2]["resolution_status"], "unresolved");

    let summary = engine.get_virtual_graph_identity_summary("identity_demo").await?;
    assert_eq!(summary.labels.len(), 2);
    assert_eq!(summary.references.len(), 1);
    assert_eq!(summary.unresolved_references.get("sample_patient"), Some(&1));
    Ok(())
}

#[tokio::test]
async fn identity_validation_reports_duplicate_aliases_and_ambiguous_references() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let patients = tmp.path().join("patients.tsv");
    let samples = tmp.path().join("samples.tsv");
    std::fs::write(&patients, "PATIENT_ID\tMRN\np1\tMRN-1\np2\tMRN-1\n")?;
    std::fs::write(&samples, "SAMPLE_ID\tPATIENT_REF\ns1\tMRN-1\n")?;

    let engine = Engine::new(EngineConfig {
        work_dir: tmp.path().to_string_lossy().to_string(),
    })?;
    engine
        .register_source(SourceRegistration {
            id: "patients".to_string(),
            display_name: None,
            format: SourceFormat::Tsv,
            location: SourceLocation::Local {
                path: patients.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;
    engine
        .register_source(SourceRegistration {
            id: "samples".to_string(),
            display_name: None,
            format: SourceFormat::Tsv,
            location: SourceLocation::Local {
                path: samples.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;

    let manifest: GraphMappingManifest = serde_json::from_value(serde_json::json!({
        "version": 1,
        "graph": "identity_conflict",
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
                    "mrn": { "column": "MRN" }
                },
                "normalizer": "trim_lower"
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
                "normalizer": "trim_lower"
            }
        ]
    }))?;
    let report = engine.validate_graph_mapping_full(&manifest, tmp.path()).await;
    assert!(!report.valid, "{report:?}");
    assert_eq!(report.metrics.duplicate_alias_keys.get("Patient"), Some(&1));
    assert_eq!(report.metrics.ambiguous_references.get("sample_patient"), Some(&1));
    assert!(report.errors.iter().any(|err| err.path == "identity.Patient"));
    assert!(report
        .errors
        .iter()
        .any(|err| err.path == "references.sample_patient"));
    Ok(())
}

#[tokio::test]
async fn schema_bound_mapping_validation_rejects_unknown_labels_and_links() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let patients = tmp.path().join("patients.tsv");
    let samples = tmp.path().join("samples.tsv");
    std::fs::write(&patients, "PATIENT_ID\tMRN\np1\tm1\n")?;
    std::fs::write(&samples, "SAMPLE_ID\tPATIENT_ID\ns1\tp1\n")?;

    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    engine
        .register_source(SourceRegistration {
            id: "patients".to_string(),
            display_name: None,
            format: SourceFormat::Tsv,
            location: SourceLocation::Local {
                path: patients.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;
    engine
        .register_source(SourceRegistration {
            id: "samples".to_string(),
            display_name: None,
            format: SourceFormat::Tsv,
            location: SourceLocation::Local {
                path: samples.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;

    let graph_schema = compile_graph_schema(&serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": {
            "Resource": {"type": "object", "properties": {"id": {"type": "string"}}},
            "Patient": {"type": "object", "properties": {"id": {"type": "string"}, "mrn": {"type": "string"}}},
            "Specimen": {"type": "object", "properties": {"id": {"type": "string"}}},
            "Gene": {"type": "object", "properties": {"id": {"type": "string"}}}
        }
    }))?;
    let manifest: GraphMappingManifest = serde_json::from_value(serde_json::json!({
        "version": 1,
        "graph": "schema_invalid",
        "sources": {
            "patients": { "source": "patients" },
            "samples": { "source": "samples" }
        },
        "vertices": [
            { "source": "patients", "label": "Patient", "id": { "column": "PATIENT_ID" }, "columns": { "mrn": { "column": "MRN" } } },
            { "source": "samples", "label": "Sample", "id": { "column": "SAMPLE_ID" } }
        ],
        "edges": [
            {
                "source": "samples",
                "label": "HAS_SPECIMEN",
                "from_label": "Patient",
                "to_label": "Gene",
                "from": { "column": "PATIENT_ID" },
                "to": { "column": "SAMPLE_ID" }
            }
        ]
    }))?;
    let report = engine
        .validate_graph_mapping_full_with_schema(&manifest, tmp.path(), Some(&graph_schema))
        .await;
    assert!(!report.valid);
    assert!(report
        .errors
        .iter()
        .any(|error| error.message.contains("vertex label `Sample`")));
    assert!(report
        .errors
        .iter()
        .any(|error| error.message.contains("edge label `HAS_SPECIMEN`")));
    Ok(())
}

#[tokio::test]
async fn schema_bound_mapping_validation_allows_valid_labels_properties_and_links() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let patients = tmp.path().join("patients.tsv");
    let samples = tmp.path().join("samples.tsv");
    std::fs::write(&patients, "PATIENT_ID\tMRN\np1\tm1\n")?;
    std::fs::write(&samples, "SAMPLE_ID\tPATIENT_ID\ns1\tp1\n")?;

    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    engine
        .register_source(SourceRegistration {
            id: "patients".to_string(),
            display_name: None,
            format: SourceFormat::Tsv,
            location: SourceLocation::Local {
                path: patients.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;
    engine
        .register_source(SourceRegistration {
            id: "samples".to_string(),
            display_name: None,
            format: SourceFormat::Tsv,
            location: SourceLocation::Local {
                path: samples.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;

    let graph_schema = compile_graph_schema(&serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": {
            "Resource": {"type": "object", "properties": {"id": {"type": "string"}}},
            "Patient": {
                "type": "object",
                "properties": {"id": {"type": "string"}, "mrn": {"type": "string"}},
                "links": [{
                    "rel": "HAS_SPECIMEN",
                    "targetSchema": {"$ref": "#/$defs/Specimen"},
                    "templatePointers": {"id": "/subject/reference"},
                    "targetHints": {"direction": ["outbound"], "multiplicity": ["has_many"]}
                }]
            },
            "Specimen": {
                "type": "object",
                "properties": {"id": {"type": "string"}, "patient_id": {"type": "string"}}
            }
        }
    }))?;
    let manifest: GraphMappingManifest = serde_json::from_value(serde_json::json!({
        "version": 1,
        "graph": "schema_valid",
        "sources": {
            "patients": { "source": "patients" },
            "samples": { "source": "samples" }
        },
        "vertices": [
            { "source": "patients", "label": "Patient", "id": { "column": "PATIENT_ID" }, "columns": { "mrn": { "column": "MRN" } } },
            { "source": "samples", "label": "Specimen", "id": { "column": "SAMPLE_ID" }, "columns": { "patient_id": { "column": "PATIENT_ID" } } }
        ],
        "edges": [
            {
                "source": "samples",
                "label": "HAS_SPECIMEN",
                "from_label": "Patient",
                "to_label": "Specimen",
                "from": { "column": "PATIENT_ID" },
                "to": { "column": "SAMPLE_ID" }
            }
        ]
    }))?;
    let report = engine
        .validate_graph_mapping_full_with_schema(&manifest, tmp.path(), Some(&graph_schema))
        .await;
    assert!(report.valid, "{:?}", report.errors);
    Ok(())
}

#[tokio::test]
async fn schema_bound_mapping_validation_rejects_missing_required_fields() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let patients = tmp.path().join("patients.tsv");
    std::fs::write(&patients, "PATIENT_ID\tMRN\np1\tm1\n")?;

    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    engine
        .register_source(SourceRegistration {
            id: "patients".to_string(),
            display_name: None,
            format: SourceFormat::Tsv,
            location: SourceLocation::Local {
                path: patients.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;

    let graph_schema = compile_graph_schema(&serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": {
            "Patient": {
                "type": "object",
                "required": ["id", "resourceType", "mrn", "birthDate"],
                "properties": {
                    "id": {"type": "string"},
                    "resourceType": {"type": "string"},
                    "mrn": {"type": "string"},
                    "birthDate": {"type": "string"}
                }
            }
        }
    }))?;
    let manifest: GraphMappingManifest = serde_json::from_value(serde_json::json!({
        "version": 1,
        "graph": "schema_required",
        "sources": { "patients": { "source": "patients" } },
        "vertices": [
            { "source": "patients", "label": "Patient", "id": { "column": "PATIENT_ID" }, "columns": { "mrn": { "column": "MRN" } } }
        ],
        "edges": []
    }))?;
    let report = engine
        .validate_graph_mapping_full_with_schema(&manifest, tmp.path(), Some(&graph_schema))
        .await;
    assert!(!report.valid);
    assert!(report
        .errors
        .iter()
        .any(|error| error.message.contains("required fields: birthDate")));
    assert_eq!(
        report.metrics.missing_required_fields.get("Patient"),
        Some(&vec!["birthDate".to_string()])
    );
    Ok(())
}

#[tokio::test]
async fn schema_bound_mapping_validation_accepts_required_fields_via_props_all() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let patients = tmp.path().join("patients.tsv");
    std::fs::write(&patients, "id\tmrn\tbirthDate\np1\tm1\t2000-01-01\n")?;

    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    engine
        .register_source(SourceRegistration {
            id: "patients".to_string(),
            display_name: None,
            format: SourceFormat::Tsv,
            location: SourceLocation::Local {
                path: patients.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;

    let graph_schema = compile_graph_schema(&serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": {
            "Patient": {
                "type": "object",
                "required": ["id", "resourceType", "mrn", "birthDate"],
                "properties": {
                    "id": {"type": "string"},
                    "resourceType": {"type": "string"},
                    "mrn": {"type": "string"},
                    "birthDate": {"type": "string"}
                }
            }
        }
    }))?;
    let manifest: GraphMappingManifest = serde_json::from_value(serde_json::json!({
        "version": 1,
        "graph": "schema_required_props",
        "sources": { "patients": { "source": "patients" } },
        "vertices": [
            { "source": "patients", "label": "Patient", "id": { "column": "id" }, "props": "all" }
        ],
        "edges": []
    }))?;
    let report = engine
        .validate_graph_mapping_full_with_schema(&manifest, tmp.path(), Some(&graph_schema))
        .await;
    assert!(report.valid, "{:?}", report.errors);
    Ok(())
}
