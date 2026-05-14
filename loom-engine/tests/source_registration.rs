use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use loom_engine::source::{
    SourceCatalog, SourceCatalogSnapshot, SourceFormat, SourceLocation, SourceRegistration,
};
use loom_engine::schema::compile_graph_schema;
use loom_engine::{Engine, EngineConfig};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::Value;
use std::fs::File;
use std::sync::Arc;
use tempfile::tempdir;

#[path = "support/cbio_small.rs"]
mod cbio_small;

#[tokio::test]
async fn registers_profiles_and_queries_source_tables() -> anyhow::Result<()> {
    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    let samples = engine
        .register_source(cbio_small::local_source(
            "samples",
            "data_clinical_sample.txt",
            SourceFormat::CbioTsv,
        ))
        .await?;
    assert_eq!(samples.table_name.as_deref(), Some("source_samples"));
    assert_eq!(samples.tables.len(), 1);
    assert_eq!(samples.tables[0].schema.skipped_comment_rows, 4);
    assert!(samples.tables[0]
        .schema
        .columns
        .iter()
        .any(|col| col.name == "CUSTOM_BATCH"));

    let profile = engine.profile_source("samples")?;
    assert!(profile
        .suggestions
        .iter()
        .any(|s| s.column == "PATIENT_ID" && s.suggested_mapping == "Patient.id"));
    assert!(profile
        .suggestions
        .iter()
        .any(|s| s.column == "SAMPLE_ID" && s.suggested_mapping == "Specimen.id"));
    assert_eq!(profile.row_count, 3);
    let sample_profile = profile
        .column_profiles
        .iter()
        .find(|column| column.column == "SAMPLE_ID")
        .expect("SAMPLE_ID profile");
    assert_eq!(sample_profile.distinct_count, 3);
    assert!(sample_profile.uniqueness_ratio > 0.99);
    assert!(sample_profile
        .semantic_roles
        .iter()
        .any(|role| role == "Specimen.id"));
    let batch_profile = profile
        .column_profiles
        .iter()
        .find(|column| column.column == "CUSTOM_BATCH")
        .expect("CUSTOM_BATCH profile");
    assert!(batch_profile.distinct_count < batch_profile.non_empty_count);

    let rows = engine
        .query_sql_json_rows("SELECT COUNT(*) AS count FROM source_samples")
        .await?;
    let row: Value = serde_json::from_str(&rows[0])?;
    assert_eq!(row["count"], 3);
    let provenance_rows = engine
        .query_sql_json_rows(
            "SELECT __source_row FROM source_samples ORDER BY \"SAMPLE_ID\" LIMIT 1",
        )
        .await?;
    let provenance: Value = serde_json::from_str(&provenance_rows[0])?;
    assert_eq!(provenance["__source_row"], "6");

    let sample = engine.sample_source("samples", 1)?;
    assert_eq!(sample.len(), 1);
    assert_eq!(
        sample[0].get("SAMPLE_ID").map(String::as_str),
        Some("TCGA-A1-A0SB-01")
    );
    Ok(())
}

#[tokio::test]
async fn schema_aware_profile_derives_candidates_from_bound_schema() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let source_path = tmp.path().join("measurements.tsv");
    std::fs::write(
        &source_path,
        "sample_id\tpatient_id\tvalue\tunit\tstart\tend\tcode\tdisplay\nS1\tP1\t1.5\tmg\t2024-01-01\t2024-01-31\tTP53\tTP53 panel\n",
    )?;
    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    engine
        .register_source(SourceRegistration {
            id: "measurements".to_string(),
            display_name: None,
            format: SourceFormat::Tsv,
            location: SourceLocation::Local {
                path: source_path.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;
    let schema_doc = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": {
            "Patient": {
                "type": "object",
                "properties": {"id": {"type": "string"}}
            },
            "Specimen": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "identifier": {"type": "string"}
                },
                "links": [{
                    "rel": "subject_Patient",
                    "targetSchema": {"$ref": "#/$defs/Patient"}
                }]
            },
            "Observation": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "valueQuantity": {"$ref": "#/$defs/Quantity"},
                    "effectivePeriod": {"$ref": "#/$defs/Period"},
                    "code": {"$ref": "#/$defs/CodeableConcept"}
                },
                "links": [{
                    "rel": "specimen_Specimen",
                    "targetSchema": {"$ref": "#/$defs/Specimen"}
                }]
            },
            "Quantity": {"type":"object","properties":{"value":{"type":"number"},"unit":{"type":"string"}}},
            "Period": {"type":"object","properties":{"start":{"type":"string"},"end":{"type":"string"}}},
            "CodeableConcept": {"type":"object","properties":{"code":{"type":"string"},"display":{"type":"string"}}}
        }
    });
    let compiled = compile_graph_schema(&schema_doc)?;
    let profile = engine
        .profile_source_with_schema("measurements", Some(&compiled))?;
    assert!(profile.schema_derived);
    assert!(profile
        .entity_candidates
        .iter()
        .any(|candidate| candidate.entity == "Observation"));
    assert!(profile
        .clusters
        .iter()
        .any(|cluster| matches!(cluster.kind, loom_engine::schema_binding::ColumnClusterKind::Quantity)));
    assert!(profile
        .clusters
        .iter()
        .any(|cluster| matches!(cluster.kind, loom_engine::schema_binding::ColumnClusterKind::Period)));
    let value_profile = profile
        .column_profiles
        .iter()
        .find(|column| column.column == "value")
        .expect("value column profile");
    assert!(value_profile
        .property_matches
        .iter()
        .any(|candidate| candidate.entity == "Observation" && candidate.property == "valueQuantity"));
    let patient_profile = profile
        .column_profiles
        .iter()
        .find(|column| column.column == "patient_id")
        .expect("patient_id column profile");
    assert!(patient_profile.likely_foreign_key);
    Ok(())
}

#[tokio::test]
async fn registers_cbio_case_list_as_normalized_source_rows() -> anyhow::Result<()> {
    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    let desc = engine
        .register_source(cbio_small::local_source(
            "sequenced_cases",
            "case_lists/cases_sequenced.txt",
            SourceFormat::CbioCaseList,
        ))
        .await?;
    assert_eq!(desc.stats.row_count, Some(2));
    let rows = engine
        .query_sql_json_rows("SELECT sample_id FROM source_sequenced_cases ORDER BY sample_id")
        .await?;
    let parsed = rows
        .iter()
        .map(|row| serde_json::from_str::<Value>(row))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(parsed[0]["sample_id"], "TCGA-A1-A0SB-01");
    assert_eq!(parsed[1]["sample_id"], "TCGA-A2-A04P-01");
    Ok(())
}

#[tokio::test]
async fn source_catalog_snapshot_reloads_and_reregisters_tables() -> anyhow::Result<()> {
    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    cbio_small::register_fixture_sources(&engine).await?;
    let snapshot = engine.source_catalog.snapshot()?;
    assert_eq!(snapshot.sources.len(), 4);

    let restored = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    let errors = restored.load_source_catalog_snapshot(snapshot).await?;
    assert!(errors.is_empty());
    let rows = restored
        .query_sql_json_rows("SELECT COUNT(*) AS count FROM source_mutations")
        .await?;
    let row: Value = serde_json::from_str(&rows[0])?;
    assert_eq!(row["count"], 3);
    Ok(())
}

#[tokio::test]
async fn parquet_source_registers_as_datafusion_table() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let parquet_path = tmp.path().join("mini.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["1", "2"])) as ArrayRef,
        ],
    )?;
    let file = File::create(&parquet_path)?;
    let mut writer = ArrowWriter::try_new(file, schema, Some(WriterProperties::builder().build()))?;
    writer.write(&batch)?;
    writer.close()?;

    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    engine
        .register_source(SourceRegistration {
            id: "mini_parquet".to_string(),
            display_name: None,
            format: SourceFormat::Parquet,
            location: SourceLocation::Local {
                path: parquet_path.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;
    let rows = engine
        .query_sql_json_rows("SELECT id, value FROM source_mini_parquet ORDER BY id")
        .await?;
    assert_eq!(rows.len(), 2);
    Ok(())
}

#[test]
fn source_catalog_snapshot_persists_descriptors() -> anyhow::Result<()> {
    let catalog = SourceCatalog::new();
    let descriptor = loom_engine::source::infer_and_read_source(cbio_small::local_source(
        "patients",
        "data_clinical_patient.txt",
        SourceFormat::CbioTsv,
    ))?
    .0;
    catalog.upsert(descriptor)?;
    let snapshot = catalog.snapshot()?;
    let bytes = serde_json::to_vec_pretty(&snapshot)?;
    let decoded: SourceCatalogSnapshot = serde_json::from_slice(&bytes)?;
    assert_eq!(decoded.sources.len(), 1);
    assert_eq!(decoded.sources[0].id, "patients");
    assert_eq!(decoded.sources[0].tables.len(), 1);
    assert_eq!(decoded.sources[0].tables[0].schema.skipped_comment_rows, 4);
    Ok(())
}

#[tokio::test]
async fn xlsx_source_discovers_visible_sheets_and_registers_lazily() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let workbook_path = tmp.path().join("workbook.xlsx");
    cbio_small::create_workbook_fixture(&workbook_path)?;

    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    let descriptor = engine
        .register_source(SourceRegistration {
            id: "bio_workbook".to_string(),
            display_name: Some("bio_workbook".to_string()),
            format: SourceFormat::Xlsx,
            location: SourceLocation::Local {
                path: workbook_path.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;

    assert_eq!(descriptor.tables.len(), 2);
    assert!(descriptor.tables.iter().all(|table| !table.registered));
    assert_eq!(
        descriptor
            .metadata
            .get("visible_sheet_count")
            .map(String::as_str),
        Some("2")
    );
    assert_eq!(
        descriptor.tables.iter().map(|table| table.id.as_str()).collect::<Vec<_>>(),
        vec!["sheet_0001_tumor_data", "sheet_0002_tumor_data"]
    );
    assert_eq!(
        descriptor.tables[0].metadata.get("sheet_name").map(String::as_str),
        Some("Tumor Data")
    );
    assert_eq!(descriptor.tables[0].header_row_index, Some(0));
    assert_eq!(descriptor.tables[0].data_start_row_index, Some(1));
    assert!(descriptor.tables[0]
        .quality
        .possible_id_columns
        .iter()
        .any(|column| column == "patient_id"));

    let source_level_profile = engine.profile_source("bio_workbook");
    assert!(source_level_profile.is_err());

    let tables = engine.list_source_tables("bio_workbook")?;
    assert_eq!(tables.len(), 2);
    let first = engine
        .profile_source_table("bio_workbook", "sheet_0001_tumor_data")
        .await?;
    assert_eq!(first.row_count, 2);
    assert!(first.columns.iter().any(|column| column == "patient_id"));

    let first_table = engine.get_source_table("bio_workbook", "sheet_0001_tumor_data")?;
    assert!(first_table.registered);

    let query_rows = engine
        .query_sql_json_rows(
            "SELECT patient_id, sample_id FROM source_bio_workbook_sheet_0001_tumor_data ORDER BY patient_id",
        )
        .await?;
    assert_eq!(query_rows.len(), 2);

    let sampled = engine
        .sample_source_table("bio_workbook", "sheet_0002_tumor_data", 1)
        .await?;
    assert_eq!(sampled.len(), 1);
    assert_eq!(sampled[0].get("assay").map(String::as_str), Some("WGS"));

    Ok(())
}
