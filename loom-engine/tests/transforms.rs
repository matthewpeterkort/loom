use datafusion::datasource::ViewTable;
use loom_engine::mapping;
use loom_engine::source::{SourceFormat, SourceLocation, SourceRegistration};
use loom_engine::transform;
use loom_engine::{Engine, EngineConfig};
use rust_xlsxwriter::Workbook;
use serde_json::Value;
use tempfile::tempdir;

#[path = "support/cbio_small.rs"]
mod cbio_small;

#[tokio::test]
async fn transform_preview_and_registration_work_for_delimited_sources() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let path = tmp.path().join("messy.tsv");
    std::fs::write(
        &path,
        "patient id\tsample id\tassay\tstatus\n P001 \tS001\tWGS\tkeep\nP002\tS002\tRNA\tignore\n",
    )?;
    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    engine
        .register_source(SourceRegistration {
            id: "messy".to_string(),
            display_name: Some("messy".to_string()),
            format: SourceFormat::Tsv,
            location: SourceLocation::Local {
                path: path.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;
    let spec = transform::TransformSpec {
        input: loom_engine::source::SourceTableRef {
            source_id: "messy".to_string(),
            table_id: "primary".to_string(),
        },
        output_table_id: "samples_clean".to_string(),
        display_name: Some("Samples Clean".to_string()),
        operations: vec![
            transform::TransformOperation::RenameColumn {
                from: "patient id".to_string(),
                to: "participant_id".to_string(),
            },
            transform::TransformOperation::Trim {
                columns: vec!["participant_id".to_string()],
            },
            transform::TransformOperation::FilterRows(transform::FilterRowsOp {
                predicate: mapping::Predicate::Eq {
                    eq: mapping::BinaryPredicate {
                        left: mapping::Expr::Column {
                            column: "status".to_string(),
                        },
                        right: mapping::Expr::Literal {
                            literal: "keep".to_string(),
                        },
                    },
                },
            }),
            transform::TransformOperation::DeriveColumn(transform::DeriveColumnOp {
                column: "sample_key".to_string(),
                expr: mapping::Expr::Concat {
                    concat: vec![
                        mapping::Expr::Column {
                            column: "participant_id".to_string(),
                        },
                        mapping::Expr::Literal {
                            literal: ":".to_string(),
                        },
                        mapping::Expr::Column {
                            column: "sample id".to_string(),
                        },
                    ],
                },
            }),
            transform::TransformOperation::DropColumn {
                column: "status".to_string(),
            },
        ],
        metadata: Default::default(),
    };

    let preview = engine.preview_transform_spec(&spec, Some(10)).await?;
    assert_eq!(preview.plan_kind, transform::TransformPlanKind::Compiled);
    assert_eq!(preview.rows.len(), 1);
    assert_eq!(
        preview.rows[0].get("participant_id").map(String::as_str),
        Some("P001")
    );
    assert_eq!(
        preview.rows[0].get("sample_key").map(String::as_str),
        Some("P001:S001")
    );

    let descriptor = engine.register_transform(spec).await?;
    assert_eq!(descriptor.output_table.kind, loom_engine::source::SourceTableKind::Derived);
    let listed = engine.list_source_tables("messy")?;
    assert!(listed.iter().any(|table| table.id == "samples_clean"));
    let before = engine.get_source_table("messy", "samples_clean")?;
    assert!(!before.registered);

    let sampled = engine.sample_source_table("messy", "samples_clean", 10).await?;
    assert_eq!(sampled.len(), 1);
    let after = engine.get_source_table("messy", "samples_clean")?;
    assert!(after.registered);

    let rows = engine
        .query_sql_json_rows(
            "SELECT participant_id, sample_key, __transform_id FROM source_messy_samples_clean",
        )
        .await?;
    let row: Value = serde_json::from_str(&rows[0])?;
    assert_eq!(row["participant_id"], "P001");
    assert_eq!(row["sample_key"], "P001:S001");
    assert_eq!(row["__transform_id"], "transform_messy_samples_clean");
    let provider = engine
        .session
        .table_provider("source_messy_samples_clean")
        .await?;
    assert!(provider.as_any().is::<ViewTable>());
    Ok(())
}

#[tokio::test]
async fn transform_supports_layout_split_explode_and_coerce() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let workbook_path = tmp.path().join("messy_layout.xlsx");
    let mut workbook = Workbook::new();
    let sheet = workbook.add_worksheet().set_name("Sheet1")?;
    sheet.write_string(0, 0, "Study metadata")?;
    sheet.write_string(2, 0, "patient id")?;
    sheet.write_string(2, 1, "genes")?;
    sheet.write_string(2, 2, "score_pair")?;
    sheet.write_string(2, 3, "flag")?;
    sheet.write_string(3, 0, "P001")?;
    sheet.write_string(3, 1, "TP53|EGFR")?;
    sheet.write_string(3, 2, "7|9")?;
    sheet.write_string(3, 3, "yes")?;
    workbook.save(&workbook_path)?;

    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })?;
    engine
        .register_source(SourceRegistration {
            id: "layout_book".to_string(),
            display_name: Some("layout_book".to_string()),
            format: SourceFormat::Xlsx,
            location: SourceLocation::Local {
                path: workbook_path.to_string_lossy().to_string(),
            },
            read_options: Default::default(),
        })
        .await?;

    let validation = engine
        .validate_transform_spec(&transform::TransformSpec {
            input: loom_engine::source::SourceTableRef {
                source_id: "layout_book".to_string(),
                table_id: "sheet_0001_sheet1".to_string(),
            },
            output_table_id: "normalized".to_string(),
            display_name: None,
            operations: vec![
                transform::TransformOperation::ChooseHeaderRow { header_row_index: 2 },
                transform::TransformOperation::SetDataStartRow { data_start_row_index: 3 },
                transform::TransformOperation::SplitColumn(transform::SplitColumnOp {
                    column: "score_pair".to_string(),
                    delimiter: "|".to_string(),
                    into: vec!["score_a".to_string(), "score_b".to_string()],
                    behavior: transform::SplitColumnBehavior::DropOriginal,
                }),
                transform::TransformOperation::ExplodeColumn(transform::ExplodeColumnOp {
                    column: "genes".to_string(),
                    delimiter: "|".to_string(),
                    trim_values: true,
                    drop_empty: true,
                }),
                transform::TransformOperation::CoerceType {
                    column: "score_a".to_string(),
                    output_type: mapping::OutputType::Integer,
                    on_error: transform::CoerceOnErrorPolicy::Strict,
                },
                transform::TransformOperation::CoerceType {
                    column: "flag".to_string(),
                    output_type: mapping::OutputType::Boolean,
                    on_error: transform::CoerceOnErrorPolicy::Strict,
                },
            ],
            metadata: Default::default(),
        })
        .await?;
    assert!(validation.valid);

    let preview = engine
        .preview_transform_spec(
            &transform::TransformSpec {
                input: loom_engine::source::SourceTableRef {
                    source_id: "layout_book".to_string(),
                    table_id: "sheet_0001_sheet1".to_string(),
                },
                output_table_id: "normalized".to_string(),
                display_name: None,
                operations: vec![
                    transform::TransformOperation::ChooseHeaderRow { header_row_index: 2 },
                    transform::TransformOperation::SetDataStartRow { data_start_row_index: 3 },
                    transform::TransformOperation::SplitColumn(transform::SplitColumnOp {
                        column: "score_pair".to_string(),
                        delimiter: "|".to_string(),
                        into: vec!["score_a".to_string(), "score_b".to_string()],
                        behavior: transform::SplitColumnBehavior::DropOriginal,
                    }),
                    transform::TransformOperation::ExplodeColumn(transform::ExplodeColumnOp {
                        column: "genes".to_string(),
                        delimiter: "|".to_string(),
                        trim_values: true,
                        drop_empty: true,
                    }),
                ],
                metadata: Default::default(),
            },
            Some(10),
        )
        .await?;
    assert_eq!(preview.plan_kind, transform::TransformPlanKind::Fallback);
    assert_eq!(preview.output_table.header_row_index, Some(2));
    assert_eq!(preview.output_table.data_start_row_index, Some(3));
    assert_eq!(preview.rows.len(), 2);
    assert_eq!(preview.rows[0].get("genes").map(String::as_str), Some("TP53"));
    assert_eq!(preview.rows[0].get("score_a").map(String::as_str), Some("7"));
    Ok(())
}

#[tokio::test]
async fn transform_validation_rejects_bad_columns() -> anyhow::Result<()> {
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
    let report = engine
        .validate_transform_spec(&transform::TransformSpec {
            input: loom_engine::source::SourceTableRef {
                source_id: "samples".to_string(),
                table_id: "primary".to_string(),
            },
            output_table_id: "broken".to_string(),
            display_name: None,
            operations: vec![transform::TransformOperation::RenameColumn {
                from: "missing".to_string(),
                to: "new_name".to_string(),
            }],
            metadata: Default::default(),
        })
        .await?;
    assert!(!report.valid);
    assert!(report.errors.iter().any(|error| error.contains("missing")));
    Ok(())
}
