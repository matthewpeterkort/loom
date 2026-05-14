use loom_engine::graph_schema::GraphSchemaStatus;
use loom_engine::schema::compile_graph_schema;
use loom_engine::{Engine, EngineConfig};
use loomql_ast::{Query, Step};
use serde_json::Value;
use tempfile::tempdir;

#[path = "support/cbio_demo.rs"]
mod cbio_demo;

#[tokio::test]
#[ignore = "uses local cbio-demo composite fixture under workspace root; run explicitly for demo coverage"]
async fn cbioportal_public_raw_files_build_schema_bound_virtual_graph() -> anyhow::Result<()> {
    let demo_dir = cbio_demo::cbio_demo_dir();
    anyhow::ensure!(
        demo_dir.exists(),
        "local cbio-demo fixture missing at {}",
        demo_dir.display()
    );

    let tmp = tempdir()?;
    let fixture_dir = tmp.path().join("cbio-demo-tsv");
    cbio_demo::materialize_cbio_demo_fixture(&fixture_dir)?;

    let engine = Engine::new(EngineConfig {
        work_dir: tmp.path().to_string_lossy().to_string(),
    })?;
    cbio_demo::register_cbio_demo_sources(&engine, &fixture_dir).await?;

    let schema_doc = cbio_demo::load_sanitized_master_graph_schema()?;
    let graph_schema = compile_graph_schema(&schema_doc)?;
    let spec = cbio_demo::build_cbio_demo_graph_schema_spec(&engine)?;

    let schema_descriptor = engine.register_graph_schema(spec).await?;
    eprintln!(
        "[cbio-demo] graph schema registered id={} graph={}",
        schema_descriptor.id, schema_descriptor.graph
    );
    let preview = engine
        .preview_graph_schema_with_schema(
            &schema_descriptor.id,
            &fixture_dir,
            Some(&graph_schema),
            Some("master".to_string()),
        )
        .await;
    eprintln!(
        "[cbio-demo] graph schema preview: nodes={} edges={} warnings={}",
        preview
            .as_ref()
            .map(|preview| preview.node_counts.values().sum::<usize>())
            .unwrap_or_default(),
        preview
            .as_ref()
            .map(|preview| preview.edge_counts.values().sum::<usize>())
            .unwrap_or_default(),
        preview
            .as_ref()
            .map(|preview| preview.warnings.len())
            .unwrap_or_default()
    );
    let preview = preview?;
    let validation = preview
        .mapping_validation
        .clone()
        .expect("graph schema preview mapping validation");
    eprintln!(
        "[cbio-demo] mapping validation: valid={} warnings={} errors={}",
        validation.valid,
        validation.warnings.len(),
        validation.errors.len()
    );
    for warning in validation.warnings.iter().take(5) {
        eprintln!("[cbio-demo]   validation warning {warning}");
    }
    assert!(validation.valid, "{:?}", validation.errors);

    let descriptor = engine
        .register_graph_schema_runtime_with_schema(
            &schema_descriptor.id,
            &fixture_dir,
            false,
            Some(&graph_schema),
            Some("master".to_string()),
        )
        .await?;
    assert_eq!(descriptor.status, GraphSchemaStatus::Registered);
    assert_eq!(descriptor.bound_schema.as_deref(), Some("master"));
    eprintln!(
        "[cbio-demo] registered graph={} status={:?} bound_schema={:?}",
        descriptor.graph, descriptor.status, descriptor.bound_schema
    );
    let binding = engine
        .get_virtual_graph_descriptor("cbio_public_raw")?
        .virtual_binding
        .expect("virtual binding");
    assert_eq!(binding.node_view_names.len(), 6);
    assert_eq!(binding.edge_view_names.len(), 7);

    let patient_view = &binding.node_view_names[0];
    let specimen_view = &binding.node_view_names[1];
    let gene_view = &binding.node_view_names[2];
    let mutation_view = &binding.node_view_names[3];
    let cna_view = &binding.node_view_names[4];
    let expression_view = &binding.node_view_names[5];
    let specimen_patient_edge = &binding.edge_view_names[0];
    let mutation_specimen_edge = &binding.edge_view_names[1];
    let mutation_focus_edge = &binding.edge_view_names[2];
    let cna_specimen_edge = &binding.edge_view_names[3];
    let cna_focus_edge = &binding.edge_view_names[4];
    let expression_specimen_edge = &binding.edge_view_names[5];
    let expression_focus_edge = &binding.edge_view_names[6];

    eprintln!("[cbio-demo] node views:");
    for view in &binding.node_view_names {
        eprintln!("[cbio-demo]   {view}");
    }
    eprintln!("[cbio-demo] edge views:");
    for view in &binding.edge_view_names {
        eprintln!("[cbio-demo]   {view}");
    }

    let multimodal_sql = format!(
        r#"
        SELECT DISTINCT
            p.id AS patient_id,
            s.identifier AS sample_id,
            s.type AS sample_type,
            g.id AS gene_id,
            g.symbol AS gene_symbol,
            mut.id AS mutation_observation_id,
            mut."valueString" AS protein_change,
            cna.id AS cna_observation_id,
            cna."valueString" AS cna_value,
            expr.id AS expression_observation_id,
            expr."valueString" AS expr_value
        FROM {patient_view} p
        JOIN {specimen_patient_edge} specimen_patient
          ON specimen_patient.to_id = p.id
        JOIN {specimen_view} s
          ON s.id = specimen_patient.from_id
        JOIN {mutation_specimen_edge} mut_specimen
          ON mut_specimen.to_id = s.id
        JOIN {mutation_view} mut
          ON mut.id = mut_specimen.from_id
         AND mut.code = 'somatic-variant'
        JOIN {mutation_focus_edge} mut_gene
          ON mut_gene.from_id = mut.id
        JOIN {gene_view} g
          ON g.id = mut_gene.to_id
         AND g.symbol = 'TP53'
        JOIN {cna_specimen_edge} cna_specimen
          ON cna_specimen.to_id = s.id
        JOIN {cna_view} cna
          ON cna.id = cna_specimen.from_id
         AND cna.code = 'copy-number'
        JOIN {cna_focus_edge} cna_gene
          ON cna_gene.from_id = cna.id
         AND cna_gene.to_id = g.id
        JOIN {expression_specimen_edge} expr_specimen
          ON expr_specimen.to_id = s.id
        JOIN {expression_view} expr
          ON expr.id = expr_specimen.from_id
         AND expr.code = 'expression-zscore'
        JOIN {expression_focus_edge} expr_gene
          ON expr_gene.from_id = expr.id
         AND expr_gene.to_id = g.id
        ORDER BY patient_id, sample_id
        LIMIT 25
    "#
    );
    eprintln!(
        "[cbio-demo] query intent: find patient/specimen paths where TP53 has mutation, CNA, and expression observations on the same specimen"
    );
    eprintln!("[cbio-demo] multimodal SQL:\n{multimodal_sql}");
    let rows = engine.query_sql_json_rows(multimodal_sql.as_str()).await?;
    assert!(
        !rows.is_empty(),
        "expected multimodal TP53 patient/specimen/gene paths in cbio-demo fixture"
    );
    eprintln!("[cbio-demo] multimodal SQL returned {} rows", rows.len());
    let parsed = rows
        .iter()
        .map(|row| serde_json::from_str::<Value>(row))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(row) = parsed.first() {
        eprintln!("[cbio-demo] DEMO QUESTION");
        eprintln!(
            "[cbio-demo]   Which patients have a specimen where TP53 has all three signals at once:"
        );
        eprintln!("[cbio-demo]   1. a somatic mutation observation");
        eprintln!("[cbio-demo]   2. a copy-number observation");
        eprintln!("[cbio-demo]   3. an expression observation");
        eprintln!("[cbio-demo] PROOF RESULT");
        eprintln!(
            "[cbio-demo]   {} -> Specimen/{} -> {{ {}, {}, {} }} -> {}",
            row["patient_id"].as_str().unwrap_or_default(),
            row["sample_id"].as_str().unwrap_or_default(),
            format!(
                "{} protein_change={}",
                row["mutation_observation_id"].as_str().unwrap_or_default(),
                row["protein_change"].as_str().unwrap_or_default()
            ),
            format!(
                "{} cna_value={}",
                row["cna_observation_id"].as_str().unwrap_or_default(),
                row["cna_value"].as_str().unwrap_or_default()
            ),
            format!(
                "{} expr_value={}",
                row["expression_observation_id"].as_str().unwrap_or_default(),
                row["expr_value"].as_str().unwrap_or_default()
            ),
            row["gene_id"].as_str().unwrap_or_default(),
        );
    }
    for (index, row) in parsed.iter().take(5).enumerate() {
        eprintln!("[cbio-demo]   sql_row[{index}] {}", serde_json::to_string_pretty(row)?);
    }
    assert!(parsed.iter().all(|row| row["gene_symbol"] == "TP53"));
    assert!(parsed.iter().any(|row| row["sample_id"] == "TCGA-3C-AALI-01"));
    assert!(parsed.iter().any(|row| row["protein_change"] == "S183*"));
    assert!(parsed.iter().any(|row| row["cna_value"] == "-1"));

    let loomql_query = Query {
        graph: "cbio_public_raw".to_string(),
        steps: vec![
            Step::V {
                ids: vec!["Patient/TCGA-3C-AALI".to_string()],
            },
            Step::In {
                labels: vec!["subject_Patient".to_string()],
            },
            Step::In {
                labels: vec!["specimen_Specimen".to_string()],
            },
            Step::Has {
                field: "code".to_string(),
                eq: Value::String("somatic-variant".to_string()),
            },
            Step::Out {
                labels: vec!["focus".to_string()],
            },
            Step::Has {
                field: "symbol".to_string(),
                eq: Value::String("TP53".to_string()),
            },
            Step::Render {
                fields: vec!["id".to_string(), "symbol".to_string()],
            },
        ],
    };
    eprintln!(
        "[cbio-demo] LoomQL probe: Patient/TCGA-3C-AALI <-subject_Patient- Specimen <-specimen_Specimen- Observation -focus-> Gene(symbol=TP53)"
    );
    let loomql_rows = engine.query_json_rows(&loomql_query).await?;
    assert!(
        !loomql_rows.is_empty(),
        "expected LoomQL traversal from patient to TP53 gene through specimen and observation"
    );
    eprintln!("[cbio-demo] LoomQL returned {} row(s)", loomql_rows.len());
    for (index, row) in loomql_rows.iter().take(3).enumerate() {
        eprintln!("[cbio-demo]   loomql_row[{index}] {row}");
    }
    let loomql_gene: Value = serde_json::from_str(&loomql_rows[0])?;
    assert_eq!(loomql_gene["symbol"], "TP53");
    Ok(())
}
