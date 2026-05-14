use anyhow::Result;
use loom_engine::graph_schema::{GraphEdgeSpec, GraphNodeSpec, GraphSchemaSpec};
use loom_engine::mapping::{load_mapping_manifest, GraphMappingManifest};
use loom_engine::schema::CompiledGraphSchema;
use loom_engine::source::{SourceFormat, SourceLocation, SourceRegistration, SourceTableRef};
use loom_engine::Engine;
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

pub fn cbio_demo_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join("cbio-demo")
}

pub fn master_schema_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join("master-graph-schema.json")
}

pub fn cbio_demo_mapping_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("cbioportal")
        .join("cbio_demo_mapping.json")
}

#[allow(dead_code)]
pub async fn register_cbio_demo_sources(engine: &Engine, fixture_dir: &Path) -> Result<()> {
    for (id, file) in [
        ("patients", "patients.tsv"),
        ("samples", "samples.tsv"),
        ("genes", "genes.tsv"),
        ("mutations", "mutations.tsv"),
        ("cna", "cna.tsv"),
        ("expression", "expression.tsv"),
    ] {
        eprintln!("[cbio-demo] registering source {id} from {file}");
        engine
            .register_source(SourceRegistration {
                id: id.to_string(),
                display_name: Some(id.to_string()),
                format: SourceFormat::Tsv,
                location: SourceLocation::Local {
                    path: fixture_dir.join(file).to_string_lossy().to_string(),
                },
                read_options: Default::default(),
            })
            .await?;
    }
    Ok(())
}

#[allow(dead_code)]
pub fn build_cbio_demo_graph_schema_spec(engine: &Engine) -> Result<GraphSchemaSpec> {
    let manifest = load_mapping_manifest(&cbio_demo_mapping_path())?;
    graph_schema_spec_from_manifest(
        engine,
        manifest,
        Some("cbio_public_raw_schema".to_string()),
        None,
        Some("master".to_string()),
    )
}

#[allow(dead_code)]
pub async fn build_cbio_demo_suggested_graph_schema_spec(
    engine: &Engine,
    graph_schema: &CompiledGraphSchema,
) -> Result<GraphSchemaSpec> {
    let suggestion = engine
        .suggest_graph_mapping_with_schema(
            "cbio_demo_builder".to_string(),
            Some("cBio demo".to_string()),
            vec![
                "patients".to_string(),
                "samples".to_string(),
                "genes".to_string(),
                "mutations".to_string(),
                "cna".to_string(),
                "expression".to_string(),
            ],
            Some("master".to_string()),
            Some(1000),
            Some(graph_schema),
        )
        .await?;
    graph_schema_spec_from_manifest(
        engine,
        suggestion.manifest,
        Some("cbio_demo_builder".to_string()),
        Some("cbio_demo_builder".to_string()),
        Some("master".to_string()),
    )
}

#[allow(dead_code)]
pub fn graph_schema_spec_from_manifest(
    engine: &Engine,
    manifest: GraphMappingManifest,
    id: Option<String>,
    graph: Option<String>,
    bound_schema: Option<String>,
) -> Result<GraphSchemaSpec> {
    let nodes = manifest
        .vertices
        .into_iter()
        .map(|vertex| {
            Ok(GraphNodeSpec {
                source: default_table_ref(engine, &vertex.source)?,
                label: vertex.label.clone(),
                id: vertex.id,
                predicate: vertex.predicate,
                columns: vertex.columns,
                props: vertex.props,
                prop_types: vertex.prop_types,
                schema_entity: Some(vertex.label),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let edges = manifest
        .edges
        .into_iter()
        .map(|edge| {
            Ok(GraphEdgeSpec {
                source: default_table_ref(engine, &edge.source)?,
                label: edge.label.clone(),
                from_label: edge.from_label,
                to_label: edge.to_label,
                from: edge.from,
                to: edge.to,
                id: edge.id,
                predicate: edge.predicate,
                columns: edge.columns,
                props: edge.props,
                prop_types: edge.prop_types,
                schema_relation: Some(edge.label),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(GraphSchemaSpec {
        version: manifest.version,
        id,
        graph: graph.or(manifest.graph).unwrap_or_else(|| "cbio_public_raw".to_string()),
        display_name: manifest.display_name,
        bound_schema,
        nodes,
        edges,
        metadata: manifest.metadata,
    })
}

#[allow(dead_code)]
fn default_table_ref(engine: &Engine, source_id: &str) -> Result<SourceTableRef> {
    let table = engine
        .list_source_tables(source_id)?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("source `{source_id}` has no logical tables"))?;
    Ok(SourceTableRef {
        source_id: source_id.to_string(),
        table_id: table.id,
    })
}

pub fn read_json_array(path: &Path) -> Result<Vec<Value>> {
    let value: Value = serde_json::from_str(&std::fs::read_to_string(path)?)?;
    match value {
        Value::Array(rows) => Ok(rows),
        other => Err(anyhow::anyhow!(
            "expected JSON array in {}, found {}",
            path.display(),
            other
        )),
    }
}

fn json_scalar_string(row: &Value, field: &str) -> Result<String> {
    let value = row
        .get(field)
        .ok_or_else(|| anyhow::anyhow!("missing field `{field}` in row {row}"))?;
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Number(number) => Ok(number.to_string()),
        Value::Bool(boolean) => Ok(boolean.to_string()),
        Value::Null => Ok(String::new()),
        other => Err(anyhow::anyhow!(
            "field `{field}` was not scalar in row {row}: {other}"
        )),
    }
}

fn json_nested_scalar_string(row: &Value, parent: &str, field: &str) -> Result<String> {
    let nested = row
        .get(parent)
        .ok_or_else(|| anyhow::anyhow!("missing field `{parent}` in row {row}"))?;
    let nested_object = nested.as_object().ok_or_else(|| {
        anyhow::anyhow!("field `{parent}` was not an object in row {row}: {nested}")
    })?;
    let value = nested_object.get(field).ok_or_else(|| {
        anyhow::anyhow!("missing nested field `{parent}.{field}` in row {row}")
    })?;
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Number(number) => Ok(number.to_string()),
        Value::Bool(boolean) => Ok(boolean.to_string()),
        Value::Null => Ok(String::new()),
        other => Err(anyhow::anyhow!(
            "nested field `{parent}.{field}` was not scalar in row {row}: {other}"
        )),
    }
}

fn write_simple_tsv(path: &Path, header: &[&str], rows: &[Vec<String>]) -> Result<()> {
    let mut body = String::new();
    body.push_str(&header.join("\t"));
    body.push('\n');
    for row in rows {
        body.push_str(&row.join("\t"));
        body.push('\n');
    }
    std::fs::write(path, body)?;
    Ok(())
}

pub fn log_json_probe(prefix: &str, rows: &[Value], fields: &[&str], limit: usize) {
    eprintln!(
        "[cbio-demo] {prefix}: showing {} of {} row(s)",
        rows.len().min(limit),
        rows.len()
    );
    for (index, row) in rows.iter().take(limit).enumerate() {
        let mut values = Vec::new();
        for field in fields {
            let display = row
                .get(*field)
                .and_then(|value| {
                    value.as_str().map(ToOwned::to_owned).or_else(|| {
                        if value.is_number() || value.is_boolean() {
                            Some(value.to_string())
                        } else {
                            None
                        }
                    })
                })
                .unwrap_or_else(|| "<missing>".to_string());
            values.push(format!("{field}={display}"));
        }
        eprintln!("[cbio-demo]   {prefix}[{index}] {}", values.join(" "));
    }
}

pub fn materialize_cbio_demo_fixture(output_dir: &Path) -> Result<()> {
    let demo_dir = cbio_demo_dir();
    anyhow::ensure!(
        demo_dir.exists(),
        "local cbio-demo fixture not found at {}",
        demo_dir.display()
    );
    std::fs::create_dir_all(output_dir)?;
    eprintln!(
        "[cbio-demo] materializing composite fixture from {} into {}",
        demo_dir.display(),
        output_dir.display()
    );

    let patients = read_json_array(&demo_dir.join("patients.json"))?;
    let samples = read_json_array(&demo_dir.join("samples.json"))?;
    let tp53_mutations = read_json_array(&demo_dir.join("tp53_mutations.json"))?;
    let pik3ca_mutations = read_json_array(&demo_dir.join("pik3ca_mutations.json"))?;
    let gata3_mutations = read_json_array(&demo_dir.join("gata3_mutations.json"))?;
    let tp53_cna = read_json_array(&demo_dir.join("tp53_cna.json"))?;
    let pik3ca_cna = read_json_array(&demo_dir.join("pik3ca_cna.json"))?;
    let tp53_expr = read_json_array(&demo_dir.join("tp53_expr.json"))?;
    let pik3ca_expr = read_json_array(&demo_dir.join("pik3ca_expr.json"))?;

    eprintln!(
        "[cbio-demo] rows patients={} samples={} tp53_mut={} pik3ca_mut={} gata3_mut={} tp53_cna={} pik3ca_cna={} tp53_expr={} pik3ca_expr={}",
        patients.len(),
        samples.len(),
        tp53_mutations.len(),
        pik3ca_mutations.len(),
        gata3_mutations.len(),
        tp53_cna.len(),
        pik3ca_cna.len(),
        tp53_expr.len(),
        pik3ca_expr.len()
    );
    log_json_probe("patients.json", &patients, &["patientId", "studyId"], 2);
    log_json_probe(
        "samples.json",
        &samples,
        &["sampleId", "patientId", "sampleType", "sequenced"],
        2,
    );
    log_json_probe(
        "tp53_mutations.json",
        &tp53_mutations,
        &["sampleId", "patientId", "proteinChange", "mutationType"],
        2,
    );
    log_json_probe(
        "tp53_cna.json",
        &tp53_cna,
        &["sampleId", "patientId", "value"],
        2,
    );
    log_json_probe(
        "tp53_expr.json",
        &tp53_expr,
        &["sampleId", "patientId", "value"],
        2,
    );

    let patient_rows = patients
        .iter()
        .map(|row| {
            Ok(vec![
                json_scalar_string(row, "patientId")?,
                json_scalar_string(row, "studyId")?,
            ])
        })
        .collect::<Result<Vec<_>>>()?;
    write_simple_tsv(
        &output_dir.join("patients.tsv"),
        &["patient_id", "study_id"],
        &patient_rows,
    )?;

    let sample_rows = samples
        .iter()
        .map(|row| {
            Ok(vec![
                json_scalar_string(row, "sampleId")?,
                json_scalar_string(row, "patientId")?,
                json_scalar_string(row, "sampleType")?,
                json_scalar_string(row, "sequenced")?,
                json_scalar_string(row, "studyId")?,
            ])
        })
        .collect::<Result<Vec<_>>>()?;
    write_simple_tsv(
        &output_dir.join("samples.tsv"),
        &["sample_id", "patient_id", "sample_type", "sequenced", "study_id"],
        &sample_rows,
    )?;

    let mutation_sources = [
        ("TP53", &tp53_mutations),
        ("PIK3CA", &pik3ca_mutations),
        ("GATA3", &gata3_mutations),
    ];
    let mut mutation_rows = Vec::new();
    for (gene_hint, rows) in mutation_sources {
        for (index, row) in rows.iter().enumerate() {
            let gene_symbol = json_nested_scalar_string(row, "gene", "hugoGeneSymbol")?;
            anyhow::ensure!(
                gene_symbol == gene_hint,
                "expected mutation gene {gene_hint}, found {gene_symbol}"
            );
            mutation_rows.push(vec![
                format!("mutation-{gene_symbol}-{index}"),
                json_scalar_string(row, "sampleId")?,
                json_scalar_string(row, "patientId")?,
                gene_symbol,
                "somatic-variant".to_string(),
                json_scalar_string(row, "proteinChange")?,
                "final".to_string(),
                json_scalar_string(row, "studyId")?,
                json_scalar_string(row, "referenceAllele")?,
                json_scalar_string(row, "variantAllele")?,
                json_scalar_string(row, "chr")?,
                json_scalar_string(row, "startPosition")?,
                json_scalar_string(row, "endPosition")?,
                json_scalar_string(row, "mutationType")?,
            ]);
        }
    }
    write_simple_tsv(
        &output_dir.join("mutations.tsv"),
        &[
            "observation_id",
            "sample_id",
            "patient_id",
            "gene_symbol",
            "code",
            "value_string",
            "status",
            "study_id",
            "reference_allele",
            "variant_allele",
            "chromosome",
            "start_position",
            "end_position",
            "mutation_type",
        ],
        &mutation_rows,
    )?;

    let cna_sources = [("TP53", &tp53_cna), ("PIK3CA", &pik3ca_cna)];
    let mut cna_rows = Vec::new();
    for (gene_hint, rows) in cna_sources {
        for (index, row) in rows.iter().enumerate() {
            let gene_symbol = json_nested_scalar_string(row, "gene", "hugoGeneSymbol")?;
            anyhow::ensure!(
                gene_symbol == gene_hint,
                "expected CNA gene {gene_hint}, found {gene_symbol}"
            );
            cna_rows.push(vec![
                format!("cna-{gene_symbol}-{index}"),
                json_scalar_string(row, "sampleId")?,
                json_scalar_string(row, "patientId")?,
                gene_symbol,
                "copy-number".to_string(),
                json_scalar_string(row, "value")?,
                "final".to_string(),
                json_scalar_string(row, "studyId")?,
            ]);
        }
    }
    write_simple_tsv(
        &output_dir.join("cna.tsv"),
        &[
            "observation_id",
            "sample_id",
            "patient_id",
            "gene_symbol",
            "code",
            "value_string",
            "status",
            "study_id",
        ],
        &cna_rows,
    )?;

    let expr_sources = [("TP53", &tp53_expr), ("PIK3CA", &pik3ca_expr)];
    let mut expr_rows = Vec::new();
    for (gene_hint, rows) in expr_sources {
        for (index, row) in rows.iter().enumerate() {
            let gene_symbol = json_nested_scalar_string(row, "gene", "hugoGeneSymbol")?;
            anyhow::ensure!(
                gene_symbol == gene_hint,
                "expected expression gene {gene_hint}, found {gene_symbol}"
            );
            expr_rows.push(vec![
                format!("expr-{gene_symbol}-{index}"),
                json_scalar_string(row, "sampleId")?,
                json_scalar_string(row, "patientId")?,
                gene_symbol,
                "expression-zscore".to_string(),
                json_scalar_string(row, "value")?,
                "final".to_string(),
                json_scalar_string(row, "studyId")?,
            ]);
        }
    }
    write_simple_tsv(
        &output_dir.join("expression.tsv"),
        &[
            "observation_id",
            "sample_id",
            "patient_id",
            "gene_symbol",
            "code",
            "value_string",
            "status",
            "study_id",
        ],
        &expr_rows,
    )?;

    let mut gene_symbols = BTreeSet::new();
    for rows in [
        &tp53_mutations,
        &pik3ca_mutations,
        &gata3_mutations,
        &tp53_cna,
        &pik3ca_cna,
        &tp53_expr,
        &pik3ca_expr,
    ] {
        for row in rows {
            gene_symbols.insert(json_nested_scalar_string(row, "gene", "hugoGeneSymbol")?);
        }
    }
    let gene_rows = gene_symbols
        .into_iter()
        .map(|symbol| {
            vec![
                symbol.clone(),
                symbol,
                "brca_tcga_pan_can_atlas_2018".to_string(),
            ]
        })
        .collect::<Vec<_>>();
    write_simple_tsv(
        &output_dir.join("genes.tsv"),
        &["gene_symbol", "submitter_id", "project_id"],
        &gene_rows,
    )?;

    eprintln!(
        "[cbio-demo] wrote patients.tsv={} samples.tsv={} genes.tsv={} mutations.tsv={} cna.tsv={} expression.tsv={}",
        patient_rows.len(),
        sample_rows.len(),
        gene_rows.len(),
        mutation_rows.len(),
        cna_rows.len(),
        expr_rows.len()
    );
    eprintln!(
        "[cbio-demo] multimodal overlap target: TP53 mutation + TP53 CNA + TP53 expression on the same specimen"
    );
    Ok(())
}

fn strip_external_links_property_refs(value: &mut Value, removed: &mut usize) {
    match value {
        Value::Object(object) => {
            if let Some(properties) = object.get_mut("properties").and_then(Value::as_object_mut) {
                let should_remove = properties
                    .get("links")
                    .and_then(Value::as_object)
                    .and_then(|schema| schema.get("items"))
                    .and_then(Value::as_object)
                    .and_then(|items| items.get("$ref"))
                    .and_then(Value::as_str)
                    == Some("https://json-schema.org/draft/2020-12/links");
                if should_remove {
                    properties.remove("links");
                    *removed += 1;
                }
            }
            for child in object.values_mut() {
                strip_external_links_property_refs(child, removed);
            }
        }
        Value::Array(items) => {
            for item in items {
                strip_external_links_property_refs(item, removed);
            }
        }
        _ => {}
    }
}

pub fn load_sanitized_master_graph_schema() -> Result<Value> {
    let mut schema_doc: Value = serde_json::from_str(&std::fs::read_to_string(master_schema_path())?)?;
    let mut removed = 0;
    strip_external_links_property_refs(&mut schema_doc, &mut removed);
    eprintln!("[cbio-demo] stripped {removed} external Hyper-Schema links property refs");
    Ok(schema_doc)
}
