use arrow::array::StringArray;
use arrow::record_batch::RecordBatch;
use loom_engine::{Engine, EngineConfig, IngestMode};
use loomql_ast::{Query, Step};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tempfile::TempDir;

const DEFAULT_CALYPR_META_DIR: &str = "/Users/peterkor/Desktop/BMEG/grip-benchmark/calypr/META";
const DEFAULT_CACHE_DIR: &str = "/tmp/loom-calypr-equivalence";
const DEFAULT_SAMPLE_MOD_1: usize = 5;
const DEFAULT_SAMPLE_MOD_2: usize = 9;
const OBSERVATION_LABEL: &str = "Observation";

#[tokio::test(flavor = "multi_thread")]
#[ignore = "real-world equivalence test; run explicitly"]
async fn calypr_load_and_first_four_queries() {
    let meta_dir =
        std::env::var("CALYPR_META_DIR").unwrap_or_else(|_| DEFAULT_CALYPR_META_DIR.to_string());
    let meta_path = Path::new(&meta_dir);
    assert!(meta_path.exists(), "META dir not found: {meta_dir}");

    let sample_mod_1 = parse_positive_usize_env("CALYPR_SAMPLE_MOD_1", DEFAULT_SAMPLE_MOD_1);
    let sample_mod_2 = parse_positive_usize_env("CALYPR_SAMPLE_MOD_2", DEFAULT_SAMPLE_MOD_2);
    let cache_dir =
        std::env::var("CALYPR_CACHE_DIR").unwrap_or_else(|_| DEFAULT_CACHE_DIR.to_string());
    let use_cache = std::env::var("CALYPR_CACHE")
        .ok()
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE"))
        .unwrap_or(false);

    let tmp = TempDir::new().expect("tempdir");
    let work_root = if use_cache {
        std::fs::create_dir_all(&cache_dir).expect("create cache dir");
        Path::new(&cache_dir).to_path_buf()
    } else {
        tmp.path().to_path_buf()
    };

    let sample_dir = work_root.join(format!("sampled_meta_mod_{sample_mod_1}_{sample_mod_2}"));
    let vertices_parquet = work_root.join("sampled_vertices.parquet");
    let edges_parquet = work_root.join("sampled_edges.parquet");
    let vortex_root = work_root.join("vortex");
    std::fs::create_dir_all(&vortex_root).expect("create vortex root");

    let (load_meta_dir, sample_stats) = if use_cache && sample_dir.exists() {
        let stats = infer_prep_stats_from_meta(&sample_dir).expect("infer cached prep stats");
        (sample_dir, stats)
    } else {
        let stats = write_sampled_meta_subset(meta_path, &sample_dir, sample_mod_1, sample_mod_2)
            .expect("sample meta");
        (sample_dir, stats)
    };

    let graph = format!("calypr_eq_typed_mod_{sample_mod_1}_{sample_mod_2}");

    let graph_vertices_dir = vortex_root.join(&graph).join("vertices");
    let graph_ready = graph_vertices_dir.join("vertices.arrow").exists()
        && graph_vertices_dir.join("vertices.vortex").exists();

    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })
    .unwrap();
    let load_mode = IngestMode::Overwrite;
    let load_started = Instant::now();
    if !graph_ready {
        let prep_started = Instant::now();
        write_vertices_from_sampled_meta_ndjson(&load_meta_dir, &vertices_parquet)
            .expect("write sampled parquet");
        let prep_seconds = prep_started.elapsed().as_secs_f64();
        write_empty_edges_parquet(&edges_parquet).expect("write empty edges parquet");
        let ingest_started = Instant::now();
        engine
            .ingest_parquet_graph_to_vortex(
                graph.as_str(),
                vertices_parquet.to_str().unwrap(),
                edges_parquet.to_str().unwrap(),
                vortex_root.to_str().unwrap(),
                load_mode,
            )
            .await
            .expect("bulk load from parquet to vortex");
        let ingest_seconds = ingest_started.elapsed().as_secs_f64();
        eprintln!("load_seconds_sample_prep: {prep_seconds:.4}");
        eprintln!("load_seconds_parquet_to_vortex: {ingest_seconds:.4}");
    } else {
        let register_started = Instant::now();
        engine
            .register_graph_vortex(
                graph.as_str(),
                vortex_root.join(&graph).join("vertices").to_str().unwrap(),
                vortex_root.join(&graph).join("edges").to_str().unwrap(),
            )
            .await
            .expect("register sampled graph");
        let register_seconds = register_started.elapsed().as_secs_f64();
        eprintln!("load_seconds_register_graph: {register_seconds:.4}");
    }
    eprintln!(
        "load_seconds_total: {:.4}",
        load_started.elapsed().as_secs_f64()
    );

    let total_count_query = Query {
        graph: graph.clone(),
        steps: vec![Step::V { ids: vec![] }, Step::Count],
    };
    let observation_count_query = Query {
        graph: graph.clone(),
        steps: vec![
            Step::V { ids: vec![] },
            Step::HasLabel {
                labels: vec![OBSERVATION_LABEL.to_string()],
            },
            Step::Count,
        ],
    };
    let observation_projection_query = Query {
        graph: graph.clone(),
        steps: vec![
            Step::V { ids: vec![] },
            Step::HasLabel {
                labels: vec![OBSERVATION_LABEL.to_string()],
            },
            Step::Render {
                fields: vec!["id".to_string(), "label".to_string(), "props".to_string()],
            },
        ],
    };
    let observation_scan_query = Query {
        graph: graph.clone(),
        steps: vec![
            Step::V { ids: vec![] },
            Step::HasLabel {
                labels: vec![OBSERVATION_LABEL.to_string()],
            },
        ],
    };

    let total_query_started = Instant::now();
    let total_batches = engine
        .query_batches(&total_count_query)
        .await
        .expect("run total count query batches");
    let total_query_seconds = total_query_started.elapsed().as_secs_f64();
    let total_materialize_started = Instant::now();
    let total_count = extract_count(&batches_to_json_rows(total_batches));
    let total_materialize_seconds = total_materialize_started.elapsed().as_secs_f64();

    let observation_count_started = Instant::now();
    let observation_count_batches = engine
        .query_batches(&observation_count_query)
        .await
        .expect("run observation count query batches");
    let observation_count_seconds = observation_count_started.elapsed().as_secs_f64();
    let observation_count_materialize_started = Instant::now();
    let observation_count = extract_count(&batches_to_json_rows(observation_count_batches));
    let observation_count_materialize_seconds = observation_count_materialize_started
        .elapsed()
        .as_secs_f64();

    let projection_started = Instant::now();
    let projection_batches = engine
        .query_batches(&observation_projection_query)
        .await
        .expect("run observation projection query batches");
    let projection_seconds = projection_started.elapsed().as_secs_f64();
    let projection_materialize_started = Instant::now();
    let projection_rows = batches_to_json_rows(projection_batches);
    let projection_materialize_seconds = projection_materialize_started.elapsed().as_secs_f64();

    let scan_started = Instant::now();
    let scan_batches = engine
        .query_batches(&observation_scan_query)
        .await
        .expect("run observation scan query batches");
    let scan_seconds = scan_started.elapsed().as_secs_f64();
    let scan_materialize_started = Instant::now();
    let scan_rows = batches_to_json_rows(scan_batches);
    let scan_materialize_seconds = scan_materialize_started.elapsed().as_secs_f64();

    let expected_total = sample_stats.total_loaded_records;
    let expected_observation = *sample_stats
        .loaded_label_counts
        .get(OBSERVATION_LABEL)
        .unwrap_or(&0);

    eprintln!("=== Calypr Equivalence Test ===");
    eprintln!("sample_mod_1: {sample_mod_1}");
    eprintln!("sample_mod_2: {sample_mod_2}");
    eprintln!("total_loaded_records: {expected_total}");
    eprintln!("observation_rows: {expected_observation}");
    eprintln!("query_seconds_total_count: {total_query_seconds:.4}");
    eprintln!("materialize_seconds_total_count: {total_materialize_seconds:.4}");
    eprintln!("query_seconds_observation_count: {observation_count_seconds:.4}");
    eprintln!("materialize_seconds_observation_count: {observation_count_materialize_seconds:.4}");
    eprintln!("query_seconds_observation_projection: {projection_seconds:.4}");
    eprintln!("materialize_seconds_observation_projection: {projection_materialize_seconds:.4}");
    eprintln!("query_seconds_observation_scan: {scan_seconds:.4}");
    eprintln!("materialize_seconds_observation_scan: {scan_materialize_seconds:.4}");
    eprintln!("projection_rows: {}", projection_rows.len());
    eprintln!("scan_rows: {}", scan_rows.len());

    assert_eq!(
        total_count, expected_total,
        "V().count() should match loaded sample"
    );
    assert_eq!(
        observation_count, expected_observation,
        "V().hasLabel(\"Observation\").count() should match loaded sample"
    );
    assert_eq!(
        projection_rows.len(),
        expected_observation,
        "projection query should preserve row count"
    );
    assert_eq!(
        scan_rows.len(),
        expected_observation,
        "plain Observation scan should preserve row count"
    );
}

#[derive(Debug, Default)]
struct PrepStats {
    total_source_records: usize,
    total_loaded_records: usize,
    ndjson_input_bytes: u64,
    loaded_label_counts: HashMap<String, usize>,
}

fn infer_prep_stats_from_meta(meta_dir: &Path) -> anyhow::Result<PrepStats> {
    let mut stats = PrepStats::default();
    let mut files = std::fs::read_dir(meta_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("ndjson"))
        .collect::<Vec<_>>();
    files.sort();
    for ndjson in files {
        let label = ndjson
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("Unknown")
            .to_string();
        let reader = BufReader::new(File::open(&ndjson)?);
        for line in reader.lines() {
            let line = line?;
            stats.total_source_records += 1;
            stats.ndjson_input_bytes += line.len() as u64 + 1;
            stats.total_loaded_records += 1;
            *stats.loaded_label_counts.entry(label.clone()).or_insert(0) += 1;
        }
    }
    Ok(stats)
}

fn parse_positive_usize_env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn write_sampled_meta_subset(
    meta_dir: &Path,
    out_dir: &Path,
    sample_mod_1: usize,
    sample_mod_2: usize,
) -> anyhow::Result<PrepStats> {
    assert!(sample_mod_1 > 0, "sample_mod_1 must be positive");
    assert!(sample_mod_2 > 0, "sample_mod_2 must be positive");
    std::fs::create_dir_all(out_dir)?;

    let mut stats = PrepStats::default();
    let mut files = std::fs::read_dir(meta_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("ndjson"))
        .collect::<Vec<_>>();
    files.sort();

    for ndjson in files {
        let label = ndjson
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("Unknown")
            .to_string();
        let out_path = out_dir.join(ndjson.file_name().unwrap());
        let mut writer = BufWriter::new(File::create(out_path)?);
        let reader = BufReader::new(File::open(&ndjson)?);
        let mut per_label_counter: usize = 0;

        for line in reader.lines() {
            let line = line?;
            stats.total_source_records += 1;
            stats.ndjson_input_bytes += line.len() as u64 + 1;
            per_label_counter += 1;

            let keep =
                per_label_counter % sample_mod_1 == 0 || per_label_counter % sample_mod_2 == 0;
            if !keep {
                continue;
            }

            writeln!(writer, "{line}")?;
            stats.total_loaded_records += 1;
            *stats.loaded_label_counts.entry(label.clone()).or_insert(0) += 1;
        }
    }

    Ok(stats)
}

fn write_vertices_from_sampled_meta_ndjson(
    meta_dir: &Path,
    out_parquet: &Path,
) -> anyhow::Result<()> {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("label", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("props", arrow::datatypes::DataType::Utf8, true),
    ]));

    let mut files = std::fs::read_dir(meta_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("ndjson"))
        .collect::<Vec<_>>();
    files.sort();

    let file = File::create(out_parquet)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let mut ids = Vec::<String>::new();
    let mut labels = Vec::<String>::new();
    let mut props_col = Vec::<String>::new();

    for ndjson in files {
        let label = ndjson
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("Unknown")
            .to_string();
        let reader = BufReader::new(File::open(&ndjson)?);
        let mut per_label_counter: usize = 0;
        for line in reader.lines() {
            let line = line?;
            per_label_counter += 1;
            let mut parsed: serde_json::Value = serde_json::from_str(&line)?;
            if let Some(obj) = parsed.as_object_mut() {
                if per_label_counter % 5 == 0 {
                    obj.entry("auth_resource_path".to_string())
                        .or_insert_with(|| {
                            serde_json::Value::String("/programs/calypr/projects/test".to_string())
                        });
                } else if per_label_counter % 9 == 0 {
                    obj.entry("auth_resource_path".to_string())
                        .or_insert_with(|| {
                            serde_json::Value::String(
                                "/programs/calypr/projects/testtwo".to_string(),
                            )
                        });
                }
            }
            let id = parsed
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{label}-row"));
            ids.push(id);
            labels.push(label.clone());
            props_col.push(line);
            if ids.len() >= 8192 {
                flush_vertex_batch(&schema, &mut writer, &mut ids, &mut labels, &mut props_col)?;
            }
        }
    }

    if !ids.is_empty() {
        flush_vertex_batch(&schema, &mut writer, &mut ids, &mut labels, &mut props_col)?;
    }

    writer.close()?;
    Ok(())
}

fn flush_vertex_batch(
    schema: &Arc<arrow::datatypes::Schema>,
    writer: &mut ArrowWriter<File>,
    ids: &mut Vec<String>,
    labels: &mut Vec<String>,
    props_col: &mut Vec<String>,
) -> anyhow::Result<()> {
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(std::mem::take(ids))),
            Arc::new(StringArray::from(std::mem::take(labels))),
            Arc::new(StringArray::from(std::mem::take(props_col))),
        ],
    )?;
    writer.write(&batch)?;
    Ok(())
}

fn write_empty_edges_parquet(path: &Path) -> anyhow::Result<()> {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("from_id", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("to_id", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("label", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("props", arrow::datatypes::DataType::Utf8, true),
    ]));

    let file = File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
            Arc::new(StringArray::from(Vec::<String>::new())),
        ],
    )?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn extract_count(rows: &[String]) -> usize {
    rows.iter()
        .find_map(|row| {
            let v: serde_json::Value = serde_json::from_str(row).ok()?;
            v.get("count").and_then(|c| c.as_u64()).map(|n| n as usize)
        })
        .unwrap_or(0)
}

fn batches_to_json_rows(batches: Vec<RecordBatch>) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in batches {
        let mut buffer = Vec::new();
        {
            let mut writer = arrow_json::writer::LineDelimitedWriter::new(&mut buffer);
            writer.write(&batch).expect("serialize batch to json");
        }
        let s = String::from_utf8(buffer).expect("utf8 json batch");
        for line in s.lines() {
            rows.push(line.to_string());
        }
    }
    rows
}
