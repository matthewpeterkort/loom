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
const TARGET_LABEL: &str = "Patient";
const DEFAULT_CACHE_DIR: &str = "/tmp/loom-calypr-bench";
const DEFAULT_SAMPLE_EVERY: usize = 1;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "real-world benchmark; run explicitly"]
async fn calypr_real_world_haslabel_benchmark() {
    let meta_dir =
        std::env::var("CALYPR_META_DIR").unwrap_or_else(|_| DEFAULT_CALYPR_META_DIR.to_string());
    let meta_path = Path::new(&meta_dir);
    assert!(meta_path.exists(), "META dir not found: {meta_dir}");

    let cache_dir =
        std::env::var("CALYPR_BENCH_CACHE_DIR").unwrap_or_else(|_| DEFAULT_CACHE_DIR.to_string());
    let mode = std::env::var("CALYPR_BENCH_MODE").unwrap_or_else(|_| "full".to_string());
    let sample_every = parse_positive_usize_env("CALYPR_BENCH_SAMPLE_EVERY", DEFAULT_SAMPLE_EVERY);
    let use_cache = std::env::var("CALYPR_BENCH_CACHE")
        .ok()
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE"))
        .unwrap_or(false)
        || sample_every > 1;

    let tmp = TempDir::new().expect("tempdir");
    let work_root = if use_cache {
        std::fs::create_dir_all(&cache_dir).expect("create cache dir");
        Path::new(&cache_dir).to_path_buf()
    } else {
        tmp.path().to_path_buf()
    };
    let sample_dir = work_root.join(format!("sampled_meta_x{sample_every}"));
    let vertices_parquet = work_root.join("sampled_vertices.parquet");
    let edges_parquet = work_root.join("sampled_edges.parquet");
    let vortex_root = work_root.join("vortex");
    std::fs::create_dir_all(&vortex_root).expect("create vortex root");

    let (load_meta_dir, prep_stats, prep_secs) = if sample_every > 1 {
        let t_prep = Instant::now();
        let prep_stats =
            write_sampled_meta_subset(meta_path, &sample_dir, sample_every).expect("sample meta");
        let prep_secs = t_prep.elapsed().as_secs_f64();
        (sample_dir, prep_stats, prep_secs)
    } else {
        let prep_stats = infer_prep_stats_from_meta(meta_path).expect("infer prep stats");
        (meta_path.to_path_buf(), prep_stats, 0.0)
    };
    let ndjson_input_bytes = prep_stats.ndjson_input_bytes;
    let parquet_input_bytes = 0;
    let expected_observation = *prep_stats
        .loaded_label_counts
        .get(TARGET_LABEL)
        .unwrap_or(&0);

    let engine = Engine::new(EngineConfig {
        work_dir: "/tmp".to_string(),
    })
    .unwrap();
    let graph = if sample_every > 1 {
        format!("calypr_x{sample_every}")
    } else {
        "calypr".to_string()
    };

    let graph_vertices_dir = vortex_root.join(&graph).join("vertices");
    let graph_vertices_arrow = graph_vertices_dir.join("vertices.arrow");
    let graph_vertices_vortex = graph_vertices_dir.join("vertices.vortex");
    let graph_ready = graph_vertices_arrow.exists() && graph_vertices_vortex.exists();
    let effective_mode = if mode == "query_only" || graph_ready {
        "query_only"
    } else {
        "full"
    };

    let load_secs = if effective_mode == "query_only" {
        engine
            .register_graph_vortex(
                graph.as_str(),
                vortex_root.join(&graph).join("vertices").to_str().unwrap(),
                vortex_root.join(&graph).join("edges").to_str().unwrap(),
            )
            .await
            .expect("register existing vortex graph");
        0.0
    } else {
        let load_mode = IngestMode::Overwrite;
        write_vertices_from_meta_ndjson(load_meta_dir.as_path(), &vertices_parquet, TARGET_LABEL)
            .expect("write sampled parquet");
        write_empty_edges_parquet(&edges_parquet).expect("write sampled edges parquet");
        let t_load = Instant::now();
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
        t_load.elapsed().as_secs_f64()
    };

    let query = Query {
        graph: graph.to_string(),
        steps: vec![
            Step::V { ids: vec![] },
            Step::HasLabel {
                labels: vec![TARGET_LABEL.to_string()],
            },
            Step::Render {
                fields: vec!["id".to_string()],
            },
        ],
    };

    let t_query_batches = Instant::now();
    let batches = engine.query_batches(&query).await.expect("collect batches");
    let query_batches_secs = t_query_batches.elapsed().as_secs_f64();
    let output_bytes = estimate_json_output_bytes(&batches).unwrap_or_else(|err| {
        eprintln!("JSON byte estimate skipped: {err}");
        0
    });

    let t_query_serde = Instant::now();
    let rows_serde = engine
        .query_json_rows(&query)
        .await
        .map_err(|e| {
            eprintln!("Query Error: {:?}", e);
            e
        })
        .expect("run hasLabel query serde");

    let row_count = rows_serde.len();
    eprintln!("=== Query Results for {} ===", TARGET_LABEL);
    eprintln!("Total rows returned: {}", row_count);

    let print_rows = parse_positive_usize_env("CALYPR_BENCH_PRINT_ROWS", 25);
    if print_rows > 0 {
        print_sampled_source_rows(&load_meta_dir, print_rows).expect("print sampled source rows");
    }

    let query_serde_secs = t_query_serde.elapsed().as_secs_f64();

    let t_query_sonic = Instant::now();
    let rows = engine
        .query_json_rows_sonic(&query)
        .await
        .expect("run hasLabel query sonic");
    let query_sonic_secs = t_query_sonic.elapsed().as_secs_f64();
    let query_json_secs = (query_sonic_secs - query_batches_secs).max(0.0);
    let query_secs = query_sonic_secs;

    let t_query_ndjson = Instant::now();
    let ndjson_chunks = engine
        .query_ndjson_chunks(&query)
        .await
        .expect("run hasLabel query ndjson chunks");
    let query_ndjson_secs = t_query_ndjson.elapsed().as_secs_f64();
    let ndjson_output_bytes = ndjson_chunks
        .iter()
        .map(|c: &Vec<u8>| c.len() as u64)
        .sum::<u64>();
    let ndjson_rows = ndjson_chunks
        .iter()
        .map(|c: &Vec<u8>| c.iter().filter(|b| **b == b'\n').count())
        .sum::<usize>();

    assert_eq!(
        rows_serde.len(),
        expected_observation,
        "expected {} rows from V().hasLabel(\"{}\") via serde path",
        expected_observation,
        TARGET_LABEL
    );
    assert_eq!(
        rows.len(),
        expected_observation,
        "expected {} rows from V().hasLabel(\"{}\") via sonic path",
        expected_observation,
        TARGET_LABEL
    );
    assert_eq!(
        ndjson_rows, expected_observation,
        "expected {} NDJSON rows from V().hasLabel(\"{}\")",
        expected_observation, TARGET_LABEL
    );

    let load_rps = if load_secs > 0.0 {
        prep_stats.total_loaded_records as f64 / load_secs.max(1e-9)
    } else {
        0.0
    };
    let query_rps = rows.len() as f64 / query_secs.max(1e-9);
    let ingest_ndjson_mbps = if load_secs > 0.0 {
        (ndjson_input_bytes as f64 * 8.0) / load_secs.max(1e-9) / 1_000_000.0
    } else {
        0.0
    };
    let ingest_parquet_mbps = if load_secs > 0.0 {
        (parquet_input_bytes as f64 * 8.0) / load_secs.max(1e-9) / 1_000_000.0
    } else {
        0.0
    };
    let query_mbps = (output_bytes as f64 * 8.0) / query_secs.max(1e-9) / 1_000_000.0;

    eprintln!(
        "\nCALYPR Real-World Benchmark\n\
        - meta_dir: {meta}\n\
        - sample_every: {sample_every}\n\
        - mode: {mode}\n\
        - effective_mode: {effective_mode}\n\
        - cache_enabled: {cache}\n\
        - cache_dir: {cache_dir}\n\
        - load_meta_dir: {load_meta_dir}\n\
        - total_source_records: {total_source}\n\
        - total_loaded_records: {total_loaded}\n\
        - observation_records: {obs}\n\
        - prep_seconds_sampled_meta: {prep_secs:.4}\n\
        - load_seconds_parquet_to_vortex: {load_secs:.4}\n\
        - load_records_per_sec: {load_rps:.2}\n\
        - ingest_ndjson_megabits_per_sec: {ingest_ndjson_mbps:.2}\n\
         - ingest_parquet_megabits_per_sec: {ingest_parquet_mbps:.2}\n\
         - query_seconds_haslabel_observation: {query_secs:.4}\n\
         - query_seconds_batches_collect: {query_batches_secs:.4}\n\
         - query_seconds_json_materialization_sonic: {query_json_secs:.4}\n\
         - query_seconds_full_serde_path: {query_serde_secs:.4}\n\
         - query_seconds_full_sonic_path: {query_sonic_secs:.4}\n\
         - query_seconds_ndjson_chunks: {query_ndjson_secs:.4}\n\
         - query_rows_per_sec: {query_rps:.2}\n\
         - ndjson_rows_per_sec: {ndjson_rps:.2}\n\
         - query_output_megabits_per_sec: {query_mbps:.2}\n\
         - ndjson_output_megabits_per_sec: {ndjson_mbps:.2}\n\
         - ndjson_input_bytes: {ndjson_input_bytes}\n\
         - parquet_input_bytes: {parquet_input_bytes}\n\
         - query_output_bytes: {output_bytes}\n",
        meta = meta_dir,
        sample_every = sample_every,
        mode = mode,
        effective_mode = effective_mode,
        cache = use_cache,
        cache_dir = cache_dir,
        load_meta_dir = load_meta_dir.to_string_lossy(),
        total_source = prep_stats.total_source_records,
        total_loaded = prep_stats.total_loaded_records,
        obs = expected_observation,
        prep_secs = prep_secs,
        load_secs = load_secs,
        load_rps = load_rps,
        ingest_ndjson_mbps = ingest_ndjson_mbps,
        ingest_parquet_mbps = ingest_parquet_mbps,
        query_secs = query_secs,
        query_batches_secs = query_batches_secs,
        query_json_secs = query_json_secs,
        query_serde_secs = query_serde_secs,
        query_sonic_secs = query_sonic_secs,
        query_ndjson_secs = query_ndjson_secs,
        query_rps = query_rps,
        ndjson_rps = expected_observation as f64 / query_ndjson_secs.max(1e-9),
        query_mbps = query_mbps,
        ndjson_mbps =
            (ndjson_output_bytes as f64 * 8.0) / query_ndjson_secs.max(1e-9) / 1_000_000.0,
        ndjson_input_bytes = ndjson_input_bytes,
        parquet_input_bytes = parquet_input_bytes,
        output_bytes = output_bytes,
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

fn print_sampled_source_rows(meta_dir: &Path, limit: usize) -> anyhow::Result<()> {
    eprintln!("=== Sampled Source Rows (first {limit}) ===");
    let mut files = std::fs::read_dir(meta_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("ndjson"))
        .collect::<Vec<_>>();
    files.sort();

    let mut printed = 0usize;
    for ndjson in files {
        let label = ndjson
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("Unknown")
            .to_string();
        let reader = BufReader::new(File::open(&ndjson)?);
        for line in reader.lines() {
            let line = line?;
            eprintln!("[{printed}] {label}: {line}");
            printed += 1;
            if printed >= limit {
                return Ok(());
            }
        }
    }

    if printed < limit {
        eprintln!("... truncated {} additional rows", limit - printed);
    }

    Ok(())
}

fn write_vertices_from_meta_ndjson(
    meta_dir: &Path,
    out_parquet: &Path,
    target_label: &str,
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
        for line in reader.lines() {
            let line = line?;
            let parsed: serde_json::Value = serde_json::from_str(&line)?;
            let id = parsed
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{label}-{target_label}"));
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

fn write_sampled_meta_subset(
    meta_dir: &Path,
    out_dir: &Path,
    sample_every: usize,
) -> anyhow::Result<PrepStats> {
    assert!(sample_every > 0, "sample_every must be positive");
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

            if (per_label_counter - 1) % sample_every != 0 {
                continue;
            }

            writeln!(writer, "{line}")?;
            stats.total_loaded_records += 1;
            *stats.loaded_label_counts.entry(label.clone()).or_insert(0) += 1;
        }
    }

    Ok(stats)
}

fn estimate_json_output_bytes(batches: &[RecordBatch]) -> anyhow::Result<u64> {
    if batches.is_empty() {
        return Ok(2);
    }
    let refs = batches.iter().collect::<Vec<&RecordBatch>>();
    let mut writer = arrow_json::ArrayWriter::new(Vec::<u8>::new());
    writer.write_batches(&refs)?;
    writer.finish()?;
    Ok(writer.into_inner().len() as u64)
}
