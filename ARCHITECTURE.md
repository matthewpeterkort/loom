# Loom Architecture (Current State)

This document describes the architecture that exists in the repository today (`loom` workspace), including package boundaries, execution/data flow, storage model, schema-aware paths, and known constraints.

## 1. System Intent

`loom` is a Rust graph query stack built around:

- JSON query payloads as the query frontend format.
- direct lowering to DataFusion `LogicalPlan` inside `loom-engine` (no SQL parser translation stage).
- DataFusion execution as the query runtime.
- Vortex as persisted table storage format.
- HTTP + Arrow Flight serving layers.

The current implementation is optimized for pragmatic progress on traversal + analytical workloads, not ACID OLTP behavior.

## 2. Workspace Layout and Responsibilities

Workspace members are defined in root `Cargo.toml`:

- `loom-engine`
- `loom-server`

### 2.1 Query Handling

Purpose: parse JSON payloads and convert them to DataFusion logical plans.

Key design:

- Defines a `LoweringContext` trait for graph-to-table name resolution:
  - `table_vertices(graph)`
  - `table_edges(graph)`
- `lower_to_logical_plan(session, query, ctx)` performs step-by-step DataFrame transformations.

Traversal semantics implemented via joins/unions:

- `V(ids)` resets stream to vertices (with optional id filter).
- `Out/In/Both` behave differently depending on stream kind (vertex stream vs edge stream).
- `OutE/InE/BothE` require vertex stream and transition to edge stream.

Filter/render semantics:

- `Has` supports string/number/bool/null equality (`null` -> `is_null`).
- `HasLabel` / `HasId` are `IN` filters.
- `Render` projects explicit fields or `*` expansion.
- `Count` emits aggregate `count(1) as count`.

Important constraint:

- Lowering is on DataFusion columns already present in registered tables.
- Property path intelligence (schema-aware rewriting) is not in lowering; it is done in `loom-server` before execution.

### 2.2 `loom-engine`

Purpose: execution shell + storage registration + ingest into Vortex.

Major responsibilities:

1. Owns `SessionContext` configured with `vortex-datafusion` `FileFormatFactory`.
2. Owns in-memory graph catalog (`GraphCatalog`) mapping graph name -> table names.
3. Registers graph tables from Parquet or Vortex locations.
4. Ingests Parquet into Vortex files.
5. Compiles and executes lowered plans.
6. Serializes execution output to JSON array rows or NDJSON chunks.

Notable structs:

- `Engine`
- `GraphCatalog`
- `GraphTables`
- `VertexColumn` (name + SQL-like type declaration)
- `GraphStorageUris`
- `IngestMode` (`Create`, `Append`, `Overwrite`)

### 2.3 `loom-server`

Purpose: public API and schema-aware orchestration.

Responsibilities:

1. HTTP API (`axum`) for query/schema/ingest endpoints.
2. Arrow Flight service (`tonic`) for `DoGet` streaming.
3. Schema registry and graph->schema bindings.
4. JSON Schema -> Arrow shape compilation.
5. Promoted typed column derivation and schema-aware query rewrite.
6. NDJSON + schema ingest preprocessing (typed vertex parquet + extracted edge parquet).
7. Full-resource reconstruction modes from typed projected columns.
8. Persistence of schema metadata and graph bindings to local JSON file.

## 3. Query Execution Architecture

## 3.1 End-to-End Query Pipeline

1. HTTP/Flight receives query JSON.
2. Parse to an internal JSON query value.
3. Server applies schema-aware field rewrite when graph is schema-bound.
4. Server applies response-mode projection defaults (`id`,`label` or `*` for full modes).
5. Engine lowers query JSON -> DataFusion logical plan.
6. Engine executes DataFusion plan and collects `RecordBatch`.
7. Server/engine serializes output according to endpoint mode.

## 3.2 Query to Plan Semantics (No SQL middle layer)

The design intentionally bypasses SQL parser translation.

- Input is AST, not SQL string.
- Input is JSON query structure, not SQL string.
- Lowering builds DataFusion DataFrame ops directly.
- This avoids parse/format churn and gives explicit operator control.

## 3.3 Stream-Kind Model

Lowering tracks `StreamKind`:

- `Vertex`: row shape expected to be vertex columns.
- `Edge`: row shape expected to be edge columns.

This governs legality and join strategy for steps like `OutE` vs `Out`.

## 4. Storage Architecture

## 4.1 Physical Storage

- Vortex files are the persisted dataset format for runtime graphs.
- Each graph maps to two table roots:
  - `<graph>/vertices/`
  - `<graph>/edges/`

## 4.2 Current Ingest Write Path

Current implemented path is:

- Parquet -> Arrow `RecordBatch` -> Vortex arrays/files.

`loom-engine` details:

1. `ParquetRecordBatchReaderBuilder` reads input parquet.
2. Batches are partitioned across parallel tasks.
3. Each partition writes one or more `.vortex` files.
4. The parquet ingest path also emits `vertices.arrow` so the graph can be re-registered from Arrow IPC without reparsing parquet.
5. Conversion uses `ArrayRef::from_arrow` and Vortex `write_options().write(...)`.

So Vortex is the persisted format, while Arrow/Parquet remain intermediate structures in ingest.

## 4.3 Table Registration

After ingest:

- Engine registers Vortex listing tables in DataFusion with explicit schemas.
- Registration reads Arrow IPC files from `vertices/` when present.
- Vertex schema comes from `VertexColumn[]` declarations.
- Edge schema is currently fixed to:
  - `id`, `from_id`, `to_id`, `label`, `props`.

## 4.4 Graph Catalog

`GraphCatalog` is in-memory and process-local:

- Maps graph name -> vertex/edge DataFusion table names.
- Used by lowering via `LoweringContext`.

No external catalog service exists yet.

## 5. Schema-Aware Layer

## 5.1 Schema Registry Model

`loom-server` stores:

- `schemas: HashMap<String, StoredSchema>`
- `graph_schema_bindings: HashMap<String, String>`

`StoredSchema` includes:

- Raw JSON schema doc.
- `SchemaSummary` metadata.
- Compiled Arrow resource shapes.
- Derived promoted columns.

## 5.2 Persistence

Schema state is persisted to disk:

- File path: env `LOOM_SCHEMA_STATE_FILE` or `./loom_schema_state.json`.
- Loaded at startup, saved on schema upsert and graph-schema binding updates.

## 5.3 JSON Schema -> Arrow Shape Compilation

`compile_arrow_shapes` behavior:

- Requires `$defs` / `definitions`.
- Resolves `$ref`.
- Handles `oneOf`/`anyOf` by first non-null branch.
- Supports primitive/object/list mapping:
  - integer -> `Int64`
  - number -> `Float64`
  - boolean -> `Boolean`
  - string -> `Utf8`
  - array -> `List(item_type)`
  - object -> `Struct(fields...)`
- Enforces max nesting depth (32) for safety.

## 5.4 Promoted Columns

Promoted columns flatten schema fields into typed vertex columns.

Current mapping:

- scalar types become native typed promoted columns (`Utf8`, `Int64`, `Float64`, `Bool`)
- struct/list become `Json` promoted columns (stored as serialized JSON string)
- `id` and `resourceType` are excluded from promoted set

In addition to promoted columns, schema-aware ingest now stores an opaque document payload on each vertex:

- `payload_codec` (`VARCHAR NOT NULL`) currently set to `flexbuf_v1`
- `payload_bin` (`BLOB NOT NULL`) containing FlexBuffers-encoded document bytes
- `payload_json_bin` (`BLOB NOT NULL`) containing raw JSON bytes for pass-through full response

Column naming convention:

- `t_<resource_type>_<dotted_path>` sanitized to alphanumeric/underscore.

## 5.5 Schema-Aware Query Rewrite

Before execution, server rewrites:

- `Has.field`
- `Render.fields`

to matching promoted physical columns where unambiguous for the graph schema binding.

This lets clients keep semantic field names while queries hit typed columns.

## 6. Ingest Paths

## 6.1 Generic Parquet Bulk Load

Endpoint:

- `POST /v1/graph/{graph}/bulk-load`

Request:

- `vertices_parquet_path`
- `edges_parquet_path`
- `vortex_root_uri`
- `mode`

This path delegates directly to engine ingest.

## 6.2 Schema-Aware NDJSON Bulk Load

Endpoint:

- `POST /v1/graph/{graph}/bulk-load/ndjson-schema`

Flow:

1. Resolve schema by `schema_name`.
2. Build temporary typed vertices parquet:
  - columns: `id`, `label`, `payload_codec`, `payload_bin`, `payload_json_bin`, plus promoted columns
  - payload bytes are FlexBuffers (`payload_codec = flexbuf_v1`)
  - no vertex `props` fallback column in this path
3. Build temporary edges parquet from `reference` extraction:
  - edge columns: `id`, `from_id`, `to_id`, `label`, `props`
4. Build `VertexColumn[]` from promoted column types.
5. Call engine ingest parquet->vortex.
6. Bind graph to schema and persist state.

Edge extraction specifics:

- Recursively scans every object/array for `reference` string keys.
- Canonicalizes references to `ResourceType/id` when possible.
- Emits edge label `ref:<path>` and edge props payload `{"path":...,"reference":...}`.

## 7. API Surface

## 7.1 HTTP Endpoints

Schema:

- `GET /v1/schema`
- `GET /v1/schema/{name}`
- `POST /v1/schema/{name}`

Graph/query:

- `GET /v1/graph`
- `POST /v1/graph/{graph}/query` (inline JSON rows, capped)
- `POST /v1/graph/{graph}/query/compact` (JSON bytes)
- `POST /v1/graph/{graph}/query/full`
- `POST /v1/graph/{graph}/query/full/fast`
- `POST /v1/graph/{graph}/query/ndjson`
- `POST /v1/graph/{graph}/bulk-load`
- `POST /v1/graph/{graph}/bulk-load/ndjson-schema`

Compatibility aliases:

- `POST /v1/query`
- `POST /v1/query/compact`
- `POST /v1/query/full`
- `POST /v1/query/full/fast`
- `POST /v1/query/ndjson`

## 7.2 Response Modes

- Inline `/query`: materializes rows and enforces max row limit.
  - env: `LOOM_HTTP_MAX_INLINE_ROWS` (default `10_000`)
- `/query/compact`: JSON array bytes from engine serialization.
- `/query/ndjson`: chunked NDJSON stream.
- `/query/full*`: schema-bound reconstructed resource JSON.

## 7.3 Arrow Flight

Service:

- Runs on `127.0.0.1:50051`.
- `DoGet` implemented: ticket payload is UTF-8 JSON query.
- Other Flight operations currently return `unimplemented`.

## 8. Full Reconstruction Design

`full` and `full/fast` now share schema-bound typed reconstruction logic.
These modes use a minimal fast projection (`id`, `label`, payload columns) instead of `*`.

Behavior:

1. Force projection to include `*`.
2. Execute query to batches.
3. Require stored schema binding for graph.
4. Reconstruct resource JSON:
  - first try direct pass-through from `payload_json_bin`
  - otherwise try decoding `payload_bin` when codec is `flexbuf_v1`
  - if payload decode is unavailable/fails, fall back to promoted-column reconstruction
  - ensure `id` and `resourceType` are present

Late-materialization filter path:

- for `has` on non-promoted fields, lowering can call `payload_extract(payload_json_bin, field_path)`
- this UDF extracts scalar values from payload bytes as UTF-8 and compares after core column filters

Current implementation focus:

- batch-oriented serializer (`reconstruct_full_rows_fast_from_batches`) to avoid extra row materialization overhead.

## 9. Runtime and Concurrency Model

- Single process hosts both HTTP and Flight servers.
- Shared `Arc<Engine>` and RwLock-protected schema maps.
- Async runtime: Tokio multi-thread.
- Ingest write fanout: parallel Vortex part writers using `tokio::task::JoinSet`.
- NDJSON query endpoints run as streamed batch pipelines (no full collect before response).

## 10. Build/Test/Bench Tooling

## 10.1 Make Targets

`Makefile`:

- `build`: `cargo build --workspace`
- `check`: `cargo check --workspace`
- `test`: `cargo test --workspace`
- `fmt`: `cargo fmt --all`
- `run-server`: `cargo run -p loom-server`

## 10.2 Benchmarks

Engine tests (ignored by default):

- `mvp_conformance_bench.rs`
- `calypr_real_world_bench.rs`
- `calypr_load_and_first_four_queries.rs`

Server tests include schema-aware benchmark functions and reconstruction benchmark paths (also ignored unless explicit run).

Benchmark env knobs used across tests:

- `CALYPR_META_DIR`
- `CALYPR_BENCH_CACHE`
- `CALYPR_BENCH_CACHE_DIR`
- `CALYPR_BENCH_MODE`
- `CALYPR_BENCH_SAMPLE_EVERY`
- `BENCH_MAX_ROWS`

`CALYPR_BENCH_SAMPLE_EVERY=10` runs the Calypr benchmark on roughly 1/10 of the full META rows across all labels, writes a sampled parquet snapshot, and uses a separate graph name so the sampled run does not reuse the full benchmark cache. If the sampled graph already exists in the cache directory, reruns skip ingestion and go straight to query-only.

Benchmark metric interpretation:

- `total_source_records`: raw sampled source rows scanned from META files.
- `total_loaded_records`: rows kept after sampling and written into the sampled parquet snapshot.
- `observation_records`: rows returned by `V().hasLabel("Patient")` after ingest.
- `prep_seconds_sampled_meta`: time to build the sampled snapshot.
- `load_seconds_parquet_to_vortex`: time to ingest sampled parquet into the cached graph.
- `query_seconds_haslabel_observation`: full end-to-end query time for the benchmark query.
- `query_seconds_full_serde_path` vs `query_seconds_full_sonic_path`: JSON serialization cost on two response paths.
- `query_rows_per_sec` and `ndjson_rows_per_sec`: query throughput in returned rows/sec.
- `query_output_bytes`: response payload size for the benchmark query.

Equivalence-test knobs:

- `CALYPR_SAMPLE_MOD_1`
- `CALYPR_SAMPLE_MOD_2`
- `CALYPR_CACHE`
- `CALYPR_CACHE_DIR`

`calypr_load_and_first_four_queries.rs` uses the same reduced-load pattern as the Python script: keep rows where the per-file counter matches either modulo, then validate load behavior and the early query shapes instead of benchmark timing.

## 11. Design Decisions (What Exists Today)

1. **No SQL parser middle layer** between LoomQL and DataFusion.
2. **DataFusion logical lowering is the optimizer boundary**.
3. **Vortex is persisted storage format**, with DataFusion integration via `vortex-datafusion`.
4. **Schema-aware path is typed-first for vertices** (promoted typed columns + FlexBuffers payload, no vertex props fallback in schema-aware ingest).
5. **Edges still retain `props` column** in current engine/table schema.
6. **Schema state and graph-schema bindings are persisted locally** as JSON.
7. **HTTP and Flight coexist in same process with hardcoded bind addresses**:
  - HTTP: `127.0.0.1:8080`
  - Flight: `127.0.0.1:50051`
8. **Ingest write ordering clusters by label when available** to improve locality for label-heavy reads.

## 12. Current Limitations / Gaps

1. **Ingest still has Parquet intermediate** for both generic and schema-aware paths.
2. **DataFusion execution interface remains Arrow batch oriented**, so full end-to-end Vortex-native operator pipeline is not implemented.
3. **Edge schema is partially legacy-style** (`props` string), not fully typed/promoted like vertices.
4. **Flight API coverage is minimal** (`DoGet` only).
5. **No transactional guarantees** or external metadata/catalog durability beyond local schema JSON + Vortex files.
6. **Schema promotion policy is broad by default** and can generate many columns for large schema surfaces.

## 13. Practical Mental Model

You can model the system as three stacked layers:

1. **Execution Layer** (`loom-engine`):
  - parse query JSON, lower traversal intent into DataFusion operators, and run over registered tables.
2. **Schema/Serving Layer** (`loom-server`):
  - schema registry, typed-column derivation, query rewrite, full reconstruction, HTTP/Flight APIs.

This structure keeps custom graph logic relatively thin while reusing DataFusion + Vortex heavily.

## 14. Suggested Near-Term Refactors (Architecture-Consistent)

If the goal is higher throughput with minimal bespoke code, the highest-leverage next steps are:

1. Replace schema-aware `NDJSON -> typed parquet -> Vortex` with direct typed Vortex writes.
2. Move edge metadata (`path`, `reference`) to typed edge columns, reducing edge `props` parsing dependence.
3. Keep `full/fast` batch reconstruction path as the default and avoid row-by-row JSON materialization in hot routes.
4. Add optional address/env configuration for HTTP/Flight binds to improve deployability.



HORIZONTAL SCALABILITY

substrait.io datafusion plugin -> takes a query plan and shards it into multiple datafusion schedulers

logical plan -> ballista like scheduler splits the query plan into 10 seperate query fragments -> substrait packages them up -> sends them to the query engine (datafusion scheduler) -> come back via arrow flight
