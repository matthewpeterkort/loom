# loom

Rust graph query engine focused on OLAP traversal using Arrow/DataFusion.

## Workspace crates

- `loom-engine`: query compilation/execution shell.
- `loom-server`: HTTPS JSON API shell.

## Current status

- Initial vertical slice is implemented:
  - graph registration from parquet files
  - JSON query parse
  - query lowering to DataFusion LogicalPlan inside `loom-engine`
  - DataFusion execution (no SQL parser stage)
  - JSON row response
  - Arrow Flight `DoGet` query stream
- `loom-server` exposes:
  - `GET /v1/graph`
  - `POST /v1/graph/{graph}/query`
  - `POST /v1/graph/{graph}/bulk-load`
  - `POST /v1/query` (compatibility alias)
  - Flight gRPC on `127.0.0.1:50051`

## Example payload

```json
{
  "graph": "fhir",
  "steps": [
    { "op": "v", "ids": ["Patient/123"] },
    { "op": "out", "labels": ["has_observation"] },
    { "op": "has", "field": "label", "eq": "Observation" },
    { "op": "render", "fields": ["id", "label"] }
  ]
}
```

## Bulk load graph (MVP)

```json
{
  "vertices_parquet_path": "/data/raw/vertices.parquet",
  "edges_parquet_path": "/data/raw/edges.parquet",
  "vortex_root_uri": "/data/loom",
  "mode": "overwrite"
}
```
Endpoint: `POST /v1/graph/fhir/bulk-load`

`mode`: `create` | `append` | `overwrite` (default `overwrite`)

Expected parquet schemas in v1:
- vertices: `id`, `label`, `props`
- edges: `id`, `from_id`, `to_id`, `label`, `props`

## Supported traversal ops (v1 slice)

- `v`
- `out`, `in`, `both`
- `out_e`, `in_e`, `both_e`
- `has`, `has_label`, `has_id`
- `render`, `count`, `limit`, `skip`

`out/in/both` work from either vertex streams or edge streams.

## Run server

```bash
source "$HOME/.cargo/env"
cd /Users/peterkor/Desktop/BMEG/grip-complex/loom
cargo run -p loom-server
```

To make the server reachable outside localhost, set:

```bash
LOOM_HTTP_HOST=0.0.0.0 LOOM_FLIGHT_HOST=0.0.0.0 cargo run -p loom-server
```

## Run with Docker

Build:

```bash
docker build -t loom-server .
```

Run:

```bash
docker run --rm -p 8080:8080 -p 50051:50051 -v loom-data:/var/lib/loom loom-server
```

## MVP conformance benchmark

This benchmark validates and times:
- bulk load of 100,000 vertex records
- query `V().hasLabel("sfdsf")`

Run:

```bash
export PATH="/opt/homebrew/bin:$PATH"
export PROTOC="/opt/homebrew/bin/protoc"
source "$HOME/.cargo/env"
cd /Users/peterkor/Desktop/BMEG/grip-complex/loom
cargo test -p loom-engine mvp_bulk_load_and_haslabel_benchmark -- --ignored --nocapture
```

## Calypr real-world benchmark

This benchmark validates and times the Calypr bulk load plus `V().hasLabel("Patient")`.

Run the full version:

```bash
CALYPR_BENCH_SAMPLE_EVERY=1 cargo test -p loom-engine calypr_real_world_haslabel_benchmark -- --ignored --nocapture
```

For a much faster sample run, set:

```bash
CALYPR_BENCH_SAMPLE_EVERY=10 cargo test -p loom-engine calypr_real_world_haslabel_benchmark -- --ignored --nocapture
```

That keeps roughly 1/10 of the full Calypr META rows across all labels, writes a sampled parquet snapshot, and uses a separate on-disk graph name so it will not collide with the full benchmark cache. If the cached sampled graph already exists, reruns switch to query-only automatically.

## Calypr equivalence test

This is the smaller load + first-four-query test that tracks the Python script more closely than the benchmark.

Run:

```bash
CALYPR_SAMPLE_MOD_1=5 CALYPR_SAMPLE_MOD_2=9 cargo test -p loom-engine calypr_load_and_first_four_queries -- --ignored --nocapture
```

It uses the same reduced-load pattern as the Python script: keep rows where the per-file counter hits either modulo.

## Query via Flight (DoGet)

Use a `Ticket` payload that is LoomQL JSON (UTF-8 bytes). The server lowers and executes it, then streams Arrow `FlightData`.

See [ARCHITECTURE.md](./ARCHITECTURE.md) for the v1 plan and caveats.
