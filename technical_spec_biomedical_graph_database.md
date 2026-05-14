# Technical Spec: Loom Graph Engine

## Product Name

**Loom**

This document describes the technical direction for Loom: a Rust graph engine that takes files from anywhere, applies explicit mappings, and exposes the result as a queryable graph.

## Product Summary

Loom is a graph construction and query system for heterogeneous files.

The core idea is:

```text
files from anywhere
        +
declarative source and graph mappings
        =
queryable graph
```

Loom should let a user:

```text
1. Register files or file-backed sources from arbitrary locations.
2. Profile their structure.
3. Define or confirm mappings.
4. Expose those sources as a graph immediately (virtual graph).
5. Optionally compile a persisted graph snapshot.
6. Query the graph through graph-style and SQL-style interfaces.
7. Return graph rows, paths, cohorts, manifests, and downstream exports.
```

Loom is not a generic OLTP graph database and not a document lake. It is a graph-oriented metadata and file-query engine.

## Problem

Real data systems do not start with one clean graph-shaped database.

They start with:

```text
JSON documents
NDJSON logs
CSV/TSV tables
Parquet files
FHIR resources
spreadsheet exports
workflow metadata
research manifests
access-control tables
custom metadata dumps
```

The painful part is not storing those files. The painful part is relating them:

```text
this patient row matches this sample row
this sample row references this assay row
this assay row points at this object record
this workflow output belongs to this study
this JSON object is the same entity as that TSV row
```

Today those joins usually live in:

```text
ad hoc notebooks
application code
one-off ETL jobs
manual export pipelines
```

Loom should make these relationships explicit, reusable, and queryable.

## Non-Goals

Loom does not try to infer a correct graph from arbitrary files with no mapping help.

Loom also does not require every source to be normalized into a heavyweight canonical standard before use.

The contract is:

```text
messy files are allowed
implicit semantics are not
```

The system requires one of:

1. a known structured source type,
2. convention-based field detection, or
3. a user-confirmed mapping manifest.

## Core Design Principle

Use **file-native ingest plus explicit graph mappings**.

The files remain the source of truth. Loom defines how to interpret them as entities and relationships.

```text
source files
        ↓
profiling
        ↓
source registration
        ↓
graph mapping
        ↓
virtual graph or compiled graph
        ↓
query runtime
```

## Architectural Model

Loom has two graph modes:

### 1. Virtual Graph Mode

The graph is exposed directly over registered sources and mappings without first materializing a full persisted graph snapshot.

Use this for:

- rapid iteration
- source exploration
- mapping validation
- lower-latency onboarding

### 2. Published Graph Mode

The graph is compiled into persisted vertex and edge storage plus adjacency artifacts for faster repeated queries.

Use this for:

- stable production workloads
- repeated analytical queries
- exportable graph snapshots
- heavier traversal workloads

## System Architecture

```text
┌──────────────────────────────────────────────────────────────┐
│                         Source Layer                         │
│                                                              │
│  JSON   NDJSON   CSV/TSV   Parquet   XLSX   FHIR   custom    │
│  local paths   object storage   mounted dirs   remote refs   │
└──────────────────────────────┬───────────────────────────────┘
                               │
                               v
┌──────────────────────────────────────────────────────────────┐
│                   Source Registration Layer                  │
│                                                              │
│  source identity                                             │
│  format detection                                            │
│  schema/profile stats                                        │
│  logical tables/views                                        │
│  refresh + fingerprinting                                    │
└──────────────────────────────┬───────────────────────────────┘
                               │
                               v
┌──────────────────────────────────────────────────────────────┐
│                     Transform Layer                          │
│                                                              │
│  projections                                                 │
│  field extraction                                            │
│  reshaping                                                   │
│  normalization                                               │
│  source-to-source derived tables                             │
└──────────────────────────────┬───────────────────────────────┘
                               │
                               v
┌──────────────────────────────────────────────────────────────┐
│                   Graph Mapping Layer                        │
│                                                              │
│  node mappings                                               │
│  edge mappings                                               │
│  identity rules                                              │
│  property mappings                                           │
│  join/reference rules                                        │
│  schema bindings                                             │
└──────────────────────────────┬───────────────────────────────┘
                               │
                     ┌─────────┴─────────┐
                     │                   │
                     v                   v
┌──────────────────────────────┐   ┌───────────────────────────┐
│      Virtual Graph Runtime   │   │     Graph Compiler        │
│                              │   │                           │
│ mapped views over sources    │   │ vertex extraction         │
│ no full graph persistence    │   │ edge extraction           │
│ immediate iteration          │   │ ID normalization          │
│                              │   │ adjacency build           │
└───────────────┬──────────────┘   │ persisted graph catalog   │
                │                  └──────────────┬────────────┘
                │                                 │
                └──────────────┬──────────────────┘
                               v
┌──────────────────────────────────────────────────────────────┐
│                       Query Runtime                          │
│                                                              │
│  DataFusion                                                  │
│  graph-aware logical planning                                │
│  traversal operators                                         │
│  predicate pushdown                                          │
│  virtual/published graph dispatch                            │
└──────────────────────────────┬───────────────────────────────┘
                               v
┌──────────────────────────────────────────────────────────────┐
│                         Outputs                              │
│                                                              │
│  rows   paths   counts   manifests   NDJSON   Arrow   SQL    │
└──────────────────────────────────────────────────────────────┘
```

## Source Model

Loom should treat files as first-class sources, not as awkward input to be normalized away up front.

### Supported Source Shapes

Initial target classes:

```text
CSV / TSV
JSON
NDJSON
Parquet
XLSX
FHIR bundles and resources
directory-backed file collections
```

### Source Registration Contract

Each source registration should capture:

```text
source id
display name
format
location
read options
logical tables/views
fingerprint / refresh metadata
```

### Important Requirement

JSON must be a first-class source type.

Loom should not require JSON fixtures to be flattened into TSV just to participate in graph construction. Internal transforms may still tabularize portions of JSON for execution, but that is an implementation detail, not a product requirement.

## Transform Model

Transforms are reusable derived views over sources.

They exist to make source data graph-ready without mutating the source of truth.

Typical transform responsibilities:

```text
flatten nested objects
extract arrays
rename fields
coerce types
normalize identifiers
derive join keys
project graph-relevant fields
```

Transforms are not the graph themselves. They prepare source tables that mappings can target.

## JSON Schema Backing

Loom is not only mapping-backed. It also has a schema-backed path for resource-shaped JSON inputs.

This matters because many important inputs are not best modeled as flat tables first. They are nested JSON documents with a known or partially known structure.

### Current Role of JSON Schema

JSON Schema in Loom is used to:

```text
validate schema documents
compile graph-aware schema metadata
derive Arrow-compatible field shapes
derive promoted typed columns
preserve full payloads alongside typed projections
support schema-aware query rewriting
```

### Expected Schema Shape

The current implementation assumes schema documents with:

```text
$schema
$defs or definitions
object/resource definitions
optional hypermedia-style links
```

At registration time, Loom should:

1. validate the schema document itself,
2. compile resource/entity descriptors,
3. derive field shapes for typed ingest,
4. persist the compiled schema and promoted-column metadata.

### Compiled Schema Outputs

The JSON Schema-backed path should produce two related artifacts:

#### 1. Compiled Graph Schema

This describes:

```text
entity types
properties
required fields
links / relations
allowed targets
wildcard relations
```

This allows Loom to understand graph semantics directly from schema-backed resource definitions.

#### 2. Arrow Resource Shapes

These describe:

```text
resource type
field names
field nullability
field nesting
Arrow-compatible data types
```

These shapes drive typed projection and typed ingest behavior.

### Promoted Columns

Schema-backed ingest should derive promoted columns from resource fields.

The purpose is:

```text
keep important fields queryable as typed columns
avoid forcing every filter through full JSON payload extraction
preserve nested resource structure where useful
```

Promoted columns should support at least:

```text
Utf8
Int64
Float64
Bool
Json
```

The guiding rule is:

```text
scalar fields become native typed columns
complex fields can remain JSON-backed
```

### Payload Preservation

Schema-backed ingest should not discard the original resource payload.

Each ingested row should preserve:

```text
payload_codec
payload_bin
payload_json_bin
```

This gives Loom both:

1. a typed projected surface for fast query/filter behavior, and
2. a preserved original payload for full reconstruction and pass-through use cases.

### Schema-Aware Query Rewrite

When a graph is bound to a schema, Loom should rewrite logical field references onto promoted physical columns where possible.

This means a query can speak in resource/property language while the runtime can execute against:

```text
promoted typed columns when available
payload extraction when necessary
full payload reconstruction when requested
```

### Schema-Aware Ingest Path

Loom should support a schema-aware ingest mode for NDJSON/resource-style inputs.

That path should:

1. validate the schema document,
2. compile promoted columns,
3. build typed vertex records,
4. preserve full payload bytes,
5. expose the result to graph mapping and query runtime.

### Product Implication

This JSON Schema-backed path is important to Loom’s identity.

Loom is not just:

```text
files -> tables -> graph
```

It is also:

```text
resource-shaped JSON -> schema-backed typed projection -> graph
```

That distinction matters for:

```text
FHIR-like resources
document-heavy metadata
nested provenance records
JSON-native product workflows
```

## Graph Mapping Model

Graph mappings are the core contract in Loom.

They define:

```text
what entities exist
how entity IDs are formed
how edges are derived
what properties are exposed
which sources/transforms participate
```

### Required Mapping Concepts

```text
graph id
source dependencies
node specs
edge specs
identity expressions
property expressions
optional predicates
optional schema entity/relation hints
```

### Example Shape

```yaml
graph: research_graph

nodes:
  - label: Patient
    source: patients
    id: patient_id
    props:
      study_id: study_id

  - label: Sample
    source: samples
    id: sample_id
    props:
      patient_id: patient_id
      sample_type: sample_type

edges:
  - label: HAS_SAMPLE
    source: samples
    from:
      label: Patient
      expr: patient_id
    to:
      label: Sample
      expr: sample_id
```

## Data Model

Loom exposes graphs as typed vertices and edges.

### Vertex Shape

Minimum logical fields:

```text
id
label
properties / promoted columns
source_id
source_row
payload provenance
```

### Edge Shape

Minimum logical fields:

```text
id
from_id
to_id
label
source_id
source_row
properties
```

### Storage Strategy

Published graphs should be persisted as:

```text
vertex storage
edge storage
adjacency artifacts
graph metadata/catalog entries
stats and fingerprints
```

Current implementation direction uses Vortex-backed storage and DataFusion query execution.

## Query Model

Loom needs two user-facing query modes:

### 1. Graph-Oriented Query Mode

Users can express graph traversal and filtering semantics directly.

Current operation family:

```text
v
out / in / both
out_e / in_e / both_e
has
has_label
has_id
render
count
limit
skip
```

### 2. SQL Query Mode

Users can query virtual and published graph views through SQL where that is more natural.

This matters because:

- many users think relationally
- source debugging is easier in SQL
- virtual graph inspection often benefits from explicit SQL

## Schema-Bound Graph Behavior

Loom should support graphs that are explicitly bound to compiled schemas.

That binding enables:

```text
schema-aware field rewrite
schema-aware validation
entity/relation vocabulary inspection
typed full-row reconstruction
graph-schema previews before publish
```

In practice, Loom should allow both:

1. mapping-only graphs, and
2. mapping + schema-bound graphs.

Schema-bound graphs provide stronger guarantees and richer query ergonomics, especially for resource-oriented JSON sources.

## Runtime Requirements

The runtime must:

1. dispatch correctly between virtual and published graphs,
2. support predicate pushdown,
3. avoid unnecessary materialization,
4. support traversal over adjacency structures for published graphs,
5. support direct view-based execution for virtual graphs.

## Profiling and Suggestions

Loom should help users build mappings instead of forcing blank-sheet configuration.

The profiler should produce:

```text
column names
type inference
value cardinality
null density
candidate ID fields
candidate foreign-key fields
label/entity hints
reference suggestions
mapping confidence hints
```

This layer is advisory. It does not replace explicit mappings.

## Persistence Requirements

The system must persist:

```text
source catalog
transform catalog
graph mapping catalog
graph schema catalog
graph catalog
schema bindings
```

These can begin as local JSON state files but should be treated as product metadata, not incidental dev artifacts.

## API Surface

Initial server capabilities should cover:

```text
register/list/get sources
profile/sample/refresh sources
register/list/get transforms
register/list/get graph mappings
validate/compile/refresh graph mappings
register/list/get graph schemas
publish/register graph schemas to runtime
list/get graphs
query graph
query graph via SQL
stream graph results
bulk load published graphs
```

## Product Positioning

Loom is not just a biomedical graph database.

Biomedical metadata is an important target use case, but the product abstraction is broader:

```text
take structured and semi-structured files from anywhere
map them into entities and relationships
query them as a graph
optionally compile them for production use
```

That means the spec should stay source-first and mapping-first, not disease-domain-first.

## Near-Term Priorities

1. Make JSON and NDJSON true first-class sources.
2. Reduce remaining table-first assumptions in tests and ingest paths.
3. Strengthen virtual graph ergonomics so mappings can be validated before compilation.
4. Keep published graph mode for production repeatability and speed.
5. Tighten the product language so Loom is clearly about weaving files into graphs.

## Short Product Definition

```text
Loom is a graph engine that takes files from anywhere,
weaves them together with explicit mappings,
and exposes the result as a queryable graph.
```
