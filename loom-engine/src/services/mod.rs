//! Workflow-oriented services layered on top of the core query runtime.
//!
//! The crate still exposes a single `Engine` facade, but these modules group
//! the high-level responsibilities more explicitly than the old `engine/*`
//! bucket did.

mod graph;
mod graph_schema;
mod mapping;
mod query;
mod source;
mod transform;
