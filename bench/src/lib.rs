//! t4's benchmark harness. Its parts:
//!
//! - [`table_store`]: the store a table is read through, a local file or
//!   GCS, with t4 in front if asked.
//! - [`pinned_tier_policy`]: a cache policy that keeps every block in one
//!   tier, so that a warm run measures memory hits or disk hits alone.
//! - [`request_trace`]: an object store that records every call made of
//!   it, so a workload's reads (ClickBench's, TPC-H's) can be studied and
//!   replayed.
//! - [`replay`]: a trace's calls made again of another store, with nothing
//!   between them.
//! - [`host_counters`]: the bytes a drive and the network moved during a
//!   query, read from `/proc`.
//! - [`simulated_store`]: an object store over another that answers like a
//!   remote one, after a first-byte latency and at a bandwidth.
//! - [`synthetic_objects`]: an object store whose objects' bytes are
//!   computed, not stored, for the simulated store to serve.

pub mod host_counters;
pub mod pinned_tier_policy;
pub mod replay;
pub mod request_trace;
pub mod simulated_store;
pub mod synthetic_objects;
pub mod table_store;

/// Whatever went wrong: a store, t4, a file, an argument.
pub type Error = Box<dyn std::error::Error + Send + Sync>;
