//! t4's benchmark harness. Its parts:
//!
//! - [`table_store`]: the store a table is read through, a local file, GCS
//!   or S3, with t4 in front if asked.
//! - [`fixed_placement_policy`]: a cache policy that places every block
//!   alike, in one tier, so that a warm run measures memory hits or disk
//!   hits alone, or nowhere, so that a run measures t4 caching nothing.
//! - [`request_trace`]: an object store that records every call made of
//!   it, so a workload's reads (ClickBench's, TPC-H's) can be studied and
//!   replayed.
//! - [`request_cost`]: an object store that counts the requests made of it
//!   as they are billed, and what they cost.
//! - [`io_latency`]: a read policy that times every I/O t4 makes, its GETs
//!   and its disk reads.
//! - [`replay`]: a trace's calls made again of another store, with nothing
//!   between them.
//! - [`host_counters`]: the bytes a drive and the network moved during a
//!   query, and the CPU time the process spent, read from `/proc`.
//! - [`ktls_transport`]: an HTTP transport for object_store whose TLS the
//!   kernel decrypts straight into a response body's buffer, over
//!   `http1_connection`'s HTTP/1.1.
//! - [`simulated_store`]: an object store over another that answers like a
//!   remote one, after a first-byte latency and at a bandwidth.
//! - [`synthetic_objects`]: an object store whose objects' bytes are
//!   computed, not stored, for the simulated store to serve.

pub mod fixed_placement_policy;
pub mod host_counters;
pub mod io_latency;
// Only Linux builds `ktls_transport`, its user; it is tested everywhere.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod http1_connection;
pub mod ktls_transport;
pub mod replay;
pub mod request_cost;
pub mod request_trace;
pub mod simulated_store;
pub mod synthetic_objects;
pub mod table_store;

/// Whatever went wrong: a store, t4, a file, an argument.
pub type Error = Box<dyn std::error::Error + Send + Sync>;
