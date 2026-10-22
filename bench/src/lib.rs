//! t4's benchmark harness. Its parts:
//!
//! - [`request_trace`]: an object store that records every request made of
//!   it, so a workload's reads (ClickBench's, TPC-H's) can be studied and
//!   replayed against the cache.
//! - [`simulated_store`]: an object store over another that answers like a
//!   remote one, after a first-byte latency and at a bandwidth.
//! - [`synthetic_objects`]: an object store whose objects' bytes are
//!   computed, not stored, for the simulated store to serve.

pub mod request_trace;
pub mod simulated_store;
pub mod synthetic_objects;
