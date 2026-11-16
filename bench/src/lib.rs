pub mod disk_caches;
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

pub type Error = Box<dyn std::error::Error + Send + Sync>;
