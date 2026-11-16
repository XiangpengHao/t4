//! A client's side of shared mode: its connection to the process that owns
//! the cache, and its reads through it.
//!
//! - `connection`: the connection to the owner, its calls and their answers.
//! - `read`: a read through the owner.
//! - `fetch`: one of a read's fetches, its bytes handed to the owner.

mod connection;
mod fetch;
mod read;

pub(in crate::cache) use connection::OwnerConnection;
pub(super) use read::ClientRead;
