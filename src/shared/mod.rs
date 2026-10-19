//! Shared mode (`doc/02-multi-process.md`): one server process owns a
//! store and runs its engine; any number of client processes use it over a
//! Unix socket. A client starts the server if none is running, and the
//! server exits once it has had no client for its idle timeout.

mod address;
mod client;
mod launch;
mod server;
mod wire;

pub(crate) use client::SharedStore;
pub(crate) use server::Server;
