//! One cache read by several processes (`doc/11-shared-mode.md`): the
//! process that holds the cache files' lock owns the cache and serves it to
//! the others, its clients, over a Unix socket.
//!
//! - `frames`: what the socket carries, each frame decoded and checked as
//!   it arrives.
//! - `frame_socket`: a connected socket, which frames cross whole.
//! - `address`: where an owner listens, named by its first cache file.
//! - `handshake`: a client's hello, and the owner's check of it.
//! - `owner`: an owner's listener, and a session for each client.
//! - `client_reads`: a client's reads, as its owner serves them.
//! - `client`: a client's connection to its owner.

mod address;
mod client;
mod client_reads;
mod frame_socket;
// The frames of reads are used outside their tests once the socket serves
// reads (step 3's next commit).
#[allow(dead_code)]
mod frames;
mod handshake;
mod owner;

pub(super) use address::FileIdentity;
pub(super) use client::OwnerConnection;
pub(super) use frames::{FrameReader, FrameWriter};
pub(super) use owner::OwnerListener;

#[cfg(all(test, not(feature = "shuttle")))]
mod tests;
