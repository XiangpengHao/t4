//! One cache read by several processes (`doc/11-shared-mode.md`): the
//! process that holds the cache files' lock owns the cache and serves it to
//! the others, its clients, over a Unix socket.
//!
//! - `frames`: what the socket carries, each frame decoded and checked as
//!   it arrives.
//! - `frame_socket`: a connected socket, which frames cross whole.
//! - `address`: where an owner listens, named by its first cache file.
//! - `handshake`: a client's hello, and the owner's check of it.
//! - `region`: a client's shared memory, mapped by its owner too, handed
//!   out in spans.
//! - `owner`: an owner's listener, and a session for each client.
//! - `client_reads`: a client's reads, as its owner serves them.
//!
//! A client's side, its connection to its owner and its reads through it,
//! is the shared cache store's (`cache_stores::shared`).

mod address;
mod client_reads;
mod frame_socket;
mod frames;
mod handshake;
mod owner;
mod region;
mod session_waker;

pub(super) use address::{FileIdentity, OwnerAddress};
pub(super) use frame_socket::{FrameReceiver, FrameSender, FrameSocket};
pub(super) use frames::{
    ClientFrame, FetchSpec, FrameReader, FrameWriter, NextStep, OwnerFrame, PROTOCOL_VERSION,
};
pub(super) use handshake::Hello;
pub(super) use owner::OwnerListener;
pub(super) use region::{ClientRegion, RegionSpan, SpanName};

#[cfg(all(test, not(feature = "shuttle")))]
mod tests;
