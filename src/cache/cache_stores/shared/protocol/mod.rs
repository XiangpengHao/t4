//! What a cache's owner and its clients speak (`doc/11-shared-mode.md`,
//! decisions 5 and 6), each end its own half of it.
//!
//! - `frames`: what the socket carries, each frame decoded and checked as
//!   it arrives.
//! - `frame_socket`: a connected socket, which frames cross whole.
//! - `address`: where an owner listens, named by its first cache file.
//! - `handshake`: a client's hello, and the owner's check of it.
//! - `region`: a client's shared memory, mapped by its owner too, handed
//!   out in spans.

mod address;
mod frame_socket;
mod frames;
mod handshake;
mod region;

pub(super) use address::OwnerAddress;
pub(super) use frame_socket::{FrameReceiver, FrameSender, FrameSocket};
pub(super) use frames::{
    ClientFrame, FetchSpec, FrameReader, NextStep, OwnerFrame, PROTOCOL_VERSION,
};
pub(super) use handshake::Hello;
pub(super) use region::{ClientRegion, OwnerRegion, RegionSpan, SpanName};
