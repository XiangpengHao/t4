#[cfg(not(all(any(test, fuzzing), feature = "shuttle")))]
mod address;
#[cfg(all(any(test, fuzzing), feature = "shuttle"))]
#[path = "simulated_address.rs"]
mod address;
mod frame_socket;
mod frames;
mod handshake;
mod region;

#[cfg(feature = "__fuzz")]
pub use frames::FuzzedFrame;

pub(super) use address::OwnerAddress;
#[cfg(all(any(test, fuzzing), feature = "shuttle"))]
pub(in crate::cache) use frame_socket::Transport;
#[cfg(all(any(test, fuzzing), feature = "shuttle"))]
pub(super) use frame_socket::simulated::Listener;
pub(super) use frame_socket::{FrameReceiver, FrameSender, FrameSocket};
pub(super) use frames::{
    ClientFrame, FetchSpec, FrameReader, NextStep, OwnerFrame, PROTOCOL_VERSION,
};
pub(super) use handshake::Hello;
pub(super) use region::{ClientRegion, OwnerRegion, RegionSpan, SpanName};
