#[cfg(not(all(test, feature = "shuttle")))]
mod address;
#[cfg(all(test, feature = "shuttle"))]
#[path = "simulated_address.rs"]
mod address;
mod frame_socket;
mod frames;
mod handshake;
mod region;

pub(super) use address::OwnerAddress;
#[cfg(all(test, feature = "shuttle"))]
pub(in crate::cache) use frame_socket::Transport;
#[cfg(all(test, feature = "shuttle"))]
pub(super) use frame_socket::simulated::Listener;
pub(super) use frame_socket::{FrameReceiver, FrameSender, FrameSocket};
pub(super) use frames::{
    ClientFrame, FetchSpec, FrameReader, NextStep, OwnerFrame, PROTOCOL_VERSION,
};
pub(super) use handshake::Hello;
pub(super) use region::{ClientRegion, OwnerRegion, RegionSpan, SpanName};
