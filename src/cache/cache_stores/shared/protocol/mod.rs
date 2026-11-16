#[cfg(not(all(feature = "shuttle", any(test, t4_simulated))))]
mod address;
#[cfg(all(feature = "shuttle", any(test, t4_simulated)))]
#[path = "simulated_address.rs"]
mod address;
mod frame_socket;
mod frames;
mod handshake;
mod region;

#[cfg(feature = "__fuzz")]
pub use frames::FuzzedFrame;

pub(super) use address::OwnerAddress;
#[cfg(all(feature = "shuttle", any(test, t4_simulated)))]
pub(in crate::cache) use frame_socket::Transport;
#[cfg(all(feature = "shuttle", any(test, t4_simulated)))]
pub(super) use frame_socket::simulated::Listener;
pub(super) use frame_socket::{FrameReceiver, FrameSender, FrameSocket};
pub(super) use frames::{
    ClientFrame, FetchSpec, FrameReader, NextStep, OwnerFrame, PROTOCOL_VERSION,
};
pub(super) use handshake::Hello;
pub(super) use region::{ClientRegion, OwnerRegion, RegionSpan, SpanName};
