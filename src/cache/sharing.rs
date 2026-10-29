//! One cache read by several processes (`doc/11-shared-mode.md`): the
//! process that holds the cache files' lock owns the cache and serves it to
//! the others, its clients, over a Unix socket.
//!
//! - `frames`: what the socket carries, each frame decoded and checked as
//!   it arrives.

// Used outside its tests once the socket carries the frames (step 3's
// next commit).
#![allow(dead_code)]

mod frames;

pub(super) use frames::{FrameReader, FrameWriter};
