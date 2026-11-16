//! The frames' codec on bytes a peer could send, for the fuzz target `frames`.

use std::fmt::Debug;

use bytes::Bytes;

use super::{ClientFrame, OwnerFrame};

pub struct FuzzedFrame {
    body: Bytes,
}

impl FuzzedFrame {
    pub fn new(body: &[u8]) -> Self {
        Self {
            body: Bytes::copy_from_slice(body),
        }
    }

    /// Panics unless the body is refused as a client's frame, or encodes
    /// back from the frame it decodes to.
    pub fn check_as_client_frame(&self) {
        if let Ok(frame) = ClientFrame::decode(self.body.clone()) {
            self.assert_encoded(&frame.encode(), &frame);
        }
    }

    /// As [`Self::check_as_client_frame`], for an owner's frame.
    pub fn check_as_owner_frame(&self) {
        if let Ok(frame) = OwnerFrame::decode(self.body.clone()) {
            self.assert_encoded(&frame.encode(), &frame);
        }
    }

    fn assert_encoded(&self, encoded: &[u8], frame: &impl Debug) {
        let (length, body) = encoded.split_at(size_of::<u64>());
        let expected = (self.body.len() as u64).to_le_bytes();
        assert_eq!(length, expected, "{frame:?}'s length");
        assert_eq!(body, self.body, "{frame:?} encodes to other bytes");
    }
}
