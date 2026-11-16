#![no_main]

//! Shared mode's frames from bytes a peer could send: each is refused, or
//! decodes to a frame that encodes back to the same bytes, as a client's
//! frame and as an owner's.

use libfuzzer_sys::fuzz_target;
use t4::__fuzz::FuzzedFrame;

fuzz_target!(|body: &[u8]| {
    let frame = FuzzedFrame::new(body);
    frame.check_as_client_frame();
    frame.check_as_owner_frame();
});
