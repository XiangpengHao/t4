#![no_main]

use libfuzzer_sys::fuzz_target;
use t4::__fuzz::FuzzedFrame;

fuzz_target!(|body: &[u8]| {
    let frame = FuzzedFrame::new(body);
    frame.check_as_client_frame();
    frame.check_as_owner_frame();
});
