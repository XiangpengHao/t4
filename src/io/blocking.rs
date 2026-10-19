//! One submission, run with a blocking positional syscall: how the thread
//! pool and the simulated driver touch the file.

use std::fs::File;

use crate::io::common::{CompletionEvent, SubmissionEntry};

impl SubmissionEntry {
    /// Runs the operation against `file` and reports it as a completion
    /// queue entry would: the byte count, or a negated errno.
    pub(crate) fn execute(&self, file: &File) -> CompletionEvent {
        let (user_data, result) = match *self {
            Self::Read {
                buf_ptr,
                buf_len,
                offset,
                user_data,
            } => {
                // SAFETY: the backend loop keeps the buffer alive, and touches
                // it nowhere else, until this operation completes.
                let buf = unsafe { std::slice::from_raw_parts_mut(buf_ptr, buf_len as usize) };
                (user_data, read_at(file, buf, offset))
            }
            Self::Write {
                buf_ptr,
                buf_len,
                offset,
                user_data,
                ..
            } => {
                // SAFETY: as for reads.
                let buf = unsafe { std::slice::from_raw_parts(buf_ptr, buf_len as usize) };
                (user_data, write_at(file, buf, offset))
            }
        };
        let result = match result {
            Ok(n) => n as i32,
            // Fallback to EIO(5) if no raw_os_error
            Err(err) => -err.raw_os_error().unwrap_or(5),
        };
        CompletionEvent { user_data, result }
    }
}

#[cfg(unix)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.read_at(buf, offset)
}

#[cfg(windows)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    use std::os::windows::fs::FileExt;
    file.seek_read(buf, offset)
}

#[cfg(unix)]
fn write_at(file: &File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.write_at(buf, offset)
}

#[cfg(windows)]
fn write_at(file: &File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
    use std::os::windows::fs::FileExt;
    file.seek_write(buf, offset)
}
