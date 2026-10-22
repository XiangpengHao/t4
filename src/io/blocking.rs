//! One submission, run with a blocking positional syscall: how the thread
//! pool touches the files.

use std::fs::File;

use crate::io::common::{CompletionEvent, SubmissionEntry};
use crate::io::io_task::FilePosition;

impl SubmissionEntry {
    /// Runs the operation against its file of `files` and reports it as a
    /// completion queue entry would: the byte count, or a negated errno.
    pub(crate) fn execute(&self, files: &[File]) -> CompletionEvent {
        let (user_data, result) = match *self {
            Self::Read {
                buf_ptr,
                buf_len,
                at,
                user_data,
            } => {
                // SAFETY: the backend loop keeps the buffer alive, and touches
                // it nowhere else, until this operation completes.
                let buf = unsafe { std::slice::from_raw_parts_mut(buf_ptr, buf_len as usize) };
                (
                    user_data,
                    Self::file(files, at).and_then(|file| read_at(file, buf, at.offset())),
                )
            }
            Self::Write {
                buf_ptr,
                buf_len,
                at,
                user_data,
                ..
            } => {
                // SAFETY: as for reads.
                let buf = unsafe { std::slice::from_raw_parts(buf_ptr, buf_len as usize) };
                (
                    user_data,
                    Self::file(files, at).and_then(|file| write_at(file, buf, at.offset())),
                )
            }
        };
        let result = match result {
            Ok(n) => n as i32,
            // Fallback to EIO(5) if no raw_os_error
            Err(err) => -err.raw_os_error().unwrap_or(5),
        };
        CompletionEvent { user_data, result }
    }

    /// The file `at` names, or `EBADF` if `files` has none there.
    fn file(files: &[File], at: FilePosition) -> std::io::Result<&File> {
        files
            .get(at.file() as usize)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EBADF))
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
