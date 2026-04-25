use crate::buffer::AlignedBuf;
use crate::io::error::{Error, Result};

use super::io_task::{FsyncCompletion, PageWrite, ReadCompletion, WorkerRequest, WriteCompletion};

pub(super) fn worker_failed_error(message: impl Into<String>) -> Error {
    Error::Io(std::io::Error::other(message.into()))
}

pub(super) fn complete_request_with_error(request: WorkerRequest, err: Error) {
    match request {
        WorkerRequest::Read { completion, .. } => completion.complete(Err(err)),
        WorkerRequest::Write { completion, .. } => completion.complete(Err(err)),
        WorkerRequest::Fsync { completion, .. } => completion.complete(Err(err)),
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CompletionEvent {
    pub(crate) user_data: u64,
    pub(crate) result: i32,
}

pub(super) fn decode_cqe_result(result: i32) -> Result<usize> {
    if result < 0 {
        return Err(Error::Io(std::io::Error::from_raw_os_error(-result)));
    }
    Ok(result as usize)
}

pub(super) type RequestId = u32;

pub(super) struct InflightRequest {
    pub(super) remaining: usize,
    pub(super) error: Option<Error>,
    pub(super) kind: InflightRequestKind,
}

pub(super) enum InflightRequestKind {
    Read {
        buf: Option<AlignedBuf>,
        completion: ReadCompletion,
        result: Option<usize>,
    },
    Write {
        pages: Vec<PageWrite>,
        completion: WriteCompletion,
    },
    Fsync {
        completion: FsyncCompletion,
    },
}

impl InflightRequest {
    pub(super) fn complete(self) {
        match self.kind {
            InflightRequestKind::Read {
                buf,
                completion,
                result,
            } => match self.error {
                Some(err) => completion.complete(Err(err)),
                None => completion.complete(Ok((
                    buf.expect("read buffer missing at completion"),
                    result.expect("read result missing at completion"),
                ))),
            },
            InflightRequestKind::Write { completion, .. } => match self.error {
                Some(err) => completion.complete(Err(err)),
                None => completion.complete(Ok(())),
            },
            InflightRequestKind::Fsync { completion } => match self.error {
                Some(err) => completion.complete(Err(err)),
                None => completion.complete(Ok(())),
            },
        }
    }

    pub(super) fn complete_with_error(self, err: Error) {
        match self.kind {
            InflightRequestKind::Read { completion, .. } => completion.complete(Err(err)),
            InflightRequestKind::Write { completion, .. } => completion.complete(Err(err)),
            InflightRequestKind::Fsync { completion } => completion.complete(Err(err)),
        }
    }
}

pub(super) fn encode_user_data(request_id: RequestId, op_index: usize) -> u64 {
    ((request_id as u64) << 32) | (op_index as u32 as u64)
}

pub(super) fn decode_user_data(user_data: u64) -> (RequestId, usize) {
    ((user_data >> 32) as RequestId, user_data as u32 as usize)
}

pub(super) fn request_op_count(request: &WorkerRequest) -> usize {
    match request {
        WorkerRequest::Read { .. } | WorkerRequest::Fsync { .. } => 1,
        WorkerRequest::Write { writes, .. } => writes.len(),
    }
}
