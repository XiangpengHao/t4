use std::collections::HashMap;
use std::fs::File;
use std::os::fd::AsRawFd;
use std::sync::Arc;

use crate::buffer::AlignedBuf;
use crate::io::error::{Error, Result};
use crate::io::sync::mpsc;

use super::io_task::{FsyncCompletion, PageWrite, ReadCompletion, WorkerRequest, WriteCompletion};

fn worker_failed_error(message: impl Into<String>) -> Error {
    Error::Io(std::io::Error::other(message.into()))
}

fn complete_request_with_error(request: WorkerRequest, err: Error) {
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

fn decode_cqe_result(result: i32) -> Result<usize> {
    if result < 0 {
        return Err(Error::Io(std::io::Error::from_raw_os_error(-result)));
    }
    Ok(result as usize)
}

type RequestId = u32;

struct InflightRequest {
    remaining: usize,
    error: Option<Error>,
    kind: InflightRequestKind,
}

enum InflightRequestKind {
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
    fn complete(self) {
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

    fn complete_with_error(self, err: Error) {
        match self.kind {
            InflightRequestKind::Read { completion, .. } => completion.complete(Err(err)),
            InflightRequestKind::Write { completion, .. } => completion.complete(Err(err)),
            InflightRequestKind::Fsync { completion } => completion.complete(Err(err)),
        }
    }
}

fn encode_user_data(request_id: RequestId, op_index: usize) -> u64 {
    ((request_id as u64) << 32) | (op_index as u32 as u64)
}

fn decode_user_data(user_data: u64) -> (RequestId, usize) {
    ((user_data >> 32) as RequestId, user_data as u32 as usize)
}

fn request_op_count(request: &WorkerRequest) -> usize {
    match request {
        WorkerRequest::Read { .. } | WorkerRequest::Fsync { .. } => 1,
        WorkerRequest::Write { writes, .. } => writes.len(),
    }
}

/// FileType indicates whether our submission is using a raw file
/// descriptor or a rust std library [File] type.
#[derive(Clone)]
pub(crate) enum FileType {
    #[allow(dead_code)]
    RawFd(i32),
    File(Arc<File>),
}

/// Backend-agnostic submission record handed to a driver.
pub(crate) enum SubmissionEntry {
    Read {
        file: FileType,
        buf_ptr: *mut u8,
        buf_len: u32,
        offset: u64,
        user_data: u64,
    },
    Write {
        file: FileType,
        buf_ptr: *const u8,
        buf_len: u32,
        offset: u64,
        user_data: u64,
        /// is_last_in_batch only required for io_uring backend
        #[allow(dead_code)]
        is_last_in_batch: bool,
    },
    Fsync {
        file: FileType,
        user_data: u64,
    },
}

unsafe impl Send for SubmissionEntry {}

pub(crate) trait IoDriver {
    fn available_submission_slots(&mut self) -> usize;
    fn push(&mut self, entry: SubmissionEntry) -> Result<()>;
    /// Flush any staged submissions to the kernel. No-op for backends that
    /// dispatch eagerly inside `push`.
    fn submit(&mut self) -> Result<()>;
    fn pop_completion(&mut self) -> Option<CompletionEvent>;
    /// Block until either a new request lands on `request_rx` or some
    /// inflight op completes. Only invoked when at least one op is inflight.
    fn wait_for_progress(&mut self, request_rx: &mpsc::Receiver<WorkerRequest>);
    /// Should this IoDriver use a raw file descriptor? Generally only used
    /// for io_uring or on unix systems.
    fn use_raw_fd(&mut self) -> bool;
}

pub(crate) struct BackendLoop<D: IoDriver> {
    receiver: mpsc::Receiver<WorkerRequest>,
    file: Arc<File>,
    driver: D,
    queue_depth: usize,
    inflight_requests: HashMap<RequestId, InflightRequest>,
    next_request_id: RequestId,
}

impl<D: IoDriver> BackendLoop<D> {
    pub(crate) fn new(
        file: File,
        queue_depth: usize,
        rx: mpsc::Receiver<WorkerRequest>,
        driver: D,
    ) -> Self {
        Self {
            receiver: rx,
            file: Arc::new(file),
            driver,
            queue_depth,
            inflight_requests: HashMap::new(),
            next_request_id: 0,
        }
    }

    pub(crate) fn run(mut self) {
        let mut pending_request = None;
        loop {
            let disconnected = match self.submit_requests(&mut pending_request) {
                Ok(disconnected) => disconnected,
                Err(err) => {
                    self.fail_all(err, pending_request.take());
                    return;
                }
            };
            if disconnected {
                return;
            }

            self.poll_completions();

            if self.inflight_requests.is_empty() && pending_request.is_none() {
                match self.receiver.recv() {
                    Ok(request) => pending_request = Some(request),
                    Err(_) => return,
                }
            } else {
                self.driver.wait_for_progress(&self.receiver);
            }
        }
    }

    fn submit_requests(&mut self, pending_request: &mut Option<WorkerRequest>) -> Result<bool> {
        let mut submitted_any = false;

        loop {
            let request = match pending_request.take() {
                Some(request) => request,
                None => match self.receiver.try_recv() {
                    Ok(request) => request,
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => return Ok(true),
                },
            };
            let op_count = request_op_count(&request);
            assert!(op_count > 0, "request has no operations");
            if op_count > self.queue_depth {
                complete_request_with_error(
                    request,
                    Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "request needs {op_count} SQEs, queue depth is {}",
                            self.queue_depth
                        ),
                    )),
                );
                continue;
            }
            if self.driver.available_submission_slots() < op_count {
                *pending_request = Some(request);
                break;
            }
            if self.submit_request(request)? {
                submitted_any = true;
            }
        }

        if submitted_any {
            self.driver.submit()?;
        }

        Ok(false)
    }

    fn submit_request(&mut self, request: WorkerRequest) -> Result<bool> {
        let request_id = self.allocate_request_id();
        let file = if self.driver.use_raw_fd() {
            FileType::RawFd(self.file.as_raw_fd())
        } else {
            FileType::File(Arc::clone(&self.file))
        };
        match request {
            WorkerRequest::Read {
                buf,
                offset,
                completion,
            } => {
                self.inflight_requests.insert(
                    request_id,
                    InflightRequest {
                        remaining: 1,
                        error: None,
                        kind: InflightRequestKind::Read {
                            buf: Some(buf),
                            completion,
                            result: None,
                        },
                    },
                );

                let push_result = {
                    let request = self
                        .inflight_requests
                        .get_mut(&request_id)
                        .expect("read request missing after insert");
                    let InflightRequestKind::Read { buf, .. } = &mut request.kind else {
                        unreachable!("request kind changed while submitting read");
                    };
                    let buf = buf.as_mut().expect("read buffer missing while submitting");
                    self.driver.push(SubmissionEntry::Read {
                        file,
                        buf_ptr: buf.as_mut_ptr(),
                        buf_len: buf.len_u32(),
                        offset,
                        user_data: encode_user_data(request_id, 0),
                    })
                };
                self.finish_single_submit(request_id, push_result)
            }
            WorkerRequest::Fsync { completion } => {
                self.inflight_requests.insert(
                    request_id,
                    InflightRequest {
                        remaining: 1,
                        error: None,
                        kind: InflightRequestKind::Fsync { completion },
                    },
                );

                let push_result = self.driver.push(SubmissionEntry::Fsync {
                    file,
                    user_data: encode_user_data(request_id, 0),
                });
                self.finish_single_submit(request_id, push_result)
            }
            WorkerRequest::Write { writes, completion } => {
                let page_count = writes.len();
                self.inflight_requests.insert(
                    request_id,
                    InflightRequest {
                        remaining: page_count,
                        error: None,
                        kind: InflightRequestKind::Write {
                            pages: writes,
                            completion,
                        },
                    },
                );

                let mut submitted_pages = 0;
                for index in 0..page_count {
                    let is_last = index + 1 == page_count;
                    let push_result = {
                        let request = self
                            .inflight_requests
                            .get(&request_id)
                            .expect("write request missing after insert");
                        let InflightRequestKind::Write { pages, .. } = &request.kind else {
                            unreachable!("request kind changed while submitting write");
                        };
                        let page = pages
                            .get(index)
                            .expect("write page missing while submitting");
                        self.driver.push(SubmissionEntry::Write {
                            file: file.clone(),
                            buf_ptr: page.buf.as_ptr(),
                            buf_len: page.buf.len_u32(),
                            offset: page.offset,
                            user_data: encode_user_data(request_id, index),
                            is_last_in_batch: is_last,
                        })
                    };

                    match push_result {
                        Ok(()) => submitted_pages += 1,
                        Err(err) => {
                            self.handle_write_submit_error(request_id, submitted_pages, err);
                            return Ok(submitted_pages > 0);
                        }
                    }
                }

                Ok(true)
            }
        }
    }

    fn poll_completions(&mut self) {
        while let Some(cqe) = self.driver.pop_completion() {
            let (request_id, op_index) = decode_user_data(cqe.user_data);

            let Some(request) = self.inflight_requests.get_mut(&request_id) else {
                debug_assert!(false, "missing inflight request for cqe {}", cqe.user_data);
                continue;
            };

            match &mut request.kind {
                InflightRequestKind::Read { result, .. } => {
                    debug_assert_eq!(op_index, 0, "read request should only have op 0");
                    match decode_cqe_result(cqe.result) {
                        Ok(n) => *result = Some(n),
                        Err(err) if request.error.is_none() => request.error = Some(err),
                        Err(_) => {}
                    }
                }
                InflightRequestKind::Write { pages, .. } => {
                    let Some(page) = pages.get(op_index) else {
                        debug_assert!(false, "write page index out of range: {}", op_index);
                        continue;
                    };
                    let expected = page.buf.len();
                    let result = decode_cqe_result(cqe.result).and_then(|n| {
                        if n == expected {
                            Ok(())
                        } else {
                            Err(Error::Io(std::io::Error::new(
                                std::io::ErrorKind::WriteZero,
                                format!("short write: expected {expected}, got {n}"),
                            )))
                        }
                    });
                    if let Err(err) = result
                        && request.error.is_none()
                    {
                        request.error = Some(err);
                    }
                }
                InflightRequestKind::Fsync { .. } => {
                    debug_assert_eq!(op_index, 0, "fsync request should only have op 0");
                    if let Err(err) = decode_cqe_result(cqe.result).map(|_| ())
                        && request.error.is_none()
                    {
                        request.error = Some(err);
                    }
                }
            }

            if request.remaining > 0 {
                request.remaining -= 1;
            }
            if request.remaining == 0 {
                let request = self
                    .inflight_requests
                    .remove(&request_id)
                    .expect("inflight request missing at completion");
                request.complete();
            }
        }
    }

    fn fail_all(&mut self, err: Error, pending_request: Option<WorkerRequest>) {
        let msg = format!("io worker failed: {err}");

        if let Some(request) = pending_request {
            complete_request_with_error(request, worker_failed_error(msg.clone()));
        }

        for (_, request) in self.inflight_requests.drain() {
            request.complete_with_error(worker_failed_error(msg.clone()));
        }

        while let Ok(request) = self.receiver.try_recv() {
            complete_request_with_error(request, worker_failed_error(msg.clone()));
        }
    }

    fn finish_single_submit(
        &mut self,
        request_id: RequestId,
        push_result: Result<()>,
    ) -> Result<bool> {
        match push_result {
            Ok(()) => Ok(true),
            Err(err) => {
                let request = self
                    .inflight_requests
                    .remove(&request_id)
                    .expect("request missing after failed submit");
                request.complete_with_error(err);
                Ok(false)
            }
        }
    }

    fn handle_write_submit_error(
        &mut self,
        request_id: RequestId,
        submitted_pages: usize,
        err: Error,
    ) {
        if submitted_pages == 0 {
            let request = self
                .inflight_requests
                .remove(&request_id)
                .expect("write request missing after failed first submit");
            request.complete_with_error(err);
            return;
        }

        let request = self
            .inflight_requests
            .get_mut(&request_id)
            .expect("write request missing after partial submit");
        request.remaining = submitted_pages;
        if request.error.is_none() {
            request.error = Some(err);
        }
    }

    fn allocate_request_id(&mut self) -> RequestId {
        loop {
            self.next_request_id = self.next_request_id.wrapping_add(1);
            if !self.inflight_requests.contains_key(&self.next_request_id) {
                return self.next_request_id;
            }
        }
    }
}
