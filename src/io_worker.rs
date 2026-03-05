use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::num::NonZeroU32;
use std::os::fd::AsRawFd;

use io_uring::{IoUring, opcode, types};

use crate::error::{Error, Result};
use crate::io::AlignedBuf;
use crate::io_task::{
    FileFsyncTask, FileReadTask, FileWriteTask, PageWrite, WorkerRequest, worker_disconnected_error,
};
use crate::sync::Arc;
use crate::sync::mpsc;
use crate::thread;

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
use crate::io_task::{FsyncCompletion, ReadCompletion, WriteCompletion};

#[derive(Debug, Clone, Copy)]
struct CompletionEvent {
    user_data: u64,
    result: i32,
}

fn decode_cqe_result(result: i32) -> Result<usize> {
    if result < 0 {
        return Err(Error::Io(std::io::Error::from_raw_os_error(-result)));
    }
    Ok(result as usize)
}

struct UringDriver {
    ring: IoUring,
}

trait IoDriver {
    fn available_submission_slots(&mut self) -> usize;
    fn push(&mut self, entry: io_uring::squeue::Entry) -> Result<()>;
    fn submit(&mut self) -> Result<usize>;
    fn pop_completion(&mut self) -> Option<CompletionEvent>;
}

struct ReadEntry<'a> {
    fd: i32,
    buf: &'a mut AlignedBuf,
    offset: u64,
    user_data: u64,
}

impl From<ReadEntry<'_>> for io_uring::squeue::Entry {
    fn from(value: ReadEntry<'_>) -> Self {
        opcode::Read::new(
            types::Fd(value.fd),
            value.buf.as_mut_ptr(),
            value.buf.len_u32(),
        )
        .offset(value.offset)
        .build()
        .user_data(value.user_data)
    }
}

struct WriteEntry<'a> {
    fd: i32,
    buf: &'a AlignedBuf,
    offset: u64,
    user_data: u64,
    flags: io_uring::squeue::Flags,
}

impl From<WriteEntry<'_>> for io_uring::squeue::Entry {
    fn from(value: WriteEntry<'_>) -> Self {
        opcode::Write::new(types::Fd(value.fd), value.buf.as_ptr(), value.buf.len_u32())
            .offset(value.offset)
            .build()
            .flags(value.flags)
            .user_data(value.user_data)
    }
}

struct FsyncEntry {
    fd: i32,
    user_data: u64,
}

impl From<FsyncEntry> for io_uring::squeue::Entry {
    fn from(value: FsyncEntry) -> Self {
        opcode::Fsync::new(types::Fd(value.fd))
            .build()
            .user_data(value.user_data)
    }
}

impl UringDriver {
    fn new(queue_depth: u32) -> Result<Self> {
        Ok(Self {
            ring: IoUring::new(queue_depth)?,
        })
    }

    fn push_entry(&mut self, entry: io_uring::squeue::Entry) -> Result<()> {
        let mut sq = self.ring.submission();
        unsafe {
            sq.push(&entry).map_err(|_| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "submission queue is full",
                ))
            })?;
        }
        Ok(())
    }
}

impl IoDriver for UringDriver {
    fn available_submission_slots(&mut self) -> usize {
        let sq = self.ring.submission();
        sq.capacity() - sq.len()
    }

    fn push(&mut self, entry: io_uring::squeue::Entry) -> Result<()> {
        self.push_entry(entry)
    }

    fn submit(&mut self) -> Result<usize> {
        Ok(self.ring.submit()?)
    }

    fn pop_completion(&mut self) -> Option<CompletionEvent> {
        let mut cq = self.ring.completion();
        cq.next().map(|cqe| CompletionEvent {
            user_data: cqe.user_data(),
            result: cqe.result(),
        })
    }
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

struct UringBackend<D: IoDriver> {
    receiver: mpsc::Receiver<WorkerRequest>,
    file: File,
    ring: D,
    queue_depth: usize,
    inflight_requests: HashMap<RequestId, InflightRequest>,
    next_request_id: RequestId,
}

impl UringBackend<UringDriver> {
    fn new(file: File, queue_depth: u32, rx: mpsc::Receiver<WorkerRequest>) -> Result<Self> {
        let ring = UringDriver::new(queue_depth)?;
        Ok(UringBackend::with_driver(
            file,
            queue_depth as usize,
            rx,
            ring,
        ))
    }
}

impl<D: IoDriver> UringBackend<D> {
    fn with_driver(
        file: File,
        queue_depth: usize,
        rx: mpsc::Receiver<WorkerRequest>,
        ring: D,
    ) -> Self {
        Self {
            receiver: rx,
            file,
            ring,
            queue_depth,
            inflight_requests: HashMap::new(),
            next_request_id: 0,
        }
    }

    fn run(mut self) {
        self.thread_loop();
    }
}

impl<D: IoDriver> UringBackend<D> {
    fn thread_loop(&mut self) {
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

            thread::cooperative_yield();
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
            if self.ring.available_submission_slots() < op_count {
                *pending_request = Some(request);
                break;
            }
            if self.submit_request(request)? {
                submitted_any = true;
            }
        }

        if submitted_any {
            let _ = self.ring.submit()?;
        }

        Ok(false)
    }

    fn submit_request(&mut self, request: WorkerRequest) -> Result<bool> {
        let request_id = self.allocate_request_id();
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
                    self.ring.push(
                        ReadEntry {
                            fd: self.file.as_raw_fd(),
                            buf: buf.as_mut().expect("read buffer missing while submitting"),
                            offset,
                            user_data: encode_user_data(request_id, 0),
                        }
                        .into(),
                    )
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

                let push_result = self.ring.push(
                    FsyncEntry {
                        fd: self.file.as_raw_fd(),
                        user_data: encode_user_data(request_id, 0),
                    }
                    .into(),
                );
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
                        self.ring.push(
                            WriteEntry {
                                fd: self.file.as_raw_fd(),
                                buf: &page.buf,
                                offset: page.offset,
                                user_data: encode_user_data(request_id, index),
                                flags: if is_last {
                                    io_uring::squeue::Flags::empty()
                                } else {
                                    io_uring::squeue::Flags::IO_LINK
                                },
                            }
                            .into(),
                        )
                    };

                    match push_result {
                        Ok(()) => {
                            submitted_pages += 1;
                        }
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
        while let Some(cqe) = self.ring.pop_completion() {
            let (request_id, op_index) = decode_user_data(cqe.user_data);
            let mut should_complete = false;

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
                should_complete = true;
            }

            if should_complete {
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

fn request_op_count(request: &WorkerRequest) -> usize {
    match request {
        WorkerRequest::Read { .. } | WorkerRequest::Fsync { .. } => 1,
        WorkerRequest::Write { writes, .. } => writes.len(),
    }
}

/// Handle to the I/O worker thread.
///
/// Cloning shares the same underlying worker. The worker thread exits
/// automatically once every clone is dropped (channel disconnects).
#[derive(Clone)]
pub struct IoWorker {
    tx: Arc<mpsc::Sender<WorkerRequest>>,
}

impl fmt::Debug for IoWorker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoWorker").finish_non_exhaustive()
    }
}

impl IoWorker {
    pub fn new(queue_depth: NonZeroU32, file: File) -> Result<Self> {
        let queue_depth = queue_depth.get();

        let (tx, rx) = mpsc::channel::<WorkerRequest>();
        let (init_tx, init_rx) = mpsc::sync_channel::<Result<()>>(1);

        thread::spawn(move || {
            let backend = match UringBackend::new(file, queue_depth, rx) {
                Ok(backend) => {
                    let _ = init_tx.send(Ok(()));
                    backend
                }
                Err(err) => {
                    let _ = init_tx.send(Err(err));
                    return;
                }
            };
            backend.run();
        });

        match init_rx.recv() {
            Ok(Ok(())) => Ok(Self { tx: Arc::new(tx) }),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(worker_disconnected_error()),
        }
    }

    pub fn read_at(&self, buf: AlignedBuf, offset: u64) -> FileReadTask {
        FileReadTask::new((*self.tx).clone(), buf, offset)
    }

    pub fn write(&self, writes: Vec<PageWrite>) -> FileWriteTask {
        FileWriteTask::new((*self.tx).clone(), writes)
    }

    pub fn fsync(&self) -> FileFsyncTask {
        FileFsyncTask::new((*self.tx).clone())
    }

    pub async fn read_exact_at(&self, buf: AlignedBuf, offset: u64) -> Result<AlignedBuf> {
        let expected = buf.len();
        let (buf, n) = self.read_at(buf, offset).await?;
        if n != expected {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("short read: expected {expected}, got {n}"),
            )));
        }
        Ok(buf)
    }
}
