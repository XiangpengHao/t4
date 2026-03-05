use std::collections::{HashMap, VecDeque};
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
    fn push_read(
        &mut self,
        fd: i32,
        buf: &mut AlignedBuf,
        offset: u64,
        user_data: u64,
    ) -> Result<()>;
    fn push_write(&mut self, fd: i32, buf: &AlignedBuf, offset: u64, user_data: u64) -> Result<()>;
    fn push_write_link(
        &mut self,
        fd: i32,
        buf: &AlignedBuf,
        offset: u64,
        user_data: u64,
    ) -> Result<()>;
    fn push_fsync(&mut self, fd: i32, user_data: u64) -> Result<()>;
    fn submit(&mut self) -> Result<usize>;
    fn drain_completions(&mut self, out: &mut Vec<CompletionEvent>);
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

    fn push_read(
        &mut self,
        fd: i32,
        buf: &mut AlignedBuf,
        offset: u64,
        user_data: u64,
    ) -> Result<()> {
        let len_u32 = buf.len_u32();
        let entry = opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), len_u32)
            .offset(offset)
            .build()
            .user_data(user_data);
        self.push_entry(entry)
    }

    fn push_write(&mut self, fd: i32, buf: &AlignedBuf, offset: u64, user_data: u64) -> Result<()> {
        let len_u32 = buf.len_u32();
        let entry = opcode::Write::new(types::Fd(fd), buf.as_ptr(), len_u32)
            .offset(offset)
            .build()
            .user_data(user_data);
        self.push_entry(entry)
    }

    fn push_write_link(
        &mut self,
        fd: i32,
        buf: &AlignedBuf,
        offset: u64,
        user_data: u64,
    ) -> Result<()> {
        let len_u32 = buf.len_u32();
        let entry = opcode::Write::new(types::Fd(fd), buf.as_ptr(), len_u32)
            .offset(offset)
            .build()
            .flags(io_uring::squeue::Flags::IO_LINK)
            .user_data(user_data);
        self.push_entry(entry)
    }

    fn push_fsync(&mut self, fd: i32, user_data: u64) -> Result<()> {
        let entry = opcode::Fsync::new(types::Fd(fd))
            .build()
            .user_data(user_data);
        self.push_entry(entry)
    }

    fn submit(&mut self) -> Result<usize> {
        Ok(self.ring.submit()?)
    }

    fn drain_completions(&mut self, out: &mut Vec<CompletionEvent>) {
        let cq = self.ring.completion();
        for cqe in cq {
            out.push(CompletionEvent {
                user_data: cqe.user_data(),
                result: cqe.result(),
            });
        }
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
    queued: VecDeque<WorkerRequest>,
    inflight_requests: HashMap<RequestId, InflightRequest>,
    next_request_id: RequestId,
    cqe_buf: Vec<CompletionEvent>,
    shutting_down: bool,
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
            queued: VecDeque::new(),
            inflight_requests: HashMap::new(),
            next_request_id: 0,
            cqe_buf: Vec::with_capacity(queue_depth),
            shutting_down: false,
        }
    }

    fn run(mut self) {
        self.thread_loop();
    }
}

impl<D: IoDriver> UringBackend<D> {
    fn should_exit(&self) -> bool {
        self.shutting_down && self.queued.is_empty() && self.inflight_requests.is_empty()
    }

    fn thread_loop(&mut self) {
        loop {
            self.drain_requests();

            if let Err(err) = self.drain_submissions() {
                self.fail_all(err);
                return;
            }

            self.poll_completions();

            thread::cooperative_yield();

            if self.should_exit() {
                break;
            }
        }

        self.drain_requests();
        self.reject_queued_disconnected();
        self.reject_inflight_disconnected();
    }

    fn drain_requests(&mut self) {
        loop {
            match self.receiver.try_recv() {
                Ok(request) if self.shutting_down => {
                    complete_request_with_error(request, worker_disconnected_error());
                }
                Ok(request) => self.queued.push_back(request),
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    self.shutting_down = true;
                    break;
                }
            }
        }
    }

    fn drain_submissions(&mut self) -> Result<bool> {
        let mut submitted_any = false;

        while let Some(request) = self.queued.pop_front() {
            let op_count = request_op_count(&request);
            if op_count == 0 {
                complete_empty_request(request);
                continue;
            }
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
                self.queued.push_front(request);
                break;
            }
            if self.submit_request(request)? {
                submitted_any = true;
            }
        }

        if submitted_any {
            let _ = self.ring.submit()?;
        }

        Ok(submitted_any)
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
                    self.ring.push_read(
                        self.file.as_raw_fd(),
                        buf.as_mut().expect("read buffer missing while submitting"),
                        offset,
                        encode_user_data(request_id, 0),
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

                let push_result = self
                    .ring
                    .push_fsync(self.file.as_raw_fd(), encode_user_data(request_id, 0));
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
                        if is_last {
                            self.ring.push_write(
                                self.file.as_raw_fd(),
                                &page.buf,
                                page.offset,
                                encode_user_data(request_id, index),
                            )
                        } else {
                            self.ring.push_write_link(
                                self.file.as_raw_fd(),
                                &page.buf,
                                page.offset,
                                encode_user_data(request_id, index),
                            )
                        }
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
        self.cqe_buf.clear();
        self.ring.drain_completions(&mut self.cqe_buf);
        let completions: Vec<_> = self.cqe_buf.drain(..).collect();

        for cqe in completions {
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

    fn fail_all(&mut self, err: Error) {
        let msg = format!("io worker failed: {err}");

        while let Some(request) = self.queued.pop_front() {
            complete_request_with_error(request, worker_failed_error(msg.clone()));
        }

        for (_, request) in self.inflight_requests.drain() {
            request.complete_with_error(worker_failed_error(msg.clone()));
        }

        while let Ok(request) = self.receiver.try_recv() {
            complete_request_with_error(request, worker_failed_error(msg.clone()));
        }
    }

    fn reject_queued_disconnected(&mut self) {
        while let Some(request) = self.queued.pop_front() {
            complete_request_with_error(request, worker_disconnected_error());
        }
        self.reject_inflight_disconnected();
    }

    fn reject_inflight_disconnected(&mut self) {
        for (_, request) in self.inflight_requests.drain() {
            request.complete_with_error(worker_disconnected_error());
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

fn complete_empty_request(request: WorkerRequest) {
    match request {
        WorkerRequest::Write { completion, .. } => completion.complete(Ok(())),
        WorkerRequest::Read { .. } | WorkerRequest::Fsync { .. } => {
            debug_assert!(false, "only writes should be empty")
        }
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
