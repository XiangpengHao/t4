use std::collections::VecDeque;
use std::fmt;
use std::fs::File;
use std::num::NonZeroU32;
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::sync::mpsc;
use std::sync::mpsc::TryRecvError;
use std::thread;

use io_uring::{IoUring, opcode, types};

use crate::error::{Error, Result};
use crate::io::AlignedBuf;
use crate::io_task::{
    FileFsyncTask, FileReadTask, FileWalAppendTask, FileWriteTask, WalWriteOp, WorkerRequest,
    worker_disconnected_error,
};

fn worker_failed_error(message: impl Into<String>) -> Error {
    Error::Io(std::io::Error::other(message.into()))
}

fn complete_request_with_error(request: WorkerRequest, err: Error) {
    match request {
        WorkerRequest::Read { completion, .. } => completion.complete(Err(err)),
        WorkerRequest::Write { completion, .. } => completion.complete(Err(err)),
        WorkerRequest::Fsync { completion, .. } => completion.complete(Err(err)),
        WorkerRequest::WalAppend { completion, .. } => completion.complete(Err(err)),
    }
}
use crate::io_task::{FsyncCompletion, ReadCompletion, WalAppendCompletion, WriteCompletion};

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

impl UringDriver {
    fn new(queue_depth: u32) -> Result<Self> {
        Ok(Self {
            ring: IoUring::new(queue_depth)?,
        })
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

    fn submit_and_wait(&mut self, min_complete: usize) -> Result<usize> {
        Ok(self.ring.submit_and_wait(min_complete)?)
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

enum SubmittedOp {
    Read {
        buf: AlignedBuf,
        completion: ReadCompletion,
    },
    Write {
        _buf: AlignedBuf,
        completion: WriteCompletion,
    },
    Fsync {
        completion: FsyncCompletion,
    },
    WalStep {
        expected: usize,
    },
}

struct PendingWalAppend {
    writes: Vec<WalWriteOp>,
    completion: WalAppendCompletion,
}

struct ActiveWalAppend {
    remaining: usize,
    completion: WalAppendCompletion,
    error: Option<Error>,
}

fn complete_submitted_with_result(submitted: SubmittedOp, result: Result<usize>) {
    match submitted {
        SubmittedOp::Read {
            buf, completion, ..
        } => completion.complete(result.map(|n| (buf, n))),
        SubmittedOp::Write { completion, .. } => completion.complete(result),
        SubmittedOp::Fsync { completion, .. } => completion.complete(result.map(|_| ())),
        SubmittedOp::WalStep { .. } => {}
    }
}

fn complete_submitted_from_cqe(submitted: SubmittedOp, cqe_result: i32) {
    complete_submitted_with_result(submitted, decode_cqe_result(cqe_result));
}

struct UringBackend {
    receiver: mpsc::Receiver<WorkerRequest>,
    file: File,
    ring: UringDriver,
    tokens: VecDeque<usize>,
    submitted_tasks: Vec<Option<SubmittedOp>>,
    queued: VecDeque<WorkerRequest>,
    pending_wal_appends: VecDeque<PendingWalAppend>,
    active_wal_append: Option<ActiveWalAppend>,
    cqe_buf: Vec<CompletionEvent>,
    shutting_down: bool,
    inflight: usize,
}

impl UringBackend {
    fn new(file: File, queue_depth: u32, rx: mpsc::Receiver<WorkerRequest>) -> Result<Self> {
        let ring = UringDriver::new(queue_depth)?;
        let queue_depth = queue_depth as usize;

        let tokens = (0..queue_depth).collect();
        let mut submitted_tasks = Vec::with_capacity(queue_depth);
        submitted_tasks.resize_with(queue_depth, || None);

        Ok(Self {
            receiver: rx,
            file,
            ring,
            tokens,
            submitted_tasks,
            queued: VecDeque::new(),
            pending_wal_appends: VecDeque::new(),
            active_wal_append: None,
            cqe_buf: Vec::with_capacity(queue_depth),
            shutting_down: false,
            inflight: 0,
        })
    }

    fn run(mut self) {
        self.thread_loop();
    }
}

impl UringBackend {
    fn thread_loop(&mut self) {
        loop {
            self.block_for_one_request_if_idle();
            self.drain_requests();

            if let Err(err) = self.drain_submissions() {
                self.fail_all(err);
                return;
            }

            self.poll_completions();

            if self.shutting_down
                && self.queued.is_empty()
                && self.pending_wal_appends.is_empty()
                && self.active_wal_append.is_none()
                && self.inflight == 0
            {
                break;
            }

            if self.inflight > 0 && (self.queued.is_empty() || self.tokens.is_empty()) {
                if let Err(err) = self.ring.submit_and_wait(1) {
                    self.fail_all(err);
                    return;
                }
                self.poll_completions();
            }

            if self.shutting_down
                && self.queued.is_empty()
                && self.pending_wal_appends.is_empty()
                && self.active_wal_append.is_none()
                && self.inflight == 0
            {
                break;
            }
        }

        self.drain_requests();
        self.reject_queued_disconnected();
        self.reject_submitted_disconnected();
    }

    fn block_for_one_request_if_idle(&mut self) {
        if self.shutting_down
            || !self.queued.is_empty()
            || !self.pending_wal_appends.is_empty()
            || self.active_wal_append.is_some()
            || self.inflight != 0
        {
            return;
        }

        match self.receiver.recv() {
            Ok(request) => self.queued.push_back(request),
            Err(_) => self.shutting_down = true,
        }
    }

    fn drain_requests(&mut self) {
        loop {
            match self.receiver.try_recv() {
                Ok(request) if self.shutting_down => {
                    complete_request_with_error(request, worker_disconnected_error());
                }
                Ok(request) => self.queued.push_back(request),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    self.shutting_down = true;
                    break;
                }
            }
        }
    }

    fn drain_submissions(&mut self) -> Result<()> {
        let mut submitted_any = false;

        while !self.queued.is_empty() {
            let request = self.queued.pop_front().expect("queued request missing");
            match request {
                WorkerRequest::WalAppend { writes, completion } => {
                    if writes.is_empty() {
                        completion.complete(Ok(()));
                    } else {
                        self.pending_wal_appends
                            .push_back(PendingWalAppend { writes, completion });
                    }
                }
                other => {
                    if self.tokens.is_empty() {
                        self.queued.push_front(other);
                        break;
                    }
                    if self.submit_one(other)? {
                        submitted_any = true;
                    }
                }
            }
        }

        if self.submit_next_wal_step()? {
            submitted_any = true;
        }

        if submitted_any {
            let _ = self.ring.submit()?;
        }

        Ok(())
    }

    fn submit_next_wal_step(&mut self) -> Result<bool> {
        if self.active_wal_append.is_none()
            && let Some(pending) = self.pending_wal_appends.pop_front()
        {
            if self.tokens.len() < pending.writes.len() {
                self.pending_wal_appends.push_front(pending);
                return Ok(false);
            }

            let step_count = pending.writes.len();
            self.active_wal_append = Some(ActiveWalAppend {
                remaining: step_count,
                completion: pending.completion,
                error: None,
            });

            for (index, step) in pending.writes.into_iter().enumerate() {
                let token = self
                    .tokens
                    .pop_front()
                    .expect("token pool unexpectedly empty");
                let user_data = token as u64;
                let is_last = index + 1 == step_count;
                let push_result = if is_last {
                    self.ring
                        .push_write(self.file.as_raw_fd(), &step.buf, step.offset, user_data)
                } else {
                    self.ring.push_write_link(
                        self.file.as_raw_fd(),
                        &step.buf,
                        step.offset,
                        user_data,
                    )
                };

                if let Err(err) = push_result {
                    self.tokens.push_front(token);
                    if index == 0 {
                        if let Some(active) = self.active_wal_append.take() {
                            active.completion.complete(Err(err));
                        }
                    } else if let Some(active) = self.active_wal_append.as_mut() {
                        active.remaining = index;
                        if active.error.is_none() {
                            active.error = Some(err);
                        }
                    }
                    return Ok(index > 0);
                }

                let slot = self
                    .submitted_tasks
                    .get_mut(token)
                    .expect("token out of range for submitted_tasks");
                debug_assert!(slot.is_none(), "token reused before completion");
                *slot = Some(SubmittedOp::WalStep {
                    expected: step.buf.len(),
                });
                self.inflight += 1;
            }
            return Ok(true);
        }

        Ok(false)
    }

    fn complete_wal_step(&mut self, result: Result<usize>) {
        let Some(active) = self.active_wal_append.as_mut() else {
            debug_assert!(false, "wal step completion with no active wal append");
            return;
        };

        if let Err(err) = result
            && active.error.is_none()
        {
            active.error = Some(err);
        }

        if active.remaining > 0 {
            active.remaining -= 1;
        }
        if active.remaining == 0 {
            let active = self
                .active_wal_append
                .take()
                .expect("active wal append missing");
            match active.error {
                Some(err) => active.completion.complete(Err(err)),
                None => active.completion.complete(Ok(())),
            }
        }
    }

    fn submit_one(&mut self, request: WorkerRequest) -> Result<bool> {
        let mut request = match request {
            WorkerRequest::WalAppend { .. } => {
                debug_assert!(false, "wal append should not be submitted directly");
                return Ok(false);
            }
            other => other,
        };

        let token = self
            .tokens
            .pop_front()
            .expect("token pool unexpectedly empty");
        let user_data = token as u64;

        let push_result = match &mut request {
            WorkerRequest::Read { buf, offset, .. } => {
                self.ring
                    .push_read(self.file.as_raw_fd(), buf, *offset, user_data)
            }
            WorkerRequest::Write { buf, offset, .. } => {
                self.ring
                    .push_write(self.file.as_raw_fd(), buf, *offset, user_data)
            }
            WorkerRequest::Fsync { .. } => self.ring.push_fsync(self.file.as_raw_fd(), user_data),
            WorkerRequest::WalAppend { .. } => unreachable!("handled above"),
        };

        if let Err(err) = push_result {
            self.tokens.push_front(token);
            complete_request_with_error(request, err);
            return Ok(false);
        }

        let slot = self
            .submitted_tasks
            .get_mut(token)
            .expect("token out of range for submitted_tasks");
        debug_assert!(slot.is_none(), "token reused before completion");

        *slot = Some(match request {
            WorkerRequest::Read {
                buf, completion, ..
            } => SubmittedOp::Read { buf, completion },
            WorkerRequest::Write {
                buf, completion, ..
            } => SubmittedOp::Write {
                _buf: buf,
                completion,
            },
            WorkerRequest::Fsync { completion, .. } => SubmittedOp::Fsync { completion },
            WorkerRequest::WalAppend { .. } => unreachable!("handled above"),
        });
        self.inflight += 1;
        Ok(true)
    }

    fn poll_completions(&mut self) {
        self.cqe_buf.clear();
        self.ring.drain_completions(&mut self.cqe_buf);
        let completions: Vec<_> = self.cqe_buf.drain(..).collect();

        for cqe in completions {
            let token = cqe.user_data as usize;
            let Some(slot) = self.submitted_tasks.get_mut(token) else {
                debug_assert!(false, "cqe token out of range: {}", cqe.user_data);
                continue;
            };
            let Some(submitted) = slot.take() else {
                debug_assert!(false, "missing submitted task for token {}", cqe.user_data);
                continue;
            };

            self.inflight = self.inflight.saturating_sub(1);
            self.tokens.push_back(token);
            if let SubmittedOp::WalStep { expected } = submitted {
                let result = decode_cqe_result(cqe.result).and_then(|n| {
                    if n == expected {
                        Ok(n)
                    } else {
                        Err(Error::Io(std::io::Error::new(
                            std::io::ErrorKind::WriteZero,
                            format!("short write: expected {expected}, got {n}"),
                        )))
                    }
                });
                self.complete_wal_step(result);
            } else {
                complete_submitted_from_cqe(submitted, cqe.result);
            }
        }
    }

    fn fail_all(&mut self, err: Error) {
        let msg = format!("io worker failed: {err}");

        while let Some(request) = self.queued.pop_front() {
            complete_request_with_error(request, worker_failed_error(msg.clone()));
        }

        while let Some(wal) = self.pending_wal_appends.pop_front() {
            wal.completion
                .complete(Err(worker_failed_error(msg.clone())));
        }
        if let Some(wal) = self.active_wal_append.take() {
            wal.completion
                .complete(Err(worker_failed_error(msg.clone())));
        }

        for slot in &mut self.submitted_tasks {
            if let Some(submitted) = slot.take() {
                complete_submitted_with_result(submitted, Err(worker_failed_error(msg.clone())));
            }
        }
        self.inflight = 0;

        while let Ok(request) = self.receiver.try_recv() {
            complete_request_with_error(request, worker_failed_error(msg.clone()));
        }
    }

    fn reject_queued_disconnected(&mut self) {
        while let Some(request) = self.queued.pop_front() {
            complete_request_with_error(request, worker_disconnected_error());
        }
        while let Some(wal) = self.pending_wal_appends.pop_front() {
            wal.completion.complete(Err(worker_disconnected_error()));
        }
        if let Some(wal) = self.active_wal_append.take() {
            wal.completion.complete(Err(worker_disconnected_error()));
        }
    }

    fn reject_submitted_disconnected(&mut self) {
        for slot in &mut self.submitted_tasks {
            if let Some(submitted) = slot.take() {
                complete_submitted_with_result(submitted, Err(worker_disconnected_error()));
            }
        }
        self.inflight = 0;
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

    pub fn write_at(&self, buf: AlignedBuf, offset: u64) -> FileWriteTask {
        FileWriteTask::new((*self.tx).clone(), buf, offset)
    }

    pub fn fsync(&self) -> FileFsyncTask {
        FileFsyncTask::new((*self.tx).clone())
    }

    pub fn wal_append(&self, writes: Vec<WalWriteOp>) -> Result<FileWalAppendTask> {
        let completion = Arc::new(crate::io_task::TaskCompletion::new());
        let request = WorkerRequest::WalAppend {
            writes,
            completion: Arc::clone(&completion),
        };
        if self.tx.send(request).is_err() {
            return Err(worker_disconnected_error());
        }
        Ok(FileWalAppendTask::new(completion))
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

    pub async fn write_all_at(&self, buf: AlignedBuf, offset: u64) -> Result<()> {
        let expected = buf.len();
        let n = self.write_at(buf, offset).await?;
        if n != expected {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                format!("short write: expected {expected}, got {n}"),
            )));
        }
        Ok(())
    }
}
