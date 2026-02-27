use std::fmt;
use std::fs::File;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;

use crate::error::{Error, Result};
use crate::io::AlignedBuf;
use crate::uring_task::{
    FileFsyncTask, FileReadTask, FileWriteTask, WorkerRequest, worker_disconnected_error,
};

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

#[cfg(target_os = "linux")]
mod linux_impl {
    use std::collections::VecDeque;
    use std::os::fd::AsRawFd;
    use std::sync::mpsc;
    use std::sync::mpsc::TryRecvError;

    use io_uring::{IoUring, opcode, types};

    use super::{
        File, complete_request_with_error, worker_disconnected_error, worker_failed_error,
    };
    use crate::error::{Error, Result};
    use crate::io::AlignedBuf;
    use crate::uring_task::{FsyncCompletion, ReadCompletion, WorkerRequest, WriteCompletion};

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
            if queue_depth == 0 {
                return Err(Error::InvalidArgument("queue_depth must be > 0"));
            }
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
            let len_u32: u32 = buf
                .len()
                .try_into()
                .map_err(|_| Error::InvalidArgument("read buffer exceeds u32"))?;
            let entry = opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), len_u32)
                .offset(offset)
                .build()
                .user_data(user_data);
            self.push_entry(entry)
        }

        fn push_write(
            &mut self,
            fd: i32,
            buf: &AlignedBuf,
            offset: u64,
            user_data: u64,
        ) -> Result<()> {
            let len_u32: u32 = buf
                .len()
                .try_into()
                .map_err(|_| Error::InvalidArgument("write buffer exceeds u32"))?;
            let entry = opcode::Write::new(types::Fd(fd), buf.as_ptr(), len_u32)
                .offset(offset)
                .build()
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
            let mut cq = self.ring.completion();
            while let Some(cqe) = cq.next() {
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
    }

    fn complete_submitted_with_result(submitted: SubmittedOp, result: Result<usize>) {
        match submitted {
            SubmittedOp::Read {
                buf, completion, ..
            } => completion.complete(result.map(|n| (buf, n))),
            SubmittedOp::Write { completion, .. } => completion.complete(result),
            SubmittedOp::Fsync { completion, .. } => completion.complete(result.map(|_| ())),
        }
    }

    fn complete_submitted_from_cqe(submitted: SubmittedOp, cqe_result: i32) {
        complete_submitted_with_result(submitted, decode_cqe_result(cqe_result));
    }

    struct WorkerThread {
        receiver: mpsc::Receiver<WorkerRequest>,
        file: File,
        ring: UringDriver,
        tokens: VecDeque<usize>,
        submitted_tasks: Vec<Option<SubmittedOp>>,
        queued: VecDeque<WorkerRequest>,
        cqe_buf: Vec<CompletionEvent>,
        shutting_down: bool,
        inflight: usize,
    }

    impl WorkerThread {
        fn new(
            receiver: mpsc::Receiver<WorkerRequest>,
            file: File,
            queue_depth: u32,
        ) -> Result<Self> {
            let ring = UringDriver::new(queue_depth)?;
            let queue_depth = queue_depth as usize;

            let tokens = (0..queue_depth).collect();
            let mut submitted_tasks = Vec::with_capacity(queue_depth);
            submitted_tasks.resize_with(queue_depth, || None);

            Ok(Self {
                receiver,
                file,
                ring,
                tokens,
                submitted_tasks,
                queued: VecDeque::new(),
                cqe_buf: Vec::with_capacity(queue_depth),
                shutting_down: false,
                inflight: 0,
            })
        }

        fn thread_loop(&mut self) {
            loop {
                self.block_for_one_request_if_idle();
                self.drain_requests();

                if let Err(err) = self.drain_submissions() {
                    self.fail_all(err);
                    return;
                }

                self.poll_completions();

                if self.shutting_down && self.queued.is_empty() && self.inflight == 0 {
                    break;
                }

                if self.inflight > 0 && (self.queued.is_empty() || self.tokens.is_empty()) {
                    if let Err(err) = self.ring.submit_and_wait(1) {
                        self.fail_all(err);
                        return;
                    }
                    self.poll_completions();
                }

                if self.shutting_down && self.queued.is_empty() && self.inflight == 0 {
                    break;
                }
            }

            self.drain_requests();
            self.reject_queued_disconnected();
            self.reject_submitted_disconnected();
        }

        fn block_for_one_request_if_idle(&mut self) {
            if self.shutting_down || !self.queued.is_empty() || self.inflight != 0 {
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

            while !self.queued.is_empty() && !self.tokens.is_empty() {
                let request = self.queued.pop_front().expect("queued request missing");
                if self.submit_one(request)? {
                    submitted_any = true;
                }
            }

            if submitted_any {
                let _ = self.ring.submit()?;
            }

            Ok(())
        }

        fn submit_one(&mut self, request: WorkerRequest) -> Result<bool> {
            let mut request = match request {
                WorkerRequest::Read {
                    buf, completion, ..
                } if buf.is_empty() => {
                    completion.complete(Ok((buf, 0)));
                    return Ok(false);
                }
                WorkerRequest::Write {
                    buf, completion, ..
                } if buf.is_empty() => {
                    let _keep_alive = buf;
                    completion.complete(Ok(0));
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
                WorkerRequest::Fsync { .. } => {
                    self.ring.push_fsync(self.file.as_raw_fd(), user_data)
                }
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
            });
            self.inflight += 1;
            Ok(true)
        }

        fn poll_completions(&mut self) {
            self.cqe_buf.clear();
            self.ring.drain_completions(&mut self.cqe_buf);

            for cqe in self.cqe_buf.drain(..) {
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
                complete_submitted_from_cqe(submitted, cqe.result);
            }
        }

        fn fail_all(&mut self, err: Error) {
            let msg = format!("io worker failed: {err}");

            while let Some(request) = self.queued.pop_front() {
                complete_request_with_error(request, worker_failed_error(msg.clone()));
            }

            for slot in &mut self.submitted_tasks {
                if let Some(submitted) = slot.take() {
                    complete_submitted_with_result(
                        submitted,
                        Err(worker_failed_error(msg.clone())),
                    );
                }
            }
            self.inflight = 0;

            loop {
                match self.receiver.try_recv() {
                    Ok(request) => {
                        complete_request_with_error(request, worker_failed_error(msg.clone()))
                    }
                    Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
                }
            }
        }

        fn reject_queued_disconnected(&mut self) {
            while let Some(request) = self.queued.pop_front() {
                complete_request_with_error(request, worker_disconnected_error());
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

    pub(super) fn worker_main(
        queue_depth: u32,
        file: File,
        rx: mpsc::Receiver<WorkerRequest>,
        init_tx: mpsc::SyncSender<Result<()>>,
    ) {
        let mut worker = match WorkerThread::new(rx, file, queue_depth) {
            Ok(worker) => {
                let _ = init_tx.send(Ok(()));
                worker
            }
            Err(err) => {
                let _ = init_tx.send(Err(err));
                return;
            }
        };

        worker.thread_loop();
    }
}

#[cfg(not(target_os = "linux"))]
fn blocking_read_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    use std::io::{Read, Seek, SeekFrom};
    let mut clone = file.try_clone()?;
    clone.seek(SeekFrom::Start(offset))?;
    clone.read(buf)
}

#[cfg(not(target_os = "linux"))]
fn blocking_write_at(file: &File, buf: &[u8], offset: u64) -> std::io::Result<usize> {
    use std::io::{Seek, SeekFrom, Write};
    let mut clone = file.try_clone()?;
    clone.seek(SeekFrom::Start(offset))?;
    clone.write(buf)
}

#[cfg(not(target_os = "linux"))]
fn worker_main(
    file: File,
    _queue_depth: u32,
    rx: mpsc::Receiver<WorkerRequest>,
    init_tx: mpsc::SyncSender<Result<()>>,
) {
    let _ = init_tx.send(Ok(()));

    while let Ok(request) = rx.recv() {
        match request {
            WorkerRequest::Read {
                mut buf,
                offset,
                completion,
            } => {
                let result = blocking_read_at(&file, buf.as_mut_slice(), offset)
                    .map_err(Error::from)
                    .map(|n| (buf, n));
                completion.complete(result);
            }
            WorkerRequest::Write {
                buf,
                offset,
                completion,
            } => {
                let result = blocking_write_at(&file, buf.as_slice(), offset).map_err(Error::from);
                completion.complete(result);
            }
            WorkerRequest::Fsync { completion } => {
                completion.complete(file.sync_all().map_err(Error::from));
            }
        }
    }

    while let Ok(request) = rx.try_recv() {
        complete_request_with_error(request, worker_disconnected_error());
    }
}

#[cfg(target_os = "linux")]
fn worker_main(
    file: File,
    queue_depth: u32,
    rx: mpsc::Receiver<WorkerRequest>,
    init_tx: mpsc::SyncSender<Result<()>>,
) {
    linux_impl::worker_main(queue_depth, file, rx, init_tx);
}

/// Handle to the io_uring worker thread.
///
/// Cloning shares the same underlying worker. The worker thread exits
/// automatically once every clone is dropped (channel disconnects).
#[derive(Clone)]
pub struct UringWorker {
    tx: Arc<mpsc::Sender<WorkerRequest>>,
}

impl fmt::Debug for UringWorker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UringWorker").finish_non_exhaustive()
    }
}

impl UringWorker {
    pub fn new(queue_depth: u32, file: File) -> Result<Self> {
        if queue_depth == 0 {
            return Err(Error::InvalidArgument("queue_depth must be > 0"));
        }

        let (tx, rx) = mpsc::channel::<WorkerRequest>();
        let (init_tx, init_rx) = mpsc::sync_channel::<Result<()>>(1);

        thread::spawn(move || worker_main(file, queue_depth, rx, init_tx));

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

    pub async fn read_exact_at(&self, buf: AlignedBuf, offset: u64) -> Result<AlignedBuf> {
        if buf.is_empty() {
            return Ok(buf);
        }

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
        if buf.is_empty() {
            return Ok(());
        }

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
