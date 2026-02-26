use std::future::Future;
use std::os::fd::RawFd;
use std::pin::Pin;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use crate::error::{Error, Result};
use crate::io::AlignedBuf;

pub(crate) type ReadCompletion = Arc<TaskCompletion<(AlignedBuf, usize)>>;
pub(crate) type WriteCompletion = Arc<TaskCompletion<usize>>;
pub(crate) type FsyncCompletion = Arc<TaskCompletion<()>>;

pub(crate) struct TaskCompletion<T> {
    inner: Mutex<TaskCompletionState<T>>,
}

enum TaskCompletionState<T> {
    Pending { waker: Waker },
    Ready(Result<T>),
    Consumed,
}

impl<T> TaskCompletion<T> {
    pub(crate) fn new(waker: Waker) -> Self {
        Self {
            inner: Mutex::new(TaskCompletionState::Pending { waker }),
        }
    }

    pub(crate) fn complete(&self, result: Result<T>) {
        let waker = match std::mem::replace(
            &mut *self
                .inner
                .lock()
                .expect("task completion mutex poisoned while completing"),
            TaskCompletionState::Ready(result),
        ) {
            TaskCompletionState::Pending { waker } => Some(waker),
            TaskCompletionState::Ready(_) => panic!("task completion completed twice"),
            TaskCompletionState::Consumed => {
                panic!("task completion completed after result was consumed")
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    pub(crate) fn poll_result(&self, cx: &mut Context<'_>) -> Poll<Result<T>> {
        let mut inner = self
            .inner
            .lock()
            .expect("task completion mutex poisoned while polling");
        match &mut *inner {
            TaskCompletionState::Pending { waker } => {
                if !waker.will_wake(cx.waker()) {
                    *waker = cx.waker().clone();
                }
                Poll::Pending
            }
            TaskCompletionState::Ready(_) => {
                let TaskCompletionState::Ready(result) =
                    std::mem::replace(&mut *inner, TaskCompletionState::Consumed)
                else {
                    unreachable!("state changed while polling completion");
                };
                Poll::Ready(result)
            }
            TaskCompletionState::Consumed => panic!("task completion polled after result consumed"),
        }
    }
}

pub(crate) fn worker_disconnected_error() -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "io worker thread is not running",
    ))
}

pub(crate) enum WorkerRequest {
    Read {
        fd: RawFd,
        buf: AlignedBuf,
        offset: u64,
        completion: ReadCompletion,
    },
    Write {
        fd: RawFd,
        buf: AlignedBuf,
        offset: u64,
        completion: WriteCompletion,
    },
    Fsync {
        fd: RawFd,
        completion: FsyncCompletion,
    },
    Shutdown,
}

#[derive(Debug)]
struct PendingRead {
    tx: mpsc::Sender<WorkerRequest>,
    fd: RawFd,
    buf: Option<AlignedBuf>,
    offset: u64,
}

pub(crate) struct FileReadTask {
    state: FileReadTaskState,
}

enum FileReadTaskState {
    Init(PendingRead),
    Waiting(ReadCompletion),
    Done,
}

impl FileReadTask {
    pub(crate) fn new(
        tx: mpsc::Sender<WorkerRequest>,
        fd: RawFd,
        buf: AlignedBuf,
        offset: u64,
    ) -> Self {
        Self {
            state: FileReadTaskState::Init(PendingRead {
                tx,
                fd,
                buf: Some(buf),
                offset,
            }),
        }
    }
}

impl Future for FileReadTask {
    type Output = Result<(AlignedBuf, usize)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            match &mut this.state {
                FileReadTaskState::Init(pending) => {
                    let completion = Arc::new(TaskCompletion::new(cx.waker().clone()));
                    let request = WorkerRequest::Read {
                        fd: pending.fd,
                        buf: pending.buf.take().expect("read task buffer missing"),
                        offset: pending.offset,
                        completion: Arc::clone(&completion),
                    };
                    if pending.tx.send(request).is_err() {
                        this.state = FileReadTaskState::Done;
                        return Poll::Ready(Err(worker_disconnected_error()));
                    }
                    this.state = FileReadTaskState::Waiting(completion);
                }
                FileReadTaskState::Waiting(completion) => {
                    let completion = Arc::clone(completion);
                    let poll = completion.poll_result(cx);
                    if poll.is_ready() {
                        this.state = FileReadTaskState::Done;
                    }
                    return poll;
                }
                FileReadTaskState::Done => panic!("FileReadTask polled after completion"),
            }
        }
    }
}

#[derive(Debug)]
struct PendingWrite {
    tx: mpsc::Sender<WorkerRequest>,
    fd: RawFd,
    buf: Option<AlignedBuf>,
    offset: u64,
}

pub(crate) struct FileWriteTask {
    state: FileWriteTaskState,
}

enum FileWriteTaskState {
    Init(PendingWrite),
    Waiting(WriteCompletion),
    Done,
}

impl FileWriteTask {
    pub(crate) fn new(
        tx: mpsc::Sender<WorkerRequest>,
        fd: RawFd,
        buf: AlignedBuf,
        offset: u64,
    ) -> Self {
        Self {
            state: FileWriteTaskState::Init(PendingWrite {
                tx,
                fd,
                buf: Some(buf),
                offset,
            }),
        }
    }
}

impl Future for FileWriteTask {
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            match &mut this.state {
                FileWriteTaskState::Init(pending) => {
                    let completion = Arc::new(TaskCompletion::new(cx.waker().clone()));
                    let request = WorkerRequest::Write {
                        fd: pending.fd,
                        buf: pending.buf.take().expect("write task buffer missing"),
                        offset: pending.offset,
                        completion: Arc::clone(&completion),
                    };
                    if pending.tx.send(request).is_err() {
                        this.state = FileWriteTaskState::Done;
                        return Poll::Ready(Err(worker_disconnected_error()));
                    }
                    this.state = FileWriteTaskState::Waiting(completion);
                }
                FileWriteTaskState::Waiting(completion) => {
                    let completion = Arc::clone(completion);
                    let poll = completion.poll_result(cx);
                    if poll.is_ready() {
                        this.state = FileWriteTaskState::Done;
                    }
                    return poll;
                }
                FileWriteTaskState::Done => panic!("FileWriteTask polled after completion"),
            }
        }
    }
}

#[derive(Debug)]
struct PendingFsync {
    tx: mpsc::Sender<WorkerRequest>,
    fd: RawFd,
}

pub(crate) struct FileFsyncTask {
    state: FileFsyncTaskState,
}

enum FileFsyncTaskState {
    Init(PendingFsync),
    Waiting(FsyncCompletion),
    Done,
}

impl FileFsyncTask {
    pub(crate) fn new(tx: mpsc::Sender<WorkerRequest>, fd: RawFd) -> Self {
        Self {
            state: FileFsyncTaskState::Init(PendingFsync { tx, fd }),
        }
    }
}

impl Future for FileFsyncTask {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            match &mut this.state {
                FileFsyncTaskState::Init(pending) => {
                    let completion = Arc::new(TaskCompletion::new(cx.waker().clone()));
                    let request = WorkerRequest::Fsync {
                        fd: pending.fd,
                        completion: Arc::clone(&completion),
                    };
                    if pending.tx.send(request).is_err() {
                        this.state = FileFsyncTaskState::Done;
                        return Poll::Ready(Err(worker_disconnected_error()));
                    }
                    this.state = FileFsyncTaskState::Waiting(completion);
                }
                FileFsyncTaskState::Waiting(completion) => {
                    let completion = Arc::clone(completion);
                    let poll = completion.poll_result(cx);
                    if poll.is_ready() {
                        this.state = FileFsyncTaskState::Done;
                    }
                    return poll;
                }
                FileFsyncTaskState::Done => panic!("FileFsyncTask polled after completion"),
            }
        }
    }
}
