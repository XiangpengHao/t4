use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use crate::error::{Error, Result};
use crate::io::AlignedBuf;
use crate::sync::mpsc;
use crate::sync::{Arc, Mutex};
use crate::thread::cooperative_yield;

pub(crate) type ReadCompletion = Arc<TaskCompletion<(AlignedBuf, usize)>>;
pub(crate) type WriteCompletion = Arc<TaskCompletion<usize>>;
pub(crate) type FsyncCompletion = Arc<TaskCompletion<()>>;
pub(crate) type WalAppendCompletion = Arc<TaskCompletion<()>>;

#[derive(Debug)]
pub(crate) struct WalWriteOp {
    pub(crate) buf: AlignedBuf,
    pub(crate) offset: u64,
}

pub(crate) struct TaskCompletion<T> {
    inner: Mutex<TaskCompletionState<T>>,
}

enum TaskCompletionState<T> {
    PendingUnpolled,
    Pending { waker: Waker },
    Ready(Result<T>),
    Consumed,
}

impl<T> TaskCompletion<T> {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(TaskCompletionState::PendingUnpolled),
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
            TaskCompletionState::PendingUnpolled => None,
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
            TaskCompletionState::PendingUnpolled => {
                *inner = TaskCompletionState::Pending {
                    waker: cx.waker().clone(),
                };
                drop(inner);
                cooperative_yield();
                Poll::Pending
            }
            TaskCompletionState::Pending { waker } => {
                if !waker.will_wake(cx.waker()) {
                    *waker = cx.waker().clone();
                }
                drop(inner);
                cooperative_yield();
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
        buf: AlignedBuf,
        offset: u64,
        completion: ReadCompletion,
    },
    Write {
        buf: AlignedBuf,
        offset: u64,
        completion: WriteCompletion,
    },
    Fsync {
        completion: FsyncCompletion,
    },
    WalAppend {
        writes: Vec<WalWriteOp>,
        completion: WalAppendCompletion,
    },
}

struct PendingRead {
    tx: mpsc::Sender<WorkerRequest>,
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
    pub(crate) fn new(tx: mpsc::Sender<WorkerRequest>, buf: AlignedBuf, offset: u64) -> Self {
        Self {
            state: FileReadTaskState::Init(PendingRead {
                tx,
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
                    let completion = Arc::new(TaskCompletion::new());
                    let request = WorkerRequest::Read {
                        buf: pending.buf.take().expect("read task buffer missing"),
                        offset: pending.offset,
                        completion: Arc::clone(&completion),
                    };
                    if pending.tx.send(request).is_err() {
                        this.state = FileReadTaskState::Done;
                        return Poll::Ready(Err(worker_disconnected_error()));
                    }
                    cooperative_yield();
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

struct PendingWrite {
    tx: mpsc::Sender<WorkerRequest>,
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
    pub(crate) fn new(tx: mpsc::Sender<WorkerRequest>, buf: AlignedBuf, offset: u64) -> Self {
        Self {
            state: FileWriteTaskState::Init(PendingWrite {
                tx,
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
                    let completion = Arc::new(TaskCompletion::new());
                    let request = WorkerRequest::Write {
                        buf: pending.buf.take().expect("write task buffer missing"),
                        offset: pending.offset,
                        completion: Arc::clone(&completion),
                    };
                    if pending.tx.send(request).is_err() {
                        this.state = FileWriteTaskState::Done;
                        return Poll::Ready(Err(worker_disconnected_error()));
                    }
                    cooperative_yield();
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

struct PendingFsync {
    tx: mpsc::Sender<WorkerRequest>,
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
    pub(crate) fn new(tx: mpsc::Sender<WorkerRequest>) -> Self {
        Self {
            state: FileFsyncTaskState::Init(PendingFsync { tx }),
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
                    let completion = Arc::new(TaskCompletion::new());
                    let request = WorkerRequest::Fsync {
                        completion: Arc::clone(&completion),
                    };
                    if pending.tx.send(request).is_err() {
                        this.state = FileFsyncTaskState::Done;
                        return Poll::Ready(Err(worker_disconnected_error()));
                    }
                    cooperative_yield();
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

pub(crate) struct FileWalAppendTask {
    state: FileWalAppendTaskState,
}

enum FileWalAppendTaskState {
    Waiting(WalAppendCompletion),
    Done,
}

impl FileWalAppendTask {
    pub(crate) fn new(completion: WalAppendCompletion) -> Self {
        Self {
            state: FileWalAppendTaskState::Waiting(completion),
        }
    }
}

impl Future for FileWalAppendTask {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        match &mut this.state {
            FileWalAppendTaskState::Waiting(completion) => {
                let completion = Arc::clone(completion);
                let poll = completion.poll_result(cx);
                if poll.is_ready() {
                    this.state = FileWalAppendTaskState::Done;
                    poll
                } else {
                    cooperative_yield();
                    Poll::Pending
                }
            }
            FileWalAppendTaskState::Done => panic!("FileWalAppendTask polled after completion"),
        }
    }
}
