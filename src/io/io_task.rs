use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use crate::buffer::AlignedBuf;
use crate::io::error::{Error, Result};
use crate::io::sync::cooperative_yield;
use crate::io::sync::mpsc;
use crate::io::sync::{Arc, Mutex};

pub(crate) type ReadCompletion = Arc<TaskCompletion<(AlignedBuf, usize)>>;
pub(crate) type WriteCompletion = Arc<TaskCompletion<()>>;

/// Where an I/O goes: one of the worker's files, by its place in the list
/// the worker opened, and an offset in it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FilePosition {
    file: u32,
    offset: u64,
}

impl FilePosition {
    pub fn new(file: u32, offset: u64) -> Self {
        Self { file, offset }
    }

    /// The file's place in the worker's list.
    pub fn file(self) -> u32 {
        self.file
    }

    pub fn offset(self) -> u64 {
        self.offset
    }

    /// The position `bytes` further into the same file.
    pub fn advanced(self, bytes: u64) -> Self {
        Self {
            file: self.file,
            offset: self.offset + bytes,
        }
    }
}

#[derive(Debug)]
pub struct PageWrite {
    pub buf: AlignedBuf,
    pub at: FilePosition,
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
        at: FilePosition,
        completion: ReadCompletion,
    },
    Write {
        writes: Vec<PageWrite>,
        completion: WriteCompletion,
    },
}

enum FileReadTaskState {
    Waiting(ReadCompletion),
    Done,
}

pub struct FileReadTask {
    state: FileReadTaskState,
}

impl FileReadTask {
    /// Submit the read to the worker eagerly. The returned future just
    /// waits for the completion to be signalled. Submitting inside
    /// `IoWorker::read_at` (rather than on first poll) is what makes the
    /// IoWorker channel order match caller-side request order — see the
    /// comment on `FileWriteTask::new`.
    pub(crate) fn new(
        tx: mpsc::Sender<WorkerRequest>,
        buf: AlignedBuf,
        at: FilePosition,
    ) -> Result<Self> {
        let completion = Arc::new(TaskCompletion::new());
        let request = WorkerRequest::Read {
            buf,
            at,
            completion: Arc::clone(&completion),
        };
        tx.send(request).map_err(|_| worker_disconnected_error())?;
        Ok(Self {
            state: FileReadTaskState::Waiting(completion),
        })
    }
}

impl Future for FileReadTask {
    type Output = Result<(AlignedBuf, usize)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.state {
            FileReadTaskState::Waiting(completion) => {
                let poll = completion.poll_result(cx);
                if poll.is_ready() {
                    this.state = FileReadTaskState::Done;
                }
                poll
            }
            FileReadTaskState::Done => panic!("FileReadTask polled after completion"),
        }
    }
}

enum FileWriteTaskState {
    Waiting(WriteCompletion),
    Done,
}

pub struct FileWriteTask {
    state: FileWriteTaskState,
}

impl FileWriteTask {
    /// Submit the write to the worker eagerly. The request enters the
    /// IoWorker channel at construction time, *not* on first poll, so the
    /// channel's order is the order callers make requests in, not the
    /// async scheduler's choice of poll order.
    pub(crate) fn new(tx: mpsc::Sender<WorkerRequest>, writes: Vec<PageWrite>) -> Result<Self> {
        let completion = Arc::new(TaskCompletion::new());
        let request = WorkerRequest::Write {
            writes,
            completion: Arc::clone(&completion),
        };
        tx.send(request).map_err(|_| worker_disconnected_error())?;
        Ok(Self {
            state: FileWriteTaskState::Waiting(completion),
        })
    }
}

impl Future for FileWriteTask {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match &mut this.state {
            FileWriteTaskState::Waiting(completion) => {
                let poll = completion.poll_result(cx);
                if poll.is_ready() {
                    this.state = FileWriteTaskState::Done;
                }
                poll
            }
            FileWriteTaskState::Done => panic!("FileWriteTask polled after completion"),
        }
    }
}
