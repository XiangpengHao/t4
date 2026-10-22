use crate::buffer::AlignedBuf;
use crate::io::common::{BackendLoop, IoDriver};
use crate::io::io_task::{
    FileReadTask, FileWriteTask, PageWrite, WorkerRequest, worker_disconnected_error,
};
use crate::io::sync::Arc;
use crate::io::sync::mpsc;
use crate::io::sync::thread::{JoinHandle, spawn};
use crate::{Error, Result};
use core::fmt;
use std::fs::File;
use std::num::NonZeroU32;

use crate::io::new_backend;
#[cfg(all(feature = "shuttle", test))]
use crate::io::simulated::{self, SimDisk};

/// Handle to the I/O worker thread.
///
/// Cloning shares the same underlying worker. Dropping the last clone
/// disconnects the worker's channel and waits for the thread to exit, so
/// the file it owns, and the store's mount lock on it, are released by the
/// time the drop returns.
#[derive(Clone)]
pub struct IoWorker {
    // Declared before `_thread`, so it drops first: the worker sees its
    // channel disconnect before the last handle joins it.
    tx: mpsc::Sender<WorkerRequest>,
    _thread: Arc<WorkerThread>,
}

/// The worker thread, joined when the last [`IoWorker`] drops. The thread
/// holds no `IoWorker` itself, so the join never waits on itself.
struct WorkerThread(Option<JoinHandle<()>>);

impl Drop for WorkerThread {
    fn drop(&mut self) {
        if let Some(thread) = self.0.take() {
            // A panic on the worker thread was reported where it happened.
            let _ = thread.join();
        }
    }
}

impl fmt::Debug for IoWorker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoWorker").finish_non_exhaustive()
    }
}

impl IoWorker {
    pub fn new(queue_depth: NonZeroU32, file: File) -> Result<Self> {
        Self::start(move |rx| new_backend(file, queue_depth.get(), rx))
    }

    /// A worker on a simulated disk, for shuttle tests.
    #[cfg(all(feature = "shuttle", test))]
    pub(crate) fn simulated(queue_depth: NonZeroU32, disk: SimDisk) -> Result<Self> {
        Self::start(move |rx| Ok(simulated::new_backend_on(disk, queue_depth.get(), rx)))
    }

    /// Spawns the worker thread, which builds its backend with `backend`
    /// and runs it.
    fn start<D: IoDriver>(
        backend: impl FnOnce(mpsc::Receiver<WorkerRequest>) -> Result<BackendLoop<D>> + Send + 'static,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel::<WorkerRequest>();
        let (init_tx, init_rx) = mpsc::channel::<Result<()>>();

        let thread = spawn(move || {
            let backend = match backend(rx) {
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
            Ok(Ok(())) => Ok(Self {
                tx,
                _thread: Arc::new(WorkerThread(Some(thread))),
            }),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(worker_disconnected_error()),
        }
    }

    pub fn read_at(&self, buf: AlignedBuf, offset: u64) -> Result<FileReadTask> {
        FileReadTask::new(self.tx.clone(), buf, offset)
    }

    pub fn write(&self, writes: Vec<PageWrite>) -> Result<FileWriteTask> {
        FileWriteTask::new(self.tx.clone(), writes)
    }

    pub async fn read_exact_at(&self, buf: AlignedBuf, offset: u64) -> Result<AlignedBuf> {
        let expected = buf.len();
        let (buf, n) = self.read_at(buf, offset)?.await?;
        if n != expected {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("short read: expected {expected}, got {n}"),
            )));
        }
        Ok(buf)
    }
}

#[cfg(test)]
mod test {
    use std::num::NonZero;

    use pollster::block_on;
    use tempfile::tempfile;

    use crate::buffer::AlignedBuf;
    use crate::io::io_task::PageWrite;
    use crate::{PAGE_SIZE_NZ_U32, PAGE_SIZE_U64};

    use super::IoWorker;

    #[test]
    fn basic_io_worker_test() {
        let tmp_file = tempfile().unwrap();
        let io_worker = IoWorker::new(NonZero::new(12).unwrap(), tmp_file).unwrap();

        let mut write_buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32).unwrap();
        write_buf.as_mut_slice()[..5].copy_from_slice(b"hello");

        block_on(
            io_worker
                .write(vec![PageWrite {
                    buf: write_buf,
                    offset: 0,
                }])
                .unwrap(),
        )
        .unwrap();

        let read_buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32).unwrap();
        let read_buf = block_on(io_worker.read_exact_at(read_buf, 0)).unwrap();

        assert_eq!(&read_buf.as_slice()[..5], b"hello");
    }

    #[test]
    fn pool_io_worker_test() {
        const THREADS: u64 = 4;
        const PAGES_PER_THREAD: u64 = 8;
        const QUEUE_DEPTH: u32 = 32;

        let tmp_file = tempfile().unwrap();
        let io_worker = IoWorker::new(NonZero::new(QUEUE_DEPTH).unwrap(), tmp_file).unwrap();

        let marker = |t: u64, i: u64| format!("t{t}-p{i}");

        let mut handles = Vec::with_capacity(THREADS as usize);
        for t in 0..THREADS {
            let worker = io_worker.clone();
            handles.push(std::thread::spawn(move || {
                let base = t * PAGES_PER_THREAD;

                let mut writes = Vec::with_capacity(PAGES_PER_THREAD as usize);
                for i in 0..PAGES_PER_THREAD {
                    let mut buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32).unwrap();
                    let bytes = marker(t, i);
                    buf.as_mut_slice()[..bytes.len()].copy_from_slice(bytes.as_bytes());
                    writes.push(PageWrite {
                        buf,
                        offset: (base + i) * PAGE_SIZE_U64,
                    });
                }
                block_on(worker.write(writes).unwrap()).unwrap();

                for i in 0..PAGES_PER_THREAD {
                    let read_buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32).unwrap();
                    let read_buf =
                        block_on(worker.read_exact_at(read_buf, (base + i) * PAGE_SIZE_U64))
                            .unwrap();
                    let expected = marker(t, i);
                    assert_eq!(&read_buf.as_slice()[..expected.len()], expected.as_bytes(),);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }
}
