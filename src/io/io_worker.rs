use crate::buffer::AlignedBuf;
use crate::io::common::{BackendLoop, IoDriver};
use crate::io::io_task::{
    FilePosition, FileReadTask, FileWriteTask, PageWrite, WorkerRequest, worker_disconnected_error,
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

/// Handle to the I/O worker thread, which reads and writes a list of files
/// (`doc/04-multi-device.md`, decision 4): an I/O names its file by its
/// place in the list ([`FilePosition`]).
///
/// Cloning shares the same underlying worker. Dropping the last clone
/// disconnects the worker's channel and waits for the thread to exit, so
/// the files it owns are closed by the time the drop returns.
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
    pub fn new(queue_depth: NonZeroU32, files: Vec<File>) -> Result<Self> {
        if files.is_empty() {
            return Err(Error::InvalidArgument("an I/O worker needs a file"));
        }
        Self::start(move |rx| new_backend(files, queue_depth.get(), rx))
    }

    /// A worker on simulated disks, one a file, for shuttle tests.
    #[cfg(all(feature = "shuttle", test))]
    pub(crate) fn simulated(queue_depth: NonZeroU32, disks: Vec<SimDisk>) -> Result<Self> {
        Self::start(move |rx| Ok(simulated::new_backend_on(disks, queue_depth.get(), rx)))
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

    pub fn read_at(&self, buf: AlignedBuf, at: FilePosition) -> Result<FileReadTask> {
        FileReadTask::new(self.tx.clone(), buf, at)
    }

    pub fn write(&self, writes: Vec<PageWrite>) -> Result<FileWriteTask> {
        FileWriteTask::new(self.tx.clone(), writes)
    }

    pub async fn read_exact_at(&self, buf: AlignedBuf, at: FilePosition) -> Result<AlignedBuf> {
        let expected = buf.len();
        let (buf, n) = self.read_at(buf, at)?.await?;
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
    use std::fs::File;
    use std::num::NonZero;
    use std::time::Duration;

    use pollster::block_on;
    use tempfile::tempfile;

    use crate::buffer::AlignedBuf;
    use crate::io::io_task::{FilePosition, PageWrite};
    use crate::{PAGE_SIZE_NZ_U32, PAGE_SIZE_U64};

    use super::IoWorker;

    /// A page whose first bytes are `text`.
    fn page_of(text: &str) -> AlignedBuf {
        let mut buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32).unwrap();
        buf.as_mut_slice()[..text.len()].copy_from_slice(text.as_bytes());
        buf
    }

    #[test]
    fn basic_io_worker_test() {
        let tmp_file = tempfile().unwrap();
        let io_worker = IoWorker::new(NonZero::new(12).unwrap(), vec![tmp_file]).unwrap();
        let start = FilePosition::new(0, 0);

        block_on(
            io_worker
                .write(vec![PageWrite {
                    buf: page_of("hello"),
                    at: start,
                }])
                .unwrap(),
        )
        .unwrap();

        let read_buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32).unwrap();
        let read_buf = block_on(io_worker.read_exact_at(read_buf, start)).unwrap();

        assert_eq!(&read_buf.as_slice()[..5], b"hello");
    }

    /// One worker over two files: an I/O goes to the file it names, so the
    /// same offset in each holds its own bytes; a file past the list fails.
    #[test]
    fn one_worker_reads_and_writes_several_files() {
        let files = vec![tempfile().unwrap(), tempfile().unwrap()];
        let io_worker = IoWorker::new(NonZero::new(4).unwrap(), files).unwrap();
        let writes = ["zero", "one"]
            .iter()
            .enumerate()
            .map(|(file, text)| PageWrite {
                buf: page_of(text),
                at: FilePosition::new(file as u32, PAGE_SIZE_U64),
            })
            .collect();
        block_on(io_worker.write(writes).unwrap()).unwrap();

        for (file, text) in ["zero", "one"].iter().enumerate() {
            let at = FilePosition::new(file as u32, PAGE_SIZE_U64);
            let buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32).unwrap();
            let buf = block_on(io_worker.read_exact_at(buf, at)).unwrap();
            assert_eq!(&buf.as_slice()[..text.len()], text.as_bytes());
        }
        let buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32).unwrap();
        assert!(block_on(io_worker.read_exact_at(buf, FilePosition::new(2, 0))).is_err());
        assert!(IoWorker::new(NonZero::new(4).unwrap(), Vec::new()).is_err());
    }

    #[test]
    fn pool_io_worker_test() {
        const THREADS: u64 = 4;
        const PAGES_PER_THREAD: u64 = 8;
        const QUEUE_DEPTH: u32 = 32;

        let tmp_file = tempfile().unwrap();
        let io_worker = IoWorker::new(NonZero::new(QUEUE_DEPTH).unwrap(), vec![tmp_file]).unwrap();

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
                        at: FilePosition::new(0, (base + i) * PAGE_SIZE_U64),
                    });
                }
                block_on(worker.write(writes).unwrap()).unwrap();

                for i in 0..PAGES_PER_THREAD {
                    let read_buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32).unwrap();
                    let at = FilePosition::new(0, (base + i) * PAGE_SIZE_U64);
                    let read_buf = block_on(worker.read_exact_at(read_buf, at)).unwrap();
                    let expected = marker(t, i);
                    assert_eq!(&read_buf.as_slice()[..expected.len()], expected.as_bytes(),);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    /// Dropping the last handle with reads in flight waits for them: each
    /// completes with its bytes, and the worker's files are closed by the
    /// time the drop returns, so a lock on one is free at once.
    #[test]
    fn dropping_the_worker_finishes_its_reads_and_closes_its_files() {
        const READS: u32 = 64;
        const MIB: u32 = 1 << 20;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();
        file.set_len(u64::from(READS * MIB)).unwrap();
        file.try_lock().unwrap();
        let io_worker = IoWorker::new(NonZero::new(READS).unwrap(), vec![file]).unwrap();
        let reads: Vec<_> = (0..READS)
            .map(|index| {
                let buf = AlignedBuf::new_zeroed(NonZero::new(MIB).unwrap()).unwrap();
                let at = FilePosition::new(0, u64::from(index * MIB));
                io_worker.read_at(buf, at).unwrap()
            })
            .collect();

        drop(io_worker);

        let reopened = File::open(&path).unwrap();
        reopened
            .try_lock()
            .expect("the worker's file is closed once it drops");
        // A read the worker dropped unanswered would wait forever: the wait
        // is bounded, so that fails rather than hangs.
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for read in reads {
                let (buf, read_len) = block_on(read).unwrap();
                assert_eq!(read_len, buf.len());
            }
            done_tx.send(()).unwrap();
        });
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("every read in flight completes");
    }
}
