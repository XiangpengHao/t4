use crate::io::aligned_buf::AlignedBuf;
use crate::io::common::{BackendLoop, IoDriver};
use crate::io::io_task::{
    FilePosition, FileReadTask, FileWriteTask, PageWrite, WorkerRequest, worker_disconnected_error,
};
use crate::io::sync::Arc;
use crate::io::sync::mpsc;
use crate::io::sync::thread::{self, JoinHandle};
use crate::{Error, Result};
use core::fmt;
use std::num::NonZeroU32;

use crate::io::file_system::OpenFile;
use crate::io::new_backend;

/// The last clone's drop joins the thread, so its files are closed once the drop returns.
#[derive(Clone)]
pub struct IoWorker {
    // Before `_thread`, so it drops first: the worker sees its channel close before the join.
    tx: mpsc::Sender<WorkerRequest>,
    _thread: Arc<WorkerThread>,
}

/// The thread holds no `IoWorker` itself, so the join never waits on itself.
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
    pub fn new(queue_depth: NonZeroU32, files: Vec<OpenFile>) -> Result<Self> {
        if files.is_empty() {
            return Err(Error::InvalidArgument("an I/O worker needs a file"));
        }
        Self::start(move |rx| new_backend(files, queue_depth.get(), rx))
    }

    fn start<D: IoDriver>(
        backend: impl FnOnce(mpsc::Receiver<WorkerRequest>) -> Result<BackendLoop<D>> + Send + 'static,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel::<WorkerRequest>();
        let (init_tx, init_rx) = mpsc::channel::<Result<()>>();

        let thread = thread::Builder::new()
            .name("t4-io-worker".into())
            .spawn(move || {
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
            })?;

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
    use std::num::NonZero;
    #[cfg(feature = "shuttle")]
    use std::path::Path;

    #[cfg(not(feature = "shuttle"))]
    use pollster::block_on;
    #[cfg(feature = "shuttle")]
    use shuttle::future::block_on;

    use crate::io::aligned_buf::AlignedBuf;
    #[cfg(feature = "shuttle")]
    use crate::io::file_system::FileSystem;
    use crate::io::file_system::OpenFile;
    use crate::io::io_task::{FilePosition, PageWrite};
    use crate::{PAGE_SIZE_NZ_U32, PAGE_SIZE_U64};

    use super::IoWorker;

    fn page_of(text: &str) -> AlignedBuf {
        let mut buf = AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32).unwrap();
        buf.as_mut_slice()[..text.len()].copy_from_slice(text.as_bytes());
        buf
    }

    /// On the simulated disk under shuttle, else files the OS removes once closed.
    fn scratch_files(count: usize) -> Vec<OpenFile> {
        #[cfg(feature = "shuttle")]
        let open = {
            let file_system = FileSystem::default();
            move |file: usize| {
                let path = file.to_string();
                file_system
                    .open_cache_file(Path::new(&path), false)
                    .unwrap()
            }
        };
        #[cfg(not(feature = "shuttle"))]
        let open = |_| OpenFile::from(tempfile::tempfile().unwrap());
        (0..count).map(open).collect()
    }

    #[test]
    fn basic_io_worker_test() {
        crate::io::test_schedule::TestSchedule::check(|| {
            let io_worker = IoWorker::new(NonZero::new(12).unwrap(), scratch_files(1)).unwrap();
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
        });
    }

    #[test]
    fn one_worker_reads_and_writes_several_files() {
        crate::io::test_schedule::TestSchedule::check(|| {
            let io_worker = IoWorker::new(NonZero::new(4).unwrap(), scratch_files(2)).unwrap();
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
        });
    }

    #[cfg(feature = "shuttle")]
    #[test]
    fn pool_io_worker_test() {
        crate::io::test_schedule::TestSchedule::check(|| {
            const THREADS: u64 = 4;
            const PAGES_PER_THREAD: u64 = 8;
            const QUEUE_DEPTH: u32 = 32;

            let io_worker =
                IoWorker::new(NonZero::new(QUEUE_DEPTH).unwrap(), scratch_files(1)).unwrap();

            let marker = |t: u64, i: u64| format!("t{t}-p{i}");

            let mut handles = Vec::with_capacity(THREADS as usize);
            for t in 0..THREADS {
                let worker = io_worker.clone();
                handles.push(crate::io::sync::thread::spawn(move || {
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
        });
    }

    #[cfg(feature = "shuttle")]
    #[test]
    fn dropping_the_worker_finishes_its_reads_and_closes_its_files() {
        crate::io::test_schedule::TestSchedule::check(|| {
            const READS: u32 = 64;
            const MIB: u32 = 1 << 20;
            let file_system = FileSystem::default();
            let path = Path::new("file");
            let file = file_system.open_cache_file(path, false).unwrap();
            file.set_size(u64::from(READS * MIB)).unwrap();
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

            let reopened = file_system.open_existing(path, false).unwrap();
            reopened
                .try_lock()
                .expect("the worker's file is closed once it drops");
            // Shuttle detects a deadlock if a read was dropped unanswered.
            let (done_tx, done_rx) = crate::io::sync::mpsc::channel();
            crate::io::sync::thread::spawn(move || {
                for read in reads {
                    let (buf, read_len) = block_on(read).unwrap();
                    assert_eq!(read_len, buf.len());
                }
                done_tx.send(()).unwrap();
            });
            done_rx.recv().expect("every read in flight completes");
        });
    }
}
