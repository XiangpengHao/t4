use core::fmt;
use std::fs::File;
use std::num::NonZeroU32;
use std::thread::spawn;

use crate::buffer::AlignedBuf;
use crate::io::io_task::{
    FileFsyncTask, FileReadTask, FileWriteTask, PageWrite, WorkerRequest, worker_disconnected_error,
};
use crate::io::sync::mpsc;
use crate::{Error, Result};

#[cfg(all(feature = "io-uring", target_os = "linux"))]
use crate::io::io_uring::UringBackend as Backend;

#[cfg(not(all(feature = "io-uring", target_os = "linux")))]
use crate::io::generic::GenericIoBackend as Backend;

/// Handle to the I/O worker thread.
///
/// Cloning shares the same underlying worker. The worker thread exits
/// automatically once every clone is dropped (channel disconnects).
#[derive(Clone)]
pub struct IoWorker {
    tx: mpsc::Sender<WorkerRequest>,
}

impl fmt::Debug for IoWorker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoWorker").finish_non_exhaustive()
    }}

impl IoWorker {
    pub fn new(queue_depth: NonZeroU32, file: File) -> Result<Self> {
        let queue_depth = queue_depth.get();

        let (tx, rx) = mpsc::channel::<WorkerRequest>();
        let (init_tx, init_rx) = mpsc::channel::<Result<()>>();

        spawn(move || {
            let backend = match Backend::new(file, queue_depth, rx) {
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
            Ok(Ok(())) => Ok(Self { tx }),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(worker_disconnected_error()),
        }
    }

    pub fn read_at(&self, buf: AlignedBuf, offset: u64) -> FileReadTask {
        FileReadTask::new(self.tx.clone(), buf, offset)
    }

    pub fn write(&self, writes: Vec<PageWrite>) -> FileWriteTask {
        FileWriteTask::new(self.tx.clone(), writes)
    }

    pub fn fsync(&self) -> FileFsyncTask {
        FileFsyncTask::new(self.tx.clone())
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

        block_on(io_worker.write(vec![PageWrite {
            buf: write_buf,
            offset: 0,
        }]))
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
                block_on(worker.write(writes)).unwrap();

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

        block_on(io_worker.fsync()).unwrap();
    }
}
