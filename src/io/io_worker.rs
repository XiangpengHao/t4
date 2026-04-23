use core::fmt;
use std::fs::File;
use std::num::NonZeroU32;
use std::sync::{Arc, mpsc};
use std::thread::spawn;

use crate::buffer::AlignedBuf;
use crate::io::generic::GenericIoBackend;
use crate::{Error, Result};

use crate::io::io_task::{
    FileFsyncTask, FileReadTask, FileWriteTask, PageWrite, WorkerRequest, worker_disconnected_error,
};

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

        spawn(move || {
            cfg_if::cfg_if! {
                if #[cfg(all(feature = "io-uring", target_os = "linux"))] {
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
                } else {
                        let backend = match GenericIoBackend::new(file, queue_depth as usize, rx) {
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

                }
            }
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
