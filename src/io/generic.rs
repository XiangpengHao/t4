use std::collections::HashMap;
use std::fs::File;
use std::os::fd::AsRawFd;

use crossbeam_channel::{Receiver, Select, Sender, TryRecvError, bounded, unbounded};

use crate::io::common::{
    CompletionEvent, InflightRequest, InflightRequestKind, RequestId, complete_request_with_error,
    decode_cqe_result, decode_user_data, encode_user_data, request_op_count, worker_failed_error,
};
use crate::io::error::{Error, Result};
use crate::io::io_task::WorkerRequest;
use crate::io::sync::{mpsc, thread};

struct IoJob {
    fd: i32,
    user_data: u64,
    kind: IoJobKind,
}

enum IoJobKind {
    Read {
        buf: *mut u8,
        len: usize,
        offset: u64,
    },
    Write {
        buf: *const u8,
        len: usize,
        offset: u64,
    },
    Fsync,
}

unsafe impl Send for IoJob {}

impl IoJob {
    fn execute(&self) -> i32 {
        unsafe {
            let ret: isize = match self.kind {
                IoJobKind::Read { buf, len, offset } => {
                    libc::pread(self.fd, buf.cast(), len, offset as libc::off_t)
                }
                IoJobKind::Write { buf, len, offset } => {
                    libc::pwrite(self.fd, buf.cast(), len, offset as libc::off_t)
                }
                IoJobKind::Fsync => libc::fsync(self.fd) as isize,
            };
            if ret < 0 {
                -std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
            } else {
                ret as i32
            }
        }
    }
}

struct GenericIoDriver {
    queue_size: usize,
    inflight: usize,
    job_tx: Sender<IoJob>,
    completion_rx: Receiver<CompletionEvent>,
}

impl GenericIoDriver {
    fn new(queue_size: usize) -> Result<Self> {
        let num_threads = std::thread::available_parallelism()
            .map(|n| n.get())?
            .min(queue_size.max(1));
        let (job_tx, job_rx) = bounded::<IoJob>(queue_size);
        let (completion_tx, completion_rx) = unbounded::<CompletionEvent>();

        for _ in 0..num_threads {
            let job_rx = job_rx.clone();
            let completion_tx = completion_tx.clone();
            thread::spawn(move || worker_loop(job_rx, completion_tx));
        }

        Ok(Self {
            queue_size,
            inflight: 0,
            job_tx,
            completion_rx,
        })
    }

    fn available_submission_slots(&self) -> usize {
        self.queue_size.saturating_sub(self.inflight)
    }

    fn push(&mut self, job: IoJob) -> Result<()> {
        self.job_tx.try_send(job).map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "submission queue is full",
            ))
        })?;
        self.inflight += 1;
        Ok(())
    }

    fn pop_completion(&mut self) -> Option<CompletionEvent> {
        match self.completion_rx.try_recv() {
            Ok(event) => {
                self.inflight = self.inflight.saturating_sub(1);
                Some(event)
            }
            Err(_) => None,
        }
    }
}

fn worker_loop(job_rx: Receiver<IoJob>, completion_tx: Sender<CompletionEvent>) {
    while let Ok(job) = job_rx.recv() {
        let event = CompletionEvent {
            user_data: job.user_data,
            result: job.execute(),
        };
        if completion_tx.send(event).is_err() {
            return;
        }
    }
}

pub(crate) struct GenericIoBackend {
    receiver: mpsc::Receiver<WorkerRequest>,
    file: File,
    queue_depth: usize,
    inflight_requests: HashMap<RequestId, InflightRequest>,
    next_request_id: RequestId,
    driver: GenericIoDriver,
}

impl GenericIoBackend {
    pub(crate) fn new(
        file: File,
        queue_depth: u32,
        rx: mpsc::Receiver<WorkerRequest>,
    ) -> Result<Self> {
        let queue_depth = queue_depth as usize;
        let driver = GenericIoDriver::new(queue_depth)?;
        Ok(Self {
            receiver: rx,
            file,
            queue_depth,
            inflight_requests: HashMap::new(),
            next_request_id: 0,
            driver,
        })
    }

    pub fn run(mut self) {
        self.thread_loop();
    }

    fn thread_loop(&mut self) {
        let mut pending_request = None;
        loop {
            let disconnected = match self.submit_requests(&mut pending_request) {
                Ok(disconnected) => disconnected,
                Err(err) => {
                    self.fail_all(err, pending_request.take());
                    return;
                }
            };
            if disconnected {
                return;
            }

            self.poll_completions();

            // Block until either a new request arrives or a worker
            // completion lands. `Select::ready` does not consume the
            // message — the next loop iteration drains it via
            // submit_requests / poll_completions.
            if self.inflight_requests.is_empty() && pending_request.is_none() {
                match self.receiver.recv() {
                    Ok(request) => pending_request = Some(request),
                    Err(_) => return,
                }
            } else {
                let mut sel = Select::new();
                sel.recv(&self.receiver);
                sel.recv(&self.driver.completion_rx);
                sel.ready();
            }
        }
    }

    fn submit_requests(&mut self, pending_request: &mut Option<WorkerRequest>) -> Result<bool> {
        loop {
            let request = match pending_request.take() {
                Some(request) => request,
                None => match self.receiver.try_recv() {
                    Ok(request) => request,
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return Ok(true),
                },
            };
            let op_count = request_op_count(&request);
            assert!(op_count > 0, "request has no operations");
            if op_count > self.queue_depth {
                complete_request_with_error(
                    request,
                    Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "request needs {op_count} SQEs, queue depth is {}",
                            self.queue_depth
                        ),
                    )),
                );
                continue;
            }
            if self.driver.available_submission_slots() < op_count {
                *pending_request = Some(request);
                break;
            }
            self.submit_request(request)?;
        }

        Ok(false)
    }

    fn submit_request(&mut self, request: WorkerRequest) -> Result<()> {
        let request_id = self.allocate_request_id();
        let fd = self.file.as_raw_fd();
        match request {
            WorkerRequest::Read {
                buf,
                offset,
                completion,
            } => {
                self.inflight_requests.insert(
                    request_id,
                    InflightRequest {
                        remaining: 1,
                        error: None,
                        kind: InflightRequestKind::Read {
                            buf: Some(buf),
                            completion,
                            result: None,
                        },
                    },
                );

                let push_result = {
                    let request = self
                        .inflight_requests
                        .get_mut(&request_id)
                        .expect("read request missing after insert");
                    let InflightRequestKind::Read { buf, .. } = &mut request.kind else {
                        unreachable!("request kind changed while submitting read");
                    };
                    let buf = buf.as_mut().expect("read buffer missing while submitting");
                    self.driver.push(IoJob {
                        fd,
                        user_data: encode_user_data(request_id, 0),
                        kind: IoJobKind::Read {
                            buf: buf.as_mut_ptr(),
                            len: buf.len(),
                            offset,
                        },
                    })
                };
                self.finish_single_submit(request_id, push_result)
            }
            WorkerRequest::Fsync { completion } => {
                self.inflight_requests.insert(
                    request_id,
                    InflightRequest {
                        remaining: 1,
                        error: None,
                        kind: InflightRequestKind::Fsync { completion },
                    },
                );

                let push_result = self.driver.push(IoJob {
                    fd,
                    user_data: encode_user_data(request_id, 0),
                    kind: IoJobKind::Fsync,
                });
                self.finish_single_submit(request_id, push_result)
            }
            WorkerRequest::Write { writes, completion } => {
                let page_count = writes.len();
                self.inflight_requests.insert(
                    request_id,
                    InflightRequest {
                        remaining: page_count,
                        error: None,
                        kind: InflightRequestKind::Write {
                            pages: writes,
                            completion,
                        },
                    },
                );

                let mut submitted_pages = 0;
                for index in 0..page_count {
                    let push_result = {
                        let request = self
                            .inflight_requests
                            .get(&request_id)
                            .expect("write request missing after insert");
                        let InflightRequestKind::Write { pages, .. } = &request.kind else {
                            unreachable!("request kind changed while submitting write");
                        };
                        let page = pages
                            .get(index)
                            .expect("write page missing while submitting");
                        self.driver.push(IoJob {
                            fd,
                            user_data: encode_user_data(request_id, index),
                            kind: IoJobKind::Write {
                                buf: page.buf.as_ptr(),
                                len: page.buf.len(),
                                offset: page.offset,
                            },
                        })
                    };

                    match push_result {
                        Ok(()) => submitted_pages += 1,
                        Err(err) => {
                            self.handle_write_submit_error(request_id, submitted_pages, err);
                            return Ok(());
                        }
                    }
                }

                Ok(())
            }
        }
    }

    fn poll_completions(&mut self) {
        while let Some(cqe) = self.driver.pop_completion() {
            let (request_id, op_index) = decode_user_data(cqe.user_data);

            let Some(request) = self.inflight_requests.get_mut(&request_id) else {
                debug_assert!(false, "missing inflight request for cqe {}", cqe.user_data);
                continue;
            };

            match &mut request.kind {
                InflightRequestKind::Read { result, .. } => {
                    debug_assert_eq!(op_index, 0, "read request should only have op 0");
                    match decode_cqe_result(cqe.result) {
                        Ok(n) => *result = Some(n),
                        Err(err) if request.error.is_none() => request.error = Some(err),
                        Err(_) => {}
                    }
                }
                InflightRequestKind::Write { pages, .. } => {
                    let Some(page) = pages.get(op_index) else {
                        debug_assert!(false, "write page index out of range: {}", op_index);
                        continue;
                    };
                    let expected = page.buf.len();
                    let result = decode_cqe_result(cqe.result).and_then(|n| {
                        if n == expected {
                            Ok(())
                        } else {
                            Err(Error::Io(std::io::Error::new(
                                std::io::ErrorKind::WriteZero,
                                format!("short write: expected {expected}, got {n}"),
                            )))
                        }
                    });
                    if let Err(err) = result
                        && request.error.is_none()
                    {
                        request.error = Some(err);
                    }
                }
                InflightRequestKind::Fsync { .. } => {
                    debug_assert_eq!(op_index, 0, "fsync request should only have op 0");
                    if let Err(err) = decode_cqe_result(cqe.result).map(|_| ())
                        && request.error.is_none()
                    {
                        request.error = Some(err);
                    }
                }
            }

            if request.remaining > 0 {
                request.remaining -= 1;
            }
            if request.remaining == 0 {
                let request = self
                    .inflight_requests
                    .remove(&request_id)
                    .expect("inflight request missing at completion");
                request.complete();
            }
        }
    }

    fn fail_all(&mut self, err: Error, pending_request: Option<WorkerRequest>) {
        let msg = format!("io worker failed: {err}");

        if let Some(request) = pending_request {
            complete_request_with_error(request, worker_failed_error(msg.clone()));
        }

        for (_, request) in self.inflight_requests.drain() {
            request.complete_with_error(worker_failed_error(msg.clone()));
        }

        while let Ok(request) = self.receiver.try_recv() {
            complete_request_with_error(request, worker_failed_error(msg.clone()));
        }
    }

    fn finish_single_submit(
        &mut self,
        request_id: RequestId,
        push_result: Result<()>,
    ) -> Result<()> {
        if let Err(err) = push_result {
            let request = self
                .inflight_requests
                .remove(&request_id)
                .expect("request missing after failed submit");
            request.complete_with_error(err);
        }
        Ok(())
    }

    fn handle_write_submit_error(
        &mut self,
        request_id: RequestId,
        submitted_pages: usize,
        err: Error,
    ) {
        if submitted_pages == 0 {
            let request = self
                .inflight_requests
                .remove(&request_id)
                .expect("write request missing after failed first submit");
            request.complete_with_error(err);
            return;
        }

        let request = self
            .inflight_requests
            .get_mut(&request_id)
            .expect("write request missing after partial submit");
        request.remaining = submitted_pages;
        if request.error.is_none() {
            request.error = Some(err);
        }
    }

    fn allocate_request_id(&mut self) -> RequestId {
        loop {
            self.next_request_id = self.next_request_id.wrapping_add(1);
            if !self.inflight_requests.contains_key(&self.next_request_id) {
                return self.next_request_id;
            }
        }
    }
}