use std::collections::HashMap;

use crate::buffer::AlignedBuf;
use crate::io::error::{Error, Result};
use crate::io::sync::mpsc;

use super::io_task::{FilePosition, PageWrite, ReadCompletion, WorkerRequest, WriteCompletion};

fn worker_failed_error(message: impl Into<String>) -> Error {
    Error::Io(std::io::Error::other(message.into()))
}

/// Fails a request never submitted. Its buffers drop first, as a completed
/// request's do ([`InflightRequest::complete`]).
fn complete_request_with_error(request: WorkerRequest, err: Error) {
    match request {
        WorkerRequest::Read {
            buf, completion, ..
        } => {
            drop(buf);
            completion.complete(Err(err));
        }
        WorkerRequest::Write {
            writes, completion, ..
        } => {
            drop(writes);
            completion.complete(Err(err));
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CompletionEvent {
    pub(crate) user_data: u64,
    pub(crate) result: i32,
}

fn decode_cqe_result(result: i32) -> Result<usize> {
    if result < 0 {
        return Err(Error::Io(std::io::Error::from_raw_os_error(-result)));
    }
    Ok(result as usize)
}

type RequestId = u32;

struct InflightRequest {
    remaining: usize,
    error: Option<Error>,
    kind: InflightRequestKind,
}

enum InflightRequestKind {
    Read {
        buf: Option<AlignedBuf>,
        completion: ReadCompletion,
        result: Option<usize>,
    },
    Write {
        pages: Vec<PageWrite>,
        completion: WriteCompletion,
    },
}

impl InflightRequest {
    /// Tells the request's caller how it went. The buffers it doesn't hand
    /// back drop first: a write's, a failed read's. So once the caller's
    /// future is ready, they are gone, and with them whatever a lent buffer
    /// held for the I/O: the cache frees the chunks a write held then.
    fn complete(self) {
        match self.kind {
            InflightRequestKind::Read {
                buf,
                completion,
                result,
            } => match self.error {
                Some(err) => {
                    drop(buf);
                    completion.complete(Err(err));
                }
                None => completion.complete(Ok((
                    buf.expect("read buffer missing at completion"),
                    result.expect("read result missing at completion"),
                ))),
            },
            InflightRequestKind::Write { pages, completion } => {
                drop(pages);
                match self.error {
                    Some(err) => completion.complete(Err(err)),
                    None => completion.complete(Ok(())),
                }
            }
        }
    }

    /// [`Self::complete`] with `err`.
    fn complete_with_error(self, err: Error) {
        match self.kind {
            InflightRequestKind::Read {
                buf, completion, ..
            } => {
                drop(buf);
                completion.complete(Err(err));
            }
            InflightRequestKind::Write { pages, completion } => {
                drop(pages);
                completion.complete(Err(err));
            }
        }
    }
}

fn encode_user_data(request_id: RequestId, op_index: usize) -> u64 {
    ((request_id as u64) << 32) | (op_index as u32 as u64)
}

fn decode_user_data(user_data: u64) -> (RequestId, usize) {
    ((user_data >> 32) as RequestId, user_data as u32 as usize)
}

fn request_op_count(request: &WorkerRequest) -> usize {
    match request {
        WorkerRequest::Read { .. } => 1,
        WorkerRequest::Write { writes, .. } => writes.len(),
    }
}

/// Backend-agnostic submission record handed to a driver, which owns the
/// files it goes to.
pub(crate) enum SubmissionEntry {
    Read {
        buf_ptr: *mut u8,
        buf_len: u32,
        at: FilePosition,
        user_data: u64,
    },
    Write {
        buf_ptr: *const u8,
        buf_len: u32,
        at: FilePosition,
        user_data: u64,
        /// is_last_in_batch only required for io_uring backend
        #[allow(dead_code)]
        is_last_in_batch: bool,
    },
}

unsafe impl Send for SubmissionEntry {}

pub(crate) trait IoDriver {
    fn available_submission_slots(&mut self) -> usize;
    fn push(&mut self, entry: SubmissionEntry) -> Result<()>;
    /// Flush any staged submissions to the kernel. No-op for backends that
    /// dispatch eagerly inside `push`.
    fn submit(&mut self) -> Result<()>;
    /// How many of the entries pushed the backend still holds, not taken
    /// by the kernel yet: the last pushed. After a failed `submit` the loop
    /// submits nothing more, so these never run, and no one fills their
    /// buffers. 0 for a backend that dispatches eagerly inside `push`.
    fn unsubmitted(&mut self) -> usize;
    fn pop_completion(&mut self) -> Option<CompletionEvent>;
    /// Wait until a new request may have landed on `request_rx` or an
    /// inflight op may have completed. Returning early is fine: the loop
    /// checks both and waits again. Only invoked when at least one op is
    /// inflight.
    fn wait_for_progress(&mut self, request_rx: &mpsc::Receiver<WorkerRequest>);
}

pub(crate) struct BackendLoop<D: IoDriver> {
    receiver: mpsc::Receiver<WorkerRequest>,
    driver: D,
    queue_depth: usize,
    inflight_requests: HashMap<RequestId, InflightRequest>,
    next_request_id: RequestId,
}

impl<D: IoDriver> BackendLoop<D> {
    pub(crate) fn new(queue_depth: usize, rx: mpsc::Receiver<WorkerRequest>, driver: D) -> Self {
        Self {
            receiver: rx,
            driver,
            queue_depth,
            inflight_requests: HashMap::new(),
            next_request_id: 0,
        }
    }

    pub(crate) fn run(mut self) {
        let mut pending_request = None;
        loop {
            self.poll_completions();

            let disconnected = match self.submit_requests(&mut pending_request) {
                Ok(disconnected) => disconnected,
                Err(err) => {
                    self.fail_all(err, pending_request.take());
                    return;
                }
            };
            if disconnected {
                self.finish_in_flight();
                return;
            }

            if self.inflight_requests.is_empty() && pending_request.is_none() {
                match self.receiver.recv() {
                    Ok(request) => pending_request = Some(request),
                    Err(_) => return,
                }
            } else {
                self.driver.wait_for_progress(&self.receiver);
            }
        }
    }

    /// Waits for every operation in flight to complete, each request
    /// answered as usual, once no handle is left to send more: the loop
    /// returns only then, so no buffer a pool thread or the kernel still
    /// fills is freed, no caller waits forever for its answer, and no file
    /// closes under an operation. A backend whose wait returns at once,
    /// io_uring's always and the pool's with no request channel left,
    /// busy-waits here.
    fn finish_in_flight(&mut self) {
        while !self.inflight_requests.is_empty() {
            self.driver.wait_for_progress(&self.receiver);
            self.poll_completions();
        }
    }

    /// Submits the requests waiting, as many as the queue has room for.
    /// Returns whether no handle is left to send more: what was pushed
    /// before that showed is submitted all the same, for the loop to wait
    /// for ([`Self::finish_in_flight`]).
    fn submit_requests(&mut self, pending_request: &mut Option<WorkerRequest>) -> Result<bool> {
        let mut disconnected = false;
        let mut submitted_any = false;

        loop {
            let request = match pending_request.take() {
                Some(request) => request,
                None => match self.receiver.try_recv() {
                    Ok(request) => request,
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        disconnected = true;
                        break;
                    }
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
            if self.submit_request(request)? {
                submitted_any = true;
            }
        }

        if submitted_any {
            self.driver.submit()?;
        }

        Ok(disconnected)
    }

    fn submit_request(&mut self, request: WorkerRequest) -> Result<bool> {
        let request_id = self.allocate_request_id();
        match request {
            WorkerRequest::Read {
                buf,
                at,
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
                    self.driver.push(SubmissionEntry::Read {
                        buf_ptr: buf.as_mut_ptr(),
                        buf_len: buf.len_u32(),
                        at,
                        user_data: encode_user_data(request_id, 0),
                    })
                };
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
                    let is_last = index + 1 == page_count;
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
                        self.driver.push(SubmissionEntry::Write {
                            buf_ptr: page.buf.as_ptr(),
                            buf_len: page.buf.len_u32(),
                            at: page.at,
                            user_data: encode_user_data(request_id, index),
                            is_last_in_batch: is_last,
                        })
                    };

                    match push_result {
                        Ok(()) => submitted_pages += 1,
                        Err(err) => {
                            self.handle_write_submit_error(request_id, submitted_pages, err);
                            return Ok(submitted_pages > 0);
                        }
                    }
                }

                Ok(true)
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

    /// Fails every request once a submit failed with `err`, after which
    /// the loop submits nothing more. An operation the kernel took runs on
    /// into its request's buffer, so the loop first waits for each of
    /// those, answering its request as usual; only the entries the kernel
    /// never took are left then, which nothing will fill, and their
    /// requests fail with the waiting ones and those still to come.
    fn fail_all(&mut self, err: Error, pending_request: Option<WorkerRequest>) {
        let msg = format!("io worker failed: {err}");

        if let Some(request) = pending_request {
            complete_request_with_error(request, worker_failed_error(msg.clone()));
        }

        let untaken = self.driver.unsubmitted();
        while self.operations_in_flight() > untaken {
            self.driver.wait_for_progress(&self.receiver);
            self.poll_completions();
        }
        for (_, request) in self.inflight_requests.drain() {
            request.complete_with_error(worker_failed_error(msg.clone()));
        }

        while let Ok(request) = self.receiver.try_recv() {
            complete_request_with_error(request, worker_failed_error(msg.clone()));
        }
    }

    /// The operations pushed whose completions aren't in yet, across every
    /// request in flight: taken by the kernel or not.
    fn operations_in_flight(&self) -> usize {
        self.inflight_requests
            .values()
            .map(|request| request.remaining)
            .sum()
    }

    fn finish_single_submit(
        &mut self,
        request_id: RequestId,
        push_result: Result<()>,
    ) -> Result<bool> {
        match push_result {
            Ok(()) => Ok(true),
            Err(err) => {
                let request = self
                    .inflight_requests
                    .remove(&request_id)
                    .expect("request missing after failed submit");
                request.complete_with_error(err);
                Ok(false)
            }
        }
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

#[cfg(all(test, not(feature = "shuttle")))]
mod tests {
    use std::collections::VecDeque;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Waker};
    use std::time::{Duration, Instant};

    use pollster::block_on;

    use super::*;
    use crate::PAGE_SIZE_NZ_U32;
    use crate::io::io_task::FileReadTask;

    /// The kernel a [`ScriptedDriver`] stands for, which the test plays: it
    /// takes the entries pushed at each submit, unless the test made that
    /// submit fail, and completes those it took when the test says.
    #[derive(Default)]
    struct ScriptedKernel {
        /// Pushed, not taken: each entry's user data and length.
        queued: Vec<(u64, u32)>,
        /// Taken, and running until the test completes them.
        taken: Vec<(u64, u32)>,
        /// Completions the test let out, for the loop to reap.
        completed: VecDeque<CompletionEvent>,
        fail_next_submit: bool,
        failed: bool,
        /// Whether the loop waited for progress after a submit failed.
        waited_after_failure: bool,
    }

    impl ScriptedKernel {
        /// Completes every entry taken, each reading its whole length.
        fn complete_taken(&mut self) {
            for (user_data, len) in self.taken.drain(..) {
                let result = i32::try_from(len).unwrap();
                self.completed
                    .push_back(CompletionEvent { user_data, result });
            }
        }
    }

    /// The test's hold on a [`ScriptedKernel`].
    #[derive(Clone, Default)]
    struct Script(Arc<Mutex<ScriptedKernel>>);

    impl Script {
        fn with<T>(&self, act: impl FnOnce(&mut ScriptedKernel) -> T) -> T {
            act(&mut self.0.lock().unwrap())
        }

        /// Waits, 10 s at most, until `done` holds of the kernel.
        fn wait_until(&self, what: &str, done: impl Fn(&ScriptedKernel) -> bool) {
            let deadline = Instant::now() + Duration::from_secs(10);
            while !self.with(|kernel| done(kernel)) {
                assert!(Instant::now() < deadline, "timed out waiting until {what}");
                std::thread::yield_now();
            }
        }
    }

    /// A backend over a [`ScriptedKernel`].
    struct ScriptedDriver(Script);

    impl IoDriver for ScriptedDriver {
        fn available_submission_slots(&mut self) -> usize {
            8
        }

        fn push(&mut self, entry: SubmissionEntry) -> Result<()> {
            let SubmissionEntry::Read {
                buf_len, user_data, ..
            } = entry
            else {
                panic!("the test only reads");
            };
            self.0
                .with(|kernel| kernel.queued.push((user_data, buf_len)));
            Ok(())
        }

        fn submit(&mut self) -> Result<()> {
            self.0.with(|kernel| {
                if std::mem::take(&mut kernel.fail_next_submit) {
                    kernel.failed = true;
                    return Err(Error::Io(std::io::Error::other("the submit fails")));
                }
                let queued = std::mem::take(&mut kernel.queued);
                kernel.taken.extend(queued);
                Ok(())
            })
        }

        fn unsubmitted(&mut self) -> usize {
            self.0.with(|kernel| kernel.queued.len())
        }

        fn pop_completion(&mut self) -> Option<CompletionEvent> {
            self.0.with(|kernel| kernel.completed.pop_front())
        }

        fn wait_for_progress(&mut self, _request_rx: &mpsc::Receiver<WorkerRequest>) {
            self.0
                .with(|kernel| kernel.waited_after_failure |= kernel.failed);
            std::thread::yield_now();
        }
    }

    /// A failed submit fails the reads the kernel never took, but waits for
    /// those it took: their buffers are the kernel's until each completes,
    /// and each is answered with its bytes.
    #[test]
    fn a_failed_submit_waits_for_what_the_kernel_took() {
        let script = Script::default();
        let (tx, rx) = mpsc::channel();
        let driver = ScriptedDriver(script.clone());
        let backend = std::thread::spawn(move || BackendLoop::new(8, rx, driver).run());
        let page = || AlignedBuf::new_zeroed(PAGE_SIZE_NZ_U32).unwrap();

        let mut taken = FileReadTask::new(tx.clone(), page(), FilePosition::new(0, 0)).unwrap();
        script.wait_until("the kernel takes the first read", |kernel| {
            kernel.taken.len() == 1
        });
        script.with(|kernel| kernel.fail_next_submit = true);
        let untaken = FileReadTask::new(tx.clone(), page(), FilePosition::new(0, 4096)).unwrap();

        // The loop has handled the failure once it waits on the kernel, or
        // once it has returned.
        let deadline = Instant::now() + Duration::from_secs(10);
        while !script.with(|kernel| kernel.waited_after_failure) && !backend.is_finished() {
            assert!(
                Instant::now() < deadline,
                "the loop neither waits nor returns"
            );
            std::thread::yield_now();
        }
        let answered = Pin::new(&mut taken).poll(&mut Context::from_waker(Waker::noop()));
        assert!(
            answered.is_pending(),
            "a read the kernel holds isn't answered, its buffer dropped: {answered:?}"
        );

        script.with(ScriptedKernel::complete_taken);
        let (buf, read_len) = block_on(taken).unwrap();
        assert_eq!(read_len, buf.len());
        // A loop that waited for the read never taken would wait forever:
        // the wait is bounded, so that fails rather than hangs.
        let (failed_tx, failed_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || failed_tx.send(block_on(untaken).is_err()).unwrap());
        let failed = failed_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the read never taken is answered");
        assert!(failed, "a read never taken fails");
        drop(tx);
        backend.join().unwrap();
    }
}
