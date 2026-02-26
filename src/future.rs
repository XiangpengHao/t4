use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::error::Result;

pub fn leaf<T, F>(f: F) -> LeafFuture<F, T>
where
    F: FnOnce() -> Result<T>,
{
    LeafFuture::new(f)
}

pub struct LeafFuture<F, T>
where
    F: FnOnce() -> Result<T>,
{
    op: Option<F>,
    result: Option<Result<T>>,
    pending_once: bool,
}

impl<F, T> LeafFuture<F, T>
where
    F: FnOnce() -> Result<T>,
{
    pub fn new(op: F) -> Self {
        Self {
            op: Some(op),
            result: None,
            pending_once: false,
        }
    }
}

impl<F, T> Unpin for LeafFuture<F, T> where F: FnOnce() -> Result<T> {}

impl<F, T> Future for LeafFuture<F, T>
where
    F: FnOnce() -> Result<T>,
{
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if !this.pending_once {
            this.pending_once = true;
            let op = this.op.take().expect("LeafFuture polled after completion");
            this.result = Some(op());
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        Poll::Ready(
            this.result
                .take()
                .expect("LeafFuture completed result missing"),
        )
    }
}
