//! [`SimulatedStore`]: an object store over another that answers like a
//! remote one, as its [`NetworkModel`] says, so t4's misses can be
//! measured without a cloud. Writes, lists, copies and deletes pass
//! through at once: t4 only reads.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::ready;
use futures_util::stream::{self, BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use tokio::time::{Instant, sleep_until};

/// How a simulated remote store answers. Each request's first byte comes
/// `first_byte` after it is issued; its bytes then flow at
/// `stream_bandwidth` at most, and all requests share one link of
/// `link_bandwidth`.
///
/// The link sends whole responses one after another, in the order they are
/// ready, where a real one shares itself between them: a long response
/// delays the short ones behind it more than TCP would. The link's total
/// is the same, and the total is what a cache that keeps the NIC busy
/// meets.
#[derive(Clone, Copy, Debug)]
pub struct NetworkModel {
    first_byte: Duration,
    /// Bytes a second.
    stream_bandwidth: u64,
    /// Bytes a second.
    link_bandwidth: u64,
}

impl NetworkModel {
    /// # Panics
    ///
    /// If a bandwidth is 0.
    pub fn new(first_byte: Duration, stream_bandwidth: u64, link_bandwidth: u64) -> Self {
        assert!(
            stream_bandwidth > 0 && link_bandwidth > 0,
            "a bandwidth is more than 0 bytes a second"
        );
        Self {
            first_byte,
            stream_bandwidth,
            link_bandwidth,
        }
    }

    /// How long `bytes` take at `bandwidth` bytes a second.
    fn transfer(bytes: u64, bandwidth: u64) -> Duration {
        let nanos = u128::from(bytes) * 1_000_000_000 / u128::from(bandwidth);
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }
}

/// An object store over `inner` whose reads arrive as a [`NetworkModel`]
/// says, each rounded up to tokio's timer, a millisecond. The inner store
/// is asked at once, and should answer well before the model does: a
/// response it holds back past its arrival is late, and
/// [`Self::late_responses`] counts it.
#[derive(Debug)]
pub struct SimulatedStore {
    inner: Arc<dyn ObjectStore>,
    network: NetworkModel,
    /// When the link is free of the responses ahead.
    link_free_at: Mutex<Instant>,
    late_responses: AtomicU64,
}

impl SimulatedStore {
    pub fn new(inner: Arc<dyn ObjectStore>, network: NetworkModel) -> Self {
        Self {
            inner,
            network,
            link_free_at: Mutex::new(Instant::now()),
            late_responses: AtomicU64::new(0),
        }
    }

    /// The responses the inner store held back past their arrival, which
    /// arrived late: so many that the inner store, not the model, set the
    /// pace.
    pub fn late_responses(&self) -> u64 {
        self.late_responses.load(Ordering::Relaxed)
    }

    /// When a response of `bytes` to a request issued at `issued` arrives:
    /// its first byte, then its bytes over the link after the responses
    /// ahead, and no sooner than one stream carries them. Takes its turn on
    /// the link.
    fn arrival(&self, issued: Instant, bytes: u64) -> Instant {
        let first_byte = issued + self.network.first_byte;
        let sent = {
            let mut link_free_at = self
                .link_free_at
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let start = first_byte.max(*link_free_at);
            *link_free_at = start + NetworkModel::transfer(bytes, self.network.link_bandwidth);
            *link_free_at
        };
        sent.max(first_byte + NetworkModel::transfer(bytes, self.network.stream_bandwidth))
    }

    /// Waits until `arrival`, counting a response already past it late.
    async fn arrive_at(&self, arrival: Instant) {
        if Instant::now() > arrival {
            self.late_responses.fetch_add(1, Ordering::Relaxed);
        }
        sleep_until(arrival).await;
    }
}

impl fmt::Display for SimulatedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SimulatedStore({:?} over {})", self.network, self.inner)
    }
}

#[async_trait]
impl ObjectStore for SimulatedStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let issued = Instant::now();
        let head = options.head;
        let result = match self.inner.get_opts(location, options).await {
            Ok(result) => result,
            Err(error) => {
                self.arrive_at(issued + self.network.first_byte).await;
                return Err(error);
            }
        };
        let (meta, range, attributes) = (
            result.meta.clone(),
            result.range.clone(),
            result.attributes.clone(),
        );
        // A HEAD's answer has no body: it takes no turn on the link.
        let (bytes, arrival) = if head {
            (Bytes::new(), issued + self.network.first_byte)
        } else {
            let bytes = result.bytes().await?;
            let arrival = self.arrival(issued, bytes.len() as u64);
            (bytes, arrival)
        };
        self.arrive_at(arrival).await;
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(ready(Ok(bytes))).boxed()),
            meta,
            range,
            attributes,
        })
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    use super::*;

    const MB: u64 = 1_000_000;

    /// A simulated store over objects `a` and `b`, each of 10 MB: 10 ms to
    /// the first byte, 100 MB/s a stream, 1,000 MB/s on the link.
    async fn two_objects() -> SimulatedStore {
        let inner = InMemory::new();
        for name in ["a", "b"] {
            inner
                .put(&Path::from(name), vec![0; 10 * MB as usize].into())
                .await
                .unwrap();
        }
        let network = NetworkModel::new(Duration::from_millis(10), 100 * MB, 1_000 * MB);
        SimulatedStore::new(Arc::new(inner), network)
    }

    /// A read takes the first-byte latency, then its bytes at the stream's
    /// bandwidth: 100 KB 1 ms, 10 MB 100 ms.
    #[tokio::test(start_paused = true)]
    async fn one_read_takes_its_first_byte_then_its_stream() {
        let store = two_objects().await;
        let start = Instant::now();
        store.get_range(&Path::from("a"), 0..MB / 10).await.unwrap();
        assert_eq!(start.elapsed(), Duration::from_millis(11));

        let start = Instant::now();
        store.get_range(&Path::from("a"), 0..10 * MB).await.unwrap();
        assert_eq!(start.elapsed(), Duration::from_millis(110));
        assert_eq!(store.late_responses(), 0);
    }

    /// Twenty streams of 10 MB at once fill the link: 200 MB at 1,000 MB/s
    /// take 200 ms after the first byte, not one stream's 100 ms.
    #[tokio::test(start_paused = true)]
    async fn reads_at_once_share_the_link() {
        let store = two_objects().await;
        let start = Instant::now();
        let reads = (0..20).map(|i| {
            let path = Path::from(if i % 2 == 0 { "a" } else { "b" });
            let store = &store;
            async move { store.get_range(&path, 0..10 * MB).await.unwrap() }
        });
        futures_util::future::join_all(reads).await;
        assert_eq!(start.elapsed(), Duration::from_millis(210));
    }

    /// A HEAD, and a read of a missing object, take the first-byte latency.
    #[tokio::test(start_paused = true)]
    async fn heads_and_errors_take_the_first_byte() {
        let store = two_objects().await;
        let start = Instant::now();
        assert_eq!(store.head(&Path::from("b")).await.unwrap().size, 10 * MB);
        assert!(store.get_range(&Path::from("c"), 0..1).await.is_err());
        assert_eq!(start.elapsed(), Duration::from_millis(20));
    }
}
