//! [`CountedStore`]: an object store over another that counts the requests
//! made of it as an object store bills them, GETs, HEADs and LIST pages, in
//! a [`RequestCounter`]; and [`RequestPrices`], what they cost.
//!
//! Put under t4, it counts what t4 sends the origin: the requests a read
//! policy decides. Under nothing else, what the reader sends it.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use futures_util::stream::{BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

/// The requests counted so far, shared by a [`CountedStore`] and whoever
/// reads the counts.
#[derive(Debug, Default)]
pub struct RequestCounter {
    gets: AtomicU64,
    heads: AtomicU64,
    list_pages: AtomicU64,
    /// The bytes the GETs answered with.
    get_bytes: AtomicU64,
}

impl RequestCounter {
    /// The counts so far.
    pub fn counts(&self) -> RequestCounts {
        RequestCounts {
            gets: self.gets.load(Ordering::Relaxed),
            heads: self.heads.load(Ordering::Relaxed),
            list_pages: self.list_pages.load(Ordering::Relaxed),
            get_bytes: self.get_bytes.load(Ordering::Relaxed),
        }
    }
}

/// Requests counted: as billed, a request issued counts, whatever its
/// answer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RequestCounts {
    gets: u64,
    heads: u64,
    list_pages: u64,
    get_bytes: u64,
}

impl RequestCounts {
    /// The CSV columns [`Self::csv_fields`] fills.
    pub const CSV_COLUMNS: &str = "gets,heads,list_pages,get_bytes,request_dollars";

    /// The counts between `earlier` and `self`.
    pub fn since(&self, earlier: &Self) -> Self {
        Self {
            gets: self.gets - earlier.gets,
            heads: self.heads - earlier.heads,
            list_pages: self.list_pages - earlier.list_pages,
            get_bytes: self.get_bytes - earlier.get_bytes,
        }
    }

    /// What the requests cost at `prices`, in dollars.
    pub fn dollars(&self, prices: &RequestPrices) -> f64 {
        let per_request = |count: u64, per_thousand: f64| count as f64 * per_thousand / 1e3;
        per_request(self.gets + self.heads, prices.get_per_thousand)
            + per_request(self.list_pages, prices.list_per_thousand)
    }

    /// The fields of [`Self::CSV_COLUMNS`], priced at `prices`.
    pub fn csv_fields(&self, prices: &RequestPrices) -> String {
        format!(
            "{},{},{},{},{:.9}",
            self.gets,
            self.heads,
            self.list_pages,
            self.get_bytes,
            self.dollars(prices)
        )
    }

    pub fn gets(&self) -> u64 {
        self.gets
    }

    pub fn get_bytes(&self) -> u64 {
        self.get_bytes
    }
}

impl fmt::Display for RequestCounts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} GETs of {:.1} MiB, {} HEADs, {} list pages, ${:.6}",
            self.gets,
            self.get_bytes as f64 / f64::from(1 << 20),
            self.heads,
            self.list_pages,
            self.dollars(&RequestPrices::STANDARD)
        )
    }
}

/// What an object store charges a request, in dollars a thousand. Traffic
/// within a region is free.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RequestPrices {
    /// A GET or a HEAD.
    get_per_thousand: f64,
    /// A page of a list.
    list_per_thousand: f64,
}

impl RequestPrices {
    /// S3 Standard's and GCS Standard's, alike (October 2026): $0.0004 a
    /// thousand GETs or HEADs, $0.005 a thousand lists.
    pub const STANDARD: Self = Self {
        get_per_thousand: 0.0004,
        list_per_thousand: 0.005,
    };
}

/// An object store over `inner` that counts the reads made of it in a
/// [`RequestCounter`]. Writes, copies and deletes pass through uncounted.
pub struct CountedStore {
    inner: Arc<dyn ObjectStore>,
    counter: Arc<RequestCounter>,
}

impl CountedStore {
    /// A list's page holds this many objects or prefixes at most, S3's and
    /// GCS's alike.
    const LIST_PAGE: u64 = 1_000;

    pub fn new(inner: Arc<dyn ObjectStore>, counter: Arc<RequestCounter>) -> Self {
        Self { inner, counter }
    }

    /// The pages a list of `named` objects and prefixes takes: one at least.
    fn list_pages(named: u64) -> u64 {
        named.div_ceil(Self::LIST_PAGE).max(1)
    }

    /// `listed`, its pages counted once it ends or is dropped.
    fn counted_listing(
        &self,
        listed: BoxStream<'static, object_store::Result<ObjectMeta>>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let mut listing = Listing {
            counter: Arc::clone(&self.counter),
            named: 0,
        };
        listed.inspect(move |_| listing.named_one()).boxed()
    }
}

/// A list in flight, its pages counted when it drops: once its reader has
/// taken every object, or given up on it.
struct Listing {
    counter: Arc<RequestCounter>,
    named: u64,
}

impl Listing {
    /// The list named one more object or prefix.
    fn named_one(&mut self) {
        self.named += 1;
    }
}

impl Drop for Listing {
    fn drop(&mut self) {
        self.counter
            .list_pages
            .fetch_add(CountedStore::list_pages(self.named), Ordering::Relaxed);
    }
}

impl fmt::Debug for CountedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CountedStore")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for CountedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountedStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountedStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let head = options.head;
        let issued = if head {
            &self.counter.heads
        } else {
            &self.counter.gets
        };
        issued.fetch_add(1, Ordering::Relaxed);
        let result = self.inner.get_opts(location, options).await?;
        if !head {
            self.counter
                .get_bytes
                .fetch_add(result.range.end - result.range.start, Ordering::Relaxed);
        }
        Ok(result)
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
        self.counted_listing(self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.counted_listing(self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let listed = self.inner.list_with_delimiter(prefix).await;
        let named = listed.as_ref().map_or(0, |listed| {
            (listed.objects.len() + listed.common_prefixes.len()) as u64
        });
        self.counter
            .list_pages
            .fetch_add(Self::list_pages(named), Ordering::Relaxed);
        listed
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
    use futures_util::TryStreamExt;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    use super::*;

    /// A store over `objects` objects of 4 KiB under `data/`, and its
    /// counter.
    async fn counted(objects: usize) -> (CountedStore, Arc<RequestCounter>) {
        let inner = InMemory::new();
        for object in 0..objects {
            let path = Path::from(format!("data/{object}"));
            inner.put(&path, vec![7; 4096].into()).await.unwrap();
        }
        let counter = Arc::new(RequestCounter::default());
        (
            CountedStore::new(Arc::new(inner), Arc::clone(&counter)),
            counter,
        )
    }

    /// `get_ranges` reaches the store as the GETs object_store's default
    /// makes of it: ranges under a MiB apart merge into one.
    #[tokio::test]
    async fn counts_each_get_and_head_and_the_bytes_answered() {
        let (store, counter) = counted(1).await;
        let path = Path::from("data/0");
        store.head(&path).await.unwrap();
        store.get_range(&path, 0..100).await.unwrap();
        store.get_ranges(&path, &[0..10, 20..30]).await.unwrap();
        let counts = counter.counts();
        assert_eq!((counts.gets, counts.heads), (2, 1));
        assert_eq!(counts.get_bytes, 100 + 30);
    }

    #[tokio::test]
    async fn a_list_costs_a_page_a_thousand_objects() {
        let (store, counter) = counted(1_001).await;
        let listed: Vec<_> = store
            .list(Some(&Path::from("data")))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(listed.len(), 1_001);
        assert_eq!(counter.counts().list_pages, 2);
        store
            .list_with_delimiter(Some(&Path::from("none")))
            .await
            .unwrap();
        assert_eq!(counter.counts().list_pages, 3, "an empty list costs a page");
    }

    #[test]
    fn standard_prices() {
        let counts = RequestCounts {
            gets: 1_000_000,
            heads: 0,
            list_pages: 1_000,
            get_bytes: 0,
        };
        assert!((counts.dollars(&RequestPrices::STANDARD) - 0.405).abs() < 1e-9);
    }
}
