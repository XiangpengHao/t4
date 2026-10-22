//! [`TracedStore`]: an object store over another that records every read
//! made of it in a [`RequestTrace`], as t4 would see it.
//!
//! t4 answers `get_ranges` as `ObjectStore`'s default does: ranges less
//! than [`OBJECT_STORE_COALESCE_DEFAULT`] apart merge into one GET, and up
//! to ten GETs run at once. So does this store, and its trace holds both:
//! each range the reader asked for (a `range` row) and each GET made for
//! them (a `get` row), tied by their batch. HEADs and lists are recorded
//! too, so a trace names every call a reader makes. [`RecordedRequest`]
//! reads a trace back.

use std::fmt;
use std::io::{self, BufRead, Write};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::ready;
use futures_util::stream::{self, BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    OBJECT_STORE_COALESCE_DEFAULT, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, coalesce_ranges,
};

/// The reads made of a [`TracedStore`], each under the query that was
/// running, timed from the trace's start.
pub struct RequestTrace {
    started: Instant,
    query: Mutex<Arc<str>>,
    /// GETs and HEADs issued and not yet answered.
    in_flight: AtomicU64,
    next_batch: AtomicU64,
    requests: Mutex<Vec<TracedRequest>>,
}

/// What a row of a trace records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestKind {
    /// A GET: one range of an object, or all of it.
    Get,
    Head,
    /// A range a `get_ranges` call asked for; its batch's GETs fetched it.
    Range,
    /// A listing of the objects under a prefix.
    List,
}

impl RequestKind {
    const ALL: [Self; 4] = [Self::Get, Self::Head, Self::Range, Self::List];

    /// The kind's name in a trace.
    fn name(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Head => "head",
            Self::Range => "range",
            Self::List => "list",
        }
    }

    fn named(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.name() == name)
    }
}

/// One row of a [`RequestTrace`].
struct TracedRequest {
    query: Arc<str>,
    kind: RequestKind,
    /// The `get_ranges` call a range or a GET belongs to, if any.
    batch: Option<u64>,
    /// The tokio task that asked, if a task did: in DataFusion, a
    /// partition's stream, whose reads wait on each other and its decoding.
    task: Option<tokio::task::Id>,
    /// The object; for a list, the prefix.
    path: Path,
    /// The object's size; for a list, how many objects and prefixes it
    /// named.
    object_size: u64,
    range: Range<u64>,
    issued: Duration,
    done: Duration,
    /// GETs and HEADs in flight as this one was issued, itself included; 0
    /// for a range.
    in_flight: u64,
}

impl RequestTrace {
    /// An empty trace, its clock started, its query `setup`.
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            query: Mutex::new("setup".into()),
            in_flight: AtomicU64::new(0),
            next_batch: AtomicU64::new(0),
            requests: Mutex::new(Vec::new()),
        }
    }

    /// Records the requests from now on under `query`.
    pub fn begin_query(&self, query: &str) {
        *Self::locked(&self.query) = query.into();
    }

    /// How many requests the trace holds.
    pub fn len(&self) -> usize {
        Self::locked(&self.requests).len()
    }

    /// Whether the trace holds no request.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Writes the trace as CSV, a header row first, times in nanoseconds.
    ///
    /// # Errors
    ///
    /// If `out` fails.
    pub fn write_csv(&self, mut out: impl Write) -> io::Result<()> {
        writeln!(
            out,
            "query,kind,batch,task,path,object_size,start,end,issued_ns,done_ns,in_flight"
        )?;
        for request in Self::locked(&self.requests).iter() {
            request.write_csv(&mut out)?;
        }
        Ok(())
    }

    fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    fn current_query(&self) -> Arc<str> {
        Arc::clone(&Self::locked(&self.query))
    }

    fn record(&self, request: TracedRequest) {
        Self::locked(&self.requests).push(request);
    }

    fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        mutex.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Default for RequestTrace {
    fn default() -> Self {
        Self::new()
    }
}

impl TracedRequest {
    fn write_csv(&self, out: &mut impl Write) -> io::Result<()> {
        let kind = self.kind.name();
        let batch = self
            .batch
            .map(|batch| batch.to_string())
            .unwrap_or_default();
        let task = self.task.map(|task| task.to_string()).unwrap_or_default();
        // A path is quoted, its quotes doubled: it may hold commas.
        let path = self.path.as_ref().replace('"', "\"\"");
        writeln!(
            out,
            "{},{kind},{batch},{task},\"{path}\",{},{},{},{},{},{}",
            self.query,
            self.object_size,
            self.range.start,
            self.range.end,
            self.issued.as_nanos(),
            self.done.as_nanos(),
            self.in_flight,
        )
    }
}

/// A row of a trace, read back from the CSV [`RequestTrace::write_csv`]
/// wrote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedRequest {
    query: String,
    kind: RequestKind,
    batch: Option<u64>,
    task: Option<u64>,
    path: Path,
    range: Range<u64>,
    issued: Duration,
}

impl RecordedRequest {
    /// Every row of a trace, in the order written.
    ///
    /// # Errors
    ///
    /// If `input` fails, or a row isn't one `write_csv` writes.
    pub fn read_csv(input: impl BufRead) -> io::Result<Vec<Self>> {
        input
            .lines()
            .enumerate()
            .skip(1)
            .map(|(index, line)| {
                let line = line?;
                Self::parse(&line).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("trace line {}: not a request: {line}", index + 1),
                    )
                })
            })
            .collect()
    }

    /// A row: `query,kind,batch,task,"path",object_size,start,end,issued_ns,
    /// done_ns,in_flight`, the path quoted, its quotes doubled.
    fn parse(line: &str) -> Option<Self> {
        let mut fields = line.splitn(5, ',');
        let query = fields.next()?.to_owned();
        let kind = RequestKind::named(fields.next()?)?;
        let optional = |field: &str| match field {
            "" => Some(None),
            number => number.parse().ok().map(Some),
        };
        let batch = optional(fields.next()?)?;
        let task = optional(fields.next()?)?;
        let quoted = fields.next()?.strip_prefix('"')?;
        let mut path = String::new();
        let mut chars = quoted.char_indices().peekable();
        let after_path = loop {
            match chars.next()? {
                (_, '"') if chars.peek().is_some_and(|&(_, next)| next == '"') => {
                    chars.next();
                    path.push('"');
                }
                (at, '"') => break at + 1,
                (_, other) => path.push(other),
            }
        };
        let numbers = quoted[after_path..]
            .strip_prefix(',')?
            .split(',')
            .map(|field| field.parse::<u64>().ok())
            .collect::<Option<Vec<_>>>()?;
        let [_object_size, start, end, issued_ns, _done_ns, _in_flight] = numbers[..] else {
            return None;
        };
        Some(Self {
            query,
            kind,
            batch,
            task,
            path: Path::parse(path).ok()?,
            range: start..end,
            issued: Duration::from_nanos(issued_ns),
        })
    }

    /// The query running when it was made: `setup`, or `p{pass}q{query}`.
    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn kind(&self) -> RequestKind {
        self.kind
    }

    /// The `get_ranges` call it belongs to, if any.
    pub fn batch(&self) -> Option<u64> {
        self.batch
    }

    /// The tokio task that made it, if a task did.
    pub fn task(&self) -> Option<u64> {
        self.task
    }

    /// The object; for a list, the prefix.
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn range(&self) -> Range<u64> {
        self.range.clone()
    }

    /// When it was issued, from the trace's start.
    pub fn issued(&self) -> Duration {
        self.issued
    }
}

/// Counts a GET or HEAD in flight until it drops, answered or not.
struct InFlight<'a> {
    count: &'a AtomicU64,
    /// The count once this one was added.
    at_issue: u64,
}

impl<'a> InFlight<'a> {
    fn issue(count: &'a AtomicU64) -> Self {
        let at_issue = count.fetch_add(1, Ordering::Relaxed) + 1;
        Self { count, at_issue }
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A list in flight, recorded once its listing ends, with how many objects
/// and prefixes it named.
struct Listing {
    trace: Arc<RequestTrace>,
    query: Arc<str>,
    task: Option<tokio::task::Id>,
    prefix: Path,
    issued: Duration,
}

impl Listing {
    fn begin(trace: &Arc<RequestTrace>, prefix: Option<&Path>) -> Self {
        Self {
            trace: Arc::clone(trace),
            query: trace.current_query(),
            task: tokio::task::try_id(),
            prefix: prefix.cloned().unwrap_or_default(),
            issued: trace.elapsed(),
        }
    }

    /// `listed`, recorded once it ends: when its reader has taken every
    /// object from it.
    fn traced(
        self,
        listed: BoxStream<'static, object_store::Result<ObjectMeta>>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let named = Arc::new(AtomicU64::new(0));
        let counted = Arc::clone(&named);
        let end = stream::once(async move {
            self.finish(named.load(Ordering::Relaxed));
            None
        });
        listed
            .inspect(move |_| {
                counted.fetch_add(1, Ordering::Relaxed);
            })
            .map(Some)
            .chain(end)
            .filter_map(ready)
            .boxed()
    }

    fn finish(self, named: u64) {
        let done = self.trace.elapsed();
        self.trace.record(TracedRequest {
            query: self.query,
            kind: RequestKind::List,
            batch: None,
            task: self.task,
            path: self.prefix,
            object_size: named,
            range: 0..0,
            issued: self.issued,
            done,
            in_flight: 0,
        });
    }
}

/// An object store over `inner` that records its reads and lists in a
/// [`RequestTrace`]. Writes, copies and deletes pass through unrecorded.
pub struct TracedStore {
    inner: Arc<dyn ObjectStore>,
    trace: Arc<RequestTrace>,
}

impl TracedStore {
    pub fn new(inner: Arc<dyn ObjectStore>, trace: Arc<RequestTrace>) -> Self {
        Self { inner, trace }
    }

    /// Answers a GET or HEAD from the inner store, whole, and records it
    /// under `batch`. Its bytes are read before it is recorded done, so its
    /// time includes the transfer.
    async fn get_traced(
        &self,
        location: &Path,
        options: GetOptions,
        batch: Option<u64>,
    ) -> object_store::Result<GetResult> {
        let kind = if options.head {
            RequestKind::Head
        } else {
            RequestKind::Get
        };
        let issued = self.trace.elapsed();
        let in_flight = InFlight::issue(&self.trace.in_flight);
        let result = self.inner.get_opts(location, options).await?;
        let (meta, range, attributes) = (
            result.meta.clone(),
            result.range.clone(),
            result.attributes.clone(),
        );
        // A HEAD reads no bytes, whatever range the inner store names.
        let (bytes, read) = match kind {
            RequestKind::Head => (Bytes::new(), 0..0),
            _ => (result.bytes().await?, range.clone()),
        };
        let at_issue = in_flight.at_issue;
        drop(in_flight);
        self.trace.record(TracedRequest {
            query: self.trace.current_query(),
            kind,
            batch,
            task: tokio::task::try_id(),
            path: location.clone(),
            object_size: meta.size,
            range: read,
            issued,
            done: self.trace.elapsed(),
            in_flight: at_issue,
        });
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(ready(Ok(bytes))).boxed()),
            meta,
            range,
            attributes,
        })
    }
}

impl fmt::Debug for TracedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TracedStore")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for TracedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TracedStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for TracedStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.get_traced(location, options, None).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let batch = self.trace.next_batch.fetch_add(1, Ordering::Relaxed);
        let issued = self.trace.elapsed();
        let object_size = AtomicU64::new(0);
        let fetched = coalesce_ranges(
            ranges,
            |range| {
                let object_size = &object_size;
                async move {
                    let options = GetOptions {
                        range: Some(range.into()),
                        ..GetOptions::default()
                    };
                    let result = self.get_traced(location, options, Some(batch)).await?;
                    object_size.store(result.meta.size, Ordering::Relaxed);
                    result.bytes().await
                }
            },
            OBJECT_STORE_COALESCE_DEFAULT,
        )
        .await?;
        let done = self.trace.elapsed();
        let query = self.trace.current_query();
        for range in ranges {
            self.trace.record(TracedRequest {
                query: Arc::clone(&query),
                kind: RequestKind::Range,
                batch: Some(batch),
                task: tokio::task::try_id(),
                path: location.clone(),
                object_size: object_size.load(Ordering::Relaxed),
                range: range.clone(),
                issued,
                done,
                in_flight: 0,
            });
        }
        Ok(fetched)
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
        Listing::begin(&self.trace, prefix).traced(self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        Listing::begin(&self.trace, prefix).traced(self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let listing = Listing::begin(&self.trace, prefix);
        let listed = self.inner.list_with_delimiter(prefix).await?;
        listing.finish((listed.objects.len() + listed.common_prefixes.len()) as u64);
        Ok(listed)
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

    /// A store over one object of `size` bytes at `data`, and its trace.
    async fn traced_object(size: usize) -> (TracedStore, Arc<RequestTrace>, Path) {
        let inner = InMemory::new();
        let path = Path::from("data");
        inner.put(&path, vec![7; size].into()).await.unwrap();
        let trace = Arc::new(RequestTrace::new());
        let store = TracedStore::new(Arc::new(inner), Arc::clone(&trace));
        (store, trace, path)
    }

    fn csv_rows(trace: &RequestTrace) -> Vec<String> {
        let mut csv = Vec::new();
        trace.write_csv(&mut csv).unwrap();
        String::from_utf8(csv)
            .unwrap()
            .lines()
            .skip(1)
            .map(str::to_owned)
            .collect()
    }

    /// Ranges a megabyte or more apart are two GETs, closer ones one; the
    /// trace names every range asked for and every GET, in one batch.
    #[tokio::test]
    async fn get_ranges_records_ranges_and_the_gets_made_for_them() {
        let (store, trace, path) = traced_object(4 << 20).await;
        trace.begin_query("q");
        let ranges = [0..10, 100..200, (3 << 20)..(3 << 20) + 5];
        let fetched = store.get_ranges(&path, &ranges).await.unwrap();
        assert_eq!(
            fetched.iter().map(Bytes::len).collect::<Vec<_>>(),
            [10, 100, 5]
        );

        let rows = csv_rows(&trace);
        let kinds_and_ranges: Vec<String> = rows
            .iter()
            .map(|row| {
                let columns: Vec<&str> = row.split(',').collect();
                format!(
                    "{} {} {}..{}",
                    columns[0], columns[1], columns[6], columns[7]
                )
            })
            .collect();
        let (mut gets, ranges): (Vec<_>, Vec<_>) = kinds_and_ranges
            .into_iter()
            .partition(|row| row.contains(" get "));
        gets.sort();
        assert_eq!(gets, ["q get 0..200", "q get 3145728..3145733"]);
        assert_eq!(
            ranges,
            [
                "q range 0..10",
                "q range 100..200",
                "q range 3145728..3145733"
            ]
        );
        assert!(
            rows.iter()
                .all(|row| row.contains(",0,") && row.contains(",\"data\",4194304,"))
        );
    }

    /// A HEAD is recorded with no bytes; a plain GET outside a batch with
    /// no batch.
    #[tokio::test]
    async fn heads_and_gets_outside_a_batch() {
        let (store, trace, path) = traced_object(100).await;
        store.head(&path).await.unwrap();
        let bytes = store.get_range(&path, 10..20).await.unwrap();
        assert_eq!(bytes.len(), 10);
        let rows = csv_rows(&trace);
        assert!(rows[0].starts_with("setup,head,,"));
        assert!(rows[0].contains(",\"data\",100,0,0,"));
        assert!(rows[1].starts_with("setup,get,,"));
        assert!(rows[1].contains(",\"data\",100,10,20,"));
        assert!(rows[1].ends_with(",1"));
    }

    /// A list is recorded once its listing ends, with its prefix and how
    /// many objects it named.
    #[tokio::test]
    async fn lists_and_what_they_named() {
        let (store, trace, path) = traced_object(100).await;
        store
            .put(&Path::from("dir/a"), vec![1].into())
            .await
            .unwrap();
        let listed: Vec<_> = store.list(None).collect().await;
        assert_eq!(listed.len(), 2);
        store
            .list_with_delimiter(Some(&Path::from("dir")))
            .await
            .unwrap();
        let rows = csv_rows(&trace);
        assert_eq!(rows.len(), 2, "{rows:?}; the put of {path} isn't recorded");
        assert!(rows[0].starts_with("setup,list,,"));
        assert!(rows[0].contains(",\"\",2,0,0,"));
        assert!(rows[1].contains(",\"dir\",1,0,0,"));
    }

    /// A trace reads back as written: every kind, batches, a path with a
    /// comma and a quote in it.
    #[tokio::test]
    async fn a_trace_reads_back() {
        let (store, trace, path) = traced_object(4 << 20).await;
        let odd = Path::from("a,\"b\"");
        store.put(&odd, vec![1; 10].into()).await.unwrap();
        trace.begin_query("p0q3");
        store.head(&path).await.unwrap();
        store
            .get_ranges(&path, &[0..10, (3 << 20)..(3 << 20) + 5])
            .await
            .unwrap();
        store.get_range(&odd, 2..4).await.unwrap();
        let _: Vec<_> = store.list(None).collect().await;

        let mut csv = Vec::new();
        trace.write_csv(&mut csv).unwrap();
        let read = RecordedRequest::read_csv(csv.as_slice()).unwrap();
        let kinds: Vec<_> = read.iter().map(RecordedRequest::kind).collect();
        assert_eq!(read.len(), 7);
        assert!(read.iter().all(|request| request.query() == "p0q3"));
        let ranges: Vec<_> = read
            .iter()
            .filter(|request| request.kind() == RequestKind::Range)
            .collect();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[1].range(), (3 << 20)..(3 << 20) + 5);
        assert_eq!(ranges[0].batch(), Some(0));
        let get = read
            .iter()
            .find(|request| request.path() == &odd)
            .expect("the plain GET");
        assert_eq!(
            (get.kind(), get.batch(), get.range()),
            (RequestKind::Get, None, 2..4)
        );
        for kind in [RequestKind::Head, RequestKind::List] {
            assert!(kinds.contains(&kind), "{kind:?} in {kinds:?}");
        }
    }
}
