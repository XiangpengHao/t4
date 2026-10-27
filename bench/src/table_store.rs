//! The store a benchmark reads a table through: the table's location, a
//! file or a directory of files on the local file system, in GCS or in S3,
//! moving its HTTP as asked, and t4 in front of it if asked.

use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::ValueEnum;
use object_store::aws::AmazonS3Builder;
use object_store::client::{HttpClient, HttpConnector, ReqwestConnector, SpawnedReqwestConnector};
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::limit::LimitStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ClientOptions, ObjectStore};
use t4::T4Caching;
use t4::policy::{Admit, FetchRuns, RaceStragglers};
use tokio::runtime::{Handle, Runtime};
use url::Url;

use crate::Error;
use crate::fixed_placement_policy::FixedPlacementPolicy;
use crate::io_latency::{IoLatencies, TimedReads};
use crate::ktls_transport::KtlsConnector;

/// Where a table's files are, as a URL: one file, or a directory of them,
/// whose URL ends in `/`, as DataFusion reads a table's URL.
#[derive(Clone, Debug)]
pub struct TableLocation {
    url: Url,
}

impl TableLocation {
    /// A `gs://bucket/path` or `s3://bucket/path` URL as given, or a local
    /// path: a directory's URL gets its `/`.
    ///
    /// # Errors
    ///
    /// If `location` isn't a URL, or a local path that exists.
    pub fn parse(location: &str) -> Result<Self, String> {
        let url = if location.starts_with("gs://") || location.starts_with("s3://") {
            Url::parse(location).map_err(|error| format!("{location}: {error}"))?
        } else {
            let path =
                std::fs::canonicalize(location).map_err(|error| format!("{location}: {error}"))?;
            if path.is_dir() {
                Url::from_directory_path(&path)
            } else {
                Url::from_file_path(&path)
            }
            .map_err(|()| format!("{location}: not a file path"))?
        };
        Ok(Self { url })
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    /// The URL of the store that serves the table: its scheme and bucket.
    pub fn store_url(&self) -> Url {
        Url::parse(&self.url[..url::Position::BeforePath])
            .expect("a URL's scheme and host are a URL")
    }

    /// The store that holds the table's files, a cloud one moving its HTTP
    /// as `http` says, configured from the environment (GCS's `GOOGLE_*`,
    /// S3's `AWS_*` variables).
    ///
    /// # Errors
    ///
    /// If the URL names no store this harness knows, or the store can't be
    /// made.
    pub fn origin(&self, http: &StoreHttp) -> object_store::Result<Arc<dyn ObjectStore>> {
        let bucket = self.url.host_str().unwrap_or_default();
        match self.url.scheme() {
            "file" => Ok(Arc::new(LocalFileSystem::new())),
            "gs" => Ok(http.limited(
                GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket)
                    .with_http_connector(http.connector())
                    .build()?,
            )),
            "s3" => Ok(http.limited(
                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_http_connector(http.connector())
                    .build()?,
            )),
            scheme => Err(object_store::Error::NotSupported {
                source: format!("{}: no store for {scheme}://", self.url).into(),
            }),
        }
    }

    /// The table's file, or its directory, in its store.
    ///
    /// # Errors
    ///
    /// If the URL's path isn't a valid object path.
    pub fn path(&self) -> object_store::Result<Path> {
        Ok(Path::from_url_path(self.url.path())?)
    }

    /// The object in this table that stands for `object`, a file of the
    /// same table at another location: the table's file itself, or the file
    /// of `object`'s name in the table's directory.
    ///
    /// # Errors
    ///
    /// If the table's path isn't valid, or `object` has no file name.
    pub fn counterpart(&self, object: &Path) -> object_store::Result<Path> {
        let path = self.path()?;
        if !self.url.path().ends_with('/') {
            return Ok(path);
        }
        let name = object
            .filename()
            .ok_or_else(|| object_store::Error::Generic {
                store: "TableLocation",
                source: format!("{object}: no file name").into(),
            })?;
        Ok(path.join(name))
    }
}

/// How a cloud store moves its HTTP: its transport, the threads it runs on,
/// and the requests it has in flight at most.
#[derive(clap::Args, Debug)]
pub struct HttpOptions {
    /// The transport.
    #[arg(long = "http", value_enum, default_value_t)]
    transport: HttpTransport,
    /// Run a cloud store's HTTP, its bodies read and decrypted included, on
    /// a runtime of this many threads of its own, apart from the work it
    /// serves; on the threads of that work unless given.
    #[arg(long = "io-threads")]
    io_threads: Option<NonZeroUsize>,
    /// A cloud store's requests in flight at most, a GET's until its body
    /// is all in (object_store's `LimitStore`); no limit unless given. What
    /// fills the NIC and queues no more: its bytes a second times a GET's
    /// time unloaded, over a GET's bytes; about 190 for 50 Gbit/s and t4's
    /// 1 MiB GETs at 32 ms.
    #[arg(long = "requests-in-flight")]
    requests_in_flight: Option<NonZeroUsize>,
}

impl HttpOptions {
    /// The HTTP these options ask for, its threads started if asked.
    ///
    /// # Errors
    ///
    /// If the threads can't be started.
    pub fn start(&self) -> std::io::Result<StoreHttp> {
        Ok(StoreHttp {
            transport: self.transport,
            io_threads: self.io_threads.map(IoThreads::start).transpose()?,
            requests_in_flight: self.requests_in_flight,
        })
    }
}

/// A cloud store's HTTP as [`HttpOptions`] asked for it, its threads
/// running while it lives.
pub struct StoreHttp {
    transport: HttpTransport,
    io_threads: Option<IoThreads>,
    requests_in_flight: Option<NonZeroUsize>,
}

impl StoreHttp {
    /// A cloud store's connector: the transport, on the threads of its own
    /// if there are any.
    fn connector(&self) -> StoreConnector {
        StoreConnector {
            transport: self.transport,
            io: self.io_threads.as_ref().map(IoThreads::handle),
        }
    }

    /// `store`, its requests in flight capped if asked.
    fn limited(&self, store: impl ObjectStore) -> Arc<dyn ObjectStore> {
        match self.requests_in_flight {
            Some(most) => Arc::new(LimitStore::new(store, most.get())),
            None => Arc::new(store),
        }
    }
}

/// The threads of a cloud store's HTTP: a tokio runtime of their own. It
/// shuts down without waiting when dropped, as a runtime dropped on
/// another runtime's thread would panic.
struct IoThreads {
    runtime: Option<Runtime>,
}

impl IoThreads {
    fn start(threads: NonZeroUsize) -> std::io::Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads.get())
            .thread_name("http-io")
            .enable_all()
            .build()?;
        Ok(Self {
            runtime: Some(runtime),
        })
    }

    fn handle(&self) -> Handle {
        self.runtime
            .as_ref()
            .expect("the runtime lives until dropped")
            .handle()
            .clone()
    }
}

impl Drop for IoThreads {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

/// How a cloud store moves its HTTP.
#[derive(Clone, Copy, Debug, Default, ValueEnum)]
enum HttpTransport {
    /// object_store's own: reqwest over hyper and rustls.
    #[default]
    Reqwest,
    /// [`KtlsConnector`]: the kernel decrypts, into the body's own buffer
    /// (Linux only).
    Ktls,
}

/// A cloud store's HTTP connector: its transport, run on `io`'s threads if
/// given, else on the threads that call it.
#[derive(Debug)]
struct StoreConnector {
    transport: HttpTransport,
    io: Option<Handle>,
}

impl HttpConnector for StoreConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        match (self.transport, &self.io) {
            (HttpTransport::Reqwest, None) => ReqwestConnector::default().connect(options),
            (HttpTransport::Reqwest, Some(io)) => {
                SpawnedReqwestConnector::new(io.clone()).connect(options)
            }
            (HttpTransport::Ktls, io) => KtlsConnector::new(io.clone()).connect(options),
        }
    }
}

/// t4 in front of a table's store, if `--cache-policy` is given.
#[derive(clap::Args)]
pub struct CacheOptions {
    /// Read through t4, with this policy.
    #[arg(long = "cache-policy", value_enum)]
    policy: Option<CachePolicyChoice>,
    /// t4's memory tier, in MiB.
    #[arg(long = "cache-memory-mib")]
    memory_mib: Option<u64>,
    /// A cache file of t4's, one a drive: the disk tier stripes over them.
    #[arg(long = "cache-path")]
    paths: Vec<PathBuf>,
    /// Each cache file's size, in GiB.
    #[arg(long = "cache-disk-gib")]
    disk_gib: Option<u64>,
    /// The I/Os to the cache files in flight at most, all of them together;
    /// t4's default unless given.
    #[arg(long = "cache-queue-depth")]
    queue_depth: Option<u32>,
    /// Race each GET of t4's once more if its body isn't all in by this
    /// quantile of the GET times t4 heard lately ([`RaceStragglers`]);
    /// never unless given.
    #[arg(long = "race-at", value_enum)]
    race_at: Option<RaceAt>,
    /// The NIC's rate, in Gbit/s: t4 caps its GETs' bytes in flight at
    /// what fills it, this rate times `--cache-unloaded-get-ms`, and GETs
    /// past it wait their turn in t4. No cap unless given.
    #[arg(long = "cache-network-gbit")]
    network_gbit: Option<f64>,
    /// What a GET of a block takes with little else in flight, in ms: the
    /// other half of t4's cap.
    #[arg(long = "cache-unloaded-get-ms", default_value_t = 32.0)]
    unloaded_get_ms: f64,
    /// While t4's GETs wait their turn, those of adjacent bytes of a file
    /// go as one GET of this many MiB at most; each apart unless given.
    #[arg(long = "cache-merge-waiting-mib")]
    merge_waiting_mib: Option<u64>,
}

/// The quantile of the GET times heard that t4 races a GET past.
#[derive(Clone, Copy, ValueEnum)]
enum RaceAt {
    P90,
    P99,
}

impl RaceAt {
    fn quantile(self) -> f64 {
        match self {
            Self::P90 => 0.9,
            Self::P99 => 0.99,
        }
    }
}

/// Which policy t4 runs.
#[derive(Clone, Copy, ValueEnum)]
enum CachePolicyChoice {
    /// t4's default, a CLOCK for each tier: fetched blocks go to memory,
    /// memory's victims to disk.
    Clock,
    /// Every block in memory ([`FixedPlacementPolicy`]): memory hits alone.
    Memory,
    /// Every block on disk ([`FixedPlacementPolicy`]): disk hits alone.
    Disk,
    /// No block kept ([`FixedPlacementPolicy`]): every read from the
    /// remote through t4, what it costs to go through t4 at all.
    None,
}

impl CacheOptions {
    /// Whether t4 is asked for.
    pub fn is_set(&self) -> bool {
        self.policy.is_some()
    }

    /// `origin` read through t4, if asked, every I/O t4 makes timed in
    /// `latencies`; else `origin` itself.
    ///
    /// # Errors
    ///
    /// If a policy is given without the capacities and path, or t4 can't be
    /// built.
    pub fn in_front_of(
        &self,
        origin: Arc<dyn ObjectStore>,
        latencies: &Arc<IoLatencies>,
    ) -> Result<Arc<dyn ObjectStore>, Error> {
        let Some(policy) = self.policy else {
            return Ok(origin);
        };
        let (Some(memory_mib), false, Some(disk_gib)) =
            (self.memory_mib, self.paths.is_empty(), self.disk_gib)
        else {
            return Err(
                "--cache-policy needs --cache-memory-mib, --cache-path and --cache-disk-gib".into(),
            );
        };
        let builder = self
            .paths
            .iter()
            .fold(T4Caching::builder(origin), |builder, path| {
                builder.with_disk_file(path, disk_gib << 30)
            })
            .with_memory_capacity(memory_mib << 20);
        let builder = match self.race_at {
            None => builder.with_read_policy(TimedReads::new(FetchRuns, Arc::clone(latencies))),
            Some(race_at) => builder
                .with_read_policy(TimedReads::new(
                    RaceStragglers::at(race_at.quantile()),
                    Arc::clone(latencies),
                ))
                .with_sleep(|after| Box::pin(tokio::time::sleep(after))),
        };
        let builder = match self.queue_depth {
            Some(queue_depth) => builder.with_queue_depth(queue_depth),
            None => builder,
        };
        let builder = match self.network_gbit {
            Some(gbit) => builder.with_network_rate(
                (gbit * 1e9 / 8.0) as u64,
                Duration::from_secs_f64(self.unloaded_get_ms / 1e3),
            ),
            None => builder,
        };
        let builder = match self.merge_waiting_mib {
            Some(mib) => builder.with_waiting_gets_merged(mib << 20),
            None => builder,
        };
        let builder = match policy {
            CachePolicyChoice::Clock => builder,
            CachePolicyChoice::Memory => builder
                .with_placement_policy(|sizes| FixedPlacementPolicy::new(sizes, Admit::Memory)),
            CachePolicyChoice::Disk => {
                builder.with_placement_policy(|sizes| FixedPlacementPolicy::new(sizes, Admit::Disk))
            }
            CachePolicyChoice::None => builder
                .with_placement_policy(|sizes| FixedPlacementPolicy::new(sizes, Admit::Discard)),
        };
        Ok(Arc::new(builder.build()?))
    }
}

#[cfg(test)]
mod tests {
    use futures_util::FutureExt;
    use object_store::memory::InMemory;
    use object_store::{GetOptions, ObjectStoreExt};

    use super::*;

    fn http(io_threads: Option<usize>, requests_in_flight: Option<usize>) -> StoreHttp {
        HttpOptions {
            transport: HttpTransport::Reqwest,
            io_threads: io_threads.and_then(NonZeroUsize::new),
            requests_in_flight: requests_in_flight.and_then(NonZeroUsize::new),
        }
        .start()
        .unwrap()
    }

    /// The HTTP's own threads run what the connector hands them, and stop
    /// when dropped on another runtime's thread, which would otherwise
    /// panic.
    #[tokio::test]
    async fn http_threads_run_apart_and_stop_when_dropped() {
        let http = http(Some(2), None);
        let io = http.connector().io.expect("threads were asked for");
        let ran_on = io
            .spawn(async { std::thread::current().name().map(str::to_owned) })
            .await
            .unwrap();
        assert_eq!(ran_on.as_deref(), Some("http-io"));
        drop(http);
    }

    /// A capped store holds a GET's place until its body is dropped.
    #[tokio::test]
    async fn a_capped_store_holds_a_get_until_its_body_is_done() {
        let object = Path::from("a");
        let inner = InMemory::new();
        inner.put(&object, vec![7; 64].into()).await.unwrap();
        let store = http(None, Some(1)).limited(inner);
        let first = store
            .get_opts(&object, GetOptions::default())
            .await
            .unwrap();
        assert!(
            store
                .get_opts(&object, GetOptions::default())
                .now_or_never()
                .is_none(),
            "a second GET waits while the first's body is held"
        );
        drop(first);
        assert!(
            store
                .get_opts(&object, GetOptions::default())
                .now_or_never()
                .is_some()
        );
    }

    /// A file location stands for every file of the table; a directory
    /// for the file of the same name in it.
    #[test]
    fn counterparts_of_traced_objects() {
        let traced = Path::from("clickbench/partitioned/hits_7.parquet");
        let file = TableLocation::parse("gs://bucket/data/hits.parquet").unwrap();
        assert_eq!(
            file.counterpart(&traced).unwrap().as_ref(),
            "data/hits.parquet"
        );
        let directory = TableLocation::parse("gs://bucket/data/partitioned/").unwrap();
        assert_eq!(
            directory.counterpart(&traced).unwrap().as_ref(),
            "data/partitioned/hits_7.parquet"
        );
        assert_eq!(
            directory.store_url().as_str(),
            "gs://bucket",
            "the store is the bucket"
        );
    }
}
