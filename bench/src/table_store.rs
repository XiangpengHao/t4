use std::num::{NonZeroU8, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::ValueEnum;
use object_store::aws::AmazonS3Builder;
use object_store::client::{HttpClient, HttpConnector, ReqwestConnector, SpawnedReqwestConnector};
use object_store::gcp::{GcpCredential, GoogleCloudStorageBuilder};
use object_store::limit::LimitStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ClientOptions, ObjectStore, StaticCredentialProvider};
use t4::T4Caching;
use t4::policy::{Admit, FetchRuns, FixedPlacementPolicy, RaceStragglers};
use tokio::runtime::{Handle, Runtime};
use url::Url;

use crate::Error;
use crate::io_latency::{IoLatencies, TimedReads};
use crate::ktls_transport::KtlsConnector;

/// A directory's URL ends in `/`, as DataFusion reads a table's URL.
#[derive(Clone, Debug)]
pub struct TableLocation {
    url: Url,
}

impl TableLocation {
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

    pub fn store_url(&self) -> Url {
        Url::parse(&self.url[..url::Position::BeforePath])
            .expect("a URL's scheme and host are a URL")
    }

    /// `GCS_ACCESS_TOKEN` serves a box outside Google Cloud: `gcloud auth print-access-token`'s.
    pub fn origin(&self, http: &StoreHttp) -> object_store::Result<Arc<dyn ObjectStore>> {
        let bucket = self.url.host_str().unwrap_or_default();
        match self.url.scheme() {
            "file" => Ok(Arc::new(LocalFileSystem::new())),
            "gs" => {
                let builder = GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket)
                    .with_http_connector(http.connector());
                let builder = match std::env::var("GCS_ACCESS_TOKEN") {
                    Ok(bearer) => builder.with_credentials(Arc::new(
                        StaticCredentialProvider::new(GcpCredential { bearer }),
                    )),
                    Err(_) => builder,
                };
                Ok(http.limited(builder.build()?))
            }
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

    pub fn path(&self) -> object_store::Result<Path> {
        Ok(Path::from_url_path(self.url.path())?)
    }

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

/// How a cloud store moves its HTTP.
#[derive(clap::Args, Debug)]
pub struct HttpOptions {
    /// The transport.
    #[arg(long = "http", value_enum, default_value_t)]
    transport: HttpTransport,
    /// Run a cloud store's HTTP on a runtime of this many threads of its own.
    #[arg(long = "io-threads")]
    io_threads: Option<NonZeroUsize>,
    /// A cloud store's requests in flight at most; about 190 fills 50 Gbit/s with 1 MiB GETs.
    #[arg(long = "requests-in-flight")]
    requests_in_flight: Option<NonZeroUsize>,
}

impl HttpOptions {
    pub fn start(&self) -> std::io::Result<StoreHttp> {
        Ok(StoreHttp {
            transport: self.transport,
            io_threads: self.io_threads.map(IoThreads::start).transpose()?,
            requests_in_flight: self.requests_in_flight,
        })
    }
}

pub struct StoreHttp {
    transport: HttpTransport,
    io_threads: Option<IoThreads>,
    requests_in_flight: Option<NonZeroUsize>,
}

impl StoreHttp {
    fn connector(&self) -> StoreConnector {
        StoreConnector {
            transport: self.transport,
            io: self.io_threads.as_ref().map(IoThreads::handle),
        }
    }

    fn limited(&self, store: impl ObjectStore) -> Arc<dyn ObjectStore> {
        match self.requests_in_flight {
            Some(most) => Arc::new(LimitStore::new(store, most.get())),
            None => Arc::new(store),
        }
    }
}

/// Shuts down without waiting on drop: a runtime dropped on another runtime's thread panics.
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

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
enum HttpTransport {
    /// object_store's own: reqwest over hyper and rustls.
    #[default]
    Reqwest,
    /// The kernel decrypts, into the body's own buffer (Linux only).
    Ktls,
}

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
    /// The cache files' I/Os in flight at most, all together; t4's default unless given.
    #[arg(long = "cache-queue-depth")]
    queue_depth: Option<u32>,
    /// Race a GET not done by this quantile of recent GET times; t4's default, p90.
    #[arg(long = "race-at", value_enum, default_value_t)]
    race_at: RaceAt,
    /// Requests racing for a file's 2 MiB footer; given more than once, sessions take turns.
    #[arg(long = "footer-race")]
    footer_races: Vec<NonZeroU8>,
    /// The NIC's rate in Gbit/s, for t4's cap on GET bytes in flight; t4's default, 50.
    #[arg(long = "cache-network-gbit")]
    network_gbit: Option<f64>,
    /// A block GET's time with little else in flight, in ms: the other half of t4's cap.
    #[arg(long = "cache-unloaded-get-ms", default_value_t = 32.0)]
    unloaded_get_ms: f64,
    /// Merge waiting GETs of adjacent bytes up to this many MiB, 0 for none; t4's default, 4.
    #[arg(long = "cache-merge-waiting-mib")]
    merge_waiting_mib: Option<u64>,
}

#[derive(Clone, Copy, Default, ValueEnum)]
enum RaceAt {
    #[default]
    P90,
    P99,
    /// No GET raced late.
    Never,
}

impl RaceAt {
    fn quantile(self) -> Option<f64> {
        match self {
            Self::P90 => Some(0.9),
            Self::P99 => Some(0.99),
            Self::Never => None,
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum CachePolicyChoice {
    /// t4's default: a CLOCK per tier, memory's victims to disk.
    Clock,
    /// Every block in memory: memory hits alone.
    Memory,
    /// Every block on disk: disk hits alone.
    Disk,
    /// No block kept: what going through t4 at all costs.
    None,
}

impl CacheOptions {
    pub fn is_set(&self) -> bool {
        self.policy.is_some()
    }

    pub fn footer_race(&self, session: usize) -> Option<NonZeroU8> {
        (!self.footer_races.is_empty())
            .then(|| self.footer_races[session % self.footer_races.len()])
    }

    pub fn in_front_of(
        &self,
        origin: Arc<dyn ObjectStore>,
        latencies: &Arc<IoLatencies>,
        session: usize,
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
        let fetches = self
            .footer_race(session)
            .map_or_else(FetchRuns::default, FetchRuns::racing_footers);
        let builder = match self.race_at.quantile() {
            None => builder.with_read_policy(TimedReads::new(fetches, Arc::clone(latencies))),
            Some(quantile) => builder
                .with_read_policy(TimedReads::new(
                    RaceStragglers::at(quantile).planning_as(fetches),
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

    /// The drop is on a runtime's thread, where an IoThreads that waited would panic.
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
