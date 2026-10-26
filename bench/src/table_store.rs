//! The store a benchmark reads a table through: the table's location, a
//! file or a directory of files on the local file system, in GCS or in S3,
//! moving its HTTP as asked, and t4 in front of it if asked.

use std::path::PathBuf;
use std::sync::Arc;

use clap::ValueEnum;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use t4::T4Caching;
use t4::policy::{Admit, FetchRuns, RaceStragglers};
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
    /// through `transport`, configured from the environment (GCS's
    /// `GOOGLE_*`, S3's `AWS_*` variables).
    ///
    /// # Errors
    ///
    /// If the URL names no store this harness knows, or the store can't be
    /// made.
    pub fn origin(&self, transport: HttpTransport) -> object_store::Result<Arc<dyn ObjectStore>> {
        let bucket = self.url.host_str().unwrap_or_default();
        match self.url.scheme() {
            "file" => Ok(Arc::new(LocalFileSystem::new())),
            "gs" => {
                let builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket);
                let builder = match transport {
                    HttpTransport::Reqwest => builder,
                    HttpTransport::Ktls => builder.with_http_connector(KtlsConnector),
                };
                Ok(Arc::new(builder.build()?))
            }
            "s3" => {
                let builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
                let builder = match transport {
                    HttpTransport::Reqwest => builder,
                    HttpTransport::Ktls => builder.with_http_connector(KtlsConnector),
                };
                Ok(Arc::new(builder.build()?))
            }
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

/// How a cloud store moves its HTTP.
#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub enum HttpTransport {
    /// object_store's own: reqwest over hyper and rustls.
    #[default]
    Reqwest,
    /// [`KtlsConnector`]: the kernel decrypts, into the body's own buffer
    /// (Linux only).
    Ktls,
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
    use super::*;

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
