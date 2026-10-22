//! The store a benchmark reads a table through: the table's location, a
//! file or a directory of files on the local file system or in GCS, and t4
//! in front of it if asked.

use std::path::PathBuf;
use std::sync::Arc;

use clap::ValueEnum;
use object_store::ObjectStore;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use t4::T4Caching;
use t4::policy::Tier;
use url::Url;

use crate::Error;
use crate::pinned_tier_policy::PinnedTierPolicy;

/// Where a table's files are, as a URL: one file, or a directory of them,
/// whose URL ends in `/`, as DataFusion reads a table's URL.
#[derive(Clone, Debug)]
pub struct TableLocation {
    url: Url,
}

impl TableLocation {
    /// A `gs://bucket/path` URL as given, or a local path: a directory's
    /// URL gets its `/`.
    ///
    /// # Errors
    ///
    /// If `location` isn't a URL, or a local path that exists.
    pub fn parse(location: &str) -> Result<Self, String> {
        let url = if location.starts_with("gs://") {
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

    /// The store that holds the table's files.
    ///
    /// # Errors
    ///
    /// If the URL names no store this harness knows, or the store can't be
    /// made.
    pub fn origin(&self) -> object_store::Result<Arc<dyn ObjectStore>> {
        match self.url.scheme() {
            "file" => Ok(Arc::new(LocalFileSystem::new())),
            "gs" => {
                let bucket = self.url.host_str().unwrap_or_default();
                let store = GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket)
                    .build()?;
                Ok(Arc::new(store))
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
}

/// Which policy t4 runs.
#[derive(Clone, Copy, ValueEnum)]
enum CachePolicyChoice {
    /// t4's default, a CLOCK for each tier: fetched blocks go to memory,
    /// memory's victims to disk.
    Clock,
    /// Every block in memory ([`PinnedTierPolicy`]): memory hits alone.
    Memory,
    /// Every block on disk ([`PinnedTierPolicy`]): disk hits alone.
    Disk,
}

impl CacheOptions {
    /// Whether t4 is asked for.
    pub fn is_set(&self) -> bool {
        self.policy.is_some()
    }

    /// `origin` read through t4, if asked; else `origin` itself.
    ///
    /// # Errors
    ///
    /// If a policy is given without the capacities and path, or t4 can't be
    /// built.
    pub fn in_front_of(&self, origin: Arc<dyn ObjectStore>) -> Result<Arc<dyn ObjectStore>, Error> {
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
        let builder = match policy {
            CachePolicyChoice::Clock => builder,
            CachePolicyChoice::Memory => {
                builder.with_policy(|sizes| PinnedTierPolicy::new(sizes, Tier::Memory))
            }
            CachePolicyChoice::Disk => {
                builder.with_policy(|sizes| PinnedTierPolicy::new(sizes, Tier::Disk))
            }
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
