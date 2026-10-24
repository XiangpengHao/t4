//! Vortex tables whose segment reads reach their store as Vortex asks for
//! them, each its own range. Vortex's object-store reader merges reads less
//! than 1 MiB apart, up to 16 MiB, before its store sees them: t4, the
//! store, would see Vortex's merges, not its reads. DataFusion's Parquet
//! reader hands its store the raw ranges too, so the two formats reach t4
//! alike.
//!
//! The footers DataFusion reads to plan a scan, for a table's schema and
//! statistics, still go through Vortex's own reader, merged.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::{Result, Statistics};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileScanConfig, FileSource};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::physical_plan::ExecutionPlan;
use futures_util::future::BoxFuture;
use object_store::{ObjectMeta, ObjectStore};
use vortex::array::buffer::BufferHandle;
use vortex::buffer::Alignment;
use vortex::error::VortexResult;
use vortex::io::{ReadAtRequest, ReadAtStream, VortexReadAt};
use vortex::session::VortexSession;
use vortex_datafusion::reader::{DefaultVortexReaderFactory, VortexReaderFactory};
use vortex_datafusion::{VortexFormat, VortexSource};

/// [`VortexFormat`], its scans' segments read raw ([`RawReaders`]). It
/// reads tables; it doesn't write them.
#[derive(Debug)]
pub struct RawVortexFormat {
    format: VortexFormat,
    readers: Arc<RawReaders>,
}

impl RawVortexFormat {
    /// Reads tables in `store` with `session`.
    pub fn new(session: VortexSession, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            format: VortexFormat::new(session),
            readers: Arc::new(RawReaders(DefaultVortexReaderFactory::new(store))),
        }
    }
}

#[async_trait]
impl FileFormat for RawVortexFormat {
    fn get_ext(&self) -> String {
        self.format.get_ext()
    }

    fn get_ext_with_compression(&self, compression: &FileCompressionType) -> Result<String> {
        self.format.get_ext_with_compression(compression)
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        self.format.compression_type()
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        self.format.infer_schema(state, store, objects).await
    }

    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        self.format
            .infer_stats(state, store, table_schema, object)
            .await
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.format.create_physical_plan(state, conf).await
    }

    /// Vortex's source, its files read by [`RawReaders`].
    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        let source = self.format.file_source(table_schema);
        let source = source
            .downcast_ref::<VortexSource>()
            .expect("VortexFormat's source is a VortexSource")
            .clone();
        Arc::new(source.with_vortex_reader_factory(Arc::clone(&self.readers) as _))
    }
}

/// vortex-datafusion's readers of a store, none of their reads merged.
#[derive(Debug)]
struct RawReaders(DefaultVortexReaderFactory);

impl VortexReaderFactory for RawReaders {
    fn create_reader(
        &self,
        file: &PartitionedFile,
        session: &VortexSession,
    ) -> Result<Arc<dyn VortexReadAt>> {
        Ok(Arc::new(RawReadAt(self.0.create_reader(file, session)?)))
    }
}

/// A reader of a Vortex file with no window to merge reads in, the trait's
/// `coalesce_config`: each read Vortex asks for reaches the store as its
/// own range.
struct RawReadAt(Arc<dyn VortexReadAt>);

impl VortexReadAt for RawReadAt {
    fn uri(&self) -> Option<&Arc<str>> {
        self.0.uri()
    }

    fn concurrency(&self) -> usize {
        self.0.concurrency()
    }

    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        self.0.size()
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        self.0.read_at(offset, length, alignment)
    }

    fn read_ranges(&self, requests: Arc<[ReadAtRequest]>) -> ReadAtStream {
        self.0.read_ranges(requests)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::datasource::listing::ListingOptions;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use url::Url;
    use vortex::VortexSessionDefault;
    use vortex_datafusion::VortexFormatFactory;

    use super::*;
    use crate::request_cost::{CountedStore, RequestCounter};

    fn store_url() -> Url {
        Url::parse("memory://").unwrap()
    }

    /// A table of two columns of random floats, 200,000 rows: segments of
    /// each column, side by side in its one file.
    async fn table() -> Arc<InMemory> {
        let store = Arc::new(InMemory::new());
        let mut state = SessionStateBuilder::new().with_default_features();
        if let Some(formats) = state.file_formats() {
            formats.push(Arc::new(VortexFormatFactory::new()));
        }
        let context = SessionContext::new_with_state(state.build());
        context.register_object_store(&store_url(), Arc::clone(&store) as _);
        context
            .sql(
                "COPY (SELECT random() AS a, random() AS b FROM generate_series(1, 200000)) \
                 TO 'memory:///t/data.vortex' STORED AS VORTEX",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        store
    }

    /// The GETs a scan of every column of the table in `store` sends, the
    /// table read as `format` says.
    async fn gets_of_scan(
        store: &Arc<InMemory>,
        format: impl FnOnce(Arc<dyn ObjectStore>) -> Arc<dyn FileFormat>,
    ) -> u64 {
        let counter = Arc::new(RequestCounter::default());
        let counted: Arc<dyn ObjectStore> = Arc::new(CountedStore::new(
            Arc::clone(store) as _,
            Arc::clone(&counter),
        ));
        let context = SessionContext::new();
        context.register_object_store(&store_url(), Arc::clone(&counted));
        let options = ListingOptions::new(format(counted));
        context
            .register_listing_table("t", "memory:///t/", options, None, None)
            .await
            .unwrap();
        context
            .sql("SELECT * FROM t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        counter.counts().gets()
    }

    /// Vortex's reader merges the segments a scan reads into few GETs; read
    /// raw, each segment is a GET of its own.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_scan_reads_its_segments_unmerged() {
        let store = table().await;
        let merged = gets_of_scan(&store, |_| {
            Arc::new(VortexFormat::new(VortexSession::default()))
        })
        .await;
        let raw = gets_of_scan(&store, |store| {
            Arc::new(RawVortexFormat::new(VortexSession::default(), store))
        })
        .await;
        assert!(raw > merged, "{raw} GETs raw, {merged} merged");
    }
}
