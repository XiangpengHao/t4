//! The store every process reads, each through a view of its own that can hold its GETs, and
//! that counts the blocks the caches fetch.

use std::collections::BTreeMap;
use std::fmt;
use std::future::poll_fn;
use std::task::{Poll, Waker};

use async_trait::async_trait;
use futures_util::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

use super::model::{Content, Version};
use crate::cache::BLOCK_SIZE_U64;
use crate::io::sync::{Arc, Mutex};

#[derive(Debug, Default)]
pub(super) struct Remote {
    store: InMemory,
    versions: Mutex<BTreeMap<String, Content>>,
    fetched: Mutex<BTreeMap<(Content, u64), u32>>,
}

impl Remote {
    pub(super) fn path(file: usize) -> Path {
        Path::from(format!("file-{file}"))
    }

    pub(super) async fn write(&self, file: usize, version: Option<Version>) {
        let path = Self::path(file);
        let Some(version) = version else {
            self.store.delete(&path).await.unwrap();
            return;
        };
        let put = self.store.put(&path, version.bytes().clone().into()).await;
        let e_tag = put.unwrap().e_tag.expect("the memory store tags every put");
        self.versions
            .lock()
            .unwrap()
            .insert(e_tag, version.content());
    }

    pub(super) async fn size(&self, file: usize) -> Option<u64> {
        let head = self.store.head(&Self::path(file)).await;
        head.ok().map(|meta| meta.size)
    }

    /// How many fetches returned each block of each version: a cache's GETs carry `If-Match`, the
    /// script's own reads none.
    pub(super) fn fetched(&self) -> BTreeMap<(Content, u64), u32> {
        self.fetched.lock().unwrap().clone()
    }

    fn count_fetch(&self, options: &GetOptions, got: &GetResult) {
        let Some(e_tag) = &options.if_match else {
            return;
        };
        let content = *self
            .versions
            .lock()
            .unwrap()
            .get(e_tag)
            .expect("an e_tag of a put");
        let blocks = got.range.start / BLOCK_SIZE_U64..got.range.end.div_ceil(BLOCK_SIZE_U64);
        let mut fetched = self.fetched.lock().unwrap();
        for block in blocks {
            *fetched.entry((content, block)).or_default() += 1;
        }
    }
}

/// A process's store: the remote, its GETs held while the script says.
#[derive(Debug)]
pub(super) struct ProcessStore {
    remote: Arc<Remote>,
    gate: Mutex<Gate>,
}

#[derive(Debug, Default)]
struct Gate {
    holding: bool,
    waiting: Vec<Waker>,
}

impl ProcessStore {
    pub(super) fn new(remote: &Arc<Remote>) -> Self {
        Self {
            remote: Arc::clone(remote),
            gate: Mutex::new(Gate::default()),
        }
    }

    pub(super) fn hold(&self) {
        self.gate.lock().unwrap().holding = true;
    }

    pub(super) fn release(&self) {
        let waiting = {
            let mut gate = self.gate.lock().unwrap();
            gate.holding = false;
            std::mem::take(&mut gate.waiting)
        };
        waiting.into_iter().for_each(Waker::wake);
    }

    async fn pass(&self) {
        poll_fn(|cx| {
            let mut gate = self.gate.lock().unwrap();
            if !gate.holding {
                return Poll::Ready(());
            }
            if !gate
                .waiting
                .iter()
                .any(|waiting| waiting.will_wake(cx.waker()))
            {
                gate.waiting.push(cx.waker().clone());
            }
            Poll::Pending
        })
        .await;
    }
}

impl fmt::Display for ProcessStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a process's view of {}", self.remote.store)
    }
}

#[async_trait]
impl ObjectStore for ProcessStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.remote.store.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.remote.store.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if !options.head {
            self.pass().await;
        }
        let got = self
            .remote
            .store
            .get_opts(location, options.clone())
            .await?;
        self.remote.count_fetch(&options, &got);
        Ok(got)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.remote.store.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.remote.store.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.remote.store.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.remote.store.copy_opts(from, to, options).await
    }
}
