//! Computed bytes: terabytes need no drive or page cache, and every byte read can be checked.

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures_util::future::ready;
use futures_util::stream::{self, BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

pub struct SyntheticObjects {
    objects: BTreeMap<Path, SyntheticObject>,
    /// Twice over, so a read up to [`Self::PATTERN_LEN`] is one slice wherever it starts.
    pattern: Bytes,
}

struct SyntheticObject {
    meta: ObjectMeta,
    pattern_offset: u64,
}

impl SyntheticObjects {
    const PATTERN_LEN: u64 = 64 << 20;

    pub fn new(objects: impl IntoIterator<Item = (Path, u64)>) -> Self {
        let objects = objects
            .into_iter()
            .map(|(path, size)| {
                let pattern_offset = Self::path_hash(&path) % Self::PATTERN_LEN;
                let meta = ObjectMeta {
                    location: path.clone(),
                    last_modified: DateTime::<Utc>::default(),
                    size,
                    e_tag: Some(format!("{pattern_offset:x}-{size:x}")),
                    version: None,
                };
                (
                    path,
                    SyntheticObject {
                        meta,
                        pattern_offset,
                    },
                )
            })
            .collect();
        Self {
            objects,
            pattern: Self::pattern(),
        }
    }

    pub fn bytes(&self, path: &Path, range: Range<u64>) -> Bytes {
        self.object_bytes(&self.objects[path], range)
    }

    fn object_bytes(&self, object: &SyntheticObject, range: Range<u64>) -> Bytes {
        let len = range.end - range.start;
        let start = (object.pattern_offset + range.start) % Self::PATTERN_LEN;
        if len <= Self::PATTERN_LEN {
            return self.pattern.slice(start as usize..(start + len) as usize);
        }
        let mut bytes = Vec::with_capacity(len as usize);
        let once = &self.pattern[..Self::PATTERN_LEN as usize];
        let mut at = start as usize;
        while bytes.len() < len as usize {
            let take = (once.len() - at).min(len as usize - bytes.len());
            bytes.extend_from_slice(&once[at..at + take]);
            at = 0;
        }
        bytes.into()
    }

    /// FNV-1a, so an object's offset in the pattern is the same in every run.
    fn path_hash(path: &Path) -> u64 {
        path.as_ref()
            .bytes()
            .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
            })
    }

    /// xorshift64*.
    fn pattern() -> Bytes {
        let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
        let once: Vec<u8> = (0..Self::PATTERN_LEN / 8)
            .flat_map(|_| {
                state ^= state >> 12;
                state ^= state << 25;
                state ^= state >> 27;
                state.wrapping_mul(0x2545_f491_4f6c_dd1d).to_le_bytes()
            })
            .collect();
        once.repeat(2).into()
    }

    fn not_supported(operation: &str) -> object_store::Error {
        object_store::Error::NotSupported {
            source: format!("SyntheticObjects is read-only: {operation}").into(),
        }
    }
}

impl fmt::Debug for SyntheticObjects {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SyntheticObjects")
            .field("objects", &self.objects.len())
            .finish_non_exhaustive()
    }
}

impl fmt::Display for SyntheticObjects {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SyntheticObjects({} objects)", self.objects.len())
    }
}

#[async_trait]
impl ObjectStore for SyntheticObjects {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let object = self
            .objects
            .get(location)
            .ok_or_else(|| object_store::Error::NotFound {
                path: location.to_string(),
                source: "no synthetic object there".into(),
            })?;
        options.check_preconditions(&object.meta)?;
        let range =
            match &options.range {
                Some(range) => range.as_range(object.meta.size).map_err(|source| {
                    object_store::Error::Generic {
                        store: "SyntheticObjects",
                        source: Box::new(source),
                    }
                })?,
                None => 0..object.meta.size,
            };
        let bytes = if options.head {
            Bytes::new()
        } else {
            self.object_bytes(object, range.clone())
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(ready(Ok(bytes))).boxed()),
            meta: object.meta.clone(),
            range,
            attributes: Default::default(),
        })
    }

    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(Self::not_supported("put"))
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(Self::not_supported("put"))
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        locations
            .map(|_| Err(Self::not_supported("delete")))
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let metas: Vec<_> = self
            .objects
            .values()
            .filter(|object| {
                prefix.is_none_or(|prefix| object.meta.location.prefix_matches(prefix))
            })
            .map(|object| Ok(object.meta.clone()))
            .collect();
        stream::iter(metas).boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let offset = offset.clone();
        self.list(prefix)
            .filter(move |meta| ready(meta.as_ref().map_or(true, |meta| meta.location > offset)))
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        Err(Self::not_supported("list_with_delimiter"))
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        Err(Self::not_supported("copy"))
    }
}

#[cfg(test)]
mod tests {
    use object_store::{GetRange, ObjectStoreExt};

    use super::*;

    const LEN: u64 = SyntheticObjects::PATTERN_LEN;

    fn objects() -> SyntheticObjects {
        SyntheticObjects::new([(Path::from("a"), 3 * LEN), (Path::from("b"), 100)])
    }

    /// The long read crosses the pattern's end twice.
    #[tokio::test]
    async fn reads_agree_however_they_are_cut() {
        let store = objects();
        let path = Path::from("a");
        let whole = store.get_range(&path, 5..2 * LEN + 9).await.unwrap();
        let mut pieces = Vec::new();
        for piece in [5..LEN, LEN..LEN + 3, LEN + 3..2 * LEN + 9] {
            pieces.extend_from_slice(&store.get_range(&path, piece).await.unwrap());
        }
        assert_eq!(whole, pieces);
        assert_eq!(whole, store.bytes(&path, 5..2 * LEN + 9));
    }

    #[tokio::test]
    async fn objects_differ() {
        let store = objects();
        let a = store.get_range(&Path::from("a"), 0..100).await.unwrap();
        let b = store.get_range(&Path::from("b"), 0..100).await.unwrap();
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn answers_as_an_object_store() {
        let store = objects();
        let path = Path::from("b");
        let meta = store.head(&path).await.unwrap();
        assert_eq!(meta.size, 100);
        let options = GetOptions {
            range: Some(GetRange::Suffix(10)),
            ..GetOptions::default()
        };
        let suffix = store.get_opts(&path, options).await.unwrap();
        assert_eq!(suffix.range, 90..100);
        assert_eq!(suffix.bytes().await.unwrap(), store.bytes(&path, 90..100));
        let options = GetOptions {
            if_match: Some("another".into()),
            ..GetOptions::default()
        };
        assert!(matches!(
            store.get_opts(&path, options).await,
            Err(object_store::Error::Precondition { .. })
        ));
    }
}
