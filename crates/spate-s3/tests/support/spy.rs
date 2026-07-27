//! A delegating `ObjectStore` that counts LIST calls — the teeth behind
//! the "workers never list" acceptance criterion: a coordinated job's
//! total LIST count across every instance must be exactly the planner's.

use futures_util::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, ObjectMeta, ObjectStore, PutPayload, PutResult,
};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Wrap a root-scoped local filesystem store, counting `list` calls.
/// Returns the store (for `S3Source::with_store`) and the counter.
pub(crate) fn counting_local_store() -> (Arc<dyn ObjectStore>, Arc<AtomicUsize>) {
    let lists = Arc::new(AtomicUsize::new(0));
    let store = CountingStore {
        inner: LocalFileSystem::new(),
        lists: Arc::clone(&lists),
    };
    (Arc::new(store), lists)
}

#[derive(Debug)]
struct CountingStore {
    inner: LocalFileSystem,
    lists: Arc<AtomicUsize>,
}

impl fmt::Display for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
