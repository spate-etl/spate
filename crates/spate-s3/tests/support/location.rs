//! An in-memory store for a job location that counts reads and fails, hangs
//! or answers a conditional create as a test scripts.

use futures_util::FutureExt as _;
use futures_util::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, ObjectMeta, ObjectStore, ObjectStoreExt as _, PutMode,
    PutOptions, PutPayload, PutResult,
};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// How the store answers the next conditional create.
#[derive(Clone, Copy, Debug)]
pub(crate) enum OnCreate {
    /// Store the object, then answer `AlreadyExists`.
    StoreThenAlreadyExists,
    /// Answer `AlreadyExists` without storing it, once.
    AlreadyExistsOnce,
    /// Answer `NotImplemented`, as a client with conditional put turned off.
    NotImplemented,
}

/// The test's handle on a [`location_store`].
#[derive(Clone, Debug)]
pub(crate) struct LocationHooks(Arc<Hooks>);

#[derive(Debug, Default)]
struct Hooks {
    inner: InMemory,
    gets: AtomicUsize,
    requests: AtomicUsize,
    failing_gets: AtomicUsize,
    hang: AtomicBool,
    on_create: Mutex<Option<OnCreate>>,
    creates: AtomicUsize,
}

/// A location store and its handle.
pub(crate) fn location_store() -> (Arc<dyn ObjectStore>, LocationHooks) {
    let hooks = LocationHooks(Arc::new(Hooks::default()));
    (Arc::new(LocationStore(hooks.clone())), hooks)
}

impl LocationHooks {
    /// GETs and HEADs so far.
    pub(crate) fn gets(&self) -> usize {
        self.0.gets.load(Ordering::SeqCst)
    }

    /// Every request so far, of any kind.
    pub(crate) fn requests(&self) -> usize {
        self.0.requests.load(Ordering::SeqCst)
    }

    /// Conditional creates so far.
    pub(crate) fn creates(&self) -> usize {
        self.0.creates.load(Ordering::SeqCst)
    }

    /// Answer the next `n` GETs with a retryable error.
    pub(crate) fn fail_next_gets(&self, n: usize) {
        self.0.failing_gets.store(n, Ordering::SeqCst);
    }

    /// Leave every later GET unanswered.
    pub(crate) fn hang_gets(&self) {
        self.0.hang.store(true, Ordering::SeqCst);
    }

    pub(crate) fn on_next_create(&self, answer: OnCreate) {
        *self.0.on_create.lock().unwrap() = Some(answer);
    }

    /// Write `body` at `key` directly, uncounted.
    pub(crate) fn seed(&self, key: &str, body: &[u8]) {
        self.0
            .inner
            .put(&Path::from(key), PutPayload::from(body.to_vec()))
            .now_or_never()
            .expect("an in-memory put completes at once")
            .unwrap();
    }

    /// Delete the object at `key` directly, uncounted.
    pub(crate) fn remove(&self, key: &str) {
        self.0
            .inner
            .delete(&Path::from(key))
            .now_or_never()
            .expect("an in-memory delete completes at once")
            .unwrap();
    }

    /// The bytes at `key`, read directly, uncounted.
    pub(crate) fn read(&self, key: &str) -> Option<Vec<u8>> {
        let got = self
            .0
            .inner
            .get(&Path::from(key))
            .now_or_never()
            .expect("an in-memory get completes at once");
        let got = match got {
            Ok(got) => got,
            Err(object_store::Error::NotFound { .. }) => return None,
            Err(e) => panic!("{e}"),
        };
        Some(
            got.bytes()
                .now_or_never()
                .expect("in-memory bytes are ready")
                .unwrap()
                .to_vec(),
        )
    }
}

#[derive(Debug)]
struct LocationStore(LocationHooks);

impl fmt::Display for LocationStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LocationStore({})", self.0.0.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for LocationStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let hooks = &self.0.0;
        hooks.requests.fetch_add(1, Ordering::SeqCst);
        if matches!(opts.mode, PutMode::Create) {
            hooks.creates.fetch_add(1, Ordering::SeqCst);
            let answer = hooks.on_create.lock().unwrap().take();
            let exists = || object_store::Error::AlreadyExists {
                path: location.to_string(),
                source: "scripted".into(),
            };
            match answer {
                Some(OnCreate::StoreThenAlreadyExists) => {
                    hooks.inner.put_opts(location, payload, opts).await?;
                    return Err(exists());
                }
                Some(OnCreate::AlreadyExistsOnce) => return Err(exists()),
                Some(OnCreate::NotImplemented) => {
                    return Err(object_store::Error::NotImplemented {
                        operation: "conditional put".into(),
                        implementer: "scripted".into(),
                    });
                }
                None => {}
            }
        }
        hooks.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.0.0.requests.fetch_add(1, Ordering::SeqCst);
        self.0.0.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let hooks = &self.0.0;
        hooks.requests.fetch_add(1, Ordering::SeqCst);
        hooks.gets.fetch_add(1, Ordering::SeqCst);
        if hooks.hang.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        let failing = hooks
            .failing_gets
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1));
        if failing.is_ok() {
            return Err(object_store::Error::Generic {
                store: "scripted",
                source: "connection reset".into(),
            });
        }
        hooks.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.0.0.requests.fetch_add(1, Ordering::SeqCst);
        self.0.0.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.0.0.requests.fetch_add(1, Ordering::SeqCst);
        self.0.0.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.0.0.requests.fetch_add(1, Ordering::SeqCst);
        self.0.0.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.0.0.requests.fetch_add(1, Ordering::SeqCst);
        self.0.0.inner.copy_opts(from, to, options).await
    }
}
