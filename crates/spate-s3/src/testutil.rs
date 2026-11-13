//! Test-only framer and store wrapper shared across the crate's unit tests.

use spate_core::framing::RecordFramer;
use spate_json::NdjsonFramer;
use std::sync::atomic::{AtomicUsize, Ordering};

/// `spate-json`'s NDJSON framer under a 1 MiB record cap, which no unit test
/// reaches.
pub(crate) fn ndjson_framer() -> Box<dyn RecordFramer> {
    Box::new(NdjsonFramer::new(1 << 20))
}

/// Answers a GET for the path, or `None` to read the inner store.
pub(crate) type GetHook =
    fn(&object_store::path::Path) -> Option<object_store::Result<object_store::GetResult>>;

/// How a [`PutHook`] answers a PUT.
#[derive(Debug)]
pub(crate) enum PutAnswer {
    /// Store nothing and fail.
    Fail(object_store::Error),
    /// Store the object, then fail as a lost reply would.
    StoreThenFail(object_store::Error),
}

/// Answers a PUT for the path, or `None` to write the inner store.
pub(crate) type PutHook = fn(&object_store::path::Path) -> Option<PutAnswer>;

/// Answers a DELETE for the path with an error, or `None` to delete from
/// the inner store.
pub(crate) type DeleteHook = fn(&object_store::path::Path) -> Option<object_store::Error>;

/// One write a [`HookStore`] took, or one event a test's feed records beside
/// them, in the order they happened.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Event {
    /// A stored PUT, with its body.
    Put(String, Vec<u8>),
    /// A DELETE.
    Delete(String),
    /// A unit published, by id.
    Published(String),
    /// A unit confirmed, by id.
    Confirmed(String),
}

/// A log of [`Event`]s shared between a store and a feed.
pub(crate) type EventLog = std::sync::Arc<std::sync::Mutex<Vec<Event>>>;

/// An in-memory store whose GETs, PUTs and listing order a test can
/// override.
#[derive(Debug, Default)]
pub(crate) struct HookStore {
    pub(crate) inner: object_store::memory::InMemory,
    /// Answers a GET in place of `inner` when it returns `Some`.
    pub(crate) get: Option<GetHook>,
    /// Answers a PUT in place of `inner` when it returns `Some`.
    pub(crate) put: Option<PutHook>,
    /// Answers a DELETE in place of `inner` when it returns `Some`.
    pub(crate) delete: Option<DeleteHook>,
    /// Lists in reverse key order.
    pub(crate) reverse_list: bool,
    /// Answers every conditional create `AlreadyExists` without storing.
    pub(crate) creates_exist: bool,
    /// Stores every conditional create over an existing object, as a store
    /// that ignores `If-None-Match` does.
    pub(crate) creates_overwrite: bool,
    /// Conditional creates so far.
    pub(crate) creates: AtomicUsize,
    /// The offset of each offset listing, in order.
    pub(crate) offsets: std::sync::Mutex<Vec<object_store::path::Path>>,
    /// Where stored PUTs and DELETEs are logged.
    pub(crate) events: Option<EventLog>,
}

impl HookStore {
    fn log(&self, event: Event) {
        if let Some(events) = &self.events {
            events.lock().unwrap().push(event);
        }
    }
}

impl std::fmt::Display for HookStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HookStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for HookStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        let body: Vec<u8> = payload.iter().flat_map(|b| b.iter().copied()).collect();
        match self.put.and_then(|put| put(location)) {
            Some(PutAnswer::Fail(e)) => return Err(e),
            Some(PutAnswer::StoreThenFail(e)) => {
                self.inner.put_opts(location, payload, opts).await?;
                self.log(Event::Put(location.to_string(), body));
                return Err(e);
            }
            None => {}
        }
        let mut opts = opts;
        if matches!(opts.mode, object_store::PutMode::Create) {
            self.creates.fetch_add(1, Ordering::SeqCst);
            if self.creates_exist {
                return Err(object_store::Error::AlreadyExists {
                    path: location.to_string(),
                    source: "scripted".into(),
                });
            }
            if self.creates_overwrite {
                opts.mode = object_store::PutMode::Overwrite;
            }
        }
        let put = self.inner.put_opts(location, payload, opts).await?;
        self.log(Event::Put(location.to_string(), body));
        Ok(put)
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if let Some(answer) = self.get.and_then(|get| get(location)) {
            return answer;
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        use futures_util::StreamExt as _;
        let events = self.events.clone();
        let hook = self.delete;
        let locations = locations
            .map(move |location| match (hook, location) {
                (Some(hook), Ok(path)) => hook(&path).map_or(Ok(path), Err),
                (_, location) => location,
            })
            .boxed();
        self.inner
            .delete_stream(locations)
            .inspect(move |deleted| {
                if let (Some(events), Ok(path)) = (&events, deleted) {
                    events.lock().unwrap().push(Event::Delete(path.to_string()));
                }
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        use futures_util::StreamExt as _;
        let listing = self.inner.list(prefix);
        if !self.reverse_list {
            return listing;
        }
        futures_util::stream::once(async move {
            let mut all: Vec<_> = listing.collect().await;
            all.reverse();
            futures_util::stream::iter(all)
        })
        .flatten()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&object_store::path::Path>,
        offset: &object_store::path::Path,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.offsets.lock().unwrap().push(offset.clone());
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
