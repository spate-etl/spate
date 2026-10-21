//! Test-only framer and store wrapper shared across the crate's unit tests.
//!
//! `spate-s3` ships no record framer of its own; production wires the format's
//! framer via [`S3Source::with_framer`](crate::S3Source::with_framer). The
//! tests still need to turn newline-delimited object bytes into records, so
//! they carry this local line framer, mirroring `spate-json`'s `NdjsonFramer`
//! semantics without taking a dependency on a format crate.

use spate_core::framing::RecordFramer;
use std::collections::VecDeque;
use std::io;

/// Newline-delimited [`RecordFramer`]: split on `\n`, strip one trailing `\r`,
/// skip whitespace-only lines, keep an unterminated final line, bounded per
/// record. Behaviorally identical to `spate-json`'s `NdjsonFramer`, so the
/// resume-by-record-index tests exercise the same record boundaries.
pub(crate) struct TestLineFramer {
    partial: Vec<u8>,
    ready: VecDeque<Vec<u8>>,
    decoded_bytes: u64,
    max_record_bytes: usize,
}

impl TestLineFramer {
    pub(crate) fn new(max_record_bytes: usize) -> TestLineFramer {
        TestLineFramer {
            partial: Vec::new(),
            ready: VecDeque::new(),
            decoded_bytes: 0,
            max_record_bytes,
        }
    }

    fn push_partial(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.partial.len() + bytes.len() > self.max_record_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "record line exceeds the configured max_record_bytes ({})",
                    self.max_record_bytes
                ),
            ));
        }
        self.partial.extend_from_slice(bytes);
        Ok(())
    }

    fn complete_line(&mut self) {
        if self.partial.last() == Some(&b'\r') {
            self.partial.pop();
        }
        if self.partial.iter().all(u8::is_ascii_whitespace) {
            self.partial.clear();
            return;
        }
        self.ready.push_back(std::mem::take(&mut self.partial));
    }
}

impl RecordFramer for TestLineFramer {
    fn push(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.decoded_bytes += bytes.len() as u64;
        let mut rest = bytes;
        while let Some(nl) = rest.iter().position(|&b| b == b'\n') {
            self.push_partial(&rest[..nl])?;
            self.complete_line();
            rest = &rest[nl + 1..];
        }
        self.push_partial(rest)
    }

    fn finish(&mut self) -> io::Result<()> {
        if !self.partial.is_empty() {
            self.complete_line();
        }
        Ok(())
    }

    fn pop(&mut self) -> Option<Vec<u8>> {
        self.ready.pop_front()
    }

    fn decoded_bytes(&self) -> u64 {
        self.decoded_bytes
    }

    fn resync_delimiter(&self) -> Option<u8> {
        Some(b'\n')
    }
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
    pub(crate) creates: std::sync::atomic::AtomicUsize,
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
            self.creates
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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
