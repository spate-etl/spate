//! A local-file `ObjectStore` that lets a test overwrite an object between
//! two GETs of one read, and script the HEAD that follows.
//!
//! `LocalFileSystem` reports and compares ETags quoted, as S3 does, so a
//! notification unit's quoted pin reads a local file as it reads an S3
//! object. A test can hold one GET until it releases it, overwrite the file
//! in between, and script the answer to a HEAD.

use futures_util::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, ObjectMeta, ObjectStore, PutPayload, PutResult,
};
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

/// One request the store saw.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Seen {
    /// The object's file name.
    pub(crate) object: String,
    /// The `If-Match` sent, as sent.
    pub(crate) if_match: Option<String>,
    /// The object version sent.
    pub(crate) version: Option<String>,
    /// Whether the request was a HEAD.
    pub(crate) head: bool,
}

/// A scripted answer to one HEAD.
#[derive(Clone, Copy, Debug)]
pub(crate) enum HeadAnswer {
    /// 404.
    NotFound,
    /// 403.
    Denied,
}

/// See the module docs.
pub(crate) struct ScriptedStore {
    inner: LocalFileSystem,
    gets: AtomicU32,
    /// The GET, counted from 1 with HEADs not counted, held until
    /// [`release`](Self::release); 0 holds none.
    hold: AtomicU32,
    holding: AtomicBool,
    release: watch::Sender<bool>,
    heads: Mutex<VecDeque<HeadAnswer>>,
    seen: Mutex<Vec<Seen>>,
}

impl fmt::Debug for ScriptedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ScriptedStore")
    }
}

impl fmt::Display for ScriptedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ScriptedStore")
    }
}

impl ScriptedStore {
    /// A store over the directory `root`.
    pub(crate) fn new(root: &std::path::Path) -> Arc<ScriptedStore> {
        Arc::new(ScriptedStore {
            inner: LocalFileSystem::new_with_prefix(root).unwrap(),
            gets: AtomicU32::new(0),
            hold: AtomicU32::new(0),
            holding: AtomicBool::new(false),
            release: watch::channel(false).0,
            heads: Mutex::new(VecDeque::new()),
            seen: Mutex::new(Vec::new()),
        })
    }

    /// Hold the `n`th GET, counted from 1, until [`release`](Self::release).
    pub(crate) fn hold_get(&self, n: u32) {
        self.hold.store(n, Ordering::SeqCst);
    }

    /// Whether the held GET has arrived and waits.
    pub(crate) fn holding(&self) -> bool {
        self.holding.load(Ordering::SeqCst)
    }

    /// Let the held GET reach the files.
    pub(crate) fn release(&self) {
        self.release.send_replace(true);
    }

    /// Answer the next HEAD with `answer` (repeat to script a sequence).
    pub(crate) fn answer_head(&self, answer: HeadAnswer) {
        self.heads.lock().unwrap().push_back(answer);
    }

    /// Every request, in order.
    pub(crate) fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    /// The HEADs the store saw.
    pub(crate) fn heads(&self) -> usize {
        self.seen().iter().filter(|s| s.head).count()
    }
}

#[async_trait::async_trait]
impl ObjectStore for ScriptedStore {
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
        self.seen.lock().unwrap().push(Seen {
            object: location.filename().unwrap_or_default().to_owned(),
            if_match: options.if_match.clone(),
            version: options.version.clone(),
            head: options.head,
        });
        if options.head {
            let answer = self.heads.lock().unwrap().pop_front();
            let path = location.to_string();
            return match answer {
                Some(HeadAnswer::NotFound) => Err(object_store::Error::NotFound {
                    path,
                    source: "scripted 404".into(),
                }),
                Some(HeadAnswer::Denied) => Err(object_store::Error::PermissionDenied {
                    path,
                    source: "scripted 403".into(),
                }),
                None => self.inner.get_opts(location, options).await,
            };
        }
        let n = self.gets.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.hold.load(Ordering::SeqCst) {
            let mut released = self.release.subscribe();
            self.holding.store(true, Ordering::SeqCst);
            let _ = released.wait_for(|released| *released).await;
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
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
