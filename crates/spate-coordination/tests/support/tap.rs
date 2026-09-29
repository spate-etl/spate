//! [`TapStore`]: a store whose watch deliveries a test can hide or add to,
//! and whose writes it can observe or fail.

use futures_util::StreamExt as _;
use spate_coordination::store::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchEvent, WatchMode,
    WatchStream,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

/// Which write a [`TapStore`] hook sees.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Create,
    Update,
    Delete,
}

/// One write, as a [`TapStore`] hook sees it before it reaches the store.
#[derive(Debug)]
pub struct Write<'a> {
    pub op: Op,
    pub ks: Keyspace,
    pub key: &'a str,
    /// The value written; `None` for a delete.
    pub value: Option<&'a [u8]>,
}

type Hide = Arc<dyn Fn(Keyspace, &str) -> bool + Send + Sync>;
type Hook = Arc<dyn Fn(&Write<'_>) -> Option<StoreError> + Send + Sync>;
type ListHook = Arc<dyn Fn(Keyspace, &str) -> Option<StoreError> + Send + Sync>;
type GetHook = Arc<dyn Fn(Keyspace, &str) -> Option<StoreError> + Send + Sync>;
type Injectors = Vec<(Keyspace, mpsc::UnboundedSender<WatchEvent>)>;

/// Wraps `S`. Watches drop every event whose key [`hide`](Self::hide)
/// matches, and deliver whatever [`inject`](Self::inject) sends; every write
/// passes through the [`on_write`](Self::on_write) hook, every listing
/// through [`on_list`](Self::on_list) and every read through
/// [`on_get`](Self::on_get), any of which fails the call by returning an
/// error. Clones share their taps.
#[derive(Clone)]
pub struct TapStore<S> {
    inner: S,
    hide: Arc<Mutex<Option<Hide>>>,
    hook: Arc<Mutex<Option<Hook>>>,
    list_hook: Arc<Mutex<Option<ListHook>>>,
    get_hook: Arc<Mutex<Option<GetHook>>>,
    injectors: Arc<Mutex<Injectors>>,
}

impl<S> TapStore<S> {
    pub fn new(inner: S) -> TapStore<S> {
        TapStore {
            inner,
            hide: Arc::default(),
            hook: Arc::default(),
            list_hook: Arc::default(),
            get_hook: Arc::default(),
            injectors: Arc::default(),
        }
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Hide from every watch, snapshot included, the events whose keyspace
    /// and key match.
    pub fn hide(&self, filter: impl Fn(Keyspace, &str) -> bool + Send + Sync + 'static) {
        *self.hide.lock().expect("tap") = Some(Arc::new(filter));
    }

    /// Run `hook` on every later write; an error it returns fails the write
    /// with nothing written.
    pub fn on_write(
        &self,
        hook: impl Fn(&Write<'_>) -> Option<StoreError> + Send + Sync + 'static,
    ) {
        *self.hook.lock().expect("tap") = Some(Arc::new(hook));
    }

    /// Run `hook` on every later listing; an error it returns fails it.
    pub fn on_list(
        &self,
        hook: impl Fn(Keyspace, &str) -> Option<StoreError> + Send + Sync + 'static,
    ) {
        *self.list_hook.lock().expect("tap") = Some(Arc::new(hook));
    }

    /// Run `hook` on every later read; an error it returns fails it.
    pub fn on_get(
        &self,
        hook: impl Fn(Keyspace, &str) -> Option<StoreError> + Send + Sync + 'static,
    ) {
        *self.get_hook.lock().expect("tap") = Some(Arc::new(hook));
    }

    /// Deliver `event` on every live watch of `ks`.
    pub fn inject(&self, ks: Keyspace, event: WatchEvent) {
        self.injectors
            .lock()
            .expect("tap")
            .retain(|(space, tx)| *space != ks || tx.send(event.clone()).is_ok());
    }

    fn check(&self, write: &Write<'_>) -> Result<(), StoreError> {
        let hook = self.hook.lock().expect("tap").clone();
        match hook.and_then(|hook| hook(write)) {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl<S: CoordinationStore + Clone> CoordinationStore for TapStore<S> {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl()
    }

    fn watch_mode(&self) -> WatchMode {
        self.inner.watch_mode()
    }

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        self.check(&Write {
            op: Op::Create,
            ks,
            key,
            value: Some(&value),
        })?;
        self.inner.create(ks, key, value).await
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        self.check(&Write {
            op: Op::Update,
            ks,
            key,
            value: Some(&value),
        })?;
        self.inner.update(ks, key, value, expected).await
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        let hook = self.get_hook.lock().expect("tap").clone();
        if let Some(error) = hook.and_then(|hook| hook(ks, key)) {
            return Err(error);
        }
        self.inner.get(ks, key).await
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        self.check(&Write {
            op: Op::Delete,
            ks,
            key,
            value: None,
        })?;
        self.inner.delete(ks, key, expected).await
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        let inner = self.inner.watch(ks, prefix).await?;
        let (tx, rx) = mpsc::unbounded_channel();
        self.injectors.lock().expect("tap").push((ks, tx));
        let injected = futures_util::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|event| (Ok(event), rx))
        });
        let hide = Arc::clone(&self.hide);
        let visible = inner.filter(move |event| {
            let key = match event {
                Ok(WatchEvent::Put(entry)) => Some(entry.key.as_str()),
                Ok(WatchEvent::Delete { key, .. }) => Some(key.as_str()),
                _ => None,
            };
            let filter = hide.lock().expect("tap").clone();
            let hidden = key.is_some_and(|key| filter.is_some_and(|hide| hide(ks, key)));
            std::future::ready(!hidden)
        });
        Ok(futures_util::stream::select(visible, injected).boxed())
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        let hook = self.list_hook.lock().expect("tap").clone();
        if let Some(error) = hook.and_then(|hook| hook(ks, prefix)) {
            return Err(error);
        }
        self.inner.list(ks, prefix).await
    }
}
