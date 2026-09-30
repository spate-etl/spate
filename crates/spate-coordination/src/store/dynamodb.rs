//! A [`CoordinationStore`] over one DynamoDB table.
//!
//! Each job owns three partitions of the table: `{job}#d` holds the durable
//! keyspace, `{job}#e` the ephemeral one, and `{job}#m` the settings the
//! job was started with. The record key is the sort key.
//!
//! - Durable revisions come from the table and strictly increase across
//!   delete and re-create: a delete leaves a tombstone one revision up,
//!   which native TTL collects after seven days.
//! - Ephemeral keys carry no expiry the table enforces. Each handle judges
//!   a key expired once a consistent read that began one TTL after the
//!   handle first read its current revision still returns that revision.
//!   Every ephemeral write stamps `x` a day ahead, so native TTL collects
//!   items nothing renews; that stamp plays no part in expiry.
//! - Watches list their prefix every `poll_interval` and report the
//!   difference, so the store declares [`WatchMode::Polled`].
//! - Every write carries a random write id, and a failed condition returns
//!   the item it failed against, so a retried write whose first attempt
//!   landed resolves as won.
//!
//! Construction is synchronous and lazy: the first store operation, the
//! coordinator's startup probe, loads the AWS configuration and checks the
//! table, so a table still being created rides the startup retry budget.
//! Credentials come from the AWS provider chain. TLS uses rustls with the
//! `aws-lc-rs` provider, verifying against the system trust store, or the
//! bundled Mozilla roots when that store yields none. A rejected credential,
//! certificate or request, a missing table or region, and a table this store
//! cannot use are Fatal; throttling, server errors, timeouts and a failure to
//! load credentials are Retryable. No AWS SDK type appears in any public
//! signature.
//!
//! The [store page] documents the table, the IAM policy and the costs.
//!
//! [store page]: https://spate.kainth.dev/docs/user-guide/connectors/coordination/dynamodb

use super::{
    CasOutcome, CoordinationStore, Entry, Keyspace, Revision, StoreError, WatchMode, WatchStream,
};
use futures_util::future::BoxFuture;
use spate_core::clock::tokio::{Clock, SystemClock};
use spate_core::metrics::CoordinationMetrics;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};
use std::time::Duration;
use table::{Cond, Item, Query, Table, Write, WriteId, Written};
use tokio::time::Instant;

mod config;
mod errors;
#[cfg(any(test, feature = "testing"))]
mod fake;
mod observed;
mod poll;
mod sdk;
mod startup;
mod table;
#[cfg(test)]
mod test_http;
#[cfg(test)]
mod tests;

pub use config::DynamoDbConfig;
#[cfg(any(test, feature = "testing"))]
#[doc(hidden)]
pub use fake::{FakeOp, FakeTable, QueryGate};

/// The largest value a key holds, leaving room in DynamoDB's 400 KB item
/// for the key and the other attributes.
const MAX_VALUE_BYTES: usize = 384 * 1024;

/// How long after its last write native TTL may collect an ephemeral item.
const EPHEMERAL_COLLECT_S: u64 = 86_400;

/// How long native TTL keeps a durable tombstone.
const TOMBSTONE_KEEP_S: u64 = 7 * 86_400;

type Connect =
    Box<dyn Fn() -> BoxFuture<'static, Result<Arc<dyn Table>, StoreError>> + Send + Sync>;

struct Inner {
    config: DynamoDbConfig,
    lease_ttl: Duration,
    op_timeout: Duration,
    clock: Arc<dyn Clock>,
    /// Wall time in epoch milliseconds.
    now_ms: Box<dyn Fn() -> u64 + Send + Sync>,
    pks: [String; 3],
    connect: Connect,
    /// The table as connected, before the startup checks.
    connected: tokio::sync::OnceCell<Arc<dyn Table>>,
    /// The table once the startup checks pass.
    table: tokio::sync::OnceCell<Arc<dyn Table>>,
    observed: Mutex<observed::Observed>,
    pollers: Mutex<HashMap<(Keyspace, String), Weak<poll::Poller>>>,
    metrics: OnceLock<CoordinationMetrics>,
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamoDbStore")
            .field("table", &self.config.table)
            .field("job", &self.config.job)
            .field("lease_ttl", &self.lease_ttl)
            .field("poll_interval", &self.config.poll_interval)
            .field("connected", &self.connected.initialized())
            .field("checked", &self.table.initialized())
            .finish_non_exhaustive()
    }
}

impl Inner {
    fn pk(&self, ks: Keyspace) -> &str {
        match ks {
            Keyspace::Durable => &self.pks[0],
            Keyspace::Ephemeral => &self.pks[1],
        }
    }

    fn observed(&self) -> MutexGuard<'_, observed::Observed> {
        self.observed.lock().expect("observed cache poisoned")
    }

    /// The sequence and store-clock time a read takes before it starts.
    fn mark(&self) -> (u64, Instant) {
        (self.observed().seq(), self.clock.now())
    }

    fn now_s(&self) -> u64 {
        (self.now_ms)() / 1000
    }
}

/// See the [module docs](self).
#[derive(Clone, Debug)]
pub struct DynamoDbStore {
    inner: Arc<Inner>,
}

fn write_id() -> WriteId {
    *uuid::Uuid::new_v4().as_bytes()
}

impl DynamoDbStore {
    fn build(
        config: DynamoDbConfig,
        lease_ttl: Duration,
        op_timeout: Duration,
        clock: Arc<dyn Clock>,
        now_ms: Box<dyn Fn() -> u64 + Send + Sync>,
        connect: Connect,
    ) -> Result<DynamoDbStore, StoreError> {
        config.validate(lease_ttl)?;
        let job = &config.job;
        let pks = [format!("{job}#d"), format!("{job}#e"), format!("{job}#m")];
        Ok(DynamoDbStore {
            inner: Arc::new(Inner {
                config,
                lease_ttl,
                op_timeout,
                clock,
                now_ms,
                pks,
                connect,
                connected: tokio::sync::OnceCell::new(),
                table: tokio::sync::OnceCell::new(),
                observed: Mutex::new(observed::Observed::new(lease_ttl)),
                pollers: Mutex::default(),
                metrics: OnceLock::new(),
            }),
        })
    }

    /// Configure the store. No I/O: the first operation, the coordinator's
    /// startup probe, loads the AWS configuration and checks the table under
    /// the startup budget.
    ///
    /// `lease_ttl` and `op_timeout` must equal the coordinator's
    /// `lease_duration` and `op_timeout`. The SDK's timeouts and retries are
    /// built to finish inside `op_timeout`.
    ///
    /// # Errors
    ///
    /// Fatal on invalid configuration, naming the `dynamodb.*` key.
    pub fn new(
        config: DynamoDbConfig,
        lease_ttl: Duration,
        op_timeout: Duration,
    ) -> Result<DynamoDbStore, StoreError> {
        DynamoDbStore::with_clock(config, lease_ttl, op_timeout, Arc::new(SystemClock))
    }

    /// [`new`](Self::new) with expiry judged on `clock`.
    ///
    /// # Errors
    ///
    /// As [`new`](Self::new).
    #[doc(hidden)]
    pub fn with_clock(
        config: DynamoDbConfig,
        lease_ttl: Duration,
        op_timeout: Duration,
        clock: Arc<dyn Clock>,
    ) -> Result<DynamoDbStore, StoreError> {
        DynamoDbStore::over_sdk(config, lease_ttl, op_timeout, clock, None)
    }

    /// [`new`](Self::new) signing with a fixed key pair in place of the
    /// AWS provider chain.
    ///
    /// # Errors
    ///
    /// As [`new`](Self::new).
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub fn with_static_credentials(
        config: DynamoDbConfig,
        lease_ttl: Duration,
        op_timeout: Duration,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> Result<DynamoDbStore, StoreError> {
        let credentials = aws_sdk_dynamodb::config::Credentials::new(
            access_key_id,
            secret_access_key,
            None,
            None,
            "static",
        );
        DynamoDbStore::over_sdk(
            config,
            lease_ttl,
            op_timeout,
            Arc::new(SystemClock),
            Some(aws_sdk_dynamodb::config::SharedCredentialsProvider::new(
                credentials,
            )),
        )
    }

    fn over_sdk(
        config: DynamoDbConfig,
        lease_ttl: Duration,
        op_timeout: Duration,
        clock: Arc<dyn Clock>,
        credentials: Option<aws_sdk_dynamodb::config::SharedCredentialsProvider>,
    ) -> Result<DynamoDbStore, StoreError> {
        let settings = Arc::new(sdk::Settings {
            table: config.table.clone(),
            region: config.region.clone(),
            endpoint: config.endpoint.clone(),
            op_timeout,
            credentials,
            roots: rustls_native_certs::load_native_certs,
        });
        DynamoDbStore::build(
            config,
            lease_ttl,
            op_timeout,
            clock,
            Box::new(|| crate::records::now_ms().max(1).unsigned_abs()),
            Box::new(move || {
                let settings = Arc::clone(&settings);
                Box::pin(async move { sdk::connect(&settings).await })
            }),
        )
    }

    /// A store over `table`, whose wall time it reads.
    ///
    /// # Errors
    ///
    /// Fatal on invalid configuration.
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn over_fake_table(
        config: DynamoDbConfig,
        lease_ttl: Duration,
        op_timeout: Duration,
        clock: Arc<dyn Clock>,
        table: &FakeTable,
    ) -> Result<DynamoDbStore, StoreError> {
        let wall = table.clone();
        let fake = table.clone();
        DynamoDbStore::build(
            config,
            lease_ttl,
            op_timeout,
            clock,
            Box::new(move || wall.now_ms()),
            Box::new(move || {
                let table: Arc<dyn Table> = Arc::new(fake.clone());
                Box::pin(async move { Ok(table) })
            }),
        )
    }

    /// The table, after the startup checks pass once for this handle.
    ///
    /// The handle connects once; a check that fails or is cancelled runs
    /// again on the next call over the same connection.
    async fn table(&self) -> Result<&Arc<dyn Table>, StoreError> {
        let inner = &self.inner;
        inner
            .table
            .get_or_try_init(|| async {
                let table = inner
                    .connected
                    .get_or_try_init(|| (inner.connect)())
                    .await?;
                startup::check(&**table, &inner.config, inner.lease_ttl, &inner.pks[2]).await?;
                Ok(Arc::clone(table))
            })
            .await
    }

    fn check_value(key: &str, value: &[u8]) -> Result<(), StoreError> {
        if value.len() > MAX_VALUE_BYTES {
            return Err(StoreError::Fatal(format!(
                "the value for {key} is {} bytes; the DynamoDB store holds at most \
                 {MAX_VALUE_BYTES}",
                value.len()
            )));
        }
        Ok(())
    }

    fn own_write(&self, key: &str, v: u64) -> CasOutcome {
        let now = self.inner.clock.now();
        self.inner.observed().own_write(key, v, now);
        CasOutcome::Won(Revision(v))
    }

    /// Records a consistent read of `key` that began at `s0`.
    fn observe(&self, key: &str, item: Option<&Item>, s0: u64) {
        let now = self.inner.clock.now();
        self.inner
            .observed()
            .observe(key, item.map(|i| i.v), s0, now);
    }

    fn expired(&self, key: &str, v: u64, t0: Instant) -> bool {
        self.inner.observed().expired(key, v, t0)
    }

    fn ephemeral_put(&self, v: u64, b: Vec<u8>, w: WriteId, cond: Cond) -> Write {
        Write::Put {
            v,
            b,
            w,
            x: Some(self.inner.now_s() + EPHEMERAL_COLLECT_S),
            cond,
        }
    }

    async fn create_ephemeral(&self, key: &str, value: Vec<u8>) -> Result<CasOutcome, StoreError> {
        let table = self.table().await?;
        let pk = self.inner.pk(Keyspace::Ephemeral);
        let w = write_id();
        let (s0, t0) = self.inner.mark();
        let base = ((self.inner.now_ms)()).max(self.inner.observed().hw(key) + 1);
        let put = self.ephemeral_put(base, value.clone(), w, Cond::Absent);
        let old = match table.write(pk, key, put).await? {
            Written::Ok { .. } => return Ok(self.own_write(key, base)),
            Written::Failed { old: Some(old) } => old,
            Written::Failed { old: None } => return Ok(CasOutcome::Lost),
        };
        if old.w == Some(w) {
            return Ok(self.own_write(key, old.v));
        }
        self.observe(key, Some(&old), s0);
        if !self.expired(key, old.v, t0) {
            return Ok(CasOutcome::Lost);
        }
        // One takeover, only at the version observed expired.
        let (s0, _) = self.inner.mark();
        let hw = self.inner.observed().hw(key);
        let v = ((self.inner.now_ms)()).max(hw + 1).max(old.v + 1);
        let put = self.ephemeral_put(v, value, w, Cond::VersionIs(old.v));
        match table.write(pk, key, put).await? {
            Written::Ok { .. } => Ok(self.own_write(key, v)),
            Written::Failed { old: Some(o) } if o.w == Some(w) => Ok(self.own_write(key, o.v)),
            Written::Failed { old } => {
                self.observe(key, old.as_ref(), s0);
                Ok(CasOutcome::Lost)
            }
        }
    }

    async fn delete_ephemeral(
        &self,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        let table = self.table().await?;
        let pk = self.inner.pk(Keyspace::Ephemeral);
        let expired = self.inner.observed().expired_version(key);
        if let Some(v) = expired {
            // Gone as this handle judges it; at the expired version the
            // item is removed too, and the outcome of that changes nothing.
            if expected == Some(Revision(v))
                && let Ok(Written::Ok { .. }) = table
                    .write(pk, key, Write::Remove { expected: Some(v) })
                    .await
            {
                self.inner
                    .observed()
                    .own_delete(key, self.inner.clock.now());
            }
            return Ok(CasOutcome::Won(Revision(0)));
        }
        let (s0, t0) = self.inner.mark();
        let remove = Write::Remove {
            expected: expected.map(|r| r.0),
        };
        if let Written::Failed { old: Some(old) } = table.write(pk, key, remove).await? {
            self.observe(key, Some(&old), s0);
            return Ok(if self.expired(key, old.v, t0) {
                CasOutcome::Won(Revision(0))
            } else {
                CasOutcome::Lost
            });
        }
        self.inner
            .observed()
            .own_delete(key, self.inner.clock.now());
        Ok(CasOutcome::Won(Revision(0)))
    }

    async fn read_all(
        &self,
        ks: Keyspace,
        prefix: &str,
    ) -> Result<Vec<(String, Item)>, StoreError> {
        let table = self.table().await?;
        let mut items = Vec::new();
        let mut start = None;
        loop {
            let page = table
                .query(Query {
                    pk: self.inner.pk(ks).to_string(),
                    prefix: (!prefix.is_empty()).then(|| prefix.to_string()),
                    consistent: true,
                    start,
                    filter_tombs: ks == Keyspace::Durable,
                })
                .await?;
            items.extend(page.items);
            match page.next {
                Some(next) => start = Some(next),
                None => return Ok(items),
            }
        }
    }
}

fn entry(key: String, item: Item) -> Entry {
    Entry {
        key,
        value: item.b.unwrap_or_default(),
        revision: Revision(item.v),
    }
}

/// The item's revision when this call's write left it.
fn ours(old: Option<&Item>, w: WriteId) -> Option<u64> {
    old.filter(|o| o.w == Some(w)).map(|o| o.v)
}

impl CoordinationStore for DynamoDbStore {
    fn lease_ttl(&self) -> Duration {
        self.inner.lease_ttl
    }

    fn watch_mode(&self) -> WatchMode {
        WatchMode::Polled {
            interval: self.inner.config.poll_interval,
        }
    }

    fn op_timeout(&self) -> Option<Duration> {
        Some(self.inner.op_timeout)
    }

    fn attach_metrics(&self, metrics: &CoordinationMetrics) {
        let _ = self.inner.metrics.set(metrics.clone());
    }

    async fn create(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
    ) -> Result<CasOutcome, StoreError> {
        DynamoDbStore::check_value(key, &value)?;
        if ks == Keyspace::Ephemeral {
            return self.create_ephemeral(key, value).await;
        }
        let table = self.table().await?;
        let w = write_id();
        let create = Write::CreateDurable {
            b: value,
            w,
            now_ms: (self.inner.now_ms)(),
        };
        match table.write(self.inner.pk(ks), key, create).await? {
            Written::Ok { v: Some(v) } => Ok(CasOutcome::Won(Revision(v))),
            Written::Ok { v: None } => Err(StoreError::Fatal(format!(
                "the create of {key} returned no revision"
            ))),
            Written::Failed { old } => Ok(
                ours(old.as_ref(), w).map_or(CasOutcome::Lost, |v| CasOutcome::Won(Revision(v)))
            ),
        }
    }

    async fn update(
        &self,
        ks: Keyspace,
        key: &str,
        value: Vec<u8>,
        expected: Revision,
    ) -> Result<CasOutcome, StoreError> {
        DynamoDbStore::check_value(key, &value)?;
        let table = self.table().await?;
        let ephemeral = ks == Keyspace::Ephemeral;
        if ephemeral && self.inner.observed().expired_version(key) == Some(expected.0) {
            return Ok(CasOutcome::Lost);
        }
        let (s0, _) = self.inner.mark();
        let w = write_id();
        let v = expected.0 + 1;
        let write = if ephemeral {
            self.ephemeral_put(v, value, w, Cond::VersionIs(expected.0))
        } else {
            Write::Put {
                v,
                b: value,
                w,
                x: None,
                cond: Cond::LiveVersionIs(expected.0),
            }
        };
        let old = match table.write(self.inner.pk(ks), key, write).await? {
            Written::Ok { .. } if ephemeral => return Ok(self.own_write(key, v)),
            Written::Ok { .. } => return Ok(CasOutcome::Won(Revision(v))),
            Written::Failed { old } => old,
        };
        if let Some(v) = ours(old.as_ref(), w) {
            if ephemeral {
                return Ok(self.own_write(key, v));
            }
            return Ok(CasOutcome::Won(Revision(v)));
        }
        if ephemeral {
            self.observe(key, old.as_ref(), s0);
        }
        Ok(CasOutcome::Lost)
    }

    async fn get(&self, ks: Keyspace, key: &str) -> Result<Option<Entry>, StoreError> {
        let table = self.table().await?;
        let (s0, t0) = self.inner.mark();
        let item = table.get(self.inner.pk(ks), key).await?;
        if ks == Keyspace::Durable {
            return Ok(item.filter(|i| !i.tomb).map(|i| entry(key.to_string(), i)));
        }
        self.observe(key, item.as_ref(), s0);
        Ok(item
            .filter(|i| !self.expired(key, i.v, t0))
            .map(|i| entry(key.to_string(), i)))
    }

    async fn delete(
        &self,
        ks: Keyspace,
        key: &str,
        expected: Option<Revision>,
    ) -> Result<CasOutcome, StoreError> {
        if ks == Keyspace::Ephemeral {
            return self.delete_ephemeral(key, expected).await;
        }
        let table = self.table().await?;
        let w = write_id();
        let tombstone = Write::Tombstone {
            expected: expected.map(|r| r.0),
            w,
            x: self.inner.now_s() + TOMBSTONE_KEEP_S,
        };
        match table.write(self.inner.pk(ks), key, tombstone).await? {
            Written::Ok { v } => Ok(CasOutcome::Won(Revision(v.unwrap_or(0)))),
            Written::Failed { old } => Ok(match ours(old.as_ref(), w) {
                Some(v) => CasOutcome::Won(Revision(v)),
                None if old.as_ref().is_none_or(|o| o.tomb) => CasOutcome::Won(Revision(0)),
                None => CasOutcome::Lost,
            }),
        }
    }

    async fn watch(&self, ks: Keyspace, prefix: &str) -> Result<WatchStream, StoreError> {
        self.table().await?;
        poll::watch(&self.inner, ks, prefix).await
    }

    async fn list(&self, ks: Keyspace, prefix: &str) -> Result<Vec<Entry>, StoreError> {
        let (s0, t0) = self.inner.mark();
        let items = self.read_all(ks, prefix).await?;
        if ks == Keyspace::Durable {
            return Ok(items.into_iter().map(|(k, i)| entry(k, i)).collect());
        }
        let now = self.inner.clock.now();
        let mut observed = self.inner.observed();
        Ok(items
            .into_iter()
            .filter(|(key, item)| {
                observed.observe(key, Some(item.v), s0, now);
                !observed.expired(key, item.v, t0)
            })
            .map(|(k, i)| entry(k, i))
            .collect())
    }
}
