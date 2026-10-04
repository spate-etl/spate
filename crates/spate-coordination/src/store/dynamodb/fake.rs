//! [`FakeTable`]: an in-memory table with DynamoDB's conditional writes,
//! byte-budgeted pages and eventually consistent reads, and knobs for the
//! faults the store handles.

use super::table::{
    BoxFuture, Cond, Created, Item, KeyAttr, Meta, Page, Query, Shape, Status, Table, Ttl, Write,
    Written,
};
use crate::store::StoreError;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::watch;

/// DynamoDB's item size limit.
const MAX_ITEM_BYTES: usize = 400 * 1024;

/// DynamoDB's Query page limit.
const PAGE_BYTES: usize = 1024 * 1024;

/// One kind of call, for counts and injected failures.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FakeOp {
    Write,
    Get,
    Query,
}

#[derive(Debug, Default)]
struct Slot {
    cur: Option<Item>,
    /// The item before the last write: what an eventually consistent read
    /// returns while `stale_reads` is set.
    prev: Option<Item>,
}

#[derive(Debug)]
struct State {
    items: BTreeMap<(String, String), Slot>,
    meta: HashMap<String, Meta>,
    shape: Option<Shape>,
    ttl: Ttl,
    /// Ops that fail, mapped to whether the failure is fatal.
    failures: HashMap<FakeOp, bool>,
    counts: HashMap<FakeOp, u64>,
    queries: Vec<Query>,
    /// Keys eventually consistent reads serve as absent.
    unseen: BTreeSet<(String, String)>,
}

#[derive(Debug)]
struct Fake {
    state: Mutex<State>,
    page_bytes: AtomicUsize,
    stale_reads: AtomicBool,
    land_then_fail: AtomicBool,
    /// The next Query to read waits for this to turn true before returning.
    hold: Mutex<Option<watch::Receiver<bool>>>,
    held: watch::Sender<usize>,
    /// Frozen wall time in epoch milliseconds; zero reads the system clock.
    wall_ms: AtomicU64,
    /// A write applied before a later write: how many writes pass first,
    /// and the write.
    interpose: Mutex<Option<(usize, String, String, Write)>>,
}

/// An in-memory table shared by every store handle built over it.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct FakeTable(Arc<Fake>);

/// Releases the Query a [`FakeTable::hold_next_query`] holds.
#[doc(hidden)]
#[derive(Debug)]
pub struct QueryGate {
    release: watch::Sender<bool>,
    held: watch::Receiver<usize>,
    before: usize,
}

impl QueryGate {
    /// Waits until the held Query has read the table.
    pub async fn reached(&mut self) {
        let before = self.before;
        let _ = self.held.wait_for(|n| *n > before).await;
    }

    pub fn release(self) {
        let _ = self.release.send(true);
    }
}

pub(crate) fn good_shape() -> Shape {
    let key = |name: &str, hash| KeyAttr {
        name: name.to_string(),
        hash,
        kind: "S".to_string(),
    };
    Shape {
        status: Status::Active,
        keys: vec![key("pk", true), key("sk", false)],
        local_indexes: 0,
        global_indexes: 0,
        replicas: 0,
    }
}

fn item_bytes(pk: &str, sk: &str, item: &Item) -> usize {
    pk.len() + sk.len() + item.b.as_ref().map_or(0, Vec::len) + 40
}

impl Default for FakeTable {
    fn default() -> Self {
        FakeTable::new()
    }
}

impl FakeTable {
    /// An empty, active table with TTL on `x`.
    #[must_use]
    pub fn new() -> FakeTable {
        FakeTable(Arc::new(Fake {
            state: Mutex::new(State {
                items: BTreeMap::new(),
                meta: HashMap::new(),
                shape: Some(good_shape()),
                ttl: Ttl::On("x".to_string()),
                failures: HashMap::new(),
                counts: HashMap::new(),
                queries: Vec::new(),
                unseen: BTreeSet::new(),
            }),
            page_bytes: AtomicUsize::new(PAGE_BYTES),
            stale_reads: AtomicBool::new(false),
            land_then_fail: AtomicBool::new(false),
            hold: Mutex::new(None),
            held: watch::channel(0).0,
            wall_ms: AtomicU64::new(0),
            interpose: Mutex::new(None),
        }))
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.0.state.lock().expect("fake table poisoned")
    }

    /// Wall time in epoch milliseconds, frozen when [`freeze_wall`](Self::freeze_wall) set it.
    pub fn now_ms(&self) -> u64 {
        match self.0.wall_ms.load(Ordering::SeqCst) {
            0 => crate::records::now_ms().max(1).unsigned_abs(),
            ms => ms,
        }
    }

    /// A test that compares revisions while a frozen `TestClock` passes a
    /// lease freezes the wall too, or a re-create can reuse a revision.
    pub fn freeze_wall(&self, ms: u64) {
        self.0.wall_ms.store(ms.max(1), Ordering::SeqCst);
    }

    /// Calls of `op` so far.
    pub fn count(&self, op: FakeOp) -> u64 {
        self.state().counts.get(&op).copied().unwrap_or(0)
    }

    /// Fails every call of `op` with a retryable error while `failing` holds.
    pub fn fail(&self, op: FakeOp, failing: bool) {
        let mut state = self.state();
        if failing {
            state.failures.insert(op, false);
        } else {
            state.failures.remove(&op);
        }
    }

    /// Fails every call of `op` with a fatal error.
    #[cfg(test)]
    pub(crate) fn fail_fatally(&self, op: FakeOp) {
        self.state().failures.insert(op, true);
    }

    pub fn set_page_bytes(&self, bytes: usize) {
        self.0.page_bytes.store(bytes, Ordering::SeqCst);
    }

    /// Serves eventually consistent reads from each item's previous state.
    pub fn set_stale_reads(&self, stale: bool) {
        self.0.stale_reads.store(stale, Ordering::SeqCst);
    }

    /// While `unseen` holds, eventually consistent reads omit `sk`, as a
    /// replica that has applied neither its create nor its later writes does.
    #[cfg(test)]
    pub(crate) fn set_unseen(&self, pk: &str, sk: &str, unseen: bool) {
        let key = (pk.to_string(), sk.to_string());
        let mut state = self.state();
        if unseen {
            state.unseen.insert(key);
        } else {
            state.unseen.remove(&key);
        }
    }

    /// Applies the next write, then reports its condition failed against
    /// the item it left, as a retried call whose first attempt landed sees.
    pub fn land_then_fail_next_write(&self) {
        self.0.land_then_fail.store(true, Ordering::SeqCst);
    }

    /// Holds the next Query after it reads the table, until released.
    pub fn hold_next_query(&self) -> QueryGate {
        let (release, rx) = watch::channel(false);
        *self.0.hold.lock().expect("fake table poisoned") = Some(rx);
        let held = self.0.held.subscribe();
        let before = *held.borrow();
        QueryGate {
            release,
            held,
            before,
        }
    }

    #[cfg(test)]
    /// The item stored under `pk` and `sk`.
    pub(crate) fn item(&self, pk: &str, sk: &str) -> Option<Item> {
        let state = self.state();
        state
            .items
            .get(&(pk.to_string(), sk.to_string()))
            .and_then(|s| s.cur.clone())
    }

    /// Removes the item under `pk` and `sk` as native TTL does, with no
    /// write the store sees.
    #[cfg(test)]
    pub(crate) fn collect(&self, pk: &str, sk: &str) {
        if let Some(slot) = self
            .state()
            .items
            .get_mut(&(pk.to_string(), sk.to_string()))
        {
            slot.prev = slot.cur.take();
        }
    }

    #[cfg(test)]
    /// Every Query issued so far.
    pub(crate) fn queries(&self) -> Vec<Query> {
        self.state().queries.clone()
    }

    /// Applies `write` to `pk` and `sk` once `after` more writes have passed,
    /// just before the next one.
    #[cfg(test)]
    pub(crate) fn interpose(&self, after: usize, pk: &str, sk: &str, write: Write) {
        *self.0.interpose.lock().expect("fake table poisoned") =
            Some((after, pk.to_string(), sk.to_string(), write));
    }

    #[cfg(test)]
    pub(crate) fn set_shape(&self, shape: Option<Shape>) {
        self.state().shape = shape;
    }

    #[cfg(test)]
    pub(crate) fn set_ttl(&self, ttl: Ttl) {
        self.state().ttl = ttl;
    }

    fn call(&self, op: FakeOp) -> Result<MutexGuard<'_, State>, StoreError> {
        let mut state = self.state();
        *state.counts.entry(op).or_default() += 1;
        match state.failures.get(&op) {
            Some(true) => return Err(StoreError::Fatal(format!("injected {op:?} failure"))),
            Some(false) => return Err(StoreError::Retryable(format!("injected {op:?} failure"))),
            None => {}
        }
        Ok(state)
    }

    fn write_now(&self, pk: &str, sk: &str, write: Write) -> Result<Written, StoreError> {
        let mut state = self.call(FakeOp::Write)?;
        self.apply(&mut state, pk, sk, write)
    }

    fn create_above_now(
        &self,
        pk: &str,
        floor_pk: &str,
        sk: &str,
        put: Write,
    ) -> Result<Created, StoreError> {
        let Write::Put { v: base, .. } = put else {
            panic!("create_above takes a Write::Put");
        };
        let mut state = self.call(FakeOp::Write)?;
        let held = |state: &State, pk: &str| {
            state
                .items
                .get(&(pk.to_string(), sk.to_string()))
                .and_then(|s| s.cur.clone())
        };
        if let Some(old) = held(&state, pk) {
            return Ok(Created::Exists { old: Some(old) });
        }
        if let Some(floor) = held(&state, floor_pk).filter(|f| f.v >= base) {
            return Ok(Created::Floor(floor.v));
        }
        Ok(match self.apply(&mut state, pk, sk, put)? {
            Written::Ok { .. } => Created::Ok,
            Written::Failed { old } => Created::Exists { old },
        })
    }

    /// Applies the interposed write once its turn has come.
    fn interposed(&self) -> Result<(), StoreError> {
        let due = {
            let mut interpose = self.0.interpose.lock().expect("fake table poisoned");
            match interpose.as_mut() {
                Some((0, ..)) => interpose.take(),
                Some((after, ..)) => {
                    *after -= 1;
                    None
                }
                None => None,
            }
        };
        if let Some((_, pk, sk, write)) = due {
            self.write_now(&pk, &sk, write)?;
        }
        Ok(())
    }

    fn apply(
        &self,
        state: &mut State,
        pk: &str,
        sk: &str,
        write: Write,
    ) -> Result<Written, StoreError> {
        let slot = state
            .items
            .entry((pk.to_string(), sk.to_string()))
            .or_default();
        let old = slot.cur.clone();
        let live = old.as_ref().filter(|o| !o.tomb);
        let next = match write {
            Write::CreateDurable { b, w, now_ms } => {
                if live.is_some() {
                    return Ok(Written::Failed { old });
                }
                Some(Item {
                    v: old.as_ref().map_or(now_ms, |o| o.v) + 1,
                    b: Some(b),
                    w: Some(w),
                    tomb: false,
                    x: None,
                })
            }
            Write::Put { v, b, w, x, cond } => {
                let holds = match cond {
                    Cond::Absent => old.is_none(),
                    Cond::VersionIs(e) => old.as_ref().is_some_and(|o| o.v == e),
                    Cond::LiveVersionIs(e) => live.is_some_and(|o| o.v == e),
                };
                if !holds {
                    return Ok(Written::Failed { old });
                }
                Some(Item {
                    v,
                    b: Some(b),
                    w: Some(w),
                    tomb: false,
                    x,
                })
            }
            Write::Tombstone { expected, w, x } => {
                let Some(o) = live.filter(|o| expected.is_none_or(|e| o.v == e)) else {
                    return Ok(Written::Failed { old });
                };
                Some(Item {
                    v: o.v + 1,
                    b: None,
                    w: Some(w),
                    tomb: true,
                    x: Some(x),
                })
            }
            Write::Raise { v, x } => {
                if old.as_ref().is_some_and(|o| o.v >= v) {
                    return Ok(Written::Failed { old });
                }
                Some(Item {
                    v,
                    x: Some(x),
                    ..old.clone().unwrap_or(Item {
                        v,
                        b: None,
                        w: None,
                        tomb: false,
                        x: None,
                    })
                })
            }
            Write::Remove { expected } => {
                if let Some(e) = expected
                    && old.as_ref().is_none_or(|o| o.v != e)
                {
                    return Ok(Written::Failed { old });
                }
                None
            }
        };
        if let Some(item) = &next
            && item_bytes(pk, sk, item) > MAX_ITEM_BYTES
        {
            return Err(StoreError::Fatal(
                "ValidationException: Item size has exceeded the maximum allowed size".into(),
            ));
        }
        let v = next.as_ref().map(|i| i.v);
        slot.prev = std::mem::replace(&mut slot.cur, next.clone());
        if self.0.land_then_fail.swap(false, Ordering::SeqCst) {
            return Ok(Written::Failed { old: next });
        }
        Ok(Written::Ok { v })
    }

    fn read_page(&self, query: &Query) -> Result<Page, StoreError> {
        let mut state = self.call(FakeOp::Query)?;
        state.queries.push(query.clone());
        if query.prefix.as_deref() == Some("") {
            return Err(StoreError::Fatal(
                "ValidationException: The AttributeValue for a key attribute cannot contain an \
                 empty string value"
                    .into(),
            ));
        }
        let stale = !query.consistent && self.0.stale_reads.load(Ordering::SeqCst);
        let budget = self.0.page_bytes.load(Ordering::SeqCst);
        let from = match &query.start {
            Some(start) => Bound::Excluded((query.pk.clone(), start.clone())),
            None => Bound::Included((query.pk.clone(), String::new())),
        };
        let prefix = query.prefix.as_deref().unwrap_or("");
        let mut page = Page::default();
        let mut read = 0;
        let mut last = None;
        let matching = state
            .items
            .range((from, Bound::Unbounded))
            .take_while(|((pk, _), _)| *pk == query.pk)
            .filter(|((_, sk), _)| sk.starts_with(prefix))
            .filter(|(key, _)| query.consistent || !state.unseen.contains(*key))
            .filter_map(|((_, sk), slot)| {
                let item = if stale { &slot.prev } else { &slot.cur };
                item.as_ref().map(|i| (sk, i))
            });
        // "A single Query only returns a result set that fits within the 1 MB
        // size limit", counted before the filter:
        // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.Pagination.html
        for (sk, item) in matching {
            let size = item_bytes(&query.pk, sk, item);
            if last.is_some() && read + size > budget {
                page.next = last;
                break;
            }
            read += size;
            last = Some(sk.clone());
            if !(query.filter_tombs && item.tomb) {
                page.items.push((sk.clone(), item.clone()));
            }
        }
        Ok(page)
    }
}

impl Table for FakeTable {
    fn write<'a>(
        &'a self,
        pk: &'a str,
        sk: &'a str,
        write: Write,
    ) -> BoxFuture<'a, Result<Written, StoreError>> {
        Box::pin(async move {
            self.interposed()?;
            self.write_now(pk, sk, write)
        })
    }

    fn create_above<'a>(
        &'a self,
        pk: &'a str,
        floor_pk: &'a str,
        sk: &'a str,
        put: Write,
    ) -> BoxFuture<'a, Result<Created, StoreError>> {
        Box::pin(async move {
            self.interposed()?;
            self.create_above_now(pk, floor_pk, sk, put)
        })
    }

    fn get<'a>(
        &'a self,
        pk: &'a str,
        sk: &'a str,
    ) -> BoxFuture<'a, Result<Option<Item>, StoreError>> {
        Box::pin(async move {
            let state = self.call(FakeOp::Get)?;
            Ok(state
                .items
                .get(&(pk.to_string(), sk.to_string()))
                .and_then(|s| s.cur.clone()))
        })
    }

    fn query(&self, query: Query) -> BoxFuture<'_, Result<Page, StoreError>> {
        Box::pin(async move {
            let page = self.read_page(&query)?;
            let hold = self.0.hold.lock().expect("fake table poisoned").take();
            if let Some(mut release) = hold {
                self.0.held.send_modify(|n| *n += 1);
                let _ = release.wait_for(|r| *r).await;
            }
            Ok(page)
        })
    }

    fn put_meta<'a>(
        &'a self,
        pk: &'a str,
        meta: Meta,
    ) -> BoxFuture<'a, Result<Option<Meta>, StoreError>> {
        Box::pin(async move {
            let mut state = self.call(FakeOp::Write)?;
            Ok(match state.meta.get(pk) {
                Some(old) => Some(*old),
                None => {
                    state.meta.insert(pk.to_string(), meta);
                    None
                }
            })
        })
    }

    fn describe(&self) -> BoxFuture<'_, Result<Option<Shape>, StoreError>> {
        Box::pin(async move { Ok(self.state().shape.clone()) })
    }

    fn create_table(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            let mut state = self.state();
            if state.shape.is_none() {
                state.shape = Some(good_shape());
                state.ttl = Ttl::Off;
            }
            Ok(())
        })
    }

    fn describe_ttl(&self) -> BoxFuture<'_, Result<Ttl, StoreError>> {
        Box::pin(async move { Ok(self.state().ttl.clone()) })
    }

    fn enable_ttl(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            self.state().ttl = Ttl::On("x".to_string());
            Ok(())
        })
    }
}
