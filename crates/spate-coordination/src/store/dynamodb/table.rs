//! The interface between the store logic and a table: item-level calls whose
//! outcomes arrive decoded, with errors already classified.

use crate::store::StoreError;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A random id every write call carries, so a retried write whose first
/// attempt landed is recognised in the item it left behind.
pub(crate) type WriteId = [u8; 16];

/// One item, as the attributes `v`, `b`, `w`, `t` and `x` hold it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Item {
    /// The key's revision.
    pub(crate) v: u64,
    /// The value; absent on a tombstone.
    pub(crate) b: Option<Vec<u8>>,
    /// The id of the write that left the item.
    pub(crate) w: Option<WriteId>,
    /// A durable delete marker.
    pub(crate) tomb: bool,
    /// When native TTL may collect the item, in epoch seconds.
    pub(crate) x: Option<u64>,
}

/// The condition a [`Write::Put`] lands under.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Cond {
    /// No item exists.
    Absent,
    /// The item is at this revision.
    VersionIs(u64),
    /// The item is at this revision and is not a tombstone.
    LiveVersionIs(u64),
}

/// One conditional write. A failed condition returns the item as it stood.
#[derive(Clone, Debug)]
pub(crate) enum Write {
    /// A durable create over an absent item or a tombstone: the revision is
    /// one above the tombstone's, or `now_ms + 1` when there is none.
    CreateDurable { b: Vec<u8>, w: WriteId, now_ms: u64 },
    /// Sets `v`, `b`, `w` and, when given, `x`.
    Put {
        v: u64,
        b: Vec<u8>,
        w: WriteId,
        x: Option<u64>,
        cond: Cond,
    },
    /// Turns a live item into a tombstone one revision up.
    Tombstone {
        expected: Option<u64>,
        w: WriteId,
        x: u64,
    },
    /// Removes the item, only at `expected` when given.
    Remove { expected: Option<u64> },
}

/// A write's outcome.
#[derive(Clone, Debug)]
pub(crate) enum Written {
    /// Landed; `v` is the revision the table assigned, when it assigned one.
    Ok { v: Option<u64> },
    /// The condition failed; `old` is the item it failed against.
    Failed { old: Option<Item> },
}

/// One page of a key-ordered read of a partition.
#[derive(Clone, Debug)]
pub(crate) struct Query {
    pub(crate) pk: String,
    /// A sort-key prefix; `None` reads the whole partition.
    pub(crate) prefix: Option<String>,
    pub(crate) consistent: bool,
    /// The last key of the previous page.
    pub(crate) start: Option<String>,
    pub(crate) filter_tombs: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Page {
    pub(crate) items: Vec<(String, Item)>,
    /// Where the next page starts; `None` on the last page.
    pub(crate) next: Option<String>,
}

/// The settings a job fixes on its first start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Meta {
    pub(crate) lease_ms: u64,
    pub(crate) layout: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the fake table reports these only under test")
)]
pub(crate) enum Status {
    Creating,
    Active,
    Updating,
    Other(String),
}

/// A key attribute: its name, whether it is the hash key, and its type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct KeyAttr {
    pub(crate) name: String,
    pub(crate) hash: bool,
    pub(crate) kind: String,
}

/// What the store checks of a table before using it.
#[derive(Clone, Debug)]
pub(crate) struct Shape {
    pub(crate) status: Status,
    pub(crate) keys: Vec<KeyAttr>,
    pub(crate) local_indexes: usize,
    pub(crate) global_indexes: usize,
    pub(crate) replicas: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "the fake table reports these only under test")
)]
pub(crate) enum Ttl {
    /// Enabled or being enabled on this attribute.
    On(String),
    Off,
    /// The status could not be read, for this reason.
    Unknown(String),
}

pub(crate) trait Table: Send + Sync + fmt::Debug {
    fn write<'a>(
        &'a self,
        pk: &'a str,
        sk: &'a str,
        write: Write,
    ) -> BoxFuture<'a, Result<Written, StoreError>>;

    /// A consistent point read.
    fn get<'a>(
        &'a self,
        pk: &'a str,
        sk: &'a str,
    ) -> BoxFuture<'a, Result<Option<Item>, StoreError>>;

    fn query(&self, query: Query) -> BoxFuture<'_, Result<Page, StoreError>>;

    /// Writes `meta` if the job has none, and otherwise returns the one it has.
    fn put_meta<'a>(
        &'a self,
        pk: &'a str,
        meta: Meta,
    ) -> BoxFuture<'a, Result<Option<Meta>, StoreError>>;

    /// `None` when the table does not exist.
    fn describe(&self) -> BoxFuture<'_, Result<Option<Shape>, StoreError>>;

    /// Creates the table; one that already exists counts as created.
    fn create_table(&self) -> BoxFuture<'_, Result<(), StoreError>>;

    fn describe_ttl(&self) -> BoxFuture<'_, Result<Ttl, StoreError>>;

    /// Enables native TTL on `x`.
    fn enable_ttl(&self) -> BoxFuture<'_, Result<(), StoreError>>;
}
