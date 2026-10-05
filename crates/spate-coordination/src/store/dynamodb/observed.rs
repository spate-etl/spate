//! What one handle has observed of each ephemeral key, and the expiry rule
//! judged from it on the handle's own clock.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug)]
pub(super) struct Observed {
    ttl: Duration,
    seq: u64,
    keys: HashMap<String, Obs>,
    /// The start sequence of each poll read in flight, with its count.
    reading: BTreeMap<u64, usize>,
}

#[derive(Debug)]
struct Obs {
    /// The last version observed; `None` after an observed absence or an own delete.
    v: Option<u64>,
    /// When `v` was first observed.
    since: Instant,
    /// The highest revision read or written.
    hw: u64,
    /// The highest revision of a delete any watch of this handle reported.
    emitted: u64,
    /// Bumped by every own write, emitted delete, and observation that changed `v`.
    seq: u64,
    /// Set when `v` was judged expired and its delete emitted.
    expired: bool,
    /// The highest floor an own floor raise recorded.
    floor: u64,
    /// How many watches of this handle hold a delivered revision of the key.
    held: u32,
    touched: Instant,
}

impl Observed {
    pub(super) fn new(ttl: Duration) -> Observed {
        Observed {
            ttl,
            seq: 0,
            keys: HashMap::new(),
            reading: BTreeMap::new(),
        }
    }

    /// The sequence a read takes before it starts.
    pub(super) fn seq(&self) -> u64 {
        self.seq
    }

    /// Registers a poll read starting now and returns its start sequence.
    /// Keys the read may judge stay recorded until
    /// [`end_read`](Self::end_read).
    pub(super) fn begin_read(&mut self) -> u64 {
        *self.reading.entry(self.seq).or_default() += 1;
        self.seq
    }

    pub(super) fn end_read(&mut self, s0: u64) {
        if let Some(n) = self.reading.get_mut(&s0) {
            *n -= 1;
            if *n == 0 {
                self.reading.remove(&s0);
            }
        }
    }

    /// Whether an own write or delete, an emitted delete, or a read that
    /// changed the key's version landed after a read that began at `s0`.
    pub(super) fn newer_than(&self, key: &str, s0: u64) -> bool {
        self.keys.get(key).is_some_and(|o| o.seq > s0)
    }

    /// The highest revision this handle read, wrote or reported as a delete for the key.
    pub(super) fn above(&self, key: &str) -> u64 {
        self.keys.get(key).map_or(0, |o| o.hw.max(o.emitted))
    }

    pub(super) fn floor(&self, key: &str) -> u64 {
        self.keys.get(key).map_or(0, |o| o.floor)
    }

    fn entry(&mut self, key: &str, now: Instant) -> &mut Obs {
        self.keys.entry(key.to_string()).or_insert(Obs {
            v: None,
            since: now,
            hw: 0,
            emitted: 0,
            seq: 0,
            expired: false,
            floor: 0,
            held: 0,
            touched: now,
        })
    }

    /// An own write landed at `v`; `now` is when the call returned.
    pub(super) fn own_write(&mut self, key: &str, v: u64, now: Instant) {
        self.seq += 1;
        let seq = self.seq;
        let o = self.entry(key, now);
        *o = Obs {
            v: Some(v),
            since: now,
            hw: o.hw.max(v),
            emitted: o.emitted,
            seq,
            expired: false,
            floor: o.floor,
            held: o.held,
            touched: now,
        };
    }

    /// Raises the key's recorded floor to at least `floor`.
    pub(super) fn raise_floor(&mut self, key: &str, floor: u64, now: Instant) {
        let o = self.entry(key, now);
        o.floor = o.floor.max(floor);
    }

    /// An own delete removed the key and left its floor at least `floor`.
    pub(super) fn own_delete(&mut self, key: &str, floor: u64, now: Instant) {
        self.seq += 1;
        let seq = self.seq;
        let o = self.entry(key, now);
        o.floor = o.floor.max(floor);
        o.v = None;
        o.seq = seq;
        o.expired = false;
        o.touched = now;
    }

    /// A consistent read that began at sequence `s0` and returned at `t1`
    /// found the key at `v`, or absent. When a newer write or read of the
    /// key landed after the read began, only `v` is recorded as read.
    pub(super) fn observe(&mut self, key: &str, v: Option<u64>, s0: u64, t1: Instant) {
        if let Some(v) = v {
            self.saw(key, v, t1);
        }
        if self.newer_than(key, s0) {
            return;
        }
        let next = self.seq + 1;
        let o = self.entry(key, t1);
        o.touched = t1;
        if o.v != v {
            o.v = v;
            o.since = t1;
            o.expired = false;
            o.seq = next;
            self.seq = next;
        }
    }

    /// Whether `v` is expired as judged by a successful read of it that
    /// began at `t0`: one TTL after it was first observed, or already
    /// judged so.
    pub(super) fn expired(&self, key: &str, v: u64, t0: Instant) -> bool {
        self.keys
            .get(key)
            .is_some_and(|o| o.v == Some(v) && (o.expired || t0 >= o.since + self.ttl))
    }

    /// The version already judged expired, if the key holds one.
    pub(super) fn expired_version(&self, key: &str) -> Option<u64> {
        self.keys.get(key).filter(|o| o.expired).and_then(|o| o.v)
    }

    /// A read returned the key at `v`.
    pub(super) fn saw(&mut self, key: &str, v: u64, now: Instant) {
        let o = self.entry(key, now);
        o.hw = o.hw.max(v);
    }

    /// A watch delivered the key and holds it until [`release`](Self::release).
    pub(super) fn hold(&mut self, key: &str, now: Instant) {
        self.entry(key, now).held += 1;
    }

    pub(super) fn release(&mut self, key: &str) {
        if let Some(o) = self.keys.get_mut(key) {
            o.held = o.held.saturating_sub(1);
        }
    }

    /// The revision of a delete emitted now: one above `delivered` and every
    /// revision this handle read or wrote for the key.
    pub(super) fn emit_delete(
        &mut self,
        key: &str,
        delivered: u64,
        expiry: bool,
        now: Instant,
    ) -> u64 {
        self.seq += 1;
        let seq = self.seq;
        let o = self.entry(key, now);
        let d = o.hw.max(delivered) + 1;
        o.emitted = o.emitted.max(d);
        o.seq = seq;
        o.expired = expiry;
        o.touched = now;
        d
    }

    #[cfg(test)]
    pub(super) fn tracks(&self, key: &str) -> bool {
        self.keys.contains_key(key)
    }

    /// Drops keys that are gone or expired and untouched for two TTLs,
    /// unless a watch holds them or a poll read in flight may judge them.
    pub(super) fn evict(&mut self, now: Instant) {
        let idle = self.ttl * 2;
        let oldest = self.reading.keys().next().copied();
        self.keys.retain(|_, o| {
            o.held > 0
                || oldest.is_some_and(|s0| o.seq > s0)
                || !((o.v.is_none() || o.expired) && now >= o.touched + idle)
        });
    }
}
