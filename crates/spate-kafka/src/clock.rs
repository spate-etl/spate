//! The sync clock the source and the sink's producer context read, held so
//! the types that carry it keep their auto traits and `Debug`.

use spate_core::clock::{Clock, SystemClock};
use std::fmt;
use std::ops::Deref;
use std::panic::RefUnwindSafe;
use std::sync::Arc;

/// A shared sync clock.
///
/// `RefUnwindSafe`, so a public type holding it stays `UnwindSafe` and
/// `RefUnwindSafe`.
pub(crate) type SharedClock = Arc<dyn Clock + Send + Sync + RefUnwindSafe>;

/// A [`SharedClock`] that implements `Debug`, which the sync [`Clock`] does
/// not require.
#[derive(Clone)]
pub(crate) struct SyncClock(SharedClock);

impl SyncClock {
    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn new(clock: SharedClock) -> Self {
        SyncClock(clock)
    }

    /// The production clock.
    pub(crate) fn system() -> Self {
        SyncClock(Arc::new(SystemClock))
    }
}

impl Deref for SyncClock {
    type Target = dyn Clock + Send + Sync + RefUnwindSafe;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl fmt::Debug for SyncClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SyncClock")
    }
}
