**A shared clock holder in `spate_core::clock`** (`spate-core`)

`spate_core::clock::SharedClock` is an `Arc` of a `Clock` that is also `Send`, `Sync` and `RefUnwindSafe`, so a public type that holds one keeps those traits. `spate_core::clock::SyncClock` wraps a `SharedClock` and implements `Debug`, which `Clock` does not require, so a type that holds one can derive `Debug`. `SyncClock::system()` holds `SystemClock`, and `SyncClock::new` holds any shared clock, such as a test clock.
