**A coordinator dropped after its task stopped releases its splits through a store that opens connections** (`spate-coordination`)

When a coordinator is dropped after its background task or runtime is gone, it
hands its splits back on a private runtime that now has IO enabled. In previous
versions that runtime had timers only, so a custom store whose client opens a
connection per request panicked in `Drop`. The built-in stores were not
affected.
