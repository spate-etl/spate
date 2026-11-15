**`rate_limited_warn!` takes the event's instant** (`spate-core`)

`rate_limited_warn!(limit, at: now, ...)` decides whether to log on the `Instant` it is given, the way `RateLimit::check_at` does. A connector that reads time through an injected clock passes that clock's reading, so a test clock decides when the window rolls. In previous versions the macro always read the system clock. The form without `at:` is unchanged.
