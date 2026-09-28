**A coordinator dropped while holding splits can hand them back through a store that opens connections** (`spate-coordination`)

Dropping a coordinator that still holds splits hands them back on a private
runtime, which now has IO enabled. In previous versions that runtime had timers
only, so a custom store whose client opens a connection per request panicked in
`Drop`. The built-in stores were not affected.
