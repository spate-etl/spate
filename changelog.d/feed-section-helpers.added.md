**Shared helpers for a feed backend's `feed:` section** (`spate-core`)

A feed backend outside spate-core can read its section with the same rules as the in-process feed. `FeedSection::backend_body` decodes the backend's body and refuses a section that names another backend, with context `feed`. `FeedSection::attempt_cap_or_default` returns the section's `attempt_cap`, or `spate_core::feed::DEFAULT_ATTEMPT_CAP`, which is 10. `FeedError::gone` builds the Gone error for a receipt the feed no longer holds, and `spate_core::feed::memory::BACKEND` is the in-process feed's key, `memory`.

`MemoryFeed::from_section` refuses `location` and `settling_window` with `ConfigError::Validation`, using the same message as loading a file. In previous versions it returned `ConfigError::Component` with context `feed` and a different message.
