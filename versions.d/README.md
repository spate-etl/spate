# Tool upstream sources

`versions.mk` pins the version. This tree says where `scripts/tool-drift.sh`
checks that pin against: one directory per tool, holding a `SOURCE` file.

```
versions.d/<tool>/SOURCE
```

`SOURCE` holds one declaration, `#`-comments and blank lines aside:

- `crate:<name>` — the crate's name on crates.io, queried through the sparse
  index. Usually the tool's own name; `nextest`'s is not, since
  `taiki-e/install-action` installs it under `nextest` while it publishes as
  `cargo-nextest`.
- `github:<owner>/<repo>` — a GitHub repository's latest release, for a tool
  not on crates.io at all, such as `shellcheck`.

Every tool `versions.mk` pins needs an entry here; `make self-test` checks it.
