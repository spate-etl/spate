# NATS

[`../README.md`](../README.md) has the convention these lanes follow. This file
covers what is specific to NATS.

| Lane | Line |
| --- | --- |
| `floor` | NATS 2.11, the oldest line the NATS store supports. **Primary**. |
| `below-floor` | NATS 2.10. A fixture for the test that the store refuses it. |
| `async` | NATS 2.12. A fixture for adopted async state persistence. |

## Why the floor

The store needs 2.11 for per-message TTLs and limit markers, and refuses an
older server at connect. The suites run on the oldest line it accepts, since
that is the one a deployment can be held on.

`below-floor` exists for one test, and selecting it with `SPATE_NATS_LANE` fails
every other NATS test at the version check. `cargo xtask ci-changes` reads no
extra lanes for NATS, so it gets no CI job of its own.

`nats_persistence` explicitly selects `async` to check persistence adoption.
This fixture does not run the full suite or guarantee support for its line, and
`SPATE_NATS_LANE` does not override its selection.

Moving the floor is a maintainer edit to both lanes, to `MIN_SERVER` in
`crates/spate-coordination/src/store/nats.rs`, and to the supported versions on
the [NATS store page](../../docs/user-guide/04-connectors/coordination/nats/README.mdx).
`cargo xtask tidy supported-versions` fails until the page names the new line.
`below-floor` is listed in `UNSUPPORTED`, so the old floor's line cannot pass for
a supported one.
