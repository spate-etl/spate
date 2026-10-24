---
description: "The SQS feed backend lives in a new spate-sqs crate and the Kafka feed in spate-kafka, so a source names no backend and builds none it does not use."
---

# ADR-0069 — Feed backends live in their own crates, and a source names none

- **Status:** accepted
- **Date:** 2026-10-15
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

[ADR-0066](0066-a-feed-trait-replaces-the-split-coordination-traits.md)
defines a feed trait in `spate-core`. The SQS backend needs an AWS client, and
the Kafka backend needs rdkafka; `spate-s3` depends on neither.
[ADR-0049](0049-coordination-section-in-the-pipeline-config.md) kept backend
configuration and features out of connectors, so a new store touched no
connector. [ADR-0065](0065-the-dynamodb-coordination-store-is-removed.md)
removed the AWS SDK from the dependency graph.

## Considered options

- The SQS backend in a new `spate-sqs` crate and the Kafka feed in
  `spate-kafka`, each behind a `spate` feature; sources name no backend
- Both backends inside `spate-s3`, behind features
- SQS through a thin HTTP client without the AWS SDK
- `main()` passes the `feed:` section to the backend crate's constructor and
  hands the feed it returns to `Pipeline::feed`
- A registry in `spate-core` or the `spate` facade that builds the backend
  the section names

## Decision outcome

Chosen options: "The SQS backend in a new `spate-sqs` crate and the Kafka feed
in `spate-kafka`", and "`main()` passes the `feed:` section to the backend
crate's constructor and hands the feed it returns to `Pipeline::feed`". The
first, because a second queue-fed source then reuses both backends unchanged,
and a build pulls in an SQS client or librdkafka only when it selects that
backend. Backends inside `spate-s3` would be repeated by every later queue-fed
source. A thin HTTP client would make Spate maintain request signing, retries
and instance credentials.

The in-process feed lives beside the feed trait in `spate-core`. A source
names no backend, and a new backend touches no source. A pipeline's `feed:`
section names the backend by its key; the runtime builds the in-process feed
from it, and `main()` builds any other backend from the section and passes it
to `Pipeline::feed`.

ADR-0049 rejected an option where `main` decodes the section. Under the second
option `main()` decodes none of the backend's keys. The backend crate's
constructor decodes and checks its own section, and `run()` refuses a passed-in
feed whose backend differs from the section's key. A registry in `spate-core`
would name backend crates in core, which INV-6 rules out. A registry in the
facade would need a hook in `spate-core` for the runtime to call, to save
`main()` one call.

### Consequences

- Good, because a new backend touches no source, the property ADR-0049 gave
  coordination stores.
- Good, because an object-storage pipeline on the in-process feed builds
  neither the AWS SDK nor librdkafka.
- Bad, because the AWS SDK returns for SQS users, after ADR-0065 removed it
  with the DynamoDB store.
- Bad, because the workspace publishes and versions one more crate.

### Confirmation

Nothing yet. `crates/spate-s3/Cargo.toml` names neither `spate-sqs` nor
`spate-kafka`, and no gate checks it.

## More information

- Landed in the pull request that added `spate-sqs`, under #1090.
- [The SQS feed](../user-guide/04-connectors/feeds/sqs/README.mdx) — the
  backend this record places in its own crate.
