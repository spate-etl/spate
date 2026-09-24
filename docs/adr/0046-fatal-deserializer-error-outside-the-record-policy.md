---
description: "A deserializer can return a fatal error that stops the pipeline whatever its Skip or Fail policy, for a failure that no later payload can get past."
---

# ADR-0046 — A deserializer can stop the pipeline outside its record policy

- **Status:** accepted
- **Date:** 2026-09-24
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

A deserializer's errors reach the chain as record-level failures, which the
stage's Skip or Fail policy settles ([ADR-0010](0010-skip-or-fail-record-error-policies.md)).
Some failures are not about the payload. A schema registry that rejects the
credentials or the certificate fails every payload that needs a fetch. Under
the default Skip policy, reporting that as a record error drops and acks the
whole stream. Reporting it as `NotReady` holds the batch forever, with only a
log line to show for it.

## Considered options

- A `DeserError::Fatal` variant that the chain turns into a pipeline failure
  under either policy
- Check the dependency once at build time and fail construction
- Keep retrying, and document the stall

## Decision outcome

Chosen option: "A `DeserError::Fatal` variant", because it is the only one
that also stops a pipeline whose dependency starts rejecting it after startup.
A build-time check has to block inside a builder that may run on an async
runtime, and it would still let a credential revoked mid-run stall the
pipeline.

The chain fails the batch and the pipeline. The payload counts as a
deserializer error and never as a skip-policy drop.

### Consequences

- Good, because a component misconfiguration stops the pipeline with a named
  cause instead of stalling it or dropping the stream.
- Good, because `DeserError` is `#[non_exhaustive]`, so the variant is a minor
  API change.
- Bad, because a deserializer author now chooses between a record error and a
  pipeline failure, and choosing `Fatal` for a bad payload takes down a
  pipeline that Skip would have kept running.
- Bad, because a deserializer learns of the failure only when a payload needs
  the dependency, so an idle pipeline keeps running.

### Confirmation

`a_fatal_deser_error_stops_the_chain_under_skip` and
`a_fatal_on_replay_fails_the_batch_and_clears_the_stash` in
`crates/spate-core/src/ops/tests.rs`. INV-7 holds, because this is not a record
policy and it drops nothing.

## More information

- Landed in the pull request closing #631.
- [Error handling](../user-guide/02-concepts/04-error-handling.mdx) — the
  taxonomy this class sits in.
