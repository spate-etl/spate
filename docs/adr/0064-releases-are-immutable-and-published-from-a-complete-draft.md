---
description: "Each GitHub release is assembled as a draft and published only when complete, as an immutable release whose assets, tag and attestation GitHub locks."
---

# ADR-0064 — GitHub releases are immutable, published only from a complete draft

- **Status:** accepted
- **Date:** 2026-10-10
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

Each release's GitHub release carries the changelog section, the per-crate
SBOMs, `SHA256SUMS` and the attestation bundle. Before this decision `finish`
created the release published, then uploaded the assets one by one, and any
of them could be replaced or deleted later by anyone with write access. The
tag could also be moved or deleted after the release named it. A consumer who
downloaded an asset could not tell whether it was the one the release run
attached.

GitHub's immutable releases lock a published release's assets and tag, refuse
to reuse the tag of a deleted one, and sign a release attestation that names
the tag, the digest of the tag's git object and each asset's digest. Assets can be added only while a
release is a draft. The question is whether releases adopt it, and how
`finish` assembles a release so nothing is missing when it locks.

## Considered options

- Immutable releases, with `finish` creating a draft, attaching every asset,
  checking the set is complete, then publishing and verifying the release
  attestation.
- Keep mutable releases, and protect the assets with the attestation bundle
  and `SHA256SUMS` alone.

## Decision outcome

Chosen option: "immutable releases, published from a complete draft",
because the release a consumer reads is then the release the run made, and
`gh release verify` and `gh release verify-asset` check that without trusting
anything in this repository.

`finish` refuses to call a release done until GitHub reports it published and
immutable and its release attestation verifies. A release already published
and immutable is never edited, so a resumed run only checks it.

### Consequences

- Good, because no asset or tag of a published release can change, and the
  release attestation gives a consumer a second, independent check of each
  asset's digest.
- Good, because an asset that failed to upload stops the run while the
  release is still a draft, where a re-run can complete it.
- Bad, because a release published with an asset missing cannot be repaired;
  only a new version can. `finish` checks the asset set before it publishes.
- Bad, because immutability is a repository setting that this repository's
  files cannot hold, so `finish` fails until it is turned on.

### Confirmation

`cargo xtask release finish` checks the release state and runs
`gh release verify` before the docs deploy, and
`cargo xtask release verify` checks it again on demand. The tests in
`xtask/src/release/sequence/tests.rs` cover a mutable repository, an
incomplete draft and a published immutable release.

## More information

- Landed in #1065.
- [ADR-0063](0063-releases-are-attested-by-a-reusable-build-workflow.md)
  produces the attestations the release carries.
- [`RELEASING.md`](repo:RELEASING.md) lists the setting under what the
  release rests on.
