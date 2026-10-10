---
description: "Each release tag is signed keylessly with gitsign as release.yml on main, verified before it is pushed, and recorded in Sigstore's transparency log."
---

# ADR-0065 — Release tags carry a keyless signature from the release workflow

- **Status:** accepted
- **Date:** 2026-10-10
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

The release tag `vX.Y.Z` is an annotated tag that `finish` creates as
`spate-release[bot]` and pushes with the release App's token. Nothing about
the tag object shows who made it: anyone able to push a tag could create one
that reads the same. The commit it names is a squash merge GitHub signs, but
the tag is what a consumer checks out by name.

Signing it needs a key. A long-lived key stored as a repository secret is one
more credential to rotate and to lose. GitHub cannot sign a tag object on the
App's behalf.

## Considered options

- Sign the tag keylessly with gitsign, which obtains a short-lived certificate
  for the workflow's OIDC identity from Sigstore's Fulcio and records the
  signature in Rekor.
- Sign with a GPG or SSH key held as a repository secret.
- Leave the tag unsigned, and rely on the tag ruleset and immutable releases.

## Decision outcome

Chosen option: "sign keylessly with gitsign", because the signature then
names `release.yml` running from `main` in this repository, a certificate only
a job of that workflow can obtain, and there is no key to keep.

`finish --sign-tag` signs the tag, verifies the signature against that
identity with `gitsign verify-tag`, and only then pushes it. The gitsign
binary is pinned by the digest of its release asset.

### Consequences

- Good, because a consumer verifies the tag with one command naming the
  workflow, and the transparency log records every signature made.
- Good, because no signing key exists to leak or rotate.
- Bad, because GitHub shows a gitsign signature as unverified. Its UI trusts
  GPG, SSH and S/MIME keys tied to accounts, so verification needs `gitsign`.
- Bad, because signing depends on Fulcio and Rekor being reachable, and an
  outage stops `finish` before the tag is pushed. The signature is made with
  gitsign's offline Rekor mode, which embeds the log entry in it, so verifying
  it needs no Rekor lookup.
- Bad, because any step of the publish job can obtain the same certificate,
  so the signature is as trustworthy as that job's steps and actions.
- Neutral, because the tag ruleset and immutable releases already stop a
  published tag from moving; the signature answers who made it.

### Confirmation

`cargo xtask release finish --sign-tag` verifies the signature before the
push, and `cargo xtask release verify` fetches the tag and verifies it again.
A real release proves the identity, which no local run can.

## More information

- Landed in #1065.
- [ADR-0064](0064-releases-are-immutable-and-published-from-a-complete-draft.md)
  locks the tag once the release is published.
- [`RELEASING.md`](repo:RELEASING.md) gives the verification command.
