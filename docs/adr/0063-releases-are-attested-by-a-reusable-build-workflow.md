---
description: "A reusable workflow packages and attests each release: SLSA provenance, a CycloneDX SBOM per crate and SHA256SUMS, verified before any token exists."
---

# ADR-0063 — A reusable build workflow signs every release attestation

- **Status:** accepted
- **Date:** 2026-10-10
- **Supersedes:** —
- **Superseded by:** —

## Context and problem statement

Before this decision, the publish job in `release.yml` packaged the crates and
attested them with `actions/attest-build-provenance` in the same job that
then minted the registry token and uploaded. The attestation was signed by
`release.yml`'s own identity. Any step in that job could change what was
packaged before it was attested, so the provenance proved which workflow ran
and not that the build was isolated from it: SLSA Build Level 2.

The per-crate CycloneDX SBOMs were generated after the upload and attached to
the GitHub release unattested, so nothing tied an SBOM to the bytes it
describes. Nothing verified the attestation before the upload either; a
missing or malformed one surfaced only to a consumer.

The question is what a release attests, which identity signs it, and where
the attestations are checked.

## Considered options

- A reusable workflow, `release-build.yml`, packages and attests. It signs SLSA
  provenance over every `.crate` and a `SHA256SUMS`, and a CycloneDX SBOM
  attestation per `.crate`. The publish job verifies all of them against that
  workflow before minting the token.
- Keep attesting in the publish job, and add SBOM attestations there.
- The SLSA project's `slsa-github-generator` generic generator, which signs
  provenance from its own reusable workflow.
- Keep the provenance as it is, and sign the release assets with `cosign`
  under the publish job's identity.

## Decision outcome

Chosen option: "a reusable workflow packages and attests", because the
signing identity is then `release-build.yml` rather than the caller, which
is the isolation SLSA Build Level 3 asks for, and every artifact a consumer
downloads carries an attestation checkable with `gh attestation verify
--signer-workflow`.

The SBOM goes into each attestation as the predicate, with predicate type
`https://cyclonedx.org/bom`, rather than through `actions/attest`'s
`sbom-path`. The SBOMs are generated with `SOURCE_DATE_EPOCH` so they
regenerate byte for byte, which leaves out `serialNumber`, and the action's
CycloneDX detection requires one. The resulting predicate type is the same.

The registry upload stays in `release.yml`, because crates.io's Trusted
Publishing matches the caller's workflow and environment, and those are
configured on crates.io. `cargo publish` packages again in that job, so the
publish verifies every attestation against `release-build.yml` and the
release commit, and requires its own packaging to produce the attested bytes,
before the registry token is minted.

The attesting job builds without a cache, so nothing another workflow wrote is
restored into the build the provenance describes.

### Consequences

- Good, because the provenance names `release-build.yml` as its builder, so
  a change to how the attested artifacts are built shows up as a change to
  that one file.
- Good, because each SBOM is bound to the digest of the `.crate` it describes,
  and `SHA256SUMS` is itself attested, so every release asset traces to the
  release commit.
- Good, because the publish fails before any credential exists when an
  artifact does not match its checksum or its attestations.
- Bad, because the release runs in two jobs plus one SBOM job per pending
  crate, and the crates pass between them as workflow artifacts.
- Bad, because `gh` before 2.102.0 matches `--signer-workflow` as a prefix
  (`cli/cli` `pkg/cmd/attestation/verify/policy.go`), so a consumer on an
  older `gh` gets a weaker check than the command reads. The publish refuses
  to verify with one.
- Bad, because the attesting job builds the workspace cold on every release.
- Neutral, because `slsa-github-generator` would give the same level with a
  third-party signer, and `actions/attest` is already pinned and allowed in
  this organisation.

### Confirmation

`cargo xtask release verify-artifacts`, run by `release.yml` before the token
is minted, and its tests in `xtask/src/release/sequence/tests.rs`. The first
real release proves the signer identity, which no local run can.

## More information

- Landed in the pull request adding `release-build.yml`.
- [ADR-0062](0062-the-release-sequence-is-rust-in-xtask.md) moved the release
  sequence into `xtask`, where the verification lives.
- [`RELEASING.md`](repo:RELEASING.md) gives the commands a consumer runs to
  verify a release.
