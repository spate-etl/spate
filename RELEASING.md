# Releasing

Maintainer reference. Contributors want
[CONTRIBUTING.md](CONTRIBUTING.md) instead.

## The normal case

1. Draft the changelog entry: `git cliff --unreleased`. Edit it into a
   `## [Unreleased]` section in [CHANGELOG.md](CHANGELOG.md) and push that to
   `main`. Commit subjects say what changed; a release note says what it means
   for somebody upgrading, so this is writing, not pasting.
2. release-plz keeps a **release pull request** open against `main`, bumping
   every crate's version and the exact internal pins. Review the bump — it is
   derived from conventional-commit types, and a `feat` that should have been a
   `fix` shows up here as a version that is one step too high.
3. Move the `## [Unreleased]` heading to the new version number and date in the
   same pull request.
4. Merge it. The `release` job publishes all nine crates in dependency order,
   tags `vX.Y.Z`, and opens the GitHub release.

Nothing releases without a human merging something. `release_always = false`
is what guarantees that.

## Versioning

All nine crates move together, and this needs no configuration: they inherit
`version` from `[workspace.package]`, release-plz takes the highest next
version across the workspace and writes that back, and it preserves the `=`
operator so `[workspace.dependencies]` follows.

Pre-1.0, **a breaking change is a minor bump**. Cargo treats `0.x` minors as
incompatible, so `0.1 → 0.2` is the breaking step and `0.1.0 → 0.1.1` must not
be. `cargo-semver-checks` runs in the release pull request and will say so.

**Only the newest `0.x` minor is supported.** No maintenance branches, no
backports by default. If someone genuinely needs one, cut `release/0.1.x` from
its tag at that point — on demand, never pre-announced. A support matrix nobody
can staff is worse than a short one honestly stated.

## The first publish of any crate was manual, and had to be

**Trusted Publishing cannot create a crate that does not exist.** crates.io has
nothing to attach a trusted publisher to until the name is claimed, so the
first version of each crate was published by hand with a short-lived,
`publish-new`-scoped API token, which was revoked immediately afterwards.

Everything since goes through OIDC. There is no `CARGO_REGISTRY_TOKEN` secret
in this repository, and crates.io is configured to **refuse token-authenticated
publishes** for these crates — so a leaked token could not publish even if one
existed.

The same applies to any crate added later: publish it by hand once, configure
its trusted publisher, disable token publishing, then let the automation take
over.

## Rate limits, and why the first release was slow

From crates.io's own limiter:

| Action | Burst | Refill |
|---|---|---|
| Publishing a **new** crate | 5 | 1 per 10 minutes |
| Publishing a **version** of an existing crate | 30 | 1 per minute |

Nine new crates therefore cost about 45 minutes of waiting, once. Every release
after that publishes nine *versions*, which fits inside a burst of 30 with no
waiting at all. This is not a recurring cost and it is not a reason to merge
the workspace into one crate.

## Why the release workflow uses a GitHub App

Events raised by `GITHUB_TOKEN` do not trigger workflows. A release pull
request opened with it would never run CI, and since `ci-gate` is a required
check that pull request would be permanently unmergeable — the automation would
produce exactly one artifact and it would be a dead end.

So `release.yml` mints a token from a minimal GitHub App (contents and pull
requests, read-write, on this repository only). It needs:

- repository variable `RELEASE_APP_ID`
- repository secret `RELEASE_APP_PRIVATE_KEY`

The app is owned by the organisation rather than by a person, so it survives
somebody's account changing.

## If it breaks mid-release

The failure that matters is a partial publish: three crates on the registry,
six not. **A published version can never be replaced** — `cargo yank` hides a
version from new resolution but does not free the number, and does not let you
re-upload it.

So: do not try to re-run the same version. Fix the cause, let release-plz cut
the next patch, and yank the partial set only if it is genuinely broken for
consumers — a half-published workspace usually just fails to resolve, which is
loud and harmless.

Publishing by hand, if the automation is the thing that is broken, in this
order:

```
spate-core → spate-test → spate-avro → spate-json → spate-coordination
           → spate-kafka → spate-clickhouse → spate-s3 → spate
```

`cargo publish --dry-run --locked -p <crate>` first, every time. Note that a
manual publish needs a token, and token publishing is disabled on these crates
— re-enabling it is a deliberate act, and it should be turned off again
afterwards.

## Checklist for a crate added to the workspace

- [ ] `publish` is not set to `false` unless it genuinely should not ship
- [ ] It inherits `version`, `repository`, `homepage`, `license` and `authors`
      from `[workspace.package]`
- [ ] It has a `description` — crates.io rejects the upload without one
- [ ] It has a `README.md`; cargo finds it without a `readme` key
- [ ] `./scripts/ci-changes.sh --self-test` passes — the container-suite map is
      hand-written and this is what checks it against the crate graph
- [ ] Published by hand once, then its trusted publisher configured and token
      publishing disabled
