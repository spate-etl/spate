# Releasing

Maintainer reference. Contributors want [CONTRIBUTING.md](CONTRIBUTING.md)
instead, or [DEVELOPING.md](DEVELOPING.md) for the build and benchmark mechanics.

## The normal case

Everything a release needs from a human lands on `main` **before** the release
pull request merges, and nothing is ever committed onto that branch. release-plz
**closes and replaces** a release pull request that gains a non-bot commit, so a
commit made there is lost the next time anything lands on `main` — and the
failure is silent, because what comes back is a fresh pull request that looks
right. Steps 1 and 2 are in that order for this reason.

1. **Decide the version, from release-plz rather than by hand.** Open the release
   pull request it keeps against `main`: its title is `chore: release vX.Y.Z` and
   its body lists every crate's bump with the `cargo-semver-checks` verdict.
   Review that bump — it is derived from conventional-commit types, and a `feat`
   that should have been a `fix` shows up here as a version one step too high.

   Use the number from that pull request in the next step. Nothing reconciles the
   two automatically, and a mismatch ships a changelog headed `## [X.Y.Z]` whose
   tag link points at a tag that never gets created.

2. **Assemble the changelog and regenerate the inventory, both on `main`:**

   ```sh
   ./scripts/changelog.sh --build X.Y.Z

   cargo install cargo-about --locked --features cli --version 0.9.1
   ./scripts/attribution.sh
   ```

   The first groups everything in [`changelog.d/`](changelog.d) under a new
   version heading, resolves each entry's pull request link, adds the
   contributors, and deletes the fragments it consumed. **Read what it wrote
   before committing it** — the assembly is mechanical, the release note is not,
   and this is the last point at which a badly worded entry is cheap to fix.

   The second is `THIRD-PARTY.md`, which is not gated on ordinary pull requests —
   it would fail every dependency bump, since Dependabot cannot regenerate it —
   so by release time it is stale by however many bumps have landed. This is the
   point at which it has to be exact, because it is what somebody's legal review
   reads against the version they are actually consuming. The nightly
   `attribution` job will have said so already if it drifted.

   Cross-check what the fragments cover against what actually landed:

   ```sh
   git log --no-merges --format='%s' vX.Y.Z-previous..main
   ```

   Anything user-visible there without an entry is a fragment somebody owed and
   the gate did not catch. This is a plain `git log` on purpose — the check that
   belongs in a tool is `make check-changelog`, which already runs per pull
   request, and the release procedure should not depend on one nothing installs.

   Both go to `main` through a pull request like anything else: the branch
   ruleset requires one and carries no bypass actors, so there is no direct push.

3. **Merge the release pull request.** It rebases onto the `main` you just
   updated, and the `release` job publishes every crate in dependency order,
   tags `vX.Y.Z`, and opens the GitHub release.

   Between step 2 landing and this merge, `CHANGELOG.md` on `main` carries a
   version heading whose tag link 404s. That window is the cost of writing the
   release note before the tag exists, and it closes on merge.

   If the merge slips past the day step 2 ran, correct the date on the version
   heading by hand before merging. `--build` stamps `date -u` at assembly, and
   it will not re-run for a version the changelog already has — deliberately,
   since a published version can never be replaced. The date is the one field in
   an assembled block that is a fact about *when* rather than *what*, so it is
   the one worth a second look.

Nothing releases without a human merging something. `release_always = false`
is what guarantees that.

The `release` job is also *skipped* on ordinary merges rather than allowed to
run and no-op, because it enters the `crates-io` environment and would
otherwise write a deployment record for every commit reaching `main`. It
recognises a release by the commit subject, which is why `pr_name` is pinned in
`release-plz.toml` and why the repository squashes with the pull request title
as the subject. If you ever need to force it, the workflow takes a
`workflow_dispatch`.

## Versioning

The crates move together, and this needs no configuration: they inherit
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
after that publishes one *version* per crate, which fits inside a burst of 30
with no waiting at all. This is not a recurring cost and it is not a reason to
merge the workspace into one crate.

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

The failure that matters is a partial publish: some crates on the registry, the
rest not. **A published version can never be replaced** — `cargo yank` hides a
version from new resolution but does not free the number, and does not let you
re-upload it.

So: do not try to re-run the same version. Fix the cause, let release-plz cut
the next patch, and yank the partial set only if it is genuinely broken for
consumers — a half-published workspace usually just fails to resolve, which is
loud and harmless.

Publishing by hand, if the automation is the thing that is broken, in this
order:

```
spate-core → spate-test → spate-avro → spate-clickhouse → spate-coordination
           → spate-datagen → spate-json → spate-kafka → spate-s3 → spate
```

Prefer `cargo publish --workspace --locked`, which derives that order from the
manifests rather than trusting the list above; the list is for the case where
you are publishing one crate at a time because something went wrong mid-run.
Regenerate it rather than trusting it if the graph has changed since:

```sh
cargo metadata --no-deps --format-version 1 \
  | jq -r '.packages[] | select(.publish != []) | .name'
```

### What `--dry-run` does not check

It packages and compiles, then stops before the upload request. So it never
reaches the endpoint that enforces the registry's *acceptance* rules, and a
green dry run says nothing about them. Both of these were only discovered by
the real run:

- **A verified email address is required.** An unverified one fails the first
  upload with `400 Bad Request` and publishes nothing.
- **Rate limits.** See the table above. A dry run of the whole workspace
  completes in minutes; the real thing takes about forty-five.

Neither is a reason to skip the dry run — it catches missing metadata,
oversized payloads and compilation failures, which are the expensive ones. Just
do not read "the dry run was green" as "the publish will succeed".

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
