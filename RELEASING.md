# Releasing

How a Spate release works and how to run one. This is a maintainer reference;
contributors need [`CONTRIBUTING.md`](CONTRIBUTING.md) and the changelog
conventions in [`changelog.d/README.md`](changelog.d/README.md), not this
page.

The process itself lives in [`scripts/release.sh`](scripts/release.sh), and
[`release.yml`](.github/workflows/release.yml) runs those entry points with
credentials where a step needs one. That split is deliberate: the same code
path runs locally as a dry run, so what you rehearse is what CI executes.

## What a release is

The ten publishable crates move together at one version. They inherit
`version` from `[workspace.package]`, and `[workspace.dependencies]` pins each
sibling with `=`, so the workspace version is the only number. One tag
`vX.Y.Z`, one GitHub release, and `git show vX.Y.Z` is the whole release: a
single commit carrying every generated artefact.

Pre-1.0, **a breaking change ships in a minor bump**. Cargo treats `0.x`
minors as incompatible, so `0.2 -> 0.3` is the breaking step and `0.2.0 ->
0.2.1` must not be. An MSRV move is a minor bump for the same reason: the
Cargo book calls raising `rust-version` a minor incompatibility. Only the
newest `0.x` minor is supported.

## The one decision

A release starts with the version, and that is the only judgement a human
supplies:

```sh
gh workflow run release.yml -f version=0.3.0
```

The workflow derives the version independently and fails when the two
disagree: any commit since the last tag whose subject carries the breaking
`!`, or a `BREAKING CHANGE` footer, or a `rust-version` move, means a minor
bump; anything else means a patch. `./scripts/release-version.sh --derive`
prints the same answer locally.

Everything after the input runs unattended. The release pull request
auto-merges when `CI gate` passes, and its merge is not a review gate:
approving a diff of generated files that nobody reads protects nothing. The
controls are the version input, the derivation check, and the gates every
pull request already passes. What is worth reading on that pull request is
the assembled `CHANGELOG.md` section, which is the release notes.

## What the automation does

**Assemble**, on the dispatch: guards first (the tree matches the last tag,
the tag is free, the derivation agrees), then every artefact is generated
from the version input, in one commit on `release/vX.Y.Z`:

| Artefact | Produced by |
|---|---|
| `[workspace.package] version` and the nine `=` pins | `scripts/release-version.sh --bump` |
| `Cargo.lock` | `cargo update --workspace`, inside the bump |
| The five install snippets at `X.Y` | the same bump; `--check` holds the set closed |
| `CHANGELOG.md`, fragments consumed | `scripts/changelog.sh --build` |
| `THIRD-PARTY.md` | `scripts/attribution.sh`, as a drift backstop |

The pull request it opens is titled `chore: release vX.Y.Z`, labeled
`release`, and set to auto-merge. Re-dispatching the same version refreshes
it, which is the path for a fragment that landed after the first dispatch; a
dispatch at a different version supersedes and closes it.

**Publish**, on the squash merge: the commit subject is the trigger, since
the tag does not exist yet. In order: the subject and `Cargo.toml` must name
the same version; the set still to publish is computed from the sparse index,
so a re-run excludes what already landed; any crate already at the version
must have been published from this commit, read back from `trustpub_data`;
the metadata the dry run cannot check (`description`, `license`) is checked
explicitly; every pending crate is packaged and verify-built with no
credential in the job; only then is the 30-minute Trusted Publishing token
minted and the upload run with `--no-verify`, so none of the token's fixed
budget is spent compiling. After the upload, `trustpub_data` is read back for
all ten crates and must name the release commit; a scratch project resolves
`spate` at the exact version from the registry; the commit is tagged; the
GitHub release opens with the changelog section; and the docs deploy is
dispatched, after the crates are live so the install snippets are true the
moment the site serves them.

## Rehearse it first

```sh
make release-dry-run VERSION=0.3.0
```

This runs the same `assemble` and the credential-free half of the publish in
a throwaway git worktree: the real release commit, built for diffing, and
every pending crate packaged and verify-built. It stops where the registry
token would be minted and prints what a real run would do next. It needs
`gh` authenticated, `jq`, and `cargo-about` at exactly 0.9.1; the preflight
names anything missing. The worktree is kept for inspection and the run
prints the command that removes it.

Read a green dry run as "this assembles and packages". It cannot prove the
registry's acceptance rules (a verified email address, the rate limits), the
OIDC exchange, the environment's branch policy, or the consumer smoke test,
which needs the version to actually exist. The first three are configuration
that a previous release exercised; the last runs inside the real publish.

The same packaging proof also runs continuously: `ci.yml` runs a
simulated-bump `cargo publish --dry-run` on pushes to `main` that reach a
manifest, and `scheduled.yml` repeats it nightly, so release day is not when
a packaging problem first appears.

## Judging a release

Judge by the registry and the tag, never by the workflow reporting success;
the two diverge exactly when it matters. The publish already enforces the
mechanical half: every crate's `trustpub_data` names the tagged commit, and a
scratch project resolves the release. To check by hand:

```sh
# Every crate at the new version.
for c in $(cargo metadata --no-deps --format-version 1 \
  | jq -r '.packages[] | select(.publish != []) | .name'); do
  printf '%-20s %s\n' "$c" \
    "$(curl -s "https://crates.io/api/v1/crates/$c" \
       -H 'User-Agent: spate-release (github.com/spate-etl/spate)' \
       | jq -r '.crate.max_version')"
done

# The tag and the release exist.
git ls-remote --tags origin "refs/tags/vX.Y.Z"
gh release view vX.Y.Z
```

The site serves the new install snippets once the dispatched deploy
finishes. docs.rs builds asynchronously and lags the publish; check it later
rather than waiting on it.

## When it breaks

**A published version is permanent.** It can never be replaced. `cargo yank`
hides a version from new resolution, does not free the number, and does not
allow a re-upload; a yanked version also reads to a user as "this was
broken", so yank only what is actually broken for consumers.

**A publish that failed part way is resumed by re-running its failed jobs on
the same workflow run.** The re-run reuses the same push event and the same
commit, which is what pins the resume to the release tree, and the exclude
set skips whatever already landed. Never re-dispatch a version whose pull
request has merged. The sparse index can lag a publish by a few minutes, so
wait before re-running, or the selection will try a crate the registry
already holds and stop on its own.

**A split tree is a stop, not a repair.** If a crate carries the target
version from a different commit, or from a manual token publish with no
`trustpub_data` at all, the guard fails the run before anything else
uploads. The `=` pins make a part-published version unusable anyway, so
abandon the number and release the next patch from one commit.

**If the changelog date slips**, because the assembly ran on one day and the
merge landed on another: re-dispatch the version before the merge, which
rebuilds the section with today's date, or fix the date with an ordinary
pull request afterwards. `--build` will not re-run for a version the
changelog already carries.

**If the OIDC exchange itself is broken**, the fallback is a manual publish:
enable token publishing for the crates on crates.io, run
`cargo publish --workspace --locked` from the release commit with a token,
and disable token publishing again immediately. The read-back check will
fail on the next automated run that sees those versions, since a token
publish records no `trustpub_data`; that is expected, and the next release
proceeds normally at the next version.

## Rate limits

From crates.io's own limiter:

| Action | Burst | Refill |
|---|---|---|
| Publishing a version of an existing crate | 30 | 1 per minute |
| Publishing a new crate | 5 | 1 per 10 minutes |

Ten versions of existing crates fit in one burst with no waiting. The API is
separate and allows one request per second, which is why the selection reads
the sparse index instead and the read-back sleeps between calls.

## Adding a crate

Trusted Publishing cannot create a crate: crates.io has nothing to attach a
publisher to until the name is claimed. Before the first release that
includes a new crate:

1. Publish it once by hand, with a token, at the current workspace version,
   so the automation's next target version never has a manual publish behind
   it.
2. Configure its trusted publisher: this repository, workflow `release.yml`,
   environment `crates-io`.
3. Disable token publishing for it.
4. Give it a version through `[workspace.dependencies]` only if something
   depends on it, and never give `spate-core` a versioned dev-dependency on
   `spate-test`: dev-dependency edges with versions are part of the publish
   order, and that one closes a cycle no order can satisfy (cargo issue
   4242). The publish dry-run gate catches this.
5. Run `./scripts/ci-changes.sh --self-test`, which pins the container map to
   the crate graph.
6. If it carries an install snippet anywhere, add the file to
   `SNIPPET_FILES` in `scripts/release-version.sh`; `--check` fails until the
   snippet is in the rewritten set.

The pull-request semver gate starts diffing a new crate once it exists on
both sides of a merge base; the nightly comparison against the registry
skips it, and says so, until the name is claimed.

## What the release rests on

Configuration that lives outside this repository, verified when it changes
rather than on each release:

- **The GitHub App**: repository variable `RELEASE_APP_ID` and secret
  `RELEASE_APP_PRIVATE_KEY`. The App is owned by the organisation, so it
  survives an account change, and the workflow narrows each minted token to
  the permissions the step needs. It exists because events raised by
  `GITHUB_TOKEN` trigger no workflows: a release pull request opened with one
  would never run `CI gate` and could never merge.
- **The `crates-io` environment's deployment branch policy, restricted to
  `main`.** Trusted Publishing matches the repository, the workflow filename
  and the environment name, and discards the git ref from the OIDC claim, so
  this policy is the only thing keeping another ref from publishing. Removing
  it removes the protection silently.
- **The trusted publisher binding**: this repository, filename `release.yml`,
  environment `crates-io`. Renaming any of the three breaks the exchange
  until the publisher configuration is updated to match.
- **The `main` ruleset and merge settings**: pull requests only, no bypass
  actors, squash as the only merge method with the pull request title as the
  subject, and auto-merge enabled. The publish trigger reads the squash
  subject, so the merge-method setting is load-bearing.
- **No required reviewer on the `crates-io` environment.** Adding one pauses
  a publish part way with nothing in the workflow explaining it.

Unless a step failed and this page says otherwise, the workflow run and the
registry are the record of a release; there is nothing to write down and no
step to do by hand.
