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

The publishable crates move together at one version. They inherit
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
| `[workspace.package] version` and the `=` pins | `scripts/release-version.sh --bump` |
| `Cargo.lock` | `cargo update --workspace`, inside the bump |
| The install snippets at `X.Y` | the same bump; `--check` holds the set closed |
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
every crate and must name the release commit; a scratch project resolves
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

# The site serves the new snippets once the dispatched deploy finishes.
curl -s https://spate.kainth.dev/docs/user-guide/getting-started/installation \
  | grep -o 'version = "X.Y"'
```

The dispatched deploy is fire-and-forget: `finish` reports it started, not
that it landed, which is why the check above exists. docs.rs builds
asynchronously and lags the publish; check it later rather than waiting on
it.

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
  would never run `CI gate` and could never merge. The App's slug is
  `spate-release`: its pull requests author as `spate-release[bot]`, the
  identity the container-suite deferral in `scripts/ci-changes.sh` and the
  release commits key on, so renaming the App silently un-defers those
  suites.
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
