# Releasing

How a Spate release works and how to run one. This is a maintainer reference;
contributors need [`CONTRIBUTING.md`](CONTRIBUTING.md) and the changelog
conventions in [`changelog.d/README.md`](changelog.d/README.md), not this
page.

The process itself lives in [`scripts/release.sh`](scripts/release.sh), and
[`release.yml`](.github/workflows/release.yml) runs those entry points with
credentials where a step needs one. The same code path runs locally as a dry
run, so what you rehearse is what CI executes.

## What a release is

The publishable crates move together at one version. They inherit
`version` from `[workspace.package]`, and `[workspace.dependencies]` pins each
sibling with `=`, so the workspace version is the only number. One tag
`vX.Y.Z`, one GitHub release, and `git show vX.Y.Z` is the whole release: a
single commit carrying every generated artifact.

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
auto-merges when `CI gate` passes, and nobody approves the diff. The controls
are the version input, the derivation check, and the gates every pull request
already passes. What is worth reading on that pull request is the assembled
`CHANGELOG.md` section, which is the release notes.

## What the automation does

**Assemble**, on the dispatch: guards first (the tree matches the last tag,
the tag is free, the derivation agrees), then every artifact is generated
from the version input, in one commit on `release/vX.Y.Z`:

| Artifact | Produced by |
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

**Publish**, on the squash merge. The commit subject is the trigger, since
the tag does not exist yet.

The run guards before it packages. The subject and `Cargo.toml` must name the
same version. The set still to publish is computed from the sparse index, so a
re-run excludes what already landed. Any crate already at the version must
have been published from this commit, read back from `trustpub_data`. The
metadata the dry run cannot check is checked explicitly.

Then it packages. Every pending crate is packaged and verify-built with no
credential in the job, and the packaged `.crate` files are attested with
`actions/attest-build-provenance`. Only then is the Trusted Publishing token
minted, and the upload runs with `--no-verify`, so none of the token's fixed
30-minute budget is spent compiling.

After the upload it checks what landed. `trustpub_data` is read back for every
crate and must name the release commit. Each packaged crate's sha256 must
equal the index's `cksum`, so the attestation provably covers the bytes the
registry serves. A scratch project resolves `spate` at the exact version from
the registry. The commit is then tagged, the GitHub release opens with the
changelog section, the per-crate SBOMs and the provenance bundle as assets,
and the docs deploy is dispatched. The deploy is dispatched after the crates
are live, so the install snippets are true the moment the site serves them.

## Rehearse it first

```sh
make release-dry-run VERSION=0.3.0
```

This runs the same `assemble` and the credential-free half of the publish in
a throwaway git worktree: the real release commit, built for diffing, and
every pending crate packaged and verify-built, and the SBOMs generated. It
stops where the registry token would be minted and prints what a real run
would do next. It needs `gh` authenticated, and `jq`, `curl`, `cargo-about`
and `cargo-cyclonedx` on the path at the versions `scripts/release.sh` pins;
the preflight names anything missing, and the version it wants. The worktree
is kept for inspection and the run prints the command that removes it.

Read a green dry run as "this assembles and packages". It cannot prove the
registry's acceptance rules (a verified email address, the rate limits), the
OIDC exchange, the environment's branch policy, or the consumer smoke test,
which needs the version to actually exist. The first three are configuration
that a previous release exercised; the last runs inside the real publish.

The same packaging proof also runs continuously: `ci.yml` runs a
simulated-bump `cargo publish --dry-run` on pushes to `main` that reach a
manifest, and `scheduled.yml` repeats it nightly, so a packaging problem
surfaces before release day.

## Judging a release

Judge by the registry and the tag, never by the workflow reporting success.
The publish already enforces the mechanical half: every crate's
`trustpub_data` names the tagged commit, and a scratch project resolves the
release. To check by hand:

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

## Provenance and the SBOM

crates.io records and displays its own provenance for every Trusted
Publishing upload: the repository, workflow and run, in `trustpub_data`. The
release attaches its own artifacts on top of that. A SLSA provenance bundle
covers the `.crate` files the run packaged; a consumer verifies one against
the GitHub attestation store by fetching the served artifact first:

```sh
curl -fLO https://static.crates.io/crates/spate/spate-X.Y.Z.crate
gh attestation verify spate-X.Y.Z.crate --repo spate-etl/spate
```

That lookup needs the network; fully offline verification passes the release
asset to `--bundle` instead. The run also generates one CycloneDX SBOM per
crate (spec 1.5) from the release commit's `Cargo.lock`, so each one describes
exactly the tree that was published. All of it lands among the release assets,
and the provenance bundle is what OpenSSF Scorecard's Signed-Releases check
reads, by its `.intoto.jsonl` suffix.

The release is one commit, and the cksum check ties the attested bytes to
what the registry serves. The sha256 in the sparse index is the served
`.crate`'s checksum, and the publish fails when it differs from a file the run
packaged. On a resumed run the check covers only what that run packaged;
crates published by an earlier attempt were checked, and attested, by the
attempt that uploaded them, and the store keeps every attestation even when a
bundle asset is lost.

`actions/attest-build-provenance`, and anything it calls internally, has to be
on the organisation's Actions allowlist. A refused action does not fail the
job; the run never starts and reports `startup_failure` with nothing naming
the action.

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
