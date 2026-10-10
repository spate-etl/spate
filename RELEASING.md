# Releasing

How a Spate release works and how to run one. This is a maintainer reference;
contributors need [`CONTRIBUTING.md`](CONTRIBUTING.md) and the changelog
conventions in [`changelog.d/README.md`](changelog.d/README.md), not this
page.

The process itself lives in [`xtask/src/release/`](xtask/src/release/), behind
`cargo xtask release`, and [`release.yml`](.github/workflows/release.yml) runs
those entry points with credentials where a step needs one. The same code path runs locally as a dry
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
disagree: a changelog fragment that opens with `**Breaking:**`, or a
`rust-version` raise since the last tag, means a minor bump; anything else
means a patch. Commit subjects play no part.
`cargo xtask release version derive` prints the same answer locally.

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
| `[workspace.package] version` and the `=` pins | `cargo xtask release version bump` |
| `Cargo.lock` | `cargo update --workspace`, inside the bump |
| The install snippets at `X.Y` | the same bump; `cargo xtask release version check` holds the set closed |
| `CHANGELOG.md`, fragments consumed, moved dependency requirements listed | `cargo xtask changelog build` |
| `THIRD-PARTY.md` | `cargo xtask attribution` |

The pull request it opens is titled `release: vX.Y.Z`, labeled
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

Then it packages, in the reusable
[`release-build.yml`](.github/workflows/release-build.yml). Every pending crate
is packaged and verify-built with no credential in the job, and staged with one
CycloneDX SBOM per crate and a `SHA256SUMS` over both. The job builds without a
cache. Separate jobs, which run no repository code, then attest what it staged.
One SLSA provenance attestation covers every staged `.crate` and `SHA256SUMS`,
and each `.crate` gets an SBOM attestation of its own. Both are signed by
`release-build.yml`'s identity, not the caller's.

The publish job then checks, still with no credential:

- every staged digest against `SHA256SUMS`;
- every attestation against `release-build.yml` running from `main` at the
  release commit;
- that its own `cargo package` produces the attested bytes, since the upload
  packages again in this job.

It reads the registry for itself, so a re-run of the failed jobs alone sees
what an earlier attempt already uploaded. Only then is the Trusted Publishing
token minted, and the upload runs with `--no-verify`, so none of the token's
fixed 30-minute budget is spent compiling.

After the upload it checks what landed. `trustpub_data` is read back for every
crate and must name the release commit. Each packaged crate's sha256 must
equal the index's `cksum`, so the attestation provably covers the bytes the
registry serves. A scratch project resolves `spate` at the exact version from
the registry. The commit is then tagged, the GitHub release opens with the
changelog section, the per-crate SBOMs, `SHA256SUMS` and the attestation bundle
as assets, and the docs deploy is dispatched. The deploy is dispatched after the crates
are live, so the install snippets are true the moment the site serves them.

## Rehearse it first

```sh
cargo xtask release dry-run --version 0.3.0
```

This runs the same `assemble` and the publish up to staging in a throwaway git
worktree. That covers the real release commit, every pending crate packaged
and verify-built, and the SBOMs and `SHA256SUMS` staged. `assemble` runs its pull
request step against the real open pull requests and prints each push, close,
open and auto-merge instead of making it. The run stops before attesting
and prints what a real run would do next. It needs `gh`
authenticated, and `curl`, `cargo-about` and `cargo-cyclonedx` on the path;
the preflight names anything missing. The generator versions in CI come from
the `taiki-e/install-action` pin, so the inventory a local run produces can
differ from CI's when the installed versions differ. A successful run removes
its worktree; `--keep` keeps it for inspection, and a failed run always keeps
it and prints the command that removes it.

Read a green dry run as "this assembles and packages". It cannot prove the
registry's acceptance rules (a verified email address, the rate limits), the
OIDC exchange, the environment's branch policy, or the consumer smoke test,
which needs the version to actually exist. The first three are configuration
that a previous release exercised; the last runs inside the real publish. It
does not attest or verify; only a real release signs with
`release-build.yml`'s identity.

The same packaging proof also runs continuously: `ci.yml` runs a
simulated-bump `cargo publish --dry-run` on pushes to `main` that reach a
manifest, and `scheduled.yml` repeats it nightly, so a packaging problem
surfaces before release day. `scheduled.yml` also generates `THIRD-PARTY.md`
nightly, so a generator failure surfaces the same way.

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
release attests on top of that, with every attestation signed by
`release-build.yml` through Sigstore and recorded in the public transparency
log:

| Subject | Attestation |
|---|---|
| Each `.crate` | SLSA provenance, and its CycloneDX SBOM |
| `SHA256SUMS` | SLSA provenance |

Because the signer is a reusable workflow, the caller cannot alter how the
attested artifacts or their provenance are produced, and the job that runs the
build holds no signing token. That is what SLSA Build Level 3 asks. The upload's own packaging is held to those bytes before the
token exists, and the registry's cksum to them after.
A consumer verifies a crate the registry serves against that workflow, and
pins the branch it ran from:

```sh
curl -fLO https://static.crates.io/crates/spate/spate-X.Y.Z.crate
gh attestation verify spate-X.Y.Z.crate --repo spate-etl/spate \
  --signer-workflow spate-etl/spate/.github/workflows/release-build.yml \
  --source-ref refs/heads/main
gh attestation verify spate-X.Y.Z.crate --repo spate-etl/spate \
  --signer-workflow spate-etl/spate/.github/workflows/release-build.yml \
  --source-ref refs/heads/main --predicate-type https://cyclonedx.org/bom
```

`--source-digest <release commit>` pins the commit as well.

Use `gh` 2.102.0 or later: earlier versions match `--signer-workflow` as a
prefix. Passing the release's `spate-vX.Y.Z.intoto.jsonl` asset to `--bundle`
skips the attestation lookup; the Sigstore trust root is still fetched unless
`--custom-trusted-root` is given.

The SBOMs (CycloneDX 1.5) are generated from the release commit's
`Cargo.lock` with `SOURCE_DATE_EPOCH` set to the commit time, so each one
describes exactly the tree that was published. Each `bom-ref` carries the
absolute checkout path, so a regenerated SBOM matches byte for byte only from
the same path.
They, `SHA256SUMS` and the joined attestation bundle land among the release
assets, and the bundle is what OpenSSF Scorecard's Signed-Releases check reads,
by its `.intoto.jsonl` suffix.

The release is one commit, and the cksum check ties the attested bytes to
what the registry serves. The sha256 in the sparse index is the served
`.crate`'s checksum, and the publish fails when it differs from a file the run
attested. A build stages only while no crate of the version is published, and
a rebuild after that stages and uploads nothing. Every publish attempt
therefore reads a set that covers every crate, whether it is a re-run of the
failed jobs or of all jobs. The check, `SHA256SUMS` and the bundle cover every
crate, and the store keeps every attestation even when a bundle asset is lost.

`actions/attest`, `actions/upload-artifact` and `actions/download-artifact`,
and anything they call internally, have to be on the organisation's Actions
allowlist. A refused action does not fail the job; the run never starts and
reports `startup_failure` with nothing naming the action.

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
5. Run `cargo xtask tidy self-test`, which pins the container map to
   the crate graph.
6. If it carries an install snippet anywhere, add the file to
   `SNIPPET_FILES` in `xtask/src/release/version.rs`; `cargo xtask release
   version check` fails until the snippet is in the rewritten set.

The semver gate compares each crate against its published release, so it
skips a new crate, and says so, until the name is claimed.

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
  identity the container-suite deferral in `xtask/` and the
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
  subject and a blank body (`squash_merge_commit_message=BLANK`), and
  auto-merge enabled. The publish trigger reads the squash subject, so the
  merge-method setting is load-bearing. No other title can start with
  `release: v`, because the title gate reserves it for `release: vX.Y.Z`.
- **The `code-scanning` ruleset**, which requires CodeQL results on `main` and
  names the `spate-release` App as its only bypass actor. The rule blocks a
  merge while an analysis is pending, and the bypass is what lets the release
  pull request merge without waiting for one. It sits in a ruleset of its own
  so it reaches the code scanning rule and nothing the `main` ruleset holds.
- **No required reviewer on the `crates-io` environment.** Adding one pauses
  a publish part way with nothing in the workflow explaining it.
- **The `cloudflare-workers` environment**: secrets `CLOUDFLARE_API_TOKEN`
  and `CLOUDFLARE_ACCOUNT_ID`, where the token needs Workers Scripts edit on
  the account and Workers Routes plus DNS edit on the zone. The release
  dispatches the docs tier, so a narrower token or a missing environment
  leaves the site advertising the previous version.

Unless a step failed and this page says otherwise, the workflow run and the
registry are the record of a release; there is nothing to write down and no
step to do by hand.
