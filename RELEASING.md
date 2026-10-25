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
`CHANGELOG.md` section, which holds the full release notes.

## The release post

A minor release, `X.Y.0`, needs a post on the blog; a patch release does not.
Write it on a pull request and merge it before the dispatch, with
`slug: releases/X.Y` and `draft: true` in its front matter.
[`website/blog/README.md`](website/blog/README.md) has the convention, and
[`handbook/blog-posts.md`](handbook/blog-posts.md) the shape and the voice.

The gate and the publication are automatic. `assemble` refuses a minor release
unless exactly one post carries the slug, and the release commit removes the
`draft: true` line. The post goes live at `/blog/releases/X.Y` with the first
site deploy after the release pull request merges, normally the one the release
dispatches. The GitHub release body links it there, under the site named by
`[workspace.package] homepage`.

## What the automation does

**Assemble**, on the dispatch: guards first (the tree matches the last tag,
the tag is free, the derivation agrees, a minor release has its post), then
every artifact is generated from the version input, in one commit on
`release/vX.Y.Z`:

| Artifact | Produced by |
|---|---|
| `[workspace.package] version` and the `=` pins | `cargo xtask release version bump` |
| `Cargo.lock` | `cargo update --workspace`, inside the bump |
| The install snippets at `X.Y` | the same bump; `cargo xtask release version check` holds the set closed |
| `CHANGELOG.md`, fragments consumed, moved dependency requirements listed | `cargo xtask changelog build` |
| `THIRD-PARTY.md` | `cargo xtask attribution` |
| The release post, out of draft | `assemble`, for a minor release |

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
the registry. The commit is then tagged. The GitHub release opens as a draft
whose body is the compact form that `cargo xtask changelog notes <version>`
prints. It links the full section in `CHANGELOG.md` at the tag and lists
every entry's title on one line, the breaking ones first under their own
heading. The release takes the per-crate SBOMs, `SHA256SUMS` and the
attestation bundle as assets, and is published only once all of them are on
it. Published, it is immutable: GitHub locks its assets and its tag and signs
a release attestation, which the run verifies. The docs deploy is dispatched
last, after the crates are live, so the install snippets are true the moment
the site serves them.

## Rehearse it first

```sh
cargo xtask release dry-run --version X.Y.Z
```

`X.Y.Z` is the version the next release will take. The rehearsal assembles
from the local `HEAD`, so a minor release's post has to be committed there
first, as a draft. With `--stand-in-post`, when no post carries the release's
slug yet, the rehearsal commits a draft stand-in in its worktree instead. The
dry run runs the same `assemble` and the publish up to staging in a throwaway
git worktree. That covers the real release commit,
every pending crate packaged and verify-built, and the SBOMs and `SHA256SUMS`
staged. `assemble` runs its pull request step against the real open pull
requests and prints each push, close, open and auto-merge instead of making
it. With `GH_REPO` set, a pull request listing that `gh` refuses fails the
rehearsal. Without it, the listing reads as empty on a clone with no remote on
a GitHub host, such as one whose only remote is a local path. The run stops
before attesting and prints what a real run would do next. It needs `gh`
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

The same proof also runs continuously. The `publish-dry-run` job in
`release-checks.yml`, which `ci.yml` calls, runs
`cargo xtask release package`, the packaging step `prepare` runs, at a
simulated bump on pushes to `main` that reach a manifest. `scheduled.yml` runs
`cargo xtask release dry-run --stand-in-post` nightly at the derived version,
with `gh`, `cargo-about` and `cargo-cyclonedx` installed as a release gets
them, so a packaging problem or a change in those tools surfaces before
release day. `release-checks.yml` runs the same rehearsal on a pull request
that changes the release workflows, `.github/actions/setup-rust/`,
`xtask/src/release/`, `ci.yml`, `release-checks.yml` or `xtask/src/ci/`. While
a release is in flight, after its `release: vX.Y.Z` commit lands and before the
publish tags it, that run fails in `derive`; a re-run after the tag lands
passes. A re-run keeps the run's
merge commit, so a run made before the release commit landed needs a push, or
the pull request closed and reopened. The nightly skips the night after a
release when nothing has merged since the tag. When the derived version is a
minor with no post yet, the flag commits a stand-in draft post in the
rehearsal's worktree.
`scheduled.yml` also generates `THIRD-PARTY.md` nightly, so a generator
failure surfaces the same way.

## Judging a release

Judge by the registry and the tag, never by the workflow reporting success.
The publish already enforces the mechanical half: every crate's
`trustpub_data` names the tagged commit, and a scratch project resolves the
release. One command re-checks the rest from what anyone can read:

```sh
cargo xtask release verify X.Y.Z
```

It checks each crate's origin and its provenance and SBOM attestations
against the commit the tag names, and that the GitHub release is immutable,
attested and carries every asset. GitHub's attestation of an immutable release
binds the tag to its git object and to every asset's digest. It needs `gh`
2.102.0 or later, authenticated, and a clean checkout of the tag, whose
members are the crates it checks. It fails for 0.2.0 and earlier, which predate
attestations. `scheduled.yml` runs it nightly against the newest `v*` tag from
a checkout of that tag, so a `gh`, crates.io or API change that breaks a call
`finish` also makes surfaces there first. To check by hand:

```sh
# Every crate at the new version.
for c in $(cargo metadata --no-deps --format-version 1 \
  | jq -r '.packages[] | select(.publish != []) | .name'); do
  printf '%-20s %s\n' "$c" \
    "$(curl -s "https://crates.io/api/v1/crates/$c" \
       -H 'User-Agent: spate-release (github.com/spate-etl/spate)' \
       | jq -r '.crate.max_version')"
done

# The release is immutable, and a downloaded asset is the one it carries.
gh release verify vX.Y.Z
gh release download vX.Y.Z --pattern SHA256SUMS
gh release verify-asset vX.Y.Z SHA256SUMS

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
skips a new crate, and says so, until the name is claimed. It also skips a
proc-macro crate, which the tool reads no API from. Review catches a break in
such a crate. A change under which code using the macro stops compiling, or an
item it generates for the same input changes or goes away, needs a
**Breaking:** fragment. The tests that use the macro show such a change, in its
own crate and in any crate that re-exports it.

## What the release rests on

Configuration that lives outside this repository, verified when it changes
rather than on each release:

- **Immutable releases**, turned on in the repository's settings. `finish`
  refuses a release GitHub does not report as immutable. The setting applies
  only to releases published after it is turned on. A release published
  mutable becomes immutable only if republished, and whether that signs a
  release attestation is unverified, so the recovery replaces it: turn the
  setting on, delete the release with `gh release delete vX.Y.Z` without
  `--cleanup-tag`, and re-run the failed jobs. Check first that `gh release view vX.Y.Z --json isImmutable` reads
  false, because deleting an immutable release retires its tag for good.
- **A tag ruleset on `v*`** restricting creation, update and deletion, with
  the `spate-release` App as the only bypass actor. Between the tag push and
  the release's publication, nothing else locks the tag.
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
