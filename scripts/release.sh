#!/usr/bin/env bash
#
# The release sequence. RELEASING.md is the account of the process; this file
# is the process, and release.yml runs these entry points with credentials
# where a step needs one. `dry-run` executes the same code path locally.
#
# Usage:
#   ./scripts/release.sh assemble --version X.Y.Z [--dry-run]
#   ./scripts/release.sh prepare [--dry-run]
#   ./scripts/release.sh upload
#   ./scripts/release.sh finish
#   ./scripts/release.sh publish --dry-run
#   ./scripts/release.sh dry-run --version X.Y.Z
#   ./scripts/release.sh --self-test
#
# `assemble` builds the single release commit and opens the pull request whose
# squash merge triggers the publish. `prepare`, `upload` and `finish` are the
# publish, split where release.yml has to interpose an action: the OIDC token
# mint sits between `prepare` and `upload`, and the App token mint between
# `upload` and `finish`. `publish --dry-run` runs the credential-free half and
# stops where the token would be minted. `dry-run` is the whole release in a
# throwaway worktree: the real commit, the real packaging, nothing pushed or
# uploaded.
#
# Environment, per entry point (all set by release.yml):
#   assemble  GH_TOKEN (App token: pushes the branch, opens the pull request),
#             GITHUB_REPOSITORY
#   prepare   EXPECTED_SHA  github.sha; the split-tree guard compares against
#             it, and GITHUB_OUTPUT receives version=, excludes=, pending=
#   upload    CARGO_REGISTRY_TOKEN (from crates-io-auth-action), EXCLUDES,
#             PENDING
#   finish    VERSION, EXPECTED_SHA, GH_TOKEN (App token: tag and release),
#             DISPATCH_TOKEN (github.token: the docs deploy;
#             workflow_dispatch is the documented exception to the rule that
#             GITHUB_TOKEN events trigger nothing), GITHUB_REPOSITORY
#
# Targets `bash` 3.2, the version stock macOS ships as /bin/bash: no
# associative arrays, no `mapfile`, and every array expansion guarded.
set -euo pipefail

cd "$(dirname "$0")/.."

UA='spate-release (github.com/spate-etl/spate)'
API='https://crates.io/api/v1/crates'
INDEX='https://index.crates.io'

fail() {
    echo "release.sh: $1" >&2
    exit 1
}

# Fold a phase in the Actions log; a plain heading elsewhere.
group() {
    if [ -n "${GITHUB_ACTIONS:-}" ]; then
        echo "::group::$1"
    else
        echo "==> $1"
    fi
}
endgroup() {
    [ -n "${GITHUB_ACTIONS:-}" ] && echo "::endgroup::"
    return 0
}

# The publishable packages, from the workspace's own metadata, so the
# selection, the guards and the read-back cover exactly the set
# `cargo publish --workspace` uploads, wherever a member lives.
publishable() {
    cargo metadata --no-deps --format-version 1 |
        jq -r '.packages[] | select(.publish != []) | .name'
}

workspace_version() {
    local n v
    n=$(grep -c '^version = "' Cargo.toml) || true
    [ "$n" = "1" ] || fail "Cargo.toml carries $n 'version = \"...\"' lines, expected exactly 1"
    v=$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml)
    printf '%s\n' "$v"
}

# A crate's path in the sparse index. Matches scripts/semver-checks.sh.
index_path() {
    local crate=$1
    case "${#crate}" in
    1) printf '1/%s\n' "$crate" ;;
    2) printf '2/%s\n' "$crate" ;;
    3) printf '3/%s/%s\n' "${crate:0:1}" "$crate" ;;
    *) printf '%s/%s/%s\n' "${crate:0:2}" "${crate:2:2}" "$crate" ;;
    esac
}

# The version a release commit's subject names, or nothing. The squash merge
# appends ` (#N)`; a plain commit carries none.
version_from_subject() {
    local subject=$1
    if [[ "$subject" =~ ^chore:\ release\ v([0-9]+\.[0-9]+\.[0-9]+)(\ \(#[0-9]+\))?$ ]]; then
        printf '%s\n' "${BASH_REMATCH[1]}"
        return 0
    fi
    return 1
}

# ---------------------------------------------------------------------------
# Self-test. Runs inline on every invocation as well as under --self-test.
# ---------------------------------------------------------------------------
self_test() {
    local failures=0 line want got

    while IFS='|' read -r line want; do
        case "$line" in '' | '#'*) continue ;; esac
        [ "$want" = '-' ] && want=''
        got=$(version_from_subject "$line" || true)
        if [ "$got" != "$want" ]; then
            echo "release.sh: version_from_subject: '$line' -> '$got', expected '$want'" >&2
            failures=$((failures + 1))
        fi
    done <<'TABLE'
chore: release v0.3.0|0.3.0
chore: release v0.3.0 (#309)|0.3.0
chore: release v10.20.30 (#1)|10.20.30
# --- near misses stay misses: the publish must not fire on these ---
chore: release v0.3|-
chore: release v0.3.0 and a trailer|-
chore(workspace): release v0.3.0|-
fix: mention chore: release v0.3.0 in a doc|-
TABLE

    while IFS='|' read -r line want; do
        case "$line" in '' | '#'*) continue ;; esac
        got=$(index_path "$line")
        if [ "$got" != "$want" ]; then
            echo "release.sh: index_path '$line' -> '$got', expected '$want'" >&2
            failures=$((failures + 1))
        fi
    done <<'TABLE'
spate|sp/at/spate
spate-core|sp/at/spate-core
abc|3/a/abc
ab|2/ab
a|1/a
TABLE

    [ "$failures" -eq 0 ] || fail "$failures self-test failure(s). This script is wrong, not your change"
}

# ---------------------------------------------------------------------------
# Preflight: name what is missing before any step runs.
# ---------------------------------------------------------------------------
preflight() {
    local missing="" about
    command -v gh >/dev/null 2>&1 || missing="$missing gh"
    command -v jq >/dev/null 2>&1 || missing="$missing jq"
    command -v curl >/dev/null 2>&1 || missing="$missing curl"
    [ -z "$missing" ] || fail "missing tool(s):$missing"

    # The changelog assembly resolves pull-request numbers through the API;
    # unauthenticated, every derived reference degrades before --build's own
    # guard can say why.
    gh auth status >/dev/null 2>&1 || [ -n "${GH_TOKEN:-}" ] ||
        fail "gh is not authenticated and GH_TOKEN is unset"

    # Exactly the pinned version: a different cargo-about reorders or regroups
    # the generated inventory, and the release commit would carry that churn.
    about=$(cargo about --version 2>/dev/null || true)
    [ "$about" = "cargo-about 0.9.1" ] ||
        fail "cargo-about 0.9.1 is required, found '${about:-none}'. Install it with:
  cargo install cargo-about --locked --features cli --version 0.9.1"

    [ -z "$(git status --porcelain)" ] ||
        fail "the working tree is not clean; a release is assembled from committed state only"
}

# ---------------------------------------------------------------------------
# assemble: the single release commit, and the pull request that carries it.
# ---------------------------------------------------------------------------
assemble() {
    local version=$1 dry=$2 current last expected body pr out stale stale_pr

    group "Guards"
    preflight
    [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "'$version' is not X.Y.Z"

    current=$(workspace_version)
    [ "$version" != "$current" ] || fail "the workspace is already at $current. If its release
  pull request has merged, the publish runs from that push; resume a failed one
  by re-running its failed jobs, which reuses the same commit."

    last=$(git tag --list 'v[0-9]*' --sort=-v:refname | head -n 1)
    [ "v$current" = "$last" ] || fail "Cargo.toml is at $current but the last tag is $last: a
  release is half-finished. Finish it before assembling the next one."

    # `git ls-remote` exits zero with empty output when the tag is absent, so
    # a network failure aborts here rather than reading as absence.
    [ -z "$(git ls-remote --tags origin "refs/tags/v$version")" ] ||
        fail "v$version is already tagged on origin"

    # The dispatched version has to fall out of the history since the last
    # tag as well.
    expected=$(./scripts/release-version.sh --derive)
    [ "$expected" = "$version" ] || fail "the input says $version but the history since $last
  derives $expected. One of the two is wrong; nothing proceeds until they agree."
    endgroup

    group "Generate every artefact"
    ./scripts/release-version.sh --bump "$version"
    ./scripts/changelog.sh --build "$version"
    # The inventory holds no first-party rows, so this is a no-op unless a
    # dependency changed underneath the release.
    ./scripts/attribution.sh
    endgroup

    group "The release commit"
    git -c user.name='spate-release[bot]' \
        -c user.email='spate-release[bot]@users.noreply.github.com' \
        commit --all --quiet --message "chore: release v$version" --message \
"Every artefact is generated from the version input: the manifest rewrite,
Cargo.lock, CHANGELOG.md assembled from changelog.d/, THIRD-PARTY.md and the
install snippets. The squash merge of this pull request is what triggers the
publish."
    git show --stat --format='%h %s' HEAD
    endgroup

    if [ "$dry" = "yes" ]; then
        echo "release.sh: dry run; the commit above was not pushed and no pull request was opened."
        return 0
    fi

    group "The release pull request"
    [ -n "${GH_TOKEN:-}" ] || fail "assemble needs GH_TOKEN (the App token) to push and open the pull request"
    [ -n "${GITHUB_REPOSITORY:-}" ] || fail "assemble needs GITHUB_REPOSITORY"

    # Two live auto-merge release pull requests is how an unintended version
    # merges; any same-repository release branch at another version is
    # superseded and closed. The head filter is the exact branch shape, so a
    # human branch such as release/v2-planning stays untouched and a fork
    # branch cannot be swept; the listing is an assignment, so a failed `gh`
    # aborts rather than sweeping nothing. $version is guard-validated X.Y.Z.
    stale=$(gh pr list --state open --limit 100 \
        --json number,headRefName,isCrossRepository \
        --jq '.[] | select(.isCrossRepository | not)
              | select(.headRefName | test("^release/v[0-9]+\\.[0-9]+\\.[0-9]+$"))
              | select(.headRefName != "release/v'"$version"'") | .number')
    for stale_pr in $stale; do
        gh pr close "$stale_pr" --delete-branch \
            --comment "Superseded by the v$version dispatch."
    done

    git push --force "https://x-access-token:${GH_TOKEN}@github.com/${GITHUB_REPOSITORY}.git" \
        "HEAD:refs/heads/release/v$version"

    # Re-dispatching the same version refreshes the branch above and reuses
    # the open pull request: that is the path for a fragment that landed after
    # the first dispatch.
    pr=$(gh pr list --state open --head "release/v$version" --json number --jq '.[0].number // empty')
    if [ -z "$pr" ]; then
        body="Assembled by \`release.yml\` from the v$version dispatch. Every file in this diff is generated; the reviewed prose is the fragments it consumes, which landed with their changes. The squash merge triggers the publish. The controls are the version input and its derivation check, so a review here is reading the assembled changelog, not the mechanics."
        out=$(gh pr create --title "chore: release v$version" --label release \
            --head "release/v$version" --body "$body" 2>&1) ||
            fail "gh pr create failed: $out"
        pr=$(printf '%s\n' "$out" | grep -oE '[0-9]+$' | tail -n 1)
        [ -n "$pr" ] || fail "gh pr create printed no pull request number: $out"
    fi
    gh pr merge "$pr" --auto --squash --delete-branch ||
        fail "auto-merge could not be enabled on #$pr; is auto-merge still on for the
  repository? The pull request is open and merges by hand."
    echo "release.sh: pull request #$pr auto-merges when CI gate passes; the publish follows the merge."
    endgroup
}

# ---------------------------------------------------------------------------
# prepare: verify the commit, select what is still to publish, package it.
# ---------------------------------------------------------------------------
prepare() {
    local dry=$1 version subject crate line pending=0 pending_names="" excludes="" bad sha

    group "The release commit names the version"
    subject=$(git log -1 --format=%s)
    version=$(version_from_subject "$subject") ||
        fail "HEAD's subject is not a release commit: $subject"
    [ "$version" = "$(workspace_version)" ] ||
        fail "the subject says $version but Cargo.toml says $(workspace_version); the tree is not the release"
    # The slice the release body needs has to exist before anything uploads;
    # `finish` extracts it after the crates are permanent.
    ./scripts/changelog.sh --notes "$version" >/dev/null
    echo "release.sh: releasing v$version from $(git rev-parse --short HEAD)"
    endgroup

    group "Select the crates still to publish"
    # The sparse index answers per-version presence without the API's rate
    # limit. A yanked version still occupies its number, so it counts as
    # published.
    for crate in $(publishable); do
        line=$(curl -sS --retry 3 --max-time 30 -w '\n%{http_code}' -H "User-Agent: $UA" \
            "$INDEX/$(index_path "$crate")") || fail "the index request for $crate failed outright"
        case "${line##*$'\n'}" in
        200)
            if printf '%s\n' "${line%$'\n'*}" | jq -r '.vers' | grep -Fxq "$version"; then
                echo "already published, excluding: $crate $version"
                excludes="$excludes --exclude $crate"
            else
                echo "to publish: $crate $version"
                pending=$((pending + 1))
                pending_names="$pending_names $crate"
            fi
            ;;
        404)
            fail "$crate is not on the registry at all. Trusted Publishing cannot create a
  crate, so a new name is claimed by hand first; RELEASING.md has the checklist."
            ;;
        *)
            fail "the index answered ${line##*$'\n'} for $crate; refusing to publish against a
  registry the run cannot read, which is how a version gets published twice"
            ;;
        esac
    done
    endgroup

    group "No crate published from another tree"
    # crates.io records the publishing commit per version; a crate already at
    # this version from a different commit means the release is split across
    # trees. Null means a manual token publish, unverifiable, and fails the
    # same way.
    for crate in $(publishable); do
        case " ${pending_names# } " in *" $crate "*) continue ;; esac
        if [ -z "${EXPECTED_SHA:-}" ]; then
            fail "$crate is already published at $version and no EXPECTED_SHA is set to verify
  which tree it came from. Locally that means the version is already part-released."
        fi
        sha=$(curl -fsS --retry 3 --max-time 30 -H "User-Agent: $UA" "$API/$crate/$version" |
            jq -r '.version.trustpub_data.sha // "null"')
        [ "$sha" = "$EXPECTED_SHA" ] || fail "$crate $version was published from ${sha}, not from
  $EXPECTED_SHA. The release is split across trees; abandon $version. The squash
  subject is the publish trigger, so a hand-opened pull request titled
  'chore: release v<next>' carrying the bump publishes the next patch from one
  commit."
        sleep 1 # the API's documented limit is one request per second
    done
    endgroup

    group "Required metadata is present"
    # The dry run does not catch a missing description or license: the
    # package step warns and exits 0 while the upload rejects it (cargo issue
    # 14249, closed as not planned).
    bad=$(cargo metadata --no-deps --format-version 1 |
        jq -r '.packages[] | select(.publish != []) |
               select((.description // "") == "" or (.license // "") == "") | .name')
    [ -z "$bad" ] || fail "missing description or license in: $bad. The upload would reject these
  after publishing whatever sorted before them."
    endgroup

    if [ "$pending" -eq 0 ]; then
        echo "release.sh: every crate already carries $version; nothing to package."
    else
        group "Package and verify every pending crate"
        # Every crate is packaged and verify-built before any token exists.
        # The excludes matter here too: without them the remaining crates
        # verify against local copies of crates that are already published,
        # a combination the real publish will not use.
        local exarr=()
        [ -n "${excludes// /}" ] && read -ra exarr <<<"$excludes"
        cargo package --workspace --locked ${exarr[@]+"${exarr[@]}"}
        endgroup
    fi

    if [ -n "${GITHUB_OUTPUT:-}" ]; then
        {
            echo "version=$version"
            echo "excludes=$excludes"
            echo "pending=$pending"
        } >>"$GITHUB_OUTPUT"
    fi

    if [ "$dry" = "yes" ]; then
        group "What a real run would do next"
        echo "would mint the 30-minute registry token (crates-io-auth-action, environment crates-io)"
        echo "would run: cargo publish --workspace --locked --no-verify$excludes"
        echo "would read back trustpub_data for every crate and require the release commit"
        echo "would resolve a scratch project against the registry (the smoke test)"
        echo "would tag v$version, open the GitHub release with the CHANGELOG section, and deploy the docs"
        endgroup
    fi
}

# ---------------------------------------------------------------------------
# upload: the only step that holds the registry token.
# ---------------------------------------------------------------------------
upload() {
    [ -n "${CARGO_REGISTRY_TOKEN:-}" ] || fail "upload needs CARGO_REGISTRY_TOKEN"
    [ -n "${PENDING:-}" ] || fail "upload needs PENDING from prepare"
    if [ "$PENDING" = "0" ]; then
        echo "release.sh: nothing pending; skipping the upload."
        return 0
    fi
    # `--no-verify` because `prepare` already packaged and verify-built every
    # pending crate in this job: packaging is deterministic for one tree and
    # toolchain, so the upload re-tars the same bytes, and the token's fixed
    # 30-minute life is not spent compiling the workspace a second time.
    local exarr=() excludes=${EXCLUDES:-}
    [ -n "${excludes// /}" ] && read -ra exarr <<<"$excludes"
    cargo publish --workspace --locked --no-verify ${exarr[@]+"${exarr[@]}"}
}

# ---------------------------------------------------------------------------
# finish: verify what the registry now holds, then tag, release, deploy.
# ---------------------------------------------------------------------------
finish() {
    local version=${VERSION:-} crate sha tags peeled notes attempt resolved dir

    [ -n "$version" ] || fail "finish needs VERSION"
    [ -n "${EXPECTED_SHA:-}" ] || fail "finish needs EXPECTED_SHA"
    [ -n "${GH_TOKEN:-}" ] || fail "finish needs GH_TOKEN (the App token)"
    [ -n "${DISPATCH_TOKEN:-}" ] || fail "finish needs DISPATCH_TOKEN (github.token)"
    [ -n "${GITHUB_REPOSITORY:-}" ] || fail "finish needs GITHUB_REPOSITORY"

    group "Every crate came from this commit"
    # Judged from the registry rather than the workflow's own success: every
    # crate's publishing commit must be this one.
    for crate in $(publishable); do
        sha=$(curl -fsS --retry 3 --max-time 30 -H "User-Agent: $UA" "$API/$crate/$version" |
            jq -r '.version.trustpub_data.sha // "null"')
        [ "$sha" = "$EXPECTED_SHA" ] ||
            fail "$crate $version reports trustpub sha '$sha', expected $EXPECTED_SHA"
        echo "verified: $crate $version"
        sleep 1 # the API's documented limit is one request per second
    done
    endgroup

    group "A consumer resolves the release"
    # A scratch project depending on the facade at this exact version
    # resolves from the registry, with no path dependencies involved.
    # Retried because the CDN can lag the publish by a few minutes.
    dir=$(mktemp -d)
    cat >"$dir/Cargo.toml" <<SMOKE
[package]
name = "spate-smoke"
version = "0.0.0"
edition = "2021"

[dependencies]
spate = { version = "=$version", features = ["full"] }

[dev-dependencies]
spate-test = "=$version"
SMOKE
    mkdir "$dir/src"
    echo 'fn main() {}' >"$dir/src/main.rs"
    resolved=no
    for attempt in 1 2 3 4 5; do
        if (cd "$dir" && cargo generate-lockfile >"$dir/lock.log" 2>&1); then
            resolved=yes
            break
        fi
        echo "attempt $attempt: the registry has not served v$version yet; waiting 30s"
        sleep 30
    done
    if [ "$resolved" != "yes" ]; then
        # Cargo's own answer decides whether this is lag or a graph that
        # genuinely does not resolve, and only one of those is fixed by a
        # re-run.
        tail -n 20 "$dir/lock.log" >&2
        rm -rf "$dir"
        fail "a consumer cannot resolve spate =$version from the registry; cargo's answer is above"
    fi
    echo "resolved $(grep -c '^name = ' "$dir/Cargo.lock") packages from the registry"
    rm -rf "$dir"
    endgroup

    group "Tag and release"
    # Tag after the publish, so the tag names what the registry holds; skip
    # idempotently on a re-run, and fail when the existing tag names another
    # commit.
    tags=$(git ls-remote --tags origin "refs/tags/v$version" "refs/tags/v$version^{}")
    if [ -n "$tags" ]; then
        # The peeled line names the commit an annotated tag points at; a
        # lightweight tag has only the plain line.
        peeled=$(printf '%s\n' "$tags" | awk '/\^\{\}$/ { print $1; exit }')
        [ -n "$peeled" ] || peeled=$(printf '%s\n' "$tags" | awk 'NR == 1 { print $1 }')
        [ "$peeled" = "$EXPECTED_SHA" ] ||
            fail "v$version already exists and points at $peeled, not $EXPECTED_SHA"
        echo "v$version is already tagged on this commit."
    else
        git -c user.name='spate-release[bot]' \
            -c user.email='spate-release[bot]@users.noreply.github.com' \
            tag -a "v$version" -m "v$version" "$EXPECTED_SHA"
        git push "https://x-access-token:${GH_TOKEN}@github.com/${GITHUB_REPOSITORY}.git" \
            "refs/tags/v$version"
    fi

    if gh release view "v$version" >/dev/null 2>&1; then
        echo "the v$version release already exists."
    else
        notes=$(mktemp)
        ./scripts/changelog.sh --notes "$version" >"$notes"
        if ! gh release create "v$version" --verify-tag --title "v$version" --notes-file "$notes"; then
            # The tag pushed moments ago can lag replication on the API side.
            sleep 10
            gh release create "v$version" --verify-tag --title "v$version" --notes-file "$notes"
        fi
        rm -f "$notes"
    fi
    endgroup

    group "Deploy the documentation"
    # The site carries the install snippets, so until this deploy it
    # advertises the previous version; the deploy runs after the crates are
    # live so the snippets are true the moment the page serves them. The
    # `docs` tier builds and deploys the site alone; the nightly tier files
    # issues of its own and runs on its own schedule.
    GH_TOKEN="$DISPATCH_TOKEN" gh workflow run scheduled.yml --field tier=docs
    echo "docs deploy dispatched; docs.rs builds on its own and lags the publish."
    endgroup
}

# ---------------------------------------------------------------------------
# dry-run: the whole release in a throwaway worktree.
# ---------------------------------------------------------------------------
dry_run() {
    local version=$1 root tree
    preflight
    root=$(mktemp -d "${TMPDIR:-/tmp}/spate-release-dry-run.XXXXXX")
    tree="$root/v$version"
    git worktree add --detach "$tree" HEAD >/dev/null
    echo "release.sh: dry run in $tree"

    (cd "$tree" && ./scripts/release.sh assemble --version "$version" --dry-run)
    (cd "$tree" && ./scripts/release.sh publish --dry-run)

    echo
    echo "release.sh: dry run complete. The release commit is at:"
    echo "  $tree"
    echo "Inspect it with:"
    echo "  git -C $tree show --stat HEAD"
    echo "Remove it with:"
    echo "  git worktree remove --force $tree && rm -rf $root"
}

# ---------------------------------------------------------------------------
# Dispatch.
# ---------------------------------------------------------------------------
self_test

cmd=${1:-}
shift || true
version=""
dry=no
while [ $# -gt 0 ]; do
    case "$1" in
    --version)
        version=${2:-}
        shift 2 || fail "--version needs a value"
        ;;
    --dry-run)
        dry=yes
        shift
        ;;
    *)
        fail "unknown argument '$1'"
        ;;
    esac
done

case "$cmd" in
assemble)
    [ -n "$version" ] || fail "assemble needs --version X.Y.Z"
    assemble "$version" "$dry"
    ;;
prepare)
    prepare "$dry"
    ;;
upload)
    upload
    ;;
finish)
    finish
    ;;
publish)
    [ "$dry" = "yes" ] || fail "the real publish runs in CI as prepare/upload/finish; locally use
  'publish --dry-run', which stops where the registry token would be minted"
    prepare yes
    ;;
dry-run)
    [ -n "$version" ] || fail "dry-run needs --version X.Y.Z"
    dry_run "$version"
    ;;
--self-test)
    echo "release.sh: self-test passed"
    ;;
*)
    fail "usage: assemble --version X.Y.Z [--dry-run] | prepare [--dry-run] | upload | finish | publish --dry-run | dry-run --version X.Y.Z | --self-test"
    ;;
esac
