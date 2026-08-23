#!/usr/bin/env bash
#
# The cargo-semver-checks gate: one comparison, against the published release.
#
#   ./scripts/semver-checks.sh --against-registry --packages "spate spate-s3"
#   ./scripts/semver-checks.sh --against-registry    # every crate
#   ./scripts/semver-checks.sh --self-test           # the classifiers, alone
#
# `--packages` restricts the comparison to a named set, which ci.yml fills from
# ci-changes.sh's reverse-dependency closure over the crates a pull request
# touches. An empty set is an error rather than every crate: a workflow
# expression that resolves to nothing must fail the gate, not silently widen it.
#
# The published release is the baseline the version number is a claim about, so
# it is what the gate compares against, and it moves only at a release. A break
# is expected once an announced one has landed, so a finding passes when the
# pull request title carries the marker, or when a commit since the last tag
# already carries one.
#
# That second excuse is workspace-wide, so after the first announced break of a
# release cycle a later pull request breaking a different crate passes without
# a marker of its own. The version still derives as a minor, which is what the
# number has to get right; what is given up is the fragment and the release-note
# line for that second break. tokio, hyper and diesel accept the same trade.
#
# Pre-1.0 a breaking change ships in a minor bump, so the gate's job is the
# announcement rather than the veto. A finding without a marker fails, and
# retitling re-runs it, since ci.yml re-runs on `edited`.
#
# Environment (set by ci.yml on a pull request; both are absent elsewhere and
# the marker scan then reads up to HEAD):
#   BASE_SHA  github.event.pull_request.base.sha, the end of the marker scan
#   PR_TITLE  github.event.pull_request.title; free text somebody typed,
#             matched against, never evaluated
set -euo pipefail

cd "$(dirname "$0")/.."

UA='spate-release (github.com/spate-etl/spate)'

fail() {
    echo "semver-checks.sh: $1" >&2
    exit 1
}

# A crate's path in the sparse index. The scheme keys on name length; every
# workspace crate lands in the four-or-longer arm today, and the short arms
# keep a future short name from probing a URL that answers 404 for the wrong
# reason.
index_path() {
    local crate=$1
    case "${#crate}" in
    1) printf '1/%s\n' "$crate" ;;
    2) printf '2/%s\n' "$crate" ;;
    3) printf '3/%s/%s\n' "${crate:0:1}" "$crate" ;;
    *) printf '%s/%s/%s\n' "${crate:0:2}" "${crate:2:2}" "$crate" ;;
    esac
}

# The tool's exit codes, from its own docs: 0 is clean, 100 is "required bump
# not satisfied", 101 is "could not complete". Everything else is treated as
# could-not-complete: a gate that cannot evaluate must not pass.
classify_exit() {
    case "$1" in
    0) printf 'clean\n' ;;
    100) printf 'breaking\n' ;;
    *) printf 'error\n' ;;
    esac
}

# The conventional breaking marker. The same shape scripts/release-version.sh
# derives the bump from.
MARKER_ERE='^[a-zA-Z]+(\([^)]*\))?!:'
subject_is_breaking() {
    [[ "$1" =~ $MARKER_ERE ]]
}

# The publishable crates, by directory name under crates/.
crates_now() {
    local dir
    for dir in crates/*/; do
        [ -d "$dir" ] && basename "$dir"
    done
}

workspace_version() {
    sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml
}

# The crates this run compares: the set `--packages` named, or every crate.
# Set by the dispatch below, after packages_valid has accepted it.
REQUESTED_PKGS=""
selected_crates() {
    local -a names
    if [ -n "$REQUESTED_PKGS" ]; then
        # `read -a` splits on whitespace without globbing, so a name never
        # reaches the shell as a pattern.
        read -r -a names <<<"$REQUESTED_PKGS"
        printf '%s\n' "${names[@]}"
    else
        crates_now
    fi
}

# Is $1 a well-formed package list? Prints nothing; the answer is the exit
# status. The value arrives from a workflow expression, so it is data: a name
# is matched against a character class rather than used as a pattern, and a
# name that is not a crate directory means the closure table went stale and
# must fail rather than check nothing.
packages_valid() {
    local name
    [ -n "${1// /}" ] || return 1
    for name in $1; do
        case "$name" in
        *[!A-Za-z0-9_-]*) return 1 ;;
        esac
        [ -d "crates/$name" ] || return 1
    done
    return 0
}

# ---------------------------------------------------------------------------
# Self-test. Runs inline on every invocation as well as under --self-test.
# ---------------------------------------------------------------------------
self_test() {
    local failures=0 line want got

    while IFS='|' read -r line want; do
        case "$line" in '' | '#'*) continue ;; esac
        got=$(index_path "$line")
        if [ "$got" != "$want" ]; then
            echo "semver-checks.sh: index_path '$line' -> '$got', expected '$want'" >&2
            failures=$((failures + 1))
        fi
    done <<'TABLE'
spate|sp/at/spate
spate-core|sp/at/spate-core
spate-clickhouse|sp/at/spate-clickhouse
abc|3/a/abc
ab|2/ab
a|1/a
TABLE

    while IFS='|' read -r line want; do
        case "$line" in '' | '#'*) continue ;; esac
        got=$(classify_exit "$line")
        if [ "$got" != "$want" ]; then
            echo "semver-checks.sh: classify_exit '$line' -> '$got', expected '$want'" >&2
            failures=$((failures + 1))
        fi
    done <<'TABLE'
0|clean
100|breaking
101|error
1|error
137|error
TABLE

    while IFS='|' read -r line want; do
        case "$line" in '' | '#'*) continue ;; esac
        if subject_is_breaking "$line"; then got=breaking; else got=plain; fi
        if [ "$got" != "$want" ]; then
            echo "semver-checks.sh: marker: '$line' -> '$got', expected '$want'" >&2
            failures=$((failures + 1))
        fi
    done <<'TABLE'
feat(spate-core)!: seal the framework configuration sections|breaking
refactor!: rename the framework to spate|breaking
docs(workspace)!: migrate CLAUDE.md to AGENTS.md|breaking
feat(spate-core): a windowed operator|plain
chore: release v0.2.0|plain
revert(spate-core): back out the windowed operator!|plain
TABLE

    # The --packages validator. A crate name here is data from a workflow
    # expression, so the rejections matter as much as the acceptances: a glob
    # must not expand and a path must not traverse.
    while IFS='|' read -r line want; do
        case "$line" in '#'*) continue ;; esac
        if packages_valid "$line"; then got=ok; else got=reject; fi
        if [ "$got" != "$want" ]; then
            echo "semver-checks.sh: packages_valid '$line' -> '$got', expected '$want'" >&2
            failures=$((failures + 1))
        fi
    done <<'TABLE'
spate|ok
spate spate-kafka|ok
|reject
 |reject
spate-*|reject
../etc|reject
crates/spate|reject
spate;rm -rf /|reject
spate nonexistent-crate|reject
TABLE

    [ "$failures" -eq 0 ] || fail "$failures self-test failure(s). This script is wrong, not your change"
}

# The newest release tag, which is the version the registry serves.
last_tag() {
    git tag --list 'v[0-9]*' --sort=-v:refname | head -n 1
}

# One batched comparison over the named crates, with `--release-type $1` when
# $1 is non-empty. Accumulates into CHECKED and BREAKING.
#
# A break re-runs the group one crate at a time to name which crates broke. The
# findings are already on the log from the batch, so the re-run discards its
# output and reads only the verdict.
CHECKED=0
BREAKING=""
check_group() {
    local rt=$1 status=0 pkg
    shift
    [ $# -gt 0 ] || return 0
    local -a args=()
    for pkg in "$@"; do args+=(--package "$pkg"); done
    [ -z "$rt" ] || args+=(--release-type "$rt")

    echo "semver-checks.sh: checking $# crate(s) against their published baseline"
    cargo semver-checks "${args[@]}" || status=$?
    case "$(classify_exit "$status")" in
    clean)
        CHECKED=$((CHECKED + $#))
        ;;
    breaking)
        CHECKED=$((CHECKED + $#))
        for pkg in "$@"; do
            status=0
            if [ -z "$rt" ]; then
                cargo semver-checks --package "$pkg" >/dev/null 2>&1 || status=$?
            else
                cargo semver-checks --package "$pkg" --release-type "$rt" >/dev/null 2>&1 || status=$?
            fi
            case "$(classify_exit "$status")" in
            breaking) BREAKING="$BREAKING $pkg" ;;
            clean) ;;
            error)
                fail "cargo semver-checks exited $status for $pkg while attributing a break.
  Read its output above; the batch already reported the findings."
                ;;
            esac
        done
        ;;
    error)
        fail "cargo semver-checks exited $status without a verdict. That is the
  tool failing to complete, not an API judgement; read its output above."
        ;;
    esac
}

# ---------------------------------------------------------------------------
# --against-registry: the tree against what is published.
# ---------------------------------------------------------------------------
registry_mode() {
    local crate reply code body latest current checked=0 skipped=0 breaking=""
    local -a plain=() pinned=()
    CHECKED=0
    BREAKING=""

    current=$(workspace_version)
    while IFS= read -r crate; do
        [ -n "$crate" ] || continue

        # The sparse index answers which versions are published, with no rate
        # limit. Only 200 and 404 are answers; a transport failure or any
        # other code fails the run before it can claim anything.
        reply=$(curl -sS --retry 3 --max-time 30 -w '\n%{http_code}' -H "User-Agent: $UA" \
            "https://index.crates.io/$(index_path "$crate")") ||
            fail "the index request for $crate failed outright; the check cannot evaluate"
        code=${reply##*$'\n'}
        body=${reply%$'\n'*}
        case "$code" in
        200) ;;
        404)
            echo "::notice::$crate has no published baseline, so there is nothing to diff against."
            echo "  Its name is claimed by hand at the first release including it; see RELEASING.md."
            skipped=$((skipped + 1))
            continue
            ;;
        *)
            fail "the sparse index answered $code for $crate; refusing to report a pass it cannot evaluate"
            ;;
        esac

        # Between a release merge and its publish, the tree's version is
        # ahead of the registry and the default classification drops every
        # lint; pinning the expectation to a minor keeps the major-breaking
        # lints running across that window.
        latest=$(printf '%s\n' "$body" | jq -r 'select(.yanked | not) | .vers' | tail -n 1)
        if [ -z "$latest" ]; then
            echo "::notice::every published version of $crate is yanked; nothing to diff against."
            skipped=$((skipped + 1))
            continue
        fi
        if [ "$latest" != "$current" ]; then
            pinned+=("$crate")
        else
            plain+=("$crate")
        fi
    done < <(selected_crates)

    # One invocation per group rather than one per crate: the tool shares a
    # rustdoc build across the packages of a single run and shares nothing
    # between runs.
    check_group "" ${plain[@]+"${plain[@]}"}
    check_group minor ${pinned[@]+"${pinned[@]}"}
    checked=$CHECKED
    breaking=$BREAKING

    [ "$((checked + skipped))" -gt 0 ] ||
        fail "the selection named no crate to check, so the check evaluated nothing"

    # A crate published at the last tag and absent from the tree is a removal,
    # which is breaking whatever the tool says about what remains. It needs no
    # build, so it is judged over every crate rather than the selected set.
    for crate in $(git ls-tree --name-only "$(last_tag):crates" 2>/dev/null); do
        [ -n "$crate" ] || continue
        [ -d "crates/$crate" ] || breaking="$breaking $crate(removed)"
    done

    # A break against the registry is the expected state after an announced
    # breaking change lands, so the verdict comes from the log: the marker
    # anywhere since the last tag means the derivation already says minor.
    # No marker means a break slipped past the pull-request gate.
    if [ -n "$breaking" ]; then
        local last log
        last=$(last_tag)
        [ -n "$last" ] || fail "breaking changes found but no vX.Y.Z tag to scan for their markers"

        # This pull request's own announcement is its title and nothing else:
        # the squash subject is the title, and a constituent subject inside a
        # squash body is not a subject the release derivation reads.
        if subject_is_breaking "${PR_TITLE:-}"; then
            echo "semver-checks.sh: breaking against the registry:$breaking"
            echo "  The title carries the marker; the next release derives as a minor."
            return 0
        fi

        # Everything already announced on the base. Scanning to HEAD instead
        # would read this branch's own commit subjects, which squash into body
        # lines the derivation classifies as plain.
        log=$(git log --no-merges --format=%B "$last..${BASE_SHA:-HEAD}")
        if grep -qE "$MARKER_ERE" <<<"$log"; then
            echo "semver-checks.sh: breaking against the registry:$breaking"
            echo "  A marker since $last already announces it; the next release derives as a minor."
            return 0
        fi
        if [ -n "${GITHUB_OUTPUT:-}" ]; then
            echo "unmarked-break=true" >>"$GITHUB_OUTPUT"
        fi
        echo "::error::Breaking against the registry with no marker since $last:$breaking"
        echo "::error::The release derivation reads the log for the marker, so this break would under-bump the next version. Land a commit carrying \`!\` in its subject and a changelog fragment opening with **Breaking:**."
        exit 1
    fi

    if [ "$checked" -eq 0 ]; then
        echo "semver-checks.sh: every crate was skipped for want of a baseline; the check is silent"
        echo "  until the first release publishes one."
        return 0
    fi

    echo "semver-checks.sh: $checked crate(s) hold their published API surface."
}

# ---------------------------------------------------------------------------
# Dispatch.
# ---------------------------------------------------------------------------
self_test
mode="${1:-}"
shift || true
if [ "${1:-}" = "--packages" ]; then
    packages_valid "${2:-}" ||
        fail "--packages needs a space-separated list of crate directory names under crates/,
  and got '${2:-}'. An empty or unrecognized value is a wiring fault in the
  caller, and passing it would check the wrong set or nothing at all."
    REQUESTED_PKGS="${2:-}"
    shift 2
fi
[ $# -eq 0 ] || fail "unexpected argument '$1'"

case "$mode" in
--against-registry)
    registry_mode
    ;;
--self-test)
    echo "semver-checks.sh: self-test passed"
    ;;
*)
    fail "usage: --against-registry [--packages \"a b\"] | --self-test"
    ;;
esac
