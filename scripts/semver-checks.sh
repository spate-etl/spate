#!/usr/bin/env bash
#
# The cargo-semver-checks gates: two comparisons, two callers.
#
#   ./scripts/semver-checks.sh --against-merge-base  # ci.yml, per pull request
#   ./scripts/semver-checks.sh --against-registry    # scheduled.yml, nightly
#   ./scripts/semver-checks.sh --self-test           # the classifiers, alone
#
# The pull-request gate diffs each crate against the pull request's base, so
# a finding is a break this pull request introduces. A finding whose pull
# request title already carries the breaking marker passes with a notice:
# pre-1.0 a breaking change ships in a minor bump and the gate's job is the
# announcement, not the veto. A finding without the marker fails, and
# retitling re-runs the gate, since ci.yml re-runs on `edited`. The registry
# comparison cannot sit on pull requests: once an intentional break lands,
# every later pull request would fail against the published baseline until
# the release ships.
#
# The nightly run diffs against the registry and catches what slipped past
# the gate: a break on `main` with no marker anywhere in the log. The release
# derivation reads the log for the marker, so an unmarked break would
# under-bump the next version; the remedy is a follow-up commit carrying the
# marker.
#
# Environment (--against-merge-base only; set by ci.yml, and on a laptop the
# baseline falls back to the merge base with origin/main):
#   BASE_SHA  github.event.pull_request.base.sha
#   HEAD_SHA  github.event.pull_request.head.sha
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

    [ "$failures" -eq 0 ] || fail "$failures self-test failure(s). This script is wrong, not your change"
}

# ---------------------------------------------------------------------------
# --against-merge-base: breaks this pull request introduces.
# ---------------------------------------------------------------------------

# The baseline revision. In CI the checkout is the pull request's merge
# result against the base tip, so the base tip itself is the baseline:
# everything already on `main`, an announced break included, sits on both
# sides and is not attributed to this pull request. On a laptop the tree is
# the branch itself, so the merge base with the obvious upstream plays the
# same role.
resolve_base() {
    local ref candidate base
    if [ -n "${BASE_SHA:-}" ]; then
        git rev-parse --verify --quiet "$BASE_SHA^{commit}" >/dev/null ||
            fail "the base ${BASE_SHA:0:12} is not in this clone. A re-run of an old workflow run
  replays its frozen payload; re-run from the newest run, or push the branch."
        printf '%s\n' "$BASE_SHA"
        return 0
    fi
    for ref in origin/main upstream/main main; do
        if candidate=$(git rev-parse --verify --quiet "$ref^{commit}" 2>/dev/null) &&
            base=$(git merge-base HEAD "$candidate" 2>/dev/null) && [ -n "$base" ]; then
            printf '%s\n' "$base"
            return 0
        fi
    done
    fail "no baseline found. Set BASE_SHA, or fetch a main to compare against."
}

merge_base_mode() {
    local base base_crates base_version crate packages=() removed="" status verdict=clean

    base=$(resolve_base)
    if [ "$base" = "$(git rev-parse HEAD)" ]; then
        echo "semver-checks.sh: the baseline is HEAD; nothing to diff."
        return 0
    fi

    # A diff that moves the workspace version is a release pull request; the
    # tool classifies a 0.x bump as major and drops every lint, so running it
    # would print a pass that evaluated nothing.
    base_version=$(git show "$base:Cargo.toml" | sed -n 's/^version = "\(.*\)"$/\1/p')
    if [ -n "$base_version" ] && [ "$base_version" != "$(workspace_version)" ]; then
        echo "semver-checks.sh: the workspace version moves in this diff ($base_version -> $(workspace_version));"
        echo "  the classification drops every lint for a version bump, so there is nothing to run."
        return 0
    fi

    # Only crates present on both sides are diffed. A crate this pull request
    # adds has no baseline anywhere; one it removes is itself a breaking
    # change and is judged with the tool's findings below.
    base_crates=$(git ls-tree --name-only "$base:crates" 2>/dev/null | tr '\n' ' ') ||
        fail "the baseline $base carries no crates/ tree"
    for crate in $(crates_now); do
        case " $base_crates " in
        *" $crate "*) packages+=(--package "$crate") ;;
        *) echo "::notice::$crate is new in this pull request; there is no baseline to diff." ;;
        esac
    done
    for crate in $base_crates; do
        [ -n "$crate" ] || continue
        [ -d "crates/$crate" ] || removed="$removed $crate"
    done
    if [ "${#packages[@]}" -eq 0 ] && [ -z "$removed" ]; then
        echo "semver-checks.sh: every crate is new; nothing to diff."
        return 0
    fi

    if [ "${#packages[@]}" -gt 0 ]; then
        echo "semver-checks.sh: checking $((${#packages[@]} / 2)) crate(s) against baseline ${base:0:12}"
        status=0
        cargo semver-checks --baseline-rev "$base" "${packages[@]}" || status=$?
        verdict=$(classify_exit "$status")
        if [ "$verdict" = "error" ]; then
            fail "cargo semver-checks exited $status without a verdict. That is the tool failing
  to complete, not an API judgement; read its output above."
        fi
    fi
    if [ -n "$removed" ]; then
        echo "::notice::removed published crate(s):$removed"
        verdict=breaking
    fi

    case "$verdict" in
    clean)
        echo "semver-checks.sh: every crate holds its API against the baseline."
        ;;
    breaking)
        # The announcement is the requirement, and the squash subject is the
        # pull request title, so the title carrying the marker is what makes
        # the release derivation see this break.
        if subject_is_breaking "${PR_TITLE:-}"; then
            echo "semver-checks.sh: breaking, and the title already carries the marker; the next"
            echo "  release derives as a minor."
        else
            echo "::error::This pull request introduces a breaking API change; the findings are above."
            echo "::error::Pre-1.0 a breaking change ships in a minor bump. Carry \`!\` in the pull request title (retitling re-runs this gate), tick Breaking in the Semver section, and open the changelog fragment with **Breaking:**."
            exit 1
        fi
        ;;
    esac
}

# ---------------------------------------------------------------------------
# --against-registry: the tree against what is published.
# ---------------------------------------------------------------------------
registry_mode() {
    local crate reply code body latest current status checked=0 skipped=0 breaking="" args=()

    current=$(workspace_version)
    for dir in crates/*/; do
        [ -d "$dir" ] || continue
        crate=$(basename "$dir")

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
        args=()
        if [ -z "$latest" ]; then
            echo "::notice::every published version of $crate is yanked; nothing to diff against."
            skipped=$((skipped + 1))
            continue
        fi
        if [ "$latest" != "$current" ]; then
            args=(--release-type minor)
        fi

        echo "semver-checks.sh: checking $crate against its published baseline"
        status=0
        cargo semver-checks --package "$crate" ${args[@]+"${args[@]}"} || status=$?
        case "$(classify_exit "$status")" in
        clean)
            checked=$((checked + 1))
            ;;
        breaking)
            breaking="$breaking $crate"
            checked=$((checked + 1))
            ;;
        error)
            fail "cargo semver-checks exited $status for $crate without a verdict. That is the
  tool failing to complete, not an API judgement; read its output above."
            ;;
        esac
    done

    [ "$((checked + skipped))" -gt 0 ] || fail "no crates under crates/, so the check evaluated nothing"

    # A break against the registry is the expected state after an announced
    # breaking change lands, so the verdict comes from the log: the marker
    # anywhere since the last tag means the derivation already says minor.
    # No marker means a break slipped past the pull-request gate.
    if [ -n "$breaking" ]; then
        local last log
        last=$(git tag --list 'v[0-9]*' --sort=-v:refname | head -n 1)
        [ -n "$last" ] || fail "breaking changes found but no vX.Y.Z tag to scan for their markers"
        log=$(git log --no-merges --format=%B "$last..HEAD")
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
case "${1:-}" in
--against-merge-base)
    merge_base_mode
    ;;
--against-registry)
    registry_mode
    ;;
--self-test)
    echo "semver-checks.sh: self-test passed"
    ;;
*)
    fail "usage: --against-merge-base | --against-registry | --self-test"
    ;;
esac
