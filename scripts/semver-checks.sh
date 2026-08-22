#!/usr/bin/env bash
#
# The cargo-semver-checks gates: two comparisons, two callers.
#
#   ./scripts/semver-checks.sh --against-merge-base  # ci.yml, per pull request
#   ./scripts/semver-checks.sh --against-registry    # scheduled.yml, nightly
#   ./scripts/semver-checks.sh --self-test           # the classifiers, alone
#
# The pull-request gate diffs each crate against the pull request's merge
# base, so a finding is a break this pull request introduces, while the `!`
# can still land in the title before the squash merge freezes the subject.
# The registry comparison cannot sit there: pre-1.0 a breaking change is
# allowed, and once one lands, every later pull request would fail against
# the published baseline until the release ships.
#
# The nightly run diffs against the registry and catches what slipped past
# the gate: a break on `main` with no marker in its subject. The release
# derivation reads subjects, so an unmarked break under-bumps the next
# version; the remedy is a follow-up commit carrying the marker.
#
# Both comparisons hold the version equal on the two sides, which is what
# makes the tool run its major-breaking lints: it keeps only the lints the
# version pair cannot absorb, and a 0.x minor bump classifies as major,
# which absorbs every one.
#
# Environment (--against-merge-base only; set by ci.yml, and on a laptop the
# merge base falls back to origin/main):
#   BASE_SHA  github.event.pull_request.base.sha
#   HEAD_SHA  github.event.pull_request.head.sha
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

# The conventional-commits breaking marker on a subject. The same shape
# scripts/release-version.sh derives the bump from.
subject_is_breaking() {
    [[ "$1" =~ ^[a-zA-Z]+(\([^\)]*\))?!: ]]
}

# The publishable crates, by directory name under crates/.
crates_now() {
    local dir
    for dir in crates/*/; do
        [ -d "$dir" ] && basename "$dir"
    done
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

# The merge base: from BASE_SHA/HEAD_SHA in CI, where a missing base is a
# hard failure because it means the checkout lost `fetch-depth: 0`; from the
# obvious upstream on a laptop.
resolve_base() {
    local base="" ref candidate
    if [ -n "${BASE_SHA:-}" ] || [ -n "${HEAD_SHA:-}" ]; then
        base=$(git merge-base "${BASE_SHA:-}" "${HEAD_SHA:-}" 2>/dev/null) || base=""
        [ -n "$base" ] || fail "no merge base for ${BASE_SHA:-?}..${HEAD_SHA:-?}. Does the checkout still set fetch-depth: 0?"
        printf '%s\n' "$base"
        return 0
    fi
    for ref in origin/main upstream/main main; do
        if candidate=$(git rev-parse --verify --quiet "$ref^{commit}" 2>/dev/null) &&
            base=$(git merge-base HEAD "$candidate" 2>/dev/null) && [ -n "$base" ]; then
            printf '%s\n' "$base"
            return 0
        fi
    done
    fail "no merge base found. Set BASE_SHA and HEAD_SHA, or fetch a main to compare against."
}

merge_base_mode() {
    local base base_crates crate packages=() status

    base=$(resolve_base)
    if [ "$base" = "$(git rev-parse HEAD)" ]; then
        echo "semver-checks.sh: the merge base is HEAD; nothing to diff."
        return 0
    fi

    # Only crates present on both sides are diffed. A crate this pull
    # request adds has no baseline anywhere, on the registry included, until
    # its name is claimed by hand (RELEASING.md has the checklist).
    base_crates=$(git ls-tree --name-only "$base:crates" 2>/dev/null | tr '\n' ' ') ||
        fail "the merge base $base carries no crates/ tree"
    for crate in $(crates_now); do
        case " $base_crates " in
        *" $crate "*) packages+=(--package "$crate") ;;
        *) echo "::notice::$crate is new in this pull request; there is no baseline to diff." ;;
        esac
    done
    if [ "${#packages[@]}" -eq 0 ]; then
        echo "semver-checks.sh: every crate is new; nothing to diff."
        return 0
    fi

    echo "semver-checks.sh: checking $((${#packages[@]} / 2)) crate(s) against merge base ${base:0:12}"
    status=0
    cargo semver-checks --baseline-rev "$base" "${packages[@]}" || status=$?
    case "$(classify_exit "$status")" in
    clean)
        echo "semver-checks.sh: every crate holds its API against the merge base."
        ;;
    breaking)
        echo "::error::This pull request introduces a breaking API change; the crates are named above."
        echo "::error::Pre-1.0 a breaking change ships in a minor bump. Carry \`!\` in the pull request title, tick Breaking in the Semver section, and open the changelog fragment with **Breaking:**."
        exit 1
        ;;
    error)
        fail "cargo semver-checks exited $status without a verdict. That is the tool failing
  to complete, not an API judgement; read its output above."
        ;;
    esac
}

# ---------------------------------------------------------------------------
# --against-registry: the tree against what is published.
# ---------------------------------------------------------------------------
registry_mode() {
    local crate code status checked=0 skipped=0 breaking="" last subjects bodies line announced=no

    for dir in crates/*/; do
        [ -d "$dir" ] || continue
        crate=$(basename "$dir")

        # The sparse index answers whether any version is published, with no
        # rate limit. Only 200 and 404 are answers; anything else fails the
        # gate.
        code=$(curl -sS -o /dev/null -w '%{http_code}' -H "User-Agent: $UA" \
            "https://index.crates.io/$(index_path "$crate")")
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

        echo "semver-checks.sh: checking $crate against its published baseline"
        status=0
        cargo semver-checks --package "$crate" || status=$?
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

    [ "$((checked + skipped))" -gt 0 ] || fail "no crates under crates/, so the gate checked nothing"

    # A break against the registry is the expected state after an announced
    # breaking change lands, so the verdict comes from the history: a marker
    # anywhere since the last tag means the derivation will already say
    # minor. No marker means a break slipped past the pull-request gate and
    # the next release would under-bump.
    if [ -n "$breaking" ]; then
        last=$(git tag --list 'v[0-9]*' --sort=-v:refname | head -n 1)
        [ -n "$last" ] || fail "breaking changes found but no vX.Y.Z tag to scan for their markers"
        subjects=$(git log --no-merges --format=%s "$last..HEAD")
        bodies=$(git log --no-merges --format=%B "$last..HEAD")
        while IFS= read -r line; do
            [ -n "$line" ] || continue
            if subject_is_breaking "$line"; then
                announced=yes
                break
            fi
        done <<<"$subjects"
        if [ "$announced" = "no" ] && grep -Eq '^BREAKING[- ]CHANGE:' <<<"$bodies"; then
            announced=yes
        fi

        if [ "$announced" = "yes" ]; then
            echo "semver-checks.sh: breaking against the registry:$breaking"
            echo "  A marker since $last already announces it; the next release derives as a minor."
            return 0
        fi
        echo "::error::Breaking against the registry with no marker since $last:$breaking"
        echo "::error::The release derivation reads commit subjects, so this break would under-bump the next version. Land a commit carrying \`!\` in its subject and a changelog fragment opening with **Breaking:**."
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
