#!/usr/bin/env bash
#
# The workspace version tool: rewrites every file that carries a literal
# version, checks that they all agree, and derives the next version from
# history. RELEASING.md names where each caller sits in the release.
#
# The version appears in three shapes. `[workspace.package]` carries `X.Y.Z`;
# the `[workspace.dependencies]` pins carry `=X.Y.Z`; the install snippets in
# the READMEs and the docs carry `X.Y`, which cargo resolves to the newest
# `X.Y.z` on the registry.
#
# Usage:
#   ./scripts/release-version.sh --bump <X.Y.Z>  # rewrite everything, then --check
#   ./scripts/release-version.sh --check         # the gate: every literal agrees
#   ./scripts/release-version.sh --derive        # print the next version from history
#   ./scripts/release-version.sh --self-test     # the rewriters, alone
#
# `--check` and `--self-test` need git and no toolchain. `--bump` runs
# `cargo update --workspace` after the rewrite so Cargo.lock follows the
# manifest in the same change. `--derive` reads tags and commits, so it needs
# the full history, not a shallow clone.
#
# Targets `bash` 3.2, the version stock macOS ships as /bin/bash: no
# associative arrays, no `mapfile`, no `${var,,}`, and every array expansion
# guarded.
set -euo pipefail

cd "$(dirname "$0")/.."

manifest=Cargo.toml

# Every file holding an install snippet. A snippet anywhere else fails
# `--check` with instructions to extend this list.
SNIPPET_FILES="README.md
crates/spate/README.md
docs/user-guide/01-getting-started/01-installation.mdx
docs/user-guide/04-connectors/_securing-kafka.mdx"

# Tracked files `--check` scans for snippet-shaped lines, minus the
# exclusions below. Shell scripts are left out: this script's own self-test
# fixtures are snippet-shaped. The patterns are quoted through to git, which
# matches them against every tracked path; the shell must not expand them
# against the repository root first.
scan_files() {
    git ls-files -- '*.md' '*.mdx' '*.toml' '*.rs'
}

# CHANGELOG.md and THIRD-PARTY.md are generated with the versions they are
# meant to hold. docs/adr/ records history and an accepted record is
# immutable, so a version literal there stays as written.
scan_excluded() {
    case "$1" in
    CHANGELOG.md | THIRD-PARTY.md) return 0 ;;
    changelog.d/* | docs/adr/*) return 0 ;;
    esac
    return 1
}

fail() {
    echo "release-version.sh: $1" >&2
    exit 1
}

# ---------------------------------------------------------------------------
# Version parsing.
# ---------------------------------------------------------------------------

# Exactly `X.Y.Z`, each component decimal.
is_version() {
    [[ "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]
}

# The `[workspace.package]` version. Members inherit it, so the manifest
# carries exactly one line-anchored `version = "..."`, and more than one is
# an error.
workspace_version() {
    local n v
    n=$(grep -c '^version = "' "$manifest") || true
    [ "$n" = "1" ] || fail "$manifest carries $n 'version = \"...\"' lines, expected exactly 1"
    v=$(sed -n 's/^version = "\(.*\)"$/\1/p' "$manifest")
    is_version "$v" || fail "$manifest workspace version '$v' is not X.Y.Z"
    printf '%s\n' "$v"
}

# `X.Y` from `X.Y.Z`: the requirement an install snippet carries.
minor_of() {
    printf '%s\n' "${1%.*}"
}

# 0 when $1 < $2, component-wise.
version_lt() {
    local a1 a2 a3 b1 b2 b3
    IFS=. read -r a1 a2 a3 <<<"$1"
    IFS=. read -r b1 b2 b3 <<<"$2"
    if [ "$a1" -ne "$b1" ]; then [ "$a1" -lt "$b1" ]; return; fi
    if [ "$a2" -ne "$b2" ]; then [ "$a2" -lt "$b2" ]; return; fi
    [ "$a3" -lt "$b3" ]
}

# The next version from a current one and a bump kind.
next_version() {
    local cur=$1 kind=$2 x y z
    IFS=. read -r x y z <<<"$cur"
    case "$kind" in
    minor) printf '%s.%s.0\n' "$x" "$((y + 1))" ;;
    patch) printf '%s.%s.%s\n' "$x" "$y" "$((z + 1))" ;;
    *) fail "next_version: unknown bump kind '$kind'" ;;
    esac
}

# ---------------------------------------------------------------------------
# The rewriters. Both are stdin-to-stdout filters so the self-test can drive
# them with no repository state at all.
# ---------------------------------------------------------------------------

# The manifest: the one `[workspace.package]` version line and every
# `spate-*` pin. The pin pattern requires `version = "=`, so the versionless
# `spate-bench = { path = "bench" }` entry is out of its reach.
#
# `sub` replaces the first quoted string on the line, which is the version on
# both shapes: a pin's `path` value never contains `"=`, and extra keys after
# it (`default-features = false` on spate-coordination) sit outside the match.
rewrite_manifest() {
    local new=$1
    awk -v new="$new" '
        /^version = "/ {
            sub(/"[^"]*"/, "\"" new "\"")
            top++
        }
        /^spate-[a-z0-9-]+ = \{ version = "=/ {
            sub(/"=[^"]*"/, "\"=" new "\"")
            pins++
        }
        { print }
        END {
            if (top != 1) {
                print "release-version.sh: rewrote " top+0 " workspace version lines, expected 1" > "/dev/stderr"
                exit 1
            }
            if (pins < 1) {
                print "release-version.sh: rewrote no spate-* pins" > "/dev/stderr"
                exit 1
            }
        }
    '
}

# An install snippet: a line assigning a spate crate either a bare version
# string or an inline table with a `version` key. Only the version string
# moves; features, paths and trailing comments stay byte-identical. Exits 3
# when a file yields no snippet at all, so a caller can name the file.
rewrite_snippets() {
    local xy=$1
    awk -v xy="$xy" '
        /^[[:space:]]*spate[a-z0-9-]* *= *"[0-9]+\.[0-9]+"/ {
            sub(/"[0-9]+\.[0-9]+"/, "\"" xy "\"")
            hits++
        }
        /^[[:space:]]*spate[a-z0-9-]* *= *\{[^}]*version *= *"[0-9]+\.[0-9]+"/ {
            sub(/version *= *"[0-9]+\.[0-9]+"/, "version = \"" xy "\"")
            hits++
        }
        { print }
        END {
            if (hits < 1) exit 3
        }
    '
}

# Rewrite one file through a filter, staged next to it so the final `mv` is a
# same-filesystem rename and a failed filter leaves the file untouched.
apply_filter() {
    local file=$1 staged
    shift
    staged=$(mktemp "$file.XXXXXX")
    if ! "$@" <"$file" >"$staged"; then
        rm -f "$staged"
        return 1
    fi
    chmod 644 "$staged"
    mv "$staged" "$file"
}

# ---------------------------------------------------------------------------
# The snippet scan, shared by --check.
# ---------------------------------------------------------------------------

# The wide net: anything that looks like a spate install snippet, anywhere on
# a line, any number of version components. Deliberately wider than the
# rewriters, so a snippet they cannot reach is found rather than skipped.
SNIPPET_ERE='spate(-[a-z0-9]+)? *= *("[0-9]+\.[0-9]+(\.[0-9]+)?"|\{[^}]*version *= *"[0-9]+\.[0-9]+(\.[0-9]+)?")'

# 0 when the line is one the rewriters reach: line-anchored, two-component
# version. A hit that fails this is a snippet the release would silently skip.
line_is_rewritable() {
    local line=$1
    [[ "$line" =~ ^[[:space:]]*spate[a-z0-9-]*\ *=\ *\"[0-9]+\.[0-9]+\" ]] && return 0
    [[ "$line" =~ ^[[:space:]]*spate[a-z0-9-]*\ *=\ *\{[^}]*version\ *=\ *\"[0-9]+\.[0-9]+\" ]] && return 0
    return 1
}

# The version string inside a snippet line.
snippet_version() {
    printf '%s\n' "$1" | sed -E 's/.*"([0-9]+\.[0-9]+(\.[0-9]+)?)".*/\1/'
}

known_snippet_file() {
    local file=$1 known
    while IFS= read -r known; do
        [ "$file" = "$known" ] && return 0
    done <<<"$SNIPPET_FILES"
    return 1
}

# ---------------------------------------------------------------------------
# --check: every literal version agrees with the workspace.
# ---------------------------------------------------------------------------
check_mode() {
    local cur xy file line lineno content problems=0 hits
    cur=$(workspace_version)
    xy=$(minor_of "$cur")

    # The nine pins all carry `=<workspace version>`. Counted as a mismatch
    # scan rather than a fixed count: a tenth crate must not pass by being
    # left out of a number.
    hits=$(grep -c '^spate-[a-z0-9-]* = { version = "=' "$manifest") || true
    [ "$hits" -ge 1 ] || fail "$manifest carries no spate-* pins; the release has nothing to rewrite"
    while IFS= read -r line; do
        case "$line" in
        *"\"=$cur\""*) ;;
        *)
            echo "release-version.sh: $manifest pin does not match the workspace version $cur:" >&2
            echo "  $line" >&2
            problems=$((problems + 1))
            ;;
        esac
    done < <(grep '^spate-[a-z0-9-]* = { version = "=' "$manifest")

    # Every snippet-shaped line in the tracked tree: it must sit in a file the
    # release rewrites, in a shape the rewriters reach, at the current version.
    while IFS= read -r file; do
        scan_excluded "$file" && continue
        while IFS=: read -r lineno content; do
            [ -n "$lineno" ] || continue
            if ! known_snippet_file "$file"; then
                echo "release-version.sh: $file:$lineno looks like an install snippet, but the file is not" >&2
                echo "  in the rewritten set. Add it to SNIPPET_FILES in scripts/release-version.sh, or" >&2
                echo "  reword the line so it carries no literal version." >&2
                problems=$((problems + 1))
                continue
            fi
            if ! line_is_rewritable "$content"; then
                echo "release-version.sh: $file:$lineno is a snippet the rewriters cannot reach:" >&2
                echo "  $content" >&2
                echo "  A snippet is line-anchored and carries a two-component version (\"$xy\")." >&2
                problems=$((problems + 1))
                continue
            fi
            if [ "$(snippet_version "$content")" != "$xy" ]; then
                echo "release-version.sh: $file:$lineno carries $(snippet_version "$content"), the workspace is at $xy:" >&2
                echo "  $content" >&2
                problems=$((problems + 1))
            fi
        done < <(grep -nE "$SNIPPET_ERE" "$file" || true)
    done < <(scan_files)

    # The completeness half: a known file with no snippet means a rewrite (or
    # an edit) destroyed one, and the scan above had nothing to judge.
    while IFS= read -r file; do
        if ! grep -qE "$SNIPPET_ERE" "$file"; then
            echo "release-version.sh: $file carries no install snippet, but SNIPPET_FILES says it does" >&2
            problems=$((problems + 1))
        fi
    done <<<"$SNIPPET_FILES"

    [ "$problems" -eq 0 ] || fail "$problems version literal(s) disagree; see above"
    echo "release-version.sh: every version literal agrees with $cur"
}

# ---------------------------------------------------------------------------
# --derive: the next version, from history since the last tag.
# ---------------------------------------------------------------------------

# 0 when a commit subject carries the conventional breaking marker.
subject_is_breaking() {
    [[ "$1" =~ ^[a-zA-Z]+(\([^\)]*\))?!: ]]
}

derive_mode() {
    local last cur subjects bodies kind=patch reason="no breaking change since"
    # `v[0-9]*` and not `v*`: the repository also carries non-release tags,
    # and a bare glob would pick them.
    last=$(git tag --list 'v[0-9]*' --sort=-v:refname | head -n 1)
    [ -n "$last" ] || fail "no vX.Y.Z tag; the first release of a repository is cut by hand"

    cur=$(workspace_version)
    [ "v$cur" = "$last" ] || fail "Cargo.toml is at $cur but the last tag is $last: a release is
  half-finished. Finish it (re-run the failed publish jobs on its run) before
  deriving the next version."

    [ "$(git rev-list --no-merges --count "$last..HEAD")" -gt 0 ] ||
        fail "no commits since $last; nothing to release"

    # Pre-1.0, a breaking change is a minor bump: cargo treats 0.x minors as
    # incompatible. The marker is the subject `!`; the `BREAKING CHANGE:`
    # footer is accepted as well because conventional commits allows it. An
    # MSRV move is a minor under the same rule (the Cargo book calls raising
    # `rust-version` a minor incompatibility), and it is read from the diff
    # rather than trusted to carry a marker.
    subjects=$(git log --no-merges --format=%s "$last..HEAD")
    bodies=$(git log --no-merges --format=%B "$last..HEAD")
    while IFS= read -r line; do
        [ -n "$line" ] || continue
        if subject_is_breaking "$line"; then
            kind=minor
            reason="'$line' carries the breaking marker"
            break
        fi
    done <<<"$subjects"
    if [ "$kind" = "patch" ] && grep -Eq '^BREAKING[- ]CHANGE:' <<<"$bodies"; then
        kind=minor
        reason="a commit body carries a BREAKING CHANGE footer"
    fi
    if [ "$kind" = "patch" ] && git diff "$last" HEAD -- "$manifest" >"$scratch/manifest.diff" &&
        grep -Eq '^\+rust-version' "$scratch/manifest.diff"; then
        kind=minor
        reason="rust-version moved since $last"
    fi

    echo "release-version.sh: $kind bump ($reason $last)" >&2
    next_version "$cur" "$kind"
}

# ---------------------------------------------------------------------------
# --bump: rewrite everything, refresh the lockfile, then check.
# ---------------------------------------------------------------------------
bump_mode() {
    local new=$1 cur xy file
    is_version "$new" || fail "'$new' is not X.Y.Z"
    cur=$(workspace_version)
    [ "$new" != "$cur" ] || fail "the workspace is already at $cur"
    version_lt "$cur" "$new" || fail "$new is behind the workspace version $cur, and a published
  version can never be reused"

    apply_filter "$manifest" rewrite_manifest "$new" ||
        fail "the $manifest rewrite failed; the file is untouched"

    xy=$(minor_of "$new")
    while IFS= read -r file; do
        [ -f "$file" ] || fail "SNIPPET_FILES names '$file', which does not exist"
        apply_filter "$file" rewrite_snippets "$xy" ||
            fail "no install snippet found in $file; SNIPPET_FILES says there is one"
    done <<<"$SNIPPET_FILES"

    # The lockfile follows the manifest in the same change. `--workspace`
    # touches only the members' own entries.
    cargo update --workspace

    check_mode
    echo "release-version.sh: bumped $cur -> $new"
}

# ---------------------------------------------------------------------------
# Self-test. Runs inline on every invocation as well as under --self-test.
# ---------------------------------------------------------------------------
scratch=""
cleanup() {
    [ -n "$scratch" ] && rm -rf "$scratch"
    return 0
}
trap cleanup EXIT
scratch=$(mktemp -d)

self_test() {
    local failures=0 got line verdict

    # The manifest rewriter, against every pin shape the file carries plus the
    # lines it must not touch: rust-version, the versionless spate-bench
    # entry, and a third-party crate whose version collides with ours.
    cat >"$scratch/manifest.in" <<'FIXTURE'
[workspace.package]
version = "0.2.0"
rust-version = "1.94"

[workspace.dependencies]
spate-core = { version = "=0.2.0", path = "crates/spate-core" }
# Defaults off keeps async-nats out of a memory-only embedding.
spate-coordination = { version = "=0.2.0", path = "crates/spate-coordination", default-features = false }
spate-s3 = { version = "=0.2.0", path = "crates/spate-s3" }
spate-bench = { path = "bench" }
foldhash = "0.2"
FIXTURE
    cat >"$scratch/manifest.want" <<'FIXTURE'
[workspace.package]
version = "0.3.0"
rust-version = "1.94"

[workspace.dependencies]
spate-core = { version = "=0.3.0", path = "crates/spate-core" }
# Defaults off keeps async-nats out of a memory-only embedding.
spate-coordination = { version = "=0.3.0", path = "crates/spate-coordination", default-features = false }
spate-s3 = { version = "=0.3.0", path = "crates/spate-s3" }
spate-bench = { path = "bench" }
foldhash = "0.2"
FIXTURE
    if ! rewrite_manifest "0.3.0" <"$scratch/manifest.in" >"$scratch/manifest.got" 2>/dev/null; then
        echo "release-version.sh: the manifest rewriter refused the fixture" >&2
        failures=$((failures + 1))
    elif ! diff -u "$scratch/manifest.want" "$scratch/manifest.got" >&2; then
        echo "release-version.sh: the manifest rewrite drifted from the expected output above" >&2
        failures=$((failures + 1))
    fi

    # A manifest with two version lines must be refused, not half-rewritten.
    printf 'version = "0.2.0"\nversion = "0.2.0"\nspate-x = { version = "=0.2.0", path = "x" }\n' \
        >"$scratch/twice.in"
    if rewrite_manifest "0.3.0" <"$scratch/twice.in" >/dev/null 2>&1; then
        echo "release-version.sh: two workspace version lines were accepted" >&2
        failures=$((failures + 1))
    fi

    # The snippet rewriter, against all five real shapes plus the lines beside
    # them that must stay untouched: a non-spate dependency in the same fence,
    # and a trailing comment that has to survive byte-for-byte.
    cat >"$scratch/snippet.in" <<'FIXTURE'
spate = { version = "0.2", features = ["kafka", "clickhouse", "avro"] }
spate = { version = "0.2", features = ["full"] }
serde = { version = "1", features = ["derive"] }
spate-test = "0.2"
spate = { version = "0.2", features = ["kafka-tls"] }   # implies "kafka"
FIXTURE
    cat >"$scratch/snippet.want" <<'FIXTURE'
spate = { version = "0.3", features = ["kafka", "clickhouse", "avro"] }
spate = { version = "0.3", features = ["full"] }
serde = { version = "1", features = ["derive"] }
spate-test = "0.3"
spate = { version = "0.3", features = ["kafka-tls"] }   # implies "kafka"
FIXTURE
    if ! rewrite_snippets "0.3" <"$scratch/snippet.in" >"$scratch/snippet.got"; then
        echo "release-version.sh: the snippet rewriter refused the fixture" >&2
        failures=$((failures + 1))
    elif ! diff -u "$scratch/snippet.want" "$scratch/snippet.got" >&2; then
        echo "release-version.sh: the snippet rewrite drifted from the expected output above" >&2
        failures=$((failures + 1))
    fi

    # A file with no snippet exits 3, so --bump can name the file rather than
    # silently rewriting nothing.
    printf 'no snippet here\n' >"$scratch/empty.in"
    if rewrite_snippets "0.3" <"$scratch/empty.in" >/dev/null; then
        echo "release-version.sh: a snippet-free file passed the rewriter" >&2
        failures=$((failures + 1))
    fi

    # The scan classifier: which lines are snippets, and which of those the
    # rewriters reach. `wide` hits SNIPPET_ERE; `rewritable` passes
    # line_is_rewritable; `clean` matches neither.
    while IFS='|' read -r line verdict; do
        case "$line" in '' | '#'*) continue ;; esac
        if printf '%s\n' "$line" | grep -qE "$SNIPPET_ERE"; then
            if line_is_rewritable "$line"; then got=rewritable; else got=wide; fi
        else
            got=clean
        fi
        if [ "$got" != "$verdict" ]; then
            echo "release-version.sh: scan: '$line' -> $got, expected $verdict" >&2
            failures=$((failures + 1))
        fi
    done <<'TABLE'
# --- the five real snippets ---
spate = { version = "0.2", features = ["kafka", "clickhouse", "avro"] }|rewritable
spate = { version = "0.2", features = ["full"] }|rewritable
spate-test = "0.2"|rewritable
spate = { version = "0.2", features = ["kafka-tls"] }   # implies "kafka"|rewritable
# --- snippets the rewriters cannot reach: caught, not skipped ---
Add `spate = "0.2"` to your manifest.|wide
spate = "0.2.0"|wide
spate-test = { version = "0.2.0" }|wide
# --- not snippets at all ---
foldhash = "0.2"|clean
serde = { version = "1", features = ["derive"] }|clean
spate-core = { version = "=0.2.0", path = "crates/spate-core" }|clean
| `retry.jitter` | float | `0.2` ||clean
chore: release v0.2.0|clean
TABLE

    # The breaking-marker classifier behind --derive.
    while IFS='|' read -r line verdict; do
        case "$line" in '' | '#'*) continue ;; esac
        if subject_is_breaking "$line"; then got=breaking; else got=plain; fi
        if [ "$got" != "$verdict" ]; then
            echo "release-version.sh: derive: '$line' -> $got, expected $verdict" >&2
            failures=$((failures + 1))
        fi
    done <<'TABLE'
feat(spate-core)!: seal the framework configuration sections|breaking
refactor!: rename the framework to spate|breaking
docs(workspace)!: migrate CLAUDE.md to AGENTS.md|breaking
feat(spate-core): a windowed operator|plain
fix(spate-kafka): stop dropping offsets on revoke|plain
chore: release v0.2.0|plain
revert(spate-core): back out the windowed operator!|plain
feat(spate-core): explain what ! means in a subject|plain
TABLE

    # Version arithmetic and ordering.
    [ "$(next_version 0.2.0 minor)" = "0.3.0" ] || { echo "release-version.sh: minor of 0.2.0 is not 0.3.0" >&2; failures=$((failures + 1)); }
    [ "$(next_version 0.2.9 patch)" = "0.2.10" ] || { echo "release-version.sh: patch of 0.2.9 is not 0.2.10" >&2; failures=$((failures + 1)); }
    [ "$(minor_of 0.10.3)" = "0.10" ] || { echo "release-version.sh: minor_of 0.10.3 is not 0.10" >&2; failures=$((failures + 1)); }
    version_lt "0.2.0" "0.10.0" || { echo "release-version.sh: 0.2.0 is not below 0.10.0 (a string compare?)" >&2; failures=$((failures + 1)); }
    if version_lt "0.3.0" "0.3.0"; then
        echo "release-version.sh: 0.3.0 sorts below itself" >&2
        failures=$((failures + 1))
    fi
    if is_version "0.2"; then
        echo "release-version.sh: 'is_version' accepted a two-component version" >&2
        failures=$((failures + 1))
    fi

    [ "$failures" -eq 0 ] || fail "$failures self-test failure(s). This script is wrong, not your change"
}

# ---------------------------------------------------------------------------
# Dispatch.
# ---------------------------------------------------------------------------
self_test

case "${1:-}" in
--bump)
    [ $# -eq 2 ] || fail "usage: --bump <X.Y.Z>"
    bump_mode "$2"
    ;;
--check)
    check_mode
    ;;
--derive)
    derive_mode
    ;;
--self-test)
    echo "release-version.sh: self-test passed"
    ;;
*)
    fail "usage: --bump <X.Y.Z> | --check | --derive | --self-test"
    ;;
esac
