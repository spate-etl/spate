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
#   ./scripts/release-version.sh --bump <X.Y.Z>            # rewrite everything, then --check
#   ./scripts/release-version.sh --check                   # the gate: every literal agrees
#   ./scripts/release-version.sh --check-publish-metadata  # what a dry run cannot see
#   ./scripts/release-version.sh --derive                  # print the next version from history
#   ./scripts/release-version.sh --self-test               # the rewriters, alone
#
# `--check` and `--self-test` need git and no toolchain. `--bump` runs
# `cargo update --workspace` after the rewrite so Cargo.lock follows the
# manifest in the same change. `--derive` reads tags and commits, so it needs
# the full history, not a shallow clone. `--check-publish-metadata` needs
# cargo and jq: it checks the manifest fields crates.io rejects at upload
# while `cargo publish --dry-run` warns and exits 0 (cargo issue 14249).
#
# Runs on `bash` 3.2 and later: no associative arrays, no `mapfile`, and
# every array expansion guarded. The scan loops read from files rather than
# process substitutions, because 3.2 leaks a pipe descriptor per substitution
# and a tree-sized scan runs out.
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
    git ls-files -- '*.md' '*.mdx' '*.toml' '*.rs' '*.yml' '*.yaml' '*.ts' '*.tsx' '*.json'
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

# 0 when $1 < $2, component-wise. Accepts `X.Y` by treating the missing
# component as 0, so an MSRV pair compares directly.
version_lt() {
    local a1 a2 a3 b1 b2 b3
    IFS=. read -r a1 a2 a3 <<<"$1"
    IFS=. read -r b1 b2 b3 <<<"$2"
    a3=${a3:-0}
    b3=${b3:-0}
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
# `spate-*` pin. The pin pattern requires `version` with an `=` requirement,
# so the versionless `spate-bench = { path = "bench" }` entry is out of its
# reach. A line that names a spate crate and carries an `"=` requirement but
# does not match the rewrite pattern is refused: an unreachable pin must be
# reshaped, never silently left at the old version.
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
        /^spate-[a-z0-9-]+[[:space:]]*=[[:space:]]*\{[[:space:]]*version[[:space:]]*=[[:space:]]*"=/ {
            sub(/"=[^"]*"/, "\"=" new "\"")
            pins++
            print
            next
        }
        /^spate-[a-z0-9-]+[[:space:]]*=/ && /"=/ {
            print "release-version.sh: a pin the rewriter cannot reach: " $0 > "/dev/stderr"
            exit 1
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
        /^[[:space:]]*spate(-[a-z0-9]+)*[[:space:]]*=[[:space:]]*"[0-9]+\.[0-9]+"/ {
            sub(/"[0-9]+\.[0-9]+"/, "\"" xy "\"")
            hits++
        }
        /^[[:space:]]*spate(-[a-z0-9]+)*[[:space:]]*=[[:space:]]*\{[^}]*version[[:space:]]*=[[:space:]]*"[0-9]+\.[0-9]+"/ {
            sub(/version[[:space:]]*=[[:space:]]*"[0-9]+\.[0-9]+"/, "version = \"" xy "\"")
            hits++
        }
        { print }
        END {
            if (hits < 1) exit 3
        }
    '
}

# ---------------------------------------------------------------------------
# The snippet scan, shared by --check.
# ---------------------------------------------------------------------------

# The wide net: anything that looks like a spate install snippet, anywhere on
# a line, any number of version components. Deliberately wider than the
# rewriters, so a snippet they cannot reach is found rather than skipped. The
# unclosed-brace arm catches a snippet wrapped onto several lines, which no
# line-based rewriter can move. The leading group keeps `myspate` from
# matching without relying on grep's word-boundary extensions.
V2='[0-9]+\.[0-9]+(\.[0-9]+)?'
SNIPPET_ERE='(^|[^a-zA-Z0-9_-])spate(-[a-z0-9]+)*[[:space:]]*=[[:space:]]*("'$V2'"|\{[^}]*version[[:space:]]*=[[:space:]]*"'$V2'"|\{[^}]*$)'

# 0 when the line is one the rewriters reach: line-anchored, two-component
# version. A hit that fails this is a snippet the release would silently skip.
line_is_rewritable() {
    local line=$1
    [[ "$line" =~ ^[[:space:]]*spate(-[a-z0-9]+)*[[:space:]]*=[[:space:]]*\"[0-9]+\.[0-9]+\" ]] && return 0
    [[ "$line" =~ ^[[:space:]]*spate(-[a-z0-9]+)*[[:space:]]*=[[:space:]]*\{[^}]*version[[:space:]]*=[[:space:]]*\"[0-9]+\.[0-9]+\" ]] && return 0
    return 1
}

# The version string inside a rewritable snippet line. Anchored to the
# snippet's own construct: a trailing comment may carry other quoted
# version-shaped strings, and a greedy scan would return the last of them.
snippet_version() {
    printf '%s\n' "$1" | sed -E \
        's/^[[:space:]]*spate(-[a-z0-9]+)*[[:space:]]*=[[:space:]]*(\{[^}]*version[[:space:]]*=[[:space:]]*)?"([0-9]+\.[0-9]+(\.[0-9]+)?)".*$/\3/'
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
    local cur xy file line lineno content problems=0 rc
    cur=$(workspace_version)
    xy=$(minor_of "$cur")

    # Every pin line carries `=<workspace version>`. Scanned loose, so a pin
    # shaped in a way the rewriter cannot reach is reported here rather than
    # left unchecked: a new crate must not pass by being left out of a
    # number, and neither may a reformatted one.
    grep -E '^spate-[a-z0-9-]+[[:space:]]*=' "$manifest" >"$scratch/pins" || true
    [ -s "$scratch/pins" ] || fail "$manifest carries no spate-* pins; the release has nothing to rewrite"
    while IFS= read -r line; do
        case "$line" in
        *'"='*)
            case "$line" in
            *"\"=$cur\""*) ;;
            *)
                echo "release-version.sh: $manifest pin does not match the workspace version $cur:" >&2
                echo "  $line" >&2
                problems=$((problems + 1))
                ;;
            esac
            ;;
        esac
    done <"$scratch/pins"

    # Every snippet-shaped line in the tracked tree: it must sit in a file the
    # release rewrites, in a shape the rewriters reach, at the current version.
    # Plain files, not process substitutions: a failing `git ls-files` must
    # abort rather than hand the loop an empty scan that passes.
    scan_files >"$scratch/scan"
    while IFS= read -r file; do
        scan_excluded "$file" && continue
        rc=0
        grep -nE "$SNIPPET_ERE" "$file" >"$scratch/hits" || rc=$?
        [ "$rc" -le 1 ] || fail "grep failed on $file ($rc); the scan cannot vouch for it"
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
                echo "  A snippet is one line and carries a two-component version (\"$xy\")." >&2
                problems=$((problems + 1))
                continue
            fi
            if [ "$(snippet_version "$content")" != "$xy" ]; then
                echo "release-version.sh: $file:$lineno carries $(snippet_version "$content"), the workspace is at $xy:" >&2
                echo "  $content" >&2
                problems=$((problems + 1))
            fi
        done <"$scratch/hits"
    done <"$scratch/scan"

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
# --check-publish-metadata: the fields the registry rejects at upload.
# ---------------------------------------------------------------------------
# `cargo publish --dry-run` packages a crate with no `description` or
# `license` with a warning and exit 0, while the real upload rejects it after
# publishing whatever sorted before it (cargo issue 14249, closed as not
# planned).
check_publish_metadata() {
    local bad
    bad=$(cargo metadata --no-deps --format-version 1 |
        jq -r '.packages[] | select(.publish != []) |
               select((.description // "") == "" or (.license // "") == "") | .name')
    [ -z "$bad" ] || fail "missing description or license in: $bad"
    echo "release-version.sh: every publishable crate carries the metadata the upload requires"
}

# ---------------------------------------------------------------------------
# --derive: the next version, from history since the last tag.
# ---------------------------------------------------------------------------

# The conventional breaking marker, matched against every line of every
# message body since the last tag: the squash subject is the pull request
# title, and a squash body carries its constituent subjects, so a marker
# anywhere in the log means a breaking change shipped.
MARKER_ERE='^[a-zA-Z]+(\([^)]*\))?!:'

derive_mode() {
    local last cur kind=patch reason marker msrv_old msrv_new
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
    # incompatible. An MSRV raise is a minor under the same rule (the Cargo
    # book calls raising `rust-version` a minor incompatibility), and it is
    # judged by comparing the two values: an annotation or a lowering is not
    # a raise.
    reason="no breaking change since $last"
    git log --no-merges --format=%B "$last..HEAD" >"$scratch/log"
    marker=$(grep -E -m 1 "$MARKER_ERE" "$scratch/log") || true
    if [ -n "$marker" ]; then
        kind=minor
        reason="'$marker' carries the breaking marker"
    fi
    if [ "$kind" = "patch" ]; then
        msrv_old=$(git show "$last:Cargo.toml" | sed -n 's/^rust-version = "\(.*\)".*$/\1/p')
        msrv_new=$(sed -n 's/^rust-version = "\(.*\)".*$/\1/p' "$manifest")
        if [ -n "$msrv_old" ] && [ -n "$msrv_new" ] && version_lt "$msrv_old" "$msrv_new"; then
            kind=minor
            reason="rust-version rose from $msrv_old to $msrv_new since $last"
        fi
    fi

    echo "release-version.sh: $kind bump ($reason)" >&2
    next_version "$cur" "$kind"
}

# ---------------------------------------------------------------------------
# --bump: rewrite everything, refresh the lockfile, then check.
# ---------------------------------------------------------------------------
# Two phases: every file is filtered into a staged copy first, and the
# staged copies replace the originals only after every filter has succeeded,
# so a snippet a rewriter cannot find leaves the whole tree untouched rather
# than half-bumped. Staged copies sit next to their targets, keeping the
# final step a same-filesystem rename; their paths are recorded so the exit
# trap removes them on any earlier failure.
bump_mode() {
    local new=$1 cur xy file staged n=0
    is_version "$new" || fail "'$new' is not X.Y.Z"
    cur=$(workspace_version)
    [ "$new" != "$cur" ] || fail "the workspace is already at $cur"
    version_lt "$cur" "$new" || fail "$new is behind the workspace version $cur, and a published
  version can never be reused"

    : >"$scratch/staged-list"
    : >"$scratch/moves"
    staged=$(mktemp "$manifest.XXXXXX")
    printf '%s\n' "$staged" >>"$scratch/staged-list"
    rewrite_manifest "$new" <"$manifest" >"$staged" ||
        fail "the $manifest rewrite failed; nothing was changed"
    printf '%s\t%s\n' "$staged" "$manifest" >>"$scratch/moves"

    xy=$(minor_of "$new")
    while IFS= read -r file; do
        [ -f "$file" ] || fail "SNIPPET_FILES names '$file', which does not exist"
        staged=$(mktemp "$file.XXXXXX")
        printf '%s\n' "$staged" >>"$scratch/staged-list"
        rewrite_snippets "$xy" <"$file" >"$staged" ||
            fail "no install snippet found in $file; SNIPPET_FILES says there is one.
  Nothing was changed."
        printf '%s\t%s\n' "$staged" "$file" >>"$scratch/moves"
    done <<<"$SNIPPET_FILES"

    while IFS=$'\t' read -r staged file; do
        chmod 644 "$staged"
        mv "$staged" "$file"
        n=$((n + 1))
    done <"$scratch/moves"

    # The lockfile follows the manifest in the same change. `--workspace`
    # touches only the members' own entries.
    cargo update --workspace

    check_mode
    echo "release-version.sh: bumped $cur -> $new across $n files"
}

# ---------------------------------------------------------------------------
# Self-test. Runs inline on every invocation as well as under --self-test.
# ---------------------------------------------------------------------------
scratch=""
cleanup() {
    if [ -n "$scratch" ]; then
        if [ -f "$scratch/staged-list" ]; then
            while IFS= read -r staged; do
                [ -n "$staged" ] && rm -f "$staged"
            done <"$scratch/staged-list"
        fi
        rm -rf "$scratch"
    fi
    return 0
}
trap cleanup EXIT
scratch=$(mktemp -d)

self_test() {
    local failures=0 got line verdict

    # The manifest rewriter, against every pin shape the file carries plus the
    # lines it must not touch: rust-version, the versionless spate-bench
    # entry, and a third-party crate whose version collides with ours. The
    # spaceless pin is legal TOML and must move with the others.
    cat >"$scratch/manifest.in" <<'FIXTURE'
[workspace.package]
version = "0.2.0"
rust-version = "1.94"

[workspace.dependencies]
spate-core = { version = "=0.2.0", path = "crates/spate-core" }
# Defaults off keeps async-nats out of a memory-only embedding.
spate-coordination = { version = "=0.2.0", path = "crates/spate-coordination", default-features = false }
spate-s3 = {version = "=0.2.0", path = "crates/spate-s3"}
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
spate-s3 = {version = "=0.3.0", path = "crates/spate-s3"}
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

    # A pin carrying an `=` requirement in a shape the rewrite pattern does
    # not cover must abort the rewrite, never survive at the old version.
    printf 'version = "0.2.0"\nspate-core = { path = "x", version = "=0.2.0" }\n' \
        >"$scratch/odd-pin.in"
    if rewrite_manifest "0.3.0" <"$scratch/odd-pin.in" >/dev/null 2>&1; then
        echo "release-version.sh: a pin with version after path was silently left behind" >&2
        failures=$((failures + 1))
    fi

    # The snippet rewriter, against the real snippet shapes plus the lines
    # beside them that must stay untouched: a non-spate dependency in the
    # same fence, and a trailing comment that has to survive byte-for-byte.
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
    # line_is_rewritable; `clean` matches neither. The wrap-start case is a
    # snippet reformatted onto several lines, which must be caught rather
    # than skipped, and `myspate` must not match at all.
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
# --- the real snippets ---
spate = { version = "0.2", features = ["kafka", "clickhouse", "avro"] }|rewritable
spate = { version = "0.2", features = ["full"] }|rewritable
spate-test = "0.2"|rewritable
spate = { version = "0.2", features = ["kafka-tls"] }   # implies "kafka"|rewritable
spate-object-store = "0.2"|rewritable
# --- snippets the rewriters cannot reach: caught, not skipped ---
Add `spate = "0.2"` to your manifest.|wide
spate = "0.2.0"|wide
spate-test = { version = "0.2.0" }|wide
spate = {|wide
spate-kafka = { features = ["tls"],|wide
# --- not snippets at all ---
foldhash = "0.2"|clean
myspate = "0.2"|clean
serde = { version = "1", features = ["derive"] }|clean
spate-core = { version = "=0.2.0", path = "crates/spate-core" }|clean
the default for retry.jitter is 0.2, a plain number in prose|clean
chore: release v0.2.0|clean
TABLE

    # The version a snippet line carries, read from the snippet's own
    # construct and not from a trailing comment.
    got=$(snippet_version 'spate = { version = "0.2", features = ["kafka-tls"] }   # implies "kafka", added in "0.3"')
    if [ "$got" != "0.2" ]; then
        echo "release-version.sh: snippet_version read '$got' past the construct, expected 0.2" >&2
        failures=$((failures + 1))
    fi
    got=$(snippet_version 'spate-test = "0.2"')
    if [ "$got" != "0.2" ]; then
        echo "release-version.sh: snippet_version read '$got' from a bare snippet, expected 0.2" >&2
        failures=$((failures + 1))
    fi

    # The breaking-marker pattern behind --derive, driven line by line the
    # way the log scan sees a squash body.
    while IFS='|' read -r line verdict; do
        case "$line" in '' | '#'*) continue ;; esac
        if printf '%s\n' "$line" | grep -qE "$MARKER_ERE"; then got=breaking; else got=plain; fi
        if [ "$got" != "$verdict" ]; then
            echo "release-version.sh: derive: '$line' -> $got, expected $verdict" >&2
            failures=$((failures + 1))
        fi
    done <<'TABLE'
feat(spate-core)!: seal the framework configuration sections|breaking
refactor!: rename the framework to spate|breaking
docs(workspace)!: migrate CLAUDE.md to AGENTS.md|breaking
* feat(spate-core)!: a constituent subject inside a squash body|plain
feat(spate-core): a windowed operator|plain
chore: release v0.2.0|plain
revert(spate-core): back out the windowed operator!|plain
TABLE
    # The constituent-subject case above is `plain` per line because squash
    # bodies prefix list items; the log scan matches it through the
    # unprefixed copy git keeps on its own line. Hold that shape too:
    printf 'Rework the sink pool (#412)\n\nfeat(spate-core)!: drop flush from the trait\n' >"$scratch/body"
    if ! grep -qE "$MARKER_ERE" "$scratch/body"; then
        echo "release-version.sh: a marker on its own body line was not found" >&2
        failures=$((failures + 1))
    fi

    # Version arithmetic and ordering, including the X.Y form MSRV uses.
    [ "$(next_version 0.2.0 minor)" = "0.3.0" ] || { echo "release-version.sh: minor of 0.2.0 is not 0.3.0" >&2; failures=$((failures + 1)); }
    [ "$(next_version 0.2.9 patch)" = "0.2.10" ] || { echo "release-version.sh: patch of 0.2.9 is not 0.2.10" >&2; failures=$((failures + 1)); }
    [ "$(minor_of 0.10.3)" = "0.10" ] || { echo "release-version.sh: minor_of 0.10.3 is not 0.10" >&2; failures=$((failures + 1)); }
    version_lt "0.2.0" "0.10.0" || { echo "release-version.sh: 0.2.0 is not below 0.10.0 (a string compare?)" >&2; failures=$((failures + 1)); }
    version_lt "1.94" "1.95" || { echo "release-version.sh: 1.94 is not below 1.95 in the X.Y form" >&2; failures=$((failures + 1)); }
    if version_lt "1.94" "1.94"; then
        echo "release-version.sh: 1.94 sorts below itself" >&2
        failures=$((failures + 1))
    fi
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
--check-publish-metadata)
    check_publish_metadata
    ;;
--derive)
    derive_mode
    ;;
--self-test)
    echo "release-version.sh: self-test passed"
    ;;
*)
    fail "usage: --bump <X.Y.Z> | --check | --check-publish-metadata | --derive | --self-test"
    ;;
esac
