#!/usr/bin/env bash
#
# The repository-pointer gate: every `file=`/`region=` fence on a documentation
# page names a source and a region that exist, every `repo:` link names a path
# that exists, and every anchor marker under `crates/` is well formed.
#
# The remark plugins throw on the same things during the site build, which
# caches MDX modules and can serve a cached page against a source that has since
# moved. This script reads both sides off disk on every run.
#
# The `repo:` scan reads text, not a syntax tree, so it cannot tell a link from
# inline code quoting one. Documentation *of* the form belongs in a fence.
#
# --check holds the mechanical half only: that a page's pointer resolves.
# Whether the region is the right code for the paragraph around it is review's
# job.
#
# Usage:
#   ./scripts/transclude.sh --check      # the gate
#   ./scripts/transclude.sh --sources    # every source a page transcludes from
#   ./scripts/transclude.sh --self-test  # the parsers, alone
#
# Runs on `bash` 3.2 and later: no associative arrays, no `mapfile`, no
# `${var,,}`, and every array expansion guarded under `set -u`.
set -euo pipefail

cd "$(dirname "$0")/.."

docs=docs
# The trees a page may quote from. Anything here is compiled by
# `cargo clippy --workspace --all-targets`. Keep in step with ALLOWED_PREFIXES
# in website/src/remark/transclude.ts.
allowed_prefix=crates/

failures=0

fail_at() { # file, line, message
    echo "transclude.sh: $1:$2: $3" >&2
    failures=$((failures + 1))
}

# The value of a `key=` attribute in a fence info string, or nothing.
# Handles `k=v`, `k="v"` and `k='v'`.
meta_value() { # info, key
    local info=$1 key=$2 rest
    case " $info" in
    *" $key="*) ;;
    *) return 1 ;;
    esac
    rest=${info#*"$key="}
    case "$rest" in
    '"'*)
        rest=${rest#\"}
        printf '%s' "${rest%%\"*}"
        ;;
    "'"*)
        rest=${rest#\'}
        printf '%s' "${rest%%\'*}"
        ;;
    *) printf '%s' "${rest%% *}" ;;
    esac
}

# Is this line a fence delimiter? Echoes the run of markers if so.
#
# Only column-zero fences are recognized.
fence_marker() { # line
    case "$1" in
    '```'*) printf '```' ;;
    '~~~'*) printf '~~~' ;;
    *) return 1 ;;
    esac
}

# Every `repo:` link target on one line of prose, checked against the tree.
#
# A Markdown link destination cannot contain unescaped whitespace, so `](repo:`
# and its closing parenthesis are always on the same line however the paragraph
# wraps. Mirrors resolveTarget() in website/src/remark/repoLinks.ts.
check_repo_links() { # page, lineno, line
    local page=$1 lineno=$2 rest=$3 target
    while :; do
        case "$rest" in
        *'](repo:'*) ;;
        *) return 0 ;;
        esac
        rest=${rest#*'](repo:'}
        target=${rest%%')'*}
        case "$target" in
        '' | *[[:space:]]*)
            fail_at "$page" "$lineno" "\`repo:\` link has no closing parenthesis"
            return 0
            ;;
        /*)
            fail_at "$page" "$lineno" \
                "repo:$target must be a path relative to the repository root"
            ;;
        *..*)
            fail_at "$page" "$lineno" "repo:$target must not contain a \`..\` segment"
            ;;
        *'#'*)
            fail_at "$page" "$lineno" \
                "repo:$target carries a fragment; a link addresses a file, not lines within it"
            ;;
        *)
            # -e, not -f: a link may name a crate directory rather than a file.
            [ -e "$target" ] || fail_at "$page" "$lineno" "repo:$target does not exist"
            ;;
        esac
    done
}

# Every region name defined in a file, one per line, or a diagnostic on stderr
# and a non-zero status if the marker set is malformed.
#
# `ANCHOR_END` contains `ANCHOR`, so the end pattern is tested FIRST. Mirrors
# indexRegions() in website/src/remark/transclude.ts; the two must agree, or the
# build and the gate disagree about whether a file is well formed.
scan_markers() { # file
    # `read_from` is the redirect's variable and `file` the one that appears in
    # diagnostics: two names for one path so shellcheck does not read
    # `fail_at "$file"` as writing to the file the loop is reading (SC2094).
    local file=$1 read_from=$1 line lineno=0 name starts='' ends='' bad=0
    while IFS= read -r line || [ -n "$line" ]; do
        lineno=$((lineno + 1))
        case "$line" in
        *ANCHOR_END:*)
            name=${line#*ANCHOR_END:}
            name=${name## }
            name=${name%% *}
            [ -n "$name" ] || continue
            case "
$ends" in
            *"
$name
"*)
                fail_at "$file" "$lineno" "duplicate \`ANCHOR_END: $name\`"
                bad=1
                continue
                ;;
            esac
            ends="$ends$name
"
            ;;
        *ANCHOR:*)
            name=${line#*ANCHOR:}
            name=${name## }
            name=${name%% *}
            [ -n "$name" ] || continue
            case "
$starts" in
            *"
$name
"*)
                fail_at "$file" "$lineno" "duplicate \`ANCHOR: $name\`"
                bad=1
                continue
                ;;
            esac
            starts="$starts$name
"
            ;;
        esac
    done <"$read_from"

    # Every start needs an end and the reverse. Set arithmetic without
    # associative arrays: walk one list, membership-test against the other.
    local n
    for n in $starts; do
        case "
$ends" in
        *"
$n
"*) ;;
        *)
            fail_at "$file" 0 "\`ANCHOR: $n\` has no matching \`ANCHOR_END: $n\`"
            bad=1
            ;;
        esac
    done
    for n in $ends; do
        case "
$starts" in
        *"
$n
"*) ;;
        *)
            fail_at "$file" 0 "\`ANCHOR_END: $n\` has no matching \`ANCHOR: $n\`"
            bad=1
            ;;
        esac
    done

    printf '%s' "$starts"
    [ "$bad" -eq 0 ]
}

# Does `file` define `region`, with the end after the start?
region_ok() { # file, region
    local file=$1 want=$2 line lineno=0 start=0 end=0 name
    while IFS= read -r line || [ -n "$line" ]; do
        lineno=$((lineno + 1))
        case "$line" in
        *ANCHOR_END:*)
            name=${line#*ANCHOR_END:}
            name=${name## }
            name=${name%% *}
            [ "$name" = "$want" ] && end=$lineno
            ;;
        *ANCHOR:*)
            name=${line#*ANCHOR:}
            name=${name## }
            name=${name%% *}
            [ "$name" = "$want" ] && start=$lineno
            ;;
        esac
    done <"$file"
    [ "$start" -gt 0 ] && [ "$end" -gt "$start" ]
}

# Walk one page's fences, reporting every `file=`/`region=` problem on it.
# Emits `<source>` on stdout for each transclusion found: --sources collects
# those, and --check uses them to spot an orphaned region.
check_page() { # file
    # Two names for one path, as in scan_markers.
    local page=$1 read_from=$1 line lineno=0 marker='' open='' info='' openline=0 body=0
    local src region
    while IFS= read -r line || [ -n "$line" ]; do
        lineno=$((lineno + 1))
        if marker=$(fence_marker "$line"); then
            if [ -z "$open" ]; then
                open=$marker
                info=${line#"$marker"}
                openline=$lineno
                body=0
                continue
            elif [ "$marker" = "$open" ]; then
                # Closing delimiter. Judge the fence we just walked past.
                case " $info" in
                *" file="*)
                    src=$(meta_value "$info" file || true)
                    region=$(meta_value "$info" region || true)
                    if [ "$body" -ne 0 ]; then
                        fail_at "$page" "$openline" \
                            "fence carries \`file=\` and hand-written content"
                    fi
                    if [ -z "$src" ]; then
                        fail_at "$page" "$openline" "\`file=\` has no value"
                    else
                        case "$src" in
                        /* | *..*)
                            fail_at "$page" "$openline" \
                                "file=\"$src\" must be a repository-relative path with no \`..\`"
                            ;;
                        "$allowed_prefix"*)
                            if [ ! -f "$src" ]; then
                                fail_at "$page" "$openline" "file=\"$src\" does not exist"
                            elif [ -n "$region" ] && ! region_ok "$src" "$region"; then
                                fail_at "$page" "$openline" \
                                    "$src defines no region \"$region\""
                            else
                                printf '%s\t%s\n' "$src" "$region"
                            fi
                            ;;
                        *)
                            fail_at "$page" "$openline" \
                                "file=\"$src\" is outside $allowed_prefix"
                            ;;
                        esac
                    fi
                    ;;
                esac
                open=''
                info=''
                continue
            fi
        fi
        if [ -n "$open" ]; then
            body=1
        else
            check_repo_links "$page" "$lineno" "$line"
        fi
    done <"$read_from"
    if [ -n "$open" ]; then
        fail_at "$page" "$openline" "fence opened here is never closed"
    fi
    return 0
}

# Every documentation page, NUL-separated into a temp file. `find` writes to a
# file whose creation is checked, not to a pipeline whose exit status would be
# the reader's.
#
# Scaffolds are excluded by name: `crates/spate-NAME/src/config.rs` in a
# `_template` file is a shape, not a path that resolves.
pages_into() { # destination
    if ! find "$docs" -type f \( -name '*.md' -o -name '*.mdx' \) \
        ! -name '_template.*' -print0 >"$1"; then
        echo "transclude.sh: could not enumerate $docs" >&2
        return 1
    fi
}

# Every file under crates/ that carries a marker.
#
# grep's exit 1 means "no matches", a legitimate state; exit 2 or more is a real
# error. Distinguished explicitly rather than swallowed with `|| true`, which
# would hide a permissions failure.
marked_sources_into() { # destination
    local rc=0
    grep -rlE 'ANCHOR(_END)?:' "$allowed_prefix" >"$1" 2>/dev/null || rc=$?
    if [ "$rc" -gt 1 ]; then
        echo "transclude.sh: grep failed scanning $allowed_prefix (exit $rc)" >&2
        return 1
    fi
    return 0
}

collect() { # writes "<source>\t<region>" lines to stdout
    local list file
    list=$(mktemp)
    # shellcheck disable=SC2064  # expand the path now, not at trap time
    trap "rm -f '$list'" RETURN
    pages_into "$list" || return 1
    while IFS= read -r -d '' file; do
        check_page "$file"
    done <"$list"
}

run_check() {
    local used sources file names name
    used=$(mktemp)
    sources=$(mktemp)
    # shellcheck disable=SC2064
    trap "rm -f '$used' '$sources'" RETURN

    collect >"$used"

    # Every marker set under crates/ is well formed, whether a page uses it or
    # not.
    marked_sources_into "$sources" || return 1
    while IFS= read -r file; do
        [ -n "$file" ] || continue
        scan_markers "$file" >/dev/null || true
    done <"$sources"

    # A region nothing references is the shape a half-finished rename leaves
    # behind, and it is invisible from the page side.
    while IFS= read -r file; do
        [ -n "$file" ] || continue
        names=$(scan_markers "$file" 2>/dev/null || true)
        for name in $names; do
            if ! grep -qF "$(printf '%s\t%s' "$file" "$name")" "$used"; then
                fail_at "$file" 0 "region \"$name\" is defined but no page renders it"
            fi
        done
    done <"$sources"

    if [ "$failures" -ne 0 ]; then
        echo "transclude.sh: $failures problem(s)." >&2
        return 1
    fi
    local n
    n=$(grep -c . "$used" || true)
    echo "transclude.sh: $n transclusion(s); every region resolves."
}

run_sources() {
    collect | cut -f1 | sort -u
}

# ---------------------------------------------------------------------------
# Self-test. Runs before every dispatch: a gate whose own parsers have quietly
# stopped working reports success either way.
# ---------------------------------------------------------------------------
# shellcheck disable=SC2016  # the fixtures are Markdown fences and Rust source
self_test() {
    local tmp rc got saved_failures=$failures
    tmp=$(mktemp -d)
    # shellcheck disable=SC2064
    trap "rm -rf '$tmp'" RETURN
    local st_fail=0
    st() { # description, expected, actual
        if [ "$2" != "$3" ]; then
            echo "transclude.sh: self-test: $1: expected '$2', got '$3'" >&2
            st_fail=1
        fi
    }

    # meta_value: bare, double-quoted, single-quoted, absent.
    st "bare value" "a/b.rs" "$(meta_value 'file=a/b.rs region=x' file)"
    st "quoted value" "two words" "$(meta_value 'title="two words" file=a' title)"
    st "single-quoted" "v" "$(meta_value "k='v' j=w" k)"
    st "region after file" "x" "$(meta_value 'file=a/b.rs region=x' region)"
    got=$(meta_value 'file=a' region || printf 'ABSENT')
    st "absent key" "ABSENT" "$got"
    # `profile=` must not satisfy a query for `file=`.
    got=$(meta_value 'profile=a' file || printf 'ABSENT')
    st "suffix key is not a match" "ABSENT" "$got"

    # fence_marker
    st "backtick fence" '```' "$(fence_marker '```rust file=a')"
    st "tilde fence" '~~~' "$(fence_marker '~~~rust')"
    got=$(fence_marker 'not a fence' || printf 'NONE')
    st "prose is not a fence" "NONE" "$got"

    # scan_markers: the ANCHOR_END/ANCHOR prefix trap.
    printf '// ANCHOR: alpha\nbody\n// ANCHOR_END: alpha\n' >"$tmp/ok.rs"
    got=$(scan_markers "$tmp/ok.rs" 2>/dev/null | tr '\n' ' ')
    st "well-formed pair" "alpha " "$got"

    printf '// ANCHOR_END: solo\n' >"$tmp/orphan.rs"
    failures=0
    scan_markers "$tmp/orphan.rs" >/dev/null 2>&1 && rc=0 || rc=1
    st "orphan end rejected" "1" "$rc"

    printf '// ANCHOR: a\n// ANCHOR: a\n// ANCHOR_END: a\n' >"$tmp/dup.rs"
    failures=0
    scan_markers "$tmp/dup.rs" >/dev/null 2>&1 && rc=0 || rc=1
    st "duplicate start rejected" "1" "$rc"

    printf '// ANCHOR_END: a\n// ANCHOR: a\n' >"$tmp/inverted.rs"
    st "inverted pair is not a region" "1" "$(region_ok "$tmp/inverted.rs" a && echo 0 || echo 1)"
    st "well-formed region accepted" "0" "$(region_ok "$tmp/ok.rs" alpha && echo 0 || echo 1)"
    st "unknown region rejected" "1" "$(region_ok "$tmp/ok.rs" nope && echo 0 || echo 1)"

    # check_page: a `file=` inside a fence BODY is content, not an attribute.
    printf '# T\n\n````text\n```rust file=crates/x.rs\n```\n````\n' >"$tmp/nested.md"
    failures=0
    check_page "$tmp/nested.md" >/dev/null
    st "file= inside a fence body is ignored" "0" "$failures"

    # check_page: a non-empty transcluding fence is rejected.
    printf '# T\n\n```rust file=crates/spate/examples/memory_pipeline.rs\nlet x = 1;\n```\n' \
        >"$tmp/nonempty.md"
    failures=0
    check_page "$tmp/nonempty.md" >/dev/null 2>&1
    [ "$failures" -gt 0 ] && rc=1 || rc=0
    st "non-empty transcluding fence rejected" "1" "$rc"

    # check_page: path escapes.
    printf '# T\n\n```rust file=../../etc/passwd\n```\n' >"$tmp/escape.md"
    failures=0
    check_page "$tmp/escape.md" >/dev/null 2>&1
    [ "$failures" -gt 0 ] && rc=1 || rc=0
    st "parent-directory escape rejected" "1" "$rc"

    printf '# T\n\n```rust file=docs/STYLE.md\n```\n' >"$tmp/outside.md"
    failures=0
    check_page "$tmp/outside.md" >/dev/null 2>&1
    [ "$failures" -gt 0 ] && rc=1 || rc=0
    st "source outside crates/ rejected" "1" "$rc"

    # check_repo_links: a link is checked against the tree, not against the
    # `crates/` prefix: a page may point at any file in the repository.
    failures=0
    check_repo_links "$tmp/x.md" 1 'see [STYLE](repo:docs/STYLE.md) and [c](repo:crates).'
    st "existing file and directory targets accepted" "0" "$failures"

    failures=0
    check_repo_links "$tmp/x.md" 1 'see [gone](repo:crates/no_such_file.rs).' 2>/dev/null
    st "missing target rejected" "1" "$failures"

    failures=0
    check_repo_links "$tmp/x.md" 1 '[a](repo:../etc/passwd) [b](repo:/etc/passwd)' 2>/dev/null
    st "escapes rejected, both forms" "2" "$failures"

    failures=0
    check_repo_links "$tmp/x.md" 1 '[l](repo:docs/STYLE.md#L10)' 2>/dev/null
    st "line fragment rejected" "1" "$failures"

    failures=0
    check_repo_links "$tmp/x.md" 1 'no links here, and a bare repo: word'
    st "prose without a link is not a target" "0" "$failures"

    # A `repo:` link inside a fence is literal text; documentation of the form
    # relies on the fence skip in check_page.
    printf '# T\n\n```markdown\n[x](repo:crates/no_such_file.rs)\n```\n' >"$tmp/fenced.md"
    failures=0
    check_page "$tmp/fenced.md" >/dev/null 2>&1
    st "repo: link inside a fence is ignored" "0" "$failures"

    failures=$saved_failures
    if [ "$st_fail" -ne 0 ]; then
        echo "transclude.sh: self-test failed." >&2
        return 1
    fi
    return 0
}

self_test || exit 1

case "${1:---check}" in
--check) run_check ;;
--sources) run_sources ;;
--self-test) echo "transclude.sh: self-test passed." ;;
*)
    echo "usage: $0 [--check | --sources | --self-test]" >&2
    exit 2
    ;;
esac
