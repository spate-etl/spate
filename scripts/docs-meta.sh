#!/usr/bin/env bash
#
# The page-metadata gate: every rendered page under docs/ opens with front
# matter carrying a `description`, and that description is the length a
# search result shows in full. Front matter carries no `title`, which would
# render twice beside the H1 (docs/STYLE.md § 8).
#
# Usage:
#   ./scripts/docs-meta.sh --check       # the gate
#   ./scripts/docs-meta.sh --self-test   # the parser, alone
#
# Runs on `bash` 3.2 and later: no associative arrays, no `mapfile`.
set -euo pipefail
cd "$(dirname "$0")/.."

docs=docs
MIN=50
MAX=160

fail() {
    echo "docs-meta.sh: $1" >&2
    exit 1
}

# The rendered pages: every .md/.mdx under docs/ except partials and
# templates (an underscore-prefixed name, or one inside an underscore-prefixed
# directory) and the contributor standards file, which the site excludes.
pages() {
    find "$docs" -type f \( -name '*.md' -o -name '*.mdx' \) \
        ! -name '_*' ! -path '*/_*/*' ! -path "$docs/STYLE.md" | sort
}

# The description a file's front matter carries, unquoted, or nothing.
# Front matter is the block between a `---` on line 1 and the next `---`.
description_of() { # file
    awk '
        NR == 1 && $0 != "---" { exit }
        NR > 1 && $0 == "---" { exit }
        NR > 1 && /^description:[[:space:]]*/ {
            sub(/^description:[[:space:]]*/, "")
            if ($0 ~ /^".*"$/) { $0 = substr($0, 2, length($0) - 2); gsub(/\\"/, "\"") }
            print
            exit
        }
    ' "$1"
}

# Does the front matter carry a `title:`? Unterminated front matter (no
# closing `---`) is not a title either: the END block, not falling off the
# end of the pattern list, decides the exit status.
has_title() { # file
    awk '
        NR == 1 && $0 != "---" { exit }
        NR > 1 && $0 == "---" { exit }
        NR > 1 && /^title:/ { found = 1; exit }
        END { exit !found }
    ' "$1"
}

check_page() { # file -> prints a failure line or nothing
    local file=$1 d n
    if [ "$(head -n 1 "$file")" != "---" ]; then
        echo "$file: no front matter; every rendered page carries a description"
        return
    fi
    if has_title "$file"; then
        echo "$file: front matter carries a title, which renders twice beside the H1"
    fi
    d=$(description_of "$file")
    if [ -z "$d" ]; then
        echo "$file: front matter carries no description"
        return
    fi
    n=${#d}
    if [ "$n" -lt "$MIN" ] || [ "$n" -gt "$MAX" ]; then
        echo "$file: description is $n characters; a search result shows $MIN to $MAX"
    fi
}

self_test() {
    local dir probe failures=0
    dir=$(mktemp -d)
    probe="$dir/page.mdx"

    printf -- '---\ndescription: "A page that says what it holds, in the length a search result shows in full here."\n---\n\n# Title\n' >"$probe"
    if [ -n "$(check_page "$probe")" ]; then
        echo "docs-meta.sh: a well-formed page was rejected: $(check_page "$probe")" >&2
        failures=$((failures + 1))
    fi
    printf -- '---\ndescription: "Too short."\n---\n\n# Title\n' >"$probe"
    if [ -z "$(check_page "$probe")" ]; then
        echo "docs-meta.sh: a short description passed. The gate is fail-open" >&2
        failures=$((failures + 1))
    fi
    printf -- '# Title\n\nNo front matter.\n' >"$probe"
    if [ -z "$(check_page "$probe")" ]; then
        echo "docs-meta.sh: a page without front matter passed" >&2
        failures=$((failures + 1))
    fi
    printf -- '---\ntitle: Twice\ndescription: "A page that says what it holds, in the length a search result shows in full here."\n---\n' >"$probe"
    if ! check_page "$probe" | grep -q "title"; then
        echo "docs-meta.sh: a front-matter title passed" >&2
        failures=$((failures + 1))
    fi
    printf -- '---\ndescription: "A page whose front matter never closes, so there is no second delimiter to find here."\n' >"$probe"
    if check_page "$probe" | grep -q "title"; then
        echo "docs-meta.sh: unterminated front matter was reported as carrying a title" >&2
        failures=$((failures + 1))
    fi
    printf -- '---\ndescription: "Quotes inside, \\"like this\\", are one character each and count toward the length shown."\n---\n' >"$probe"
    if [ -n "$(check_page "$probe")" ]; then
        echo "docs-meta.sh: an escaped quote was miscounted: $(check_page "$probe")" >&2
        failures=$((failures + 1))
    fi
    rm -rf "$dir"
    [ "$failures" -eq 0 ] || fail "$failures self-test failure(s). This script is wrong, not your change"
}

check() {
    local failures=0 count=0 out
    while IFS= read -r file; do
        count=$((count + 1))
        out=$(check_page "$file")
        if [ -n "$out" ]; then
            echo "docs-meta.sh: $out" >&2
            failures=$((failures + 1))
        fi
    done < <(pages)
    [ "$count" -gt 0 ] || fail "no pages found under $docs"
    [ "$failures" -eq 0 ] || fail "$failures page(s) without a usable description"
    echo "docs-meta.sh: $count page(s); every one carries a description of $MIN to $MAX characters."
}

case "${1:-}" in
--check) self_test && check ;;
--self-test) self_test && echo "docs-meta.sh: self-test passed" ;;
*) fail "usage: $0 --check | --self-test" ;;
esac
