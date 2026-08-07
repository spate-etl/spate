#!/usr/bin/env bash
#
# The architecture-decision-record tool: scaffolds a record, and checks the set
# of them stays internally consistent.
#
# Records live one-per-file in `docs/adr/`. `docs/adr/_template.md` is both the
# template and this section's documentation — the rules are stated inline beside
# the section each governs, so there is no separate how-to page to drift from
# them. ADR-0001 records why the format was chosen over the decision-log table
# it replaced.
#
# What --check holds is the mechanical half only: numbers unique, statuses from
# the permitted set, placeholders filled in, and every record present in the
# index. Whether a decision warranted a record at all, and whether its
# `Considered options` is honest, is review's job and is not expressible here.
#
# The check is deliberately narrow in one further way: it never reads a record's
# prose for meaning. An accepted ADR is immutable, so a gate that grew opinions
# about wording would start demanding edits to files that must not be edited.
#
# Usage:
#   ./scripts/adr.sh --check         # the gate
#   ./scripts/adr.sh --new <slug>    # scaffold the next record
#   ./scripts/adr.sh --self-test     # the parsers, alone
#
# Targets `bash` 3.2, which is what stock macOS ships as /bin/bash: no
# associative arrays, no `mapfile`, no `${var,,}`, and every array expansion
# guarded, because `"${arr[@]}"` on an empty array is an unbound-variable error
# under `set -u` there.
set -euo pipefail

cd "$(dirname "$0")/.."

adrs=docs/adr
template="$adrs/_template.md"
index="$adrs/README.mdx"

# Deliberately no `proposed` and no `rejected`. An ADR here is written once the
# call has been made, so `proposed` would never be true — and it is the state a
# decision log rots in: the one comparable Rust project with an ADR tree has
# every record still sitting in it. A rejected alternative belongs in a record's
# `Considered options`, not in a file of its own.
STATUSES="accepted superseded deprecated"

# The marker the template leaves behind. A record still carrying one was copied
# and not written.
PLACEHOLDER=REPLACE-ME

# Is this record still carrying an unfilled placeholder?
#
# Inline code spans are stripped before looking, because a record may
# legitimately *name* the marker rather than contain one — ADR-0001's
# `Confirmation` section describes this very check, and matching its prose would
# make the gate fail on the record that documents it. Placeholders in the
# template are always bare words; a backticked one is always a mention.
has_placeholder() {
    # shellcheck disable=SC2016  # the backticks are markdown, not substitution
    sed -e 's/`[^`]*`//g' "$1" | grep -qF "$PLACEHOLDER"
}

fail() {
    echo "adr.sh: $1" >&2
    exit 1
}

# The four-digit number at the head of a record's filename, or nothing if the
# name is not a record. A function rather than a glob so the self-test can
# assert that the template and the index are not mistaken for records.
#
# The path must be exactly `docs/adr/NNNN-slug.md` — one level, four digits,
# a hyphen, a lowercase slug. Anything looser and `README.mdx` or a stray
# editor backup starts counting as a decision.
adr_number() {
    local path=$1 base
    case "$path" in
    */*/*/*) return 1 ;;
    esac
    base=$(basename "$path")
    printf '%s' "$base" | LC_ALL=C grep -qE '^[0-9]{4}-[a-z0-9]([a-z0-9-]*[a-z0-9])?\.md$' || return 1
    printf '%s' "${base%%-*}"
}

# Every record, in number order. `sort` over the filenames is enough because the
# numbers are zero-padded to a fixed width — which is the whole reason MADR pads
# them, and why widening past 9999 would be a breaking change to this ordering.
adr_files() {
    local file
    for file in "$adrs"/*.md; do
        [ -e "$file" ] || continue
        adr_number "$file" >/dev/null 2>&1 || continue
        printf '%s\n' "$file"
    done | sort
}

# The `- **Status:** accepted` line from a record's metadata block.
#
# Anchored to the start of the line and to the start of the file's first dozen
# lines' worth of shape, not searched for anywhere: a record may legitimately
# discuss a status in its prose ("...leaves the earlier record deprecated..."),
# and a loose match would read that as the record's own.
adr_status() {
    sed -n 's/^- \*\*Status:\*\*[[:space:]]*\([a-z]*\).*$/\1/p' "$1" | head -n 1
}

# ---------------------------------------------------------------------------
# Self-test.
# ---------------------------------------------------------------------------
# Runs inline on every invocation, like changelog.sh's: it is a few dozen
# pattern matches and costs microseconds, and a separate target is a target
# somebody forgets to run.
self_test() {
    local failures=0 got probe dir

    # --- adr_number accepts records and nothing else ---
    while IFS='|' read -r path want; do
        case "$path" in '' | '#'*) continue ;; esac
        got=$(adr_number "$path" 2>/dev/null || true)
        if [ "$got" != "$want" ]; then
            echo "adr.sh: adr_number('$path') -> '$got', expected '$want'" >&2
            failures=$((failures + 1))
        fi
    done <<'TABLE'
docs/adr/0001-record-architecture-decisions-in-adrs.md|0001
docs/adr/0042-a-thing.md|0042
# the template, the index and a category file are not records
docs/adr/_template.md|
docs/adr/README.mdx|
docs/adr/_category_.json|
# a number that is not four digits is not a record: it would sort wrong
docs/adr/1-a-thing.md|
docs/adr/00001-a-thing.md|
# a slug that is not lowercase-and-hyphens is not a record
docs/adr/0001-A-Thing.md|
docs/adr/0001_a_thing.md|
docs/adr/0001-.md|
# no slug at all
docs/adr/0001.md|
# nested paths: --check globs one level, so accepting one here would pass the
# gate on a record the index and the site both fail to see
docs/adr/sub/0001-a-thing.md|
TABLE

    # --- adr_status reads the metadata line and not the prose ---
    dir=$(mktemp -d)
    probe="$dir/probe.md"
    printf '# ADR-0001 — x\n\n- **Status:** accepted\n- **Date:** 2026-01-01\n\nIt leaves ADR-0000 deprecated.\n' >"$probe"
    got=$(adr_status "$probe")
    if [ "$got" != "accepted" ]; then
        echo "adr.sh: adr_status read '$got' from a record whose prose names another status" >&2
        failures=$((failures + 1))
    fi
    printf '# ADR-0002 — x\n\n- **Status:** superseded by [ADR-0009](0009-x.md)\n' >"$probe"
    got=$(adr_status "$probe")
    if [ "$got" != "superseded" ]; then
        echo "adr.sh: adr_status read '$got' from a superseded record, expected 'superseded'" >&2
        failures=$((failures + 1))
    fi
    # --- has_placeholder distinguishes an unfilled record from one that names
    #     the marker. ADR-0001 does the latter, so getting this wrong fails the
    #     gate on the record documenting the gate.
    printf '# ADR-0001 — x\n\nChosen option: "%s", because\n' "$PLACEHOLDER" >"$probe"
    if ! has_placeholder "$probe"; then
        echo "adr.sh: has_placeholder missed a bare $PLACEHOLDER — the gate is fail-open" >&2
        failures=$((failures + 1))
    fi
    # shellcheck disable=SC2016  # the backticks are markdown, not substitution
    printf '# ADR-0001 — x\n\nThe gate rejects an unfilled `%s` placeholder.\n' "$PLACEHOLDER" >"$probe"
    if has_placeholder "$probe"; then
        echo "adr.sh: has_placeholder matched a backticked mention of $PLACEHOLDER" >&2
        failures=$((failures + 1))
    fi
    rm -rf "$dir"

    [ "$failures" -eq 0 ] || fail "$failures self-test failure(s) — this script is wrong, not your change"
}

# ---------------------------------------------------------------------------
# --check
# ---------------------------------------------------------------------------
cmd_check() {
    local problems=0 count=0 file number status previous=""

    [ -d "$adrs" ] || fail "$adrs/ not found — it holds the architecture decision records"
    [ -f "$template" ] || fail "$template not found. It is the template AND the documentation for
  this section, so losing it loses the rules rather than just a convenience."
    [ -f "$index" ] || fail "$index not found — it is the index every record must appear in"

    while IFS= read -r file; do
        [ -n "$file" ] || continue
        count=$((count + 1))
        number=$(adr_number "$file")

        # Numbers are monotonic and never reused. Two files claiming one number
        # is the failure that breaks citation: `ADR-0007` would name two
        # decisions, and a supersession link could not say which.
        if [ "$number" = "$previous" ]; then
            echo "adr.sh: two records claim number $number — numbers are never reused" >&2
            problems=$((problems + 1))
        fi
        previous="$number"

        status=$(adr_status "$file")
        if [ -z "$status" ]; then
            echo "adr.sh: $file has no '- **Status:** ...' line" >&2
            problems=$((problems + 1))
        else
            case " $STATUSES " in
            *" $status "*) ;;
            *)
                echo "adr.sh: $file has status '$status'; the permitted values are: $STATUSES" >&2
                problems=$((problems + 1))
                ;;
            esac
        fi

        # A record still carrying the template's marker was copied and not
        # written. Checking for it is what lets the template use a visible
        # placeholder rather than an HTML comment, which would render as an
        # empty section instead of failing.
        if has_placeholder "$file"; then
            echo "adr.sh: $file still contains $PLACEHOLDER — it was copied but not written" >&2
            problems=$((problems + 1))
        fi

        # The index is hand-maintained, so it is exactly the thing that drifts.
        # Matching on the filename rather than the number means a record that is
        # listed under the wrong link is caught too.
        if ! grep -qF "($(basename "$file"))" "$index"; then
            echo "adr.sh: $file is not linked from $index" >&2
            problems=$((problems + 1))
        fi
    done < <(adr_files)

    # Zero, not a count. A hard-coded floor would fail for everybody the day a
    # record is legitimately renamed, and the failure being guarded against here
    # is the glob going blind — which looks like nothing at all, not like one
    # fewer.
    if [ "$count" -lt 1 ]; then
        fail "no records found in $adrs/ — the filename pattern and the tree have diverged,
  so this gate is now checking nothing and reporting success."
    fi

    [ "$problems" -eq 0 ] || fail "$problems problem(s) across $count record(s)"

    echo "adr.sh: $count record(s), numbers unique, statuses known, all indexed."
}

# ---------------------------------------------------------------------------
# --new
# ---------------------------------------------------------------------------
cmd_new() {
    local slug=${1:-} last next path today

    [ -n "$slug" ] || fail "usage: ./scripts/adr.sh --new <slug>
  The slug becomes the filename: a present-tense phrase naming the decision,
  lowercase, hyphenated. 'leader-computed-assignment', not 'coordination-stuff'."

    # `LC_ALL=C grep`, not a `case` glob: a bracket range in a glob is collated,
    # and under bash 3.2 in a UTF-8 locale `[!a-z0-9-]` accepts uppercase — so
    # the same check would mean different things on different machines. Same
    # reasoning, and the same footnote, as changelog.sh.
    if ! printf '%s' "$slug" | LC_ALL=C grep -qE '^[a-z0-9]([a-z0-9-]*[a-z0-9])?$'; then
        fail "'$slug' should be lowercase letters, digits and hyphens, starting and ending
  with one of the first two — it becomes a filename."
    fi

    [ -f "$template" ] || fail "$template not found — there is nothing to copy"

    # The next number is one past the highest that has ever existed, not one
    # past the count. Numbers are never reused, so a withdrawn record still
    # consumes its number and the count would hand it out again.
    last=$(adr_files | tail -n 1)
    if [ -n "$last" ]; then
        next=$(adr_number "$last")
        # `10#` forces base 10: `0008` is a valid octal literal and `0009` is
        # not, so without it the ninth record fails to allocate the tenth.
        next=$((10#$next + 1))
    else
        next=1
    fi
    printf -v next '%04d' "$next"

    path="$adrs/$next-$slug.md"
    [ -e "$path" ] && fail "$path already exists"

    today=$(date -u +%Y-%m-%d)

    # The number and date are substituted; every other placeholder is left for
    # the author, and --check refuses the record until they are gone.
    sed -e "s/^# ADR-NNNN /# ADR-$next /" \
        -e "s/^- \*\*Date:\*\* YYYY-MM-DD\$/- **Date:** $today/" \
        "$template" >"$path"

    echo "adr.sh: wrote $path"
    echo "  The rules are in the file. Fill in every $PLACEHOLDER, delete the guidance"
    echo "  comments as you go, and add a row to $index."
}

# ---------------------------------------------------------------------------
# Dispatch.
# ---------------------------------------------------------------------------
self_test

case "${1:-}" in
--self-test)
    echo "adr.sh: the filename parser accepts records and rejects the template, the index"
    echo "  and nested paths; the status parser reads the metadata line, not the prose."
    ;;
--check) cmd_check ;;
--new)
    shift
    cmd_new "$@"
    ;;
*)
    fail "usage: ./scripts/adr.sh --check | --new <slug> | --self-test"
    ;;
esac
