#!/usr/bin/env bash
#
# Regenerates the dependency attribution artifacts: THIRD-PARTY.md, the
# committed inventory, and the site's /licenses/ page. CI regenerates the
# inventory and fails on any diff, so it cannot drift from Cargo.lock.
#
# Both artifacts are third-party inventories. The workspace's own crates are
# first-party, Apache-2.0, covered by LICENSE, so they are filtered out of
# what `cargo about generate` emits; `about.toml`'s `private = { ignore }`
# only reaches unpublished members, and these are published. Both artifacts
# generate through this script, so they agree.
#
# `cargo about generate` emits one row per license *text*, in an order that
# follows directory-read order and so varies between machines. Two passes here
# make the Markdown output a property of its content alone: the crate table is
# sorted by (license id, crate, version) with duplicate rows collapsed, and
# the summary counts are recomputed from those rows, because cargo-about
# counts rows rather than crates.
#
# Usage:
#   ./scripts/attribution.sh [output-file]        # THIRD-PARTY.md
#   ./scripts/attribution.sh --html <output-file> # the /licenses/ page
set -euo pipefail

cd "$(dirname "$0")/.."

fail() {
    echo "attribution.sh: $1" >&2
    exit 1
}

mode=md
out=THIRD-PARTY.md
case "${1:-}" in
--html)
    mode=html
    out=${2:-}
    [ -n "$out" ] || fail "usage: --html <output-file>"
    ;;
--*)
    fail "unknown flag '$1'; usage: [output-file] | --html <output-file>"
    ;;
*)
    out=${1:-THIRD-PARTY.md}
    ;;
esac

tmp=$(mktemp "${TMPDIR:-/tmp}/attribution.XXXXXX")
# Staged next to the output so the final `mv` is a rename on one filesystem:
# a failure mid-pipeline must not leave a truncated artifact behind.
staged=$(mktemp "$(dirname "$out")/.attribution.XXXXXX")
trap 'rm -f "$tmp" "$staged"' EXIT

# The first-party set: the workspace's publishable packages, from its own
# metadata, so a member anywhere in the tree is covered and a stray directory
# under crates/ cannot be mistaken for one.
first_party=$(cargo metadata --no-deps --format-version 1 |
    jq -r '.packages[] | select(.publish != []) | .name' | tr '\n' ' ')
[ -n "${first_party// /}" ] || fail "the workspace metadata names no publishable packages, so the
  first-party filter is blind"

# ---------------------------------------------------------------------------
# The /licenses/ page.
# ---------------------------------------------------------------------------
# The template marks what the filter needs: one sentinel comment pair per
# license-text section, one line per crate chip carrying `data-crate`, one
# TOC row per license id carrying `data-license-id`. Handlebars HTML-escapes
# `{{text}}`, so a license text cannot fake a sentinel or a chip.
#
# Two passes. The first counts the crates each section keeps after the
# first-party chips are dropped; the second emits, skipping emptied sections
# and rewriting each TOC row's count, dropping rows that reach zero.
if [ "$mode" = html ]; then
    cargo about generate --workspace --all-features --locked --fail \
        -o "$tmp" about/html.hbs

    # Both halves per crate, mirroring the Markdown path: every first-party
    # package must appear in the generated page, or the filter has nothing
    # to drop for it and has gone blind for that name.
    for crate in $first_party; do
        grep -qF "data-crate=\"$crate\"" "$tmp" ||
            fail "first-party crate '$crate' never appeared in the generated page"
    done

    awk -v fp="$first_party" '
        BEGIN {
            n = split(fp, names, " ")
            for (i = 1; i <= n; i++) firstparty[names[i]] = 1
        }
        function crate_of(line) {
            sub(/.*<code data-crate="/, "", line)
            sub(/".*/, "", line)
            return line
        }
        # `name version`, the chip text: the distinct-count key. THIRD-PARTY.md
        # keeps one row per (crate, version), so two linked versions of one
        # crate count twice there and must count twice here.
        function chip_of(line) {
            sub(/.*<code data-crate="[^"]*">/, "", line)
            sub(/<\/code>.*/, "", line)
            return line
        }
        function id_of(line, marker) {
            sub(".*" marker "\"", "", line)
            sub(/".*/, "", line)
            return line
        }
        # Pass 1: which sections keep a crate, and how many distinct
        # (crate, version) pairs per license id. Distinct, not chips: a crate
        # shipping several notices sits in several sections, and
        # THIRD-PARTY.md counts it once, so the page counts it once too. A
        # chip outside a sentinel pair counts toward nothing, so it cannot
        # keep a section alive on another chip'\''s behalf.
        NR == FNR {
            if ($0 ~ /^<!-- BEGIN-LICENSE /) { block++; inb = 1; id = $0; sub(/^<!-- BEGIN-LICENSE /, "", id); sub(/ -->$/, "", id) }
            else if ($0 ~ /^<!-- END-LICENSE -->$/) { inb = 0 }
            else if (inb && $0 ~ /<code data-crate="/ && !firstparty[crate_of($0)]) {
                keep[block]++
                if (!seen[id, chip_of($0)]++) count[id]++
            }
            next
        }
        # Pass 2: emit.
        /^<!-- BEGIN-LICENSE / { inblock = 1; blockno++; skip = keep[blockno] ? 0 : 1; next }
        /^<!-- END-LICENSE -->$/ { inblock = 0; skip = 0; next }
        inblock && skip { next }
        inblock && /<code data-crate="/ && firstparty[crate_of($0)] { dropped++; next }
        /<li data-license-id="/ {
            toc_id = id_of($0, "data-license-id=")
            if (count[toc_id] < 1) next
            if (!sub(/[0-9]+ crates<\/li>/, count[toc_id] (count[toc_id] == 1 ? " crate" : " crates") "</li>")) {
                print "attribution.sh: a TOC row lost the count shape the rewrite expects: " $0 > "/dev/stderr"
                died = 1
                exit 1
            }
            toc++
        }
        { print }
        END {
            if (died) exit 1
            if (inblock) { print "attribution.sh: unterminated BEGIN-LICENSE section" > "/dev/stderr"; exit 1 }
            if (blockno < 1) { print "attribution.sh: no BEGIN-LICENSE sections; the template lost its sentinels" > "/dev/stderr"; exit 1 }
            if (dropped < 1) { print "attribution.sh: no first-party crate chip was dropped; the filter has gone blind" > "/dev/stderr"; exit 1 }
            # Every id that kept a crate keeps its TOC row, and no other row
            # survives; a drift between the overview ids and the section ids
            # would otherwise thin the TOC silently.
            nids = 0
            for (i in count) if (count[i] > 0) nids++
            if (toc != nids) { print "attribution.sh: " toc " TOC rows kept for " nids " license ids with crates" > "/dev/stderr"; exit 1 }
        }
    ' "$tmp" "$tmp" >"$staged"

    # Nothing first-party may survive, name by name.
    for crate in $first_party; do
        if grep -qF "data-crate=\"$crate\"" "$staged"; then
            fail "first-party crate '$crate' survived into $out"
        fi
    done

    chmod 644 "$staged"
    mv "$staged" "$out"
    echo "attribution.sh: wrote $out"
    exit 0
fi

# ---------------------------------------------------------------------------
# THIRD-PARTY.md.
# ---------------------------------------------------------------------------
# `--fail` is the real gate: non-zero if any crate's license cannot be
# determined. `--locked`, not `--frozen`: offline mode drops the
# clearlydefined.io clarifications and silently degrades accuracy.
cargo about generate --workspace --all-features --locked --fail \
    -o "$tmp" about/md.hbs

# Anchors. `-F -x` matches the whole line, so the summary rule (two columns)
# cannot match the crate table's (three). `|| true` keeps a missing anchor from
# aborting the assignment under `set -e` with no explanation.
summary_rule=$(grep -n -F -x -- '|---|---|' "$tmp" | head -1 | cut -d: -f1) || true
crate_header=$(grep -n -F -x -- '| Crate | Version | License |' "$tmp" | cut -d: -f1) || true
[ -n "$summary_rule" ] || fail "summary table not found in generated output"
[ -n "$crate_header" ] || fail "crate table header not found in generated output"
crate_rule=$((crate_header + 1)) # the |---|---|---| under the header

# The summary rows run from the rule to the first line that is not a row.
summary_end=$summary_rule
while true; do
    line=$(sed -n "$((summary_end + 1))p" "$tmp")
    case $line in
    '| '*) summary_end=$((summary_end + 1)) ;;
    *) break ;;
    esac
done
[ "$summary_end" -gt "$summary_rule" ] || fail "summary table has no rows"

# The crate table runs to EOF. Anything else down there would be silently
# dropped by the rebuild below, so refuse rather than delete it.
trailing=$(sed -n "$((crate_rule + 1)),\$p" "$tmp" | grep -v -e '^| ' -e '^$' | head -1) || true
[ -z "$trailing" ] || fail "unexpected line below the crate table: $trailing"

# Rows look like: | `crate` | version | `LICENSE` |
# Splitting on the backtick rather than the pipe means field 2 is the bare crate
# name, so names sort as names: with the pipe, the trailing backtick of `spate`
# would sort it after `spate-test`. Field 4 is the license id, field 3 the
# version.
bt='`'
rows_raw=$(sed -n "$((crate_rule + 1)),\$p" "$tmp" | grep -e '^| ') || true
[ -n "$rows_raw" ] || fail "crate table is empty"

# cargo-about's own summary against its own table, before any filtering: a
# row the generator lost would otherwise read as a legitimate removal on the
# next run.
ids_raw_rows=$(printf '%s\n' "$rows_raw" | cut -d"$bt" -f4 | LC_ALL=C sort -u)
ids_raw_summary=$(sed -n "$((summary_rule + 1)),${summary_end}p" "$tmp" | cut -d"$bt" -f2 | LC_ALL=C sort -u)
[ "$ids_raw_rows" = "$ids_raw_summary" ] ||
    fail "cargo-about's summary and table disagree on license ids"

# Drop the first-party rows before anything is counted, so every invariant
# below judges the filtered table.
rows_third=$(printf '%s\n' "$rows_raw" |
    awk -F"$bt" -v fp="$first_party" '
        BEGIN { n = split(fp, names, " "); for (i = 1; i <= n; i++) firstparty[names[i]] = 1 }
        !firstparty[$2]
    ')
[ -n "$rows_third" ] || fail "every row was filtered as first-party; the crate table is gone"

# Each first-party crate is a publishable workspace member, so cargo-about
# lists it and the filter must drop at least one row for it. Zero dropped
# means a crate was renamed away from its directory and the filter has gone
# blind for it.
for crate in $first_party; do
    if printf '%s\n' "$rows_third" | grep -qF "| ${bt}${crate}${bt} |"; then
        fail "first-party crate '$crate' survived into the table"
    fi
    if ! printf '%s\n' "$rows_raw" | grep -qF "| ${bt}${crate}${bt} |"; then
        fail "first-party crate '$crate' never appeared in the generated table: does its
  package name still match its directory under crates/?"
    fi
done

# Rows tying on all three keys are byte-identical, and `uniq` collapses them.
rows=$(printf '%s\n' "$rows_third" | LC_ALL=C sort -t"$bt" -k4,4 -k2,2 -k3,3 | uniq)

# Counts by license id, ordered as cargo-about orders them: most used first,
# ties by id ascending.
summary=$(printf '%s\n' "$rows" |
    awk -F"$bt" '{ n[$4]++ } END { for (id in n) printf "%d\t%s\n", n[id], id }' |
    LC_ALL=C sort -k1,1nr -k2,2 |
    awk -F'\t' '{ printf "| `%s` | %d |\n", $2, $1 }')

{
    head -n "$summary_rule" "$tmp"
    printf '%s\n' "$summary"
    sed -n "$((summary_end + 1)),${crate_rule}p" "$tmp"
    printf '%s\n' "$rows"
} >"$staged"

crates_after=$(printf '%s\n' "$rows" | wc -l | tr -d ' ')
filtered_raw=$(printf '%s\n' "$rows_third" | grep -c -e '^| ') || true
collapsed=$((filtered_raw - crates_after))
first_party_dropped=$(printf '%s\n' "$rows_raw" | grep -c -e '^| ')
first_party_dropped=$((first_party_dropped - filtered_raw))

chmod 644 "$staged"
mv "$staged" "$out"

echo "attribution.sh: wrote $out ($crates_after third-party crates, $first_party_dropped first-party row(s) filtered, $collapsed duplicate notice row(s) collapsed)"
