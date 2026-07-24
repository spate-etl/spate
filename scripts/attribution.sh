#!/usr/bin/env bash
#
# Regenerates THIRD-PARTY.md, the committed dependency attribution inventory.
# CI runs this and fails on any diff, so the file cannot drift from Cargo.lock.
#
# This exists instead of a bare `cargo about generate` because the generator's
# row order is not reproducible across machines. cargo-about emits one row per
# licence *text*, grouping crates by which text they share; a crate that ships
# several files scanning as the same licence at the same confidence has its
# text picked by directory-read order, so which group it lands in — and with it
# where its row falls — differs from machine to machine. The elected licence id
# never changes. That is what made the gate alternate red/green on unrelated
# commits (#87).
#
# Two passes make the output a property of its content alone:
#
#   1. the crate table is sorted by (licence id, crate, version), and rows that
#      a multi-notice crate duplicates are collapsed, so the table is one row
#      per crate however the notices were grouped;
#   2. the summary counts are recomputed from those rows, because cargo-about
#      counts rows rather than crates and would otherwise disagree with them.
#
# The site artifact (about-html.hbs) is unaffected and still reproduces every
# notice, including the several a crate like `bnum` carries.
#
# Usage: ./scripts/attribution.sh [output-file]
set -euo pipefail

cd "$(dirname "$0")/.."

out=${1:-THIRD-PARTY.md}
tmp=$(mktemp "${TMPDIR:-/tmp}/third-party.XXXXXX")
# Staged next to the output so the final `mv` is a rename on one filesystem:
# a failure mid-pipeline must not leave a truncated THIRD-PARTY.md behind.
staged=$(mktemp "$(dirname "$out")/.attribution.XXXXXX")
trap 'rm -f "$tmp" "$staged"' EXIT

fail() {
    echo "attribution.sh: $1" >&2
    exit 1
}

# `--fail` is the real gate: non-zero if any crate's licence cannot be
# determined. `--locked`, not `--frozen`: offline mode drops the
# clearlydefined.io clarifications and silently degrades accuracy.
cargo about generate --workspace --all-features --locked --fail \
    -o "$tmp" about-md.hbs

# Anchors. `-F -x` matches the whole line, so the summary rule (two columns)
# cannot match the crate table's (three). `|| true` keeps a missing anchor from
# aborting the assignment under `set -e` with no explanation.
summary_rule=$(grep -n -F -x -- '|---|---|' "$tmp" | head -1 | cut -d: -f1) || true
crate_header=$(grep -n -F -x -- '| Crate | Version | Licence |' "$tmp" | cut -d: -f1) || true
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

# Rows look like: | `crate` | version | `LICENCE` |
# Splitting on the backtick rather than the pipe means field 2 is the bare crate
# name, so names sort as names — with the pipe, the trailing backtick of `etl`
# would sort it after `etl-test`. Field 4 is the licence id (the grouping) and
# field 3 carries the version, which separates two versions of one crate. Rows
# tying on all three are byte-identical: one crate, one licence, several
# notices, which `uniq` then collapses to the single row this table promises.
bt='`'
rows=$(sed -n "$((crate_rule + 1)),\$p" "$tmp" | grep -e '^| ' |
    LC_ALL=C sort -t"$bt" -k4,4 -k2,2 -k3,3 | uniq) || true
[ -n "$rows" ] || fail "crate table is empty"

# Counts by licence id, ordered as cargo-about orders them: most used first,
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

# The rebuild reorders and rewrites two tables, and a silent loss would still
# pass the diff gate on the next run. Check the outcome rather than trust the
# pipeline: no licence id may disappear from the summary, and no crate may
# disappear from the table.
ids_before=$(sed -n "$((summary_rule + 1)),${summary_end}p" "$tmp" | cut -d"$bt" -f2 | LC_ALL=C sort)
ids_after=$(printf '%s\n' "$summary" | cut -d"$bt" -f2 | LC_ALL=C sort)
[ "$ids_before" = "$ids_after" ] || fail "licence ids changed while rebuilding the summary"

crates_before=$(sed -n "$((crate_rule + 1)),\$p" "$tmp" | grep -e '^| ' | LC_ALL=C sort -u | wc -l | tr -d ' ')
crates_after=$(printf '%s\n' "$rows" | wc -l | tr -d ' ')
[ "$crates_before" -eq "$crates_after" ] ||
    fail "lost rows while sorting ($crates_before in, $crates_after out)"

rows_raw=$(sed -n "$((crate_rule + 1)),\$p" "$tmp" | grep -c -e '^| ') || true
collapsed=$((rows_raw - crates_after))

chmod 644 "$staged"
mv "$staged" "$out"

echo "attribution.sh: wrote $out ($crates_after crates, $collapsed duplicate notice row(s) collapsed)"
