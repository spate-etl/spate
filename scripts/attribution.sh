#!/usr/bin/env bash
#
# Regenerates THIRD-PARTY.md, the committed dependency attribution inventory.
# CI runs this and fails on any diff, so the file cannot drift from Cargo.lock.
#
# `cargo about generate` emits one row per license *text*, in an order that
# follows directory-read order and so varies between machines. Two passes here
# make the output a property of its content alone: the crate table is sorted by
# (license id, crate, version) with duplicate rows collapsed, and the summary
# counts are recomputed from those rows, because cargo-about counts rows rather
# than crates.
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
# version. Rows tying on all three are byte-identical, and `uniq` collapses
# them.
bt='`'
rows=$(sed -n "$((crate_rule + 1)),\$p" "$tmp" | grep -e '^| ' |
    LC_ALL=C sort -t"$bt" -k4,4 -k2,2 -k3,3 | uniq) || true
[ -n "$rows" ] || fail "crate table is empty"

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

# A silent loss in the rebuild would look identical to a legitimate removal on
# the next run: no license id may disappear from the summary, no crate from the
# table.
ids_before=$(sed -n "$((summary_rule + 1)),${summary_end}p" "$tmp" | cut -d"$bt" -f2 | LC_ALL=C sort)
ids_after=$(printf '%s\n' "$summary" | cut -d"$bt" -f2 | LC_ALL=C sort)
[ "$ids_before" = "$ids_after" ] || fail "license ids changed while rebuilding the summary"

crates_before=$(sed -n "$((crate_rule + 1)),\$p" "$tmp" | grep -e '^| ' | LC_ALL=C sort -u | wc -l | tr -d ' ')
crates_after=$(printf '%s\n' "$rows" | wc -l | tr -d ' ')
[ "$crates_before" -eq "$crates_after" ] ||
    fail "lost rows while sorting ($crates_before in, $crates_after out)"

rows_raw=$(sed -n "$((crate_rule + 1)),\$p" "$tmp" | grep -c -e '^| ') || true
collapsed=$((rows_raw - crates_after))

chmod 644 "$staged"
mv "$staged" "$out"

echo "attribution.sh: wrote $out ($crates_after crates, $collapsed duplicate notice row(s) collapsed)"
