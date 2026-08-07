#!/usr/bin/env bash
#
# Checks that the four places stating the engine's invariants still agree.
#
# docs/INVARIANTS.md defines them; AGENTS.md, CONTRIBUTING.md and the pull
# request template each restate the list for their own audience.
#
# Two rules, because the files are not all the same kind of statement:
#
#   * a FULL restatement must cite exactly the set INVARIANTS.md defines;
#   * a SUBSET may cite fewer — the feature form asks "does this touch any of
#     these?", not "here are the invariants" — but may not cite a number that
#     does not exist.
#
# Usage: ./scripts/check-invariants.sh
set -euo pipefail

cd "$(dirname "$0")/.."

source_of_truth=docs/INVARIANTS.md
full=(AGENTS.md CONTRIBUTING.md .github/pull_request_template.md)
# Anywhere else that names invariants by number. A module doc restating a few of
# them is a subset restatement like any other, and drifts the same way.
subset=(
    .github/ISSUE_TEMPLATE/4-feature.yml
    crates/spate-core/src/checkpoint/mod.rs
)

fail() {
    echo "check-invariants.sh: $1" >&2
    exit 1
}

cites() {
    grep -oE 'INV-[0-9]+' "$1" 2>/dev/null | sort -uV || true
}

[ -f "$source_of_truth" ] || fail "$source_of_truth not found"

# The definitions are the bold-led bullets, not every mention: the file cites
# numbers in its own prose too, and a cross-reference must not be able to define
# an invariant by accident.
defined=$(grep -oE '^- \*\*INV-[0-9]+ —' "$source_of_truth" | grep -oE 'INV-[0-9]+' | sort -uV)
[ -n "$defined" ] || fail "no invariants defined in $source_of_truth — is the format still \`- **INV-n — \`?"

count=$(grep -c . <<<"$defined")
problems=0
checked=0

for f in "${full[@]}"; do
    # A missing file is a failure, not a skip. Skipping means the list it was
    # supposed to police goes unchecked while the script still reports success —
    # if one of these is genuinely retired, remove it from the array above.
    [ -f "$f" ] || fail "$f not found — it is listed as a full restatement"
    checked=$((checked + 1))
    if ! diff -q <(cites "$f") <(printf '%s\n' "$defined") >/dev/null; then
        echo "check-invariants.sh: $f does not cite the same set as $source_of_truth" >&2
        diff <(printf '%s\n' "$defined") <(cites "$f") \
            --label "$source_of_truth" --label "$f" -u | tail -n +3 >&2 || true
        problems=$((problems + 1))
    fi
done

for f in "${subset[@]}"; do
    [ -f "$f" ] || fail "$f not found — it is listed as a subset restatement"
    checked=$((checked + 1))
    # `grep -vxF -f`, not `comm`: comm is a merge of two *lexically* sorted
    # streams, and these are sorted with `-V` so that INV-10 follows INV-9
    # rather than INV-1. Feeding version order to comm gives undefined output —
    # it reported INV-10 as undefined while INV-10 was defined. A set-difference
    # that does not care about order cannot have that failure.
    extra=$(grep -vxF -f <(printf '%s\n' "$defined") <(cites "$f") || true)
    if [ -n "$extra" ]; then
        echo "check-invariants.sh: $f cites numbers not defined in $source_of_truth:" >&2
        while IFS= read -r line; do
            echo "  $line" >&2
        done <<<"$extra"
        problems=$((problems + 1))
    fi
done

[ "$problems" -eq 0 ] || fail "$problems file(s) out of step"

echo "check-invariants.sh: INV-1..INV-$count, cited consistently across $checked file(s)"
