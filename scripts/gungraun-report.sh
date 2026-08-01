#!/usr/bin/env bash
# Render gungraun's machine-readable summaries as a Markdown report.
#
#   gungraun-report.sh <summaries> [<baseline-label>]
#
# <summaries> is a concatenation of the `summary.json` files one CI run wrote
# under `target/gungraun` (`GUNGRAUN_SAVE_SUMMARY=json`), each a single JSON
# object; jq's slurp mode parses the stream whether or not the files ended in
# newlines. <baseline-label> names what the old column is — the caller knows
# whether a baseline was measured and which commit it was.
#
# The output is one table of callgrind instruction counts (the headline
# quantity: deterministic, so comparable across runners), with every other
# callgrind metric per bench behind a <details> fold. The percentage column is
# gungraun's own derived diff, not recomputed here.
#
# The report renders no verdict deliberately: no regression threshold exists
# until the noise floor is known from real pull requests, so the numbers are
# presented and not judged.
set -euo pipefail

# Bumping the gungraun workspace dependency across a summary-format major
# version silently changes the field paths the jq below walks. Failing on the
# version string is what turns "the report went quietly blank" into a build
# error naming the fix.
SCHEMA_VERSION="6"

if [[ $# -lt 1 || ! -r "${1:-}" || ! -s "${1:-}" ]]; then
    echo "::error::usage: $0 <summaries> [<baseline-label>] — needs a non-empty, readable summaries file" >&2
    exit 1
fi
summaries="$1"
baseline_label="${2:-baseline}"

drifted=$(jq -r --arg v "$SCHEMA_VERSION" 'select(.version != $v) | .version' "$summaries" | sort -u | tr '\n' ' ')
drifted="${drifted% }"
if [[ -n "$drifted" ]]; then
    echo "::error::gungraun summary schema is v${drifted}, this report is written against v${SCHEMA_VERSION} — update scripts/gungraun-report.sh against the new schema before trusting its output" >&2
    exit 1
fi

# Field-path conventions (summary.v6.schema.json): `metrics` is Left/Both/Right
# with Left = the NEW metric and Right = the OLD one; `diffs` is present only
# when both sides are, carrying the derived percentage as a string.
jq -r -s --arg base "$baseline_label" '
    def val: if type == "object" then (.Int // .Float) else . end;
    def new_side: .metrics | (if .Both then .Both[0] else .Left end) | val;
    def old_side: .metrics | (if .Both then .Both[1] elif .Right then .Right else null end)
        | if . == null then null else val end;
    def delta:
        if .diffs == null then "*new*"
        else (.diffs.diff_pct | tonumber | (. * 100 | round) / 100
              | if . > 0 then "+\(.)%" else "\(.)%" end)
        end;
    def bench_name:
        (.module_path | split("::") | .[1:] | join("::"))
        + (if .id != null and .id != "" then " \(.id)" else "" end);
    def callgrind:
        [.profiles[] | select(.tool == "Callgrind")][0]
        | .summaries.total.summary.Callgrind;

    "## Instruction counts",
    "",
    "Callgrind instructions (`Ir`) per bench: pull request vs \($base).",
    "Advisory: numbers never block a merge; a bench that stops running does.",
    "",
    "| Bench | PR | \($base) | Δ |",
    "| --- | ---: | ---: | ---: |",
    (.[] | callgrind as $cg
        | "| \(bench_name) | \($cg.Ir | new_side) | \($cg.Ir | old_side // "—") | \($cg.Ir | delta) |"),
    "",
    "<details><summary>All callgrind metrics</summary>",
    "",
    (.[] | callgrind as $cg
        | "**\(bench_name)**",
          "",
          "| Metric | PR | \($base) | Δ |",
          "| --- | ---: | ---: | ---: |",
          ($cg | to_entries[] | "| \(.key) | \(.value | new_side) | \(.value | old_side // "—") | \(.value | delta) |"),
          ""),
    "</details>"
' "$summaries"
