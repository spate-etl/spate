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
# quantity: deterministic, so comparable across runners) and, when any summary
# carries a DHAT profile, one of heap counts (allocated blocks and the t-gmax
# peak), with every other metric per bench behind a <details> fold. The
# percentage column is gungraun's own derived diff, not recomputed here.
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
# when both sides are, carrying the derived percentage as a string — including
# the strings "inf"/"-inf" (a metric that was zero at the baseline) and "NaN"
# (zero on both sides), which must not reach `tonumber` (jq renders them as
# ±1.8e308 and null rather than aborting).
#
# The report is captured whole and printed only on success: jq streams its
# output, so an error midway through would otherwise leave a truncated table
# in the step summary under a green `continue-on-error` step.
report=$(jq -r -s --arg base "$baseline_label" '
    def val: if type == "object" then (.Int // .Float) else . end;
    def new_side: .metrics | (if .Both then .Both[0] else .Left end) | val;
    def old_side: .metrics | (if .Both then .Both[1] elif .Right then .Right else null end)
        | if . == null then null else val end;
    def delta:
        if .diffs == null then "*new*"
        else (.diffs.diff_pct
            | if test("inf") then (if startswith("-") then "-∞%" else "+∞%" end)
              elif . == "NaN" then "n/a"
              else (tonumber | (. * 100 | round) / 100
                    | if . > 0 then "+\(.)%" else "\(.)%" end)
              end)
        end;
    def bench_name:
        (.module_path | split("::") | .[1:] | join("::"))
        + (if .id != null and .id != "" then " \(.id)" else "" end);
    # null when the summary carries no callgrind profile at all (a run under
    # a different valgrind tool): rendered as an explicit row, never an error
    # that truncates the report.
    def callgrind:
        [.profiles[] | select(.tool == "Callgrind")][0]
        | if . == null then null else .summaries.total.summary.Callgrind end;
    # null when the summary carries no DHAT profile: the heap table is simply
    # absent, so the report stays correct against summaries measured before
    # DHAT rode along (a merge-base leg, an old artifact).
    def dhat:
        [.profiles[] | select(.tool == "DHAT")][0]
        | if . == null then null else .summaries.total.summary.Dhat end;

    "## Instruction counts",
    "",
    "Callgrind instructions (`Ir`) per bench: pull request vs \($base).",
    "Advisory: numbers never block a merge; a bench that stops running does.",
    "",
    "| Bench | PR | \($base) | Δ |",
    "| --- | ---: | ---: | ---: |",
    (.[] | callgrind as $cg
        | if $cg == null or ($cg | has("Ir") | not)
          then "| \(bench_name) | — | — | *no callgrind profile* |"
          else "| \(bench_name) | \($cg.Ir | new_side) | \($cg.Ir | old_side // "—") | \($cg.Ir | delta) |"
          end),
    (if any(.[]; dhat != null) then
        "",
        "## Heap (DHAT)",
        "",
        "DHAT heap blocks and peak bytes per bench: pull request vs \($base).",
        "",
        "| Bench | Metric | PR | \($base) | Δ |",
        "| --- | --- | ---: | ---: | ---: |",
        (.[] | dhat as $dh
            | select($dh != null)
            | bench_name as $bn
            | (["TotalBlocks", "AtTGmaxBytes"][] as $key
                | $dh[$key]
                | select(. != null)
                | "| \($bn) | \($key) | \(new_side) | \(old_side // "—") | \(delta) |"))
    else empty end),
    "",
    "<details><summary>All metrics</summary>",
    "",
    (.[] | callgrind as $cg
        | select($cg != null)
        | "**\(bench_name)** — callgrind",
          "",
          "| Metric | PR | \($base) | Δ |",
          "| --- | ---: | ---: | ---: |",
          ($cg | to_entries[] | "| \(.key) | \(.value | new_side) | \(.value | old_side // "—") | \(.value | delta) |"),
          ""),
    (.[] | dhat as $dh
        | select($dh != null)
        | "**\(bench_name)** — DHAT",
          "",
          "| Metric | PR | \($base) | Δ |",
          "| --- | ---: | ---: | ---: |",
          ($dh | to_entries[] | "| \(.key) | \(.value | new_side) | \(.value | old_side // "—") | \(.value | delta) |"),
          ""),
    "</details>"
' "$summaries")
printf '%s\n' "$report"
