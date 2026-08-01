#!/usr/bin/env bash
# Render gungraun's machine-readable summaries as a Markdown report.
#
#   gungraun-report.sh [--regressions-out FILE] <summaries> [<baseline-label>]
#   gungraun-report.sh --self-test
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
# Rows that cross the advisory thresholds below are marked in the tables, and
# `--regressions-out FILE` writes the bare string `true` or `false` — exactly
# the two values `perf-label.yml` acts on, checked by `--self-test`. That is
# the entire verdict: the script always exits 0 on a metric moving — the
# label is the only consequence — and the thresholds are provisional,
# borrowed from the closest worked reference in the ecosystem rather than
# measured here, until enough real pull requests have been through the job to
# know the noise floor.
set -euo pipefail

# Bumping the gungraun workspace dependency across a summary-format major
# version silently changes the field paths the jq below walks. Failing on the
# version string is what turns "the report went quietly blank" into a build
# error naming the fix.
SCHEMA_VERSION="6"

# Advisory thresholds. Instructions and peak heap flag on percentage
# *increases* only; the block count flags on an absolute move in either
# direction, because a structural allocation change is worth eyes even when
# it shrinks. Rows with no baseline never flag — there is no delta to judge.
IR_THRESHOLD_PCT=5
BLOCKS_THRESHOLD_ABS=1
PEAK_THRESHOLD_PCT=5

# The write side of the perf-label.yml contract, executable. The two
# workflows can never run together before a merge (`workflow_run` executes
# the default branch's definition), so this is the only place the contract
# runs on a pull request: the flag file must be the bare `true` or `false`
# the label workflow's `case` accepts, and threshold crossings must mark
# rows. The fixtures pin schema v6 on purpose — bumping SCHEMA_VERSION makes
# this fail at the version gate until they are rebuilt against the new
# shape, which is the reminder doing its job.
if [[ "${1:-}" == "--self-test" ]]; then
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    fail_self() {
        echo "gungraun-report.sh --self-test: $1" >&2
        exit 1
    }
    printf '%s\n' \
        '{"version":"6","module_path":"decode_gungraun::decode::decode_value","id":"flat_record","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Both":[{"Int":107000},{"Int":100000}]},"diffs":{"diff_pct":"7.0"}}}}}}},{"tool":"DHAT","summaries":{"total":{"summary":{"Dhat":{"TotalBlocks":{"metrics":{"Both":[{"Int":38},{"Int":40}]},"diffs":{"diff_pct":"-5.0"}},"AtTGmaxBytes":{"metrics":{"Both":[{"Int":4342},{"Int":4096}]},"diffs":{"diff_pct":"6.0"}}}}}}}]}' \
        >"$tmp/hot.jsonl"
    printf '%s\n' \
        '{"version":"6","module_path":"decode_gungraun::decode::decode_value","id":"flat_record","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Both":[{"Int":100000},{"Int":99000}]},"diffs":{"diff_pct":"1.0"}}}}}}}]}' \
        >"$tmp/quiet.jsonl"

    "$0" --regressions-out "$tmp/flag" "$tmp/hot.jsonl" self-test >"$tmp/report.md"
    [[ "$(cat "$tmp/flag")" == "true" ]] \
        || fail_self "hot fixture: flag file holds '$(cat "$tmp/flag")', not the bare string 'true'"
    grep -q "(over threshold)" "$tmp/report.md" \
        || fail_self "hot fixture: no row carries the over-threshold marker"

    "$0" --regressions-out "$tmp/flag" "$tmp/quiet.jsonl" self-test >"$tmp/report.md"
    [[ "$(cat "$tmp/flag")" == "false" ]] \
        || fail_self "quiet fixture: flag file holds '$(cat "$tmp/flag")', not the bare string 'false'"
    if grep -q "(over threshold)" "$tmp/report.md"; then
        fail_self "quiet fixture: a row is marked over threshold"
    fi

    echo "gungraun-report.sh: self-test ok — the flag file is the bare boolean perf-label.yml parses, and markers track the thresholds"
    exit 0
fi

regressions_out=""
if [[ "${1:-}" == "--regressions-out" ]]; then
    if [[ $# -lt 2 || -z "${2:-}" ]]; then
        echo "::error::--regressions-out needs a file argument" >&2
        exit 1
    fi
    regressions_out="$2"
    shift 2
fi

if [[ $# -lt 1 || ! -r "${1:-}" || ! -s "${1:-}" ]]; then
    echo "::error::usage: $0 [--regressions-out FILE] <summaries> [<baseline-label>] — needs a non-empty, readable summaries file" >&2
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
# One definition set, two programs: the report and the has_regressions
# verdict read the same summaries through the same field paths, so the flag
# a row shows and the flag the label workflow reads cannot disagree.
#
# The dollar signs below are jq variables ($t, $flagged); not expanding them
# in the shell is the point of the single quotes.
# shellcheck disable=SC2016
jq_defs='
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
    # An increase past $t percent, judged on gungraun'\''s own diff string with
    # the same inf/NaN guards as `delta`. "+inf" (a zero baseline that grew)
    # counts as flagged; "-inf" and NaN do not; no diff means no baseline.
    def flag_pct_increase($t):
        if .diffs == null then false
        else (.diffs.diff_pct
            | if test("inf") then (startswith("-") | not)
              elif . == "NaN" then false
              else (tonumber >= $t)
              end)
        end;
    # An absolute move past $t in either direction, from the two sides
    # directly rather than the percentage — a one-block change on a small
    # baseline and on a large one are the same structural fact.
    def flag_abs_delta($t):
        old_side as $old
        | if $old == null then false
          else ((new_side - $old) | if . < 0 then -. else . end) > $t
          end;
    def marked($flagged): delta as $d
        | if $flagged then "**\($d)** (over threshold)" else $d end;
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
'

# The report is captured whole and printed only on success: jq streams its
# output, so an error midway through would otherwise leave a truncated table
# in the step summary under a green `continue-on-error` step.
report=$(jq -r -s --arg base "$baseline_label" \
    --argjson ir_pct "$IR_THRESHOLD_PCT" \
    --argjson blocks_abs "$BLOCKS_THRESHOLD_ABS" \
    --argjson peak_pct "$PEAK_THRESHOLD_PCT" \
    "$jq_defs"'
    "## Instruction counts",
    "",
    "Callgrind instructions (`Ir`) per bench: pull request vs \($base).",
    "Advisory: numbers never block a merge; a bench that stops running does.",
    "A **bold** delta crossed a provisional threshold and syncs the",
    "`affects-performance` label; nothing else happens.",
    "",
    "| Bench | PR | \($base) | Δ |",
    "| --- | ---: | ---: | ---: |",
    (.[] | callgrind as $cg
        | if $cg == null or ($cg | has("Ir") | not)
          then "| \(bench_name) | — | — | *no callgrind profile* |"
          else "| \(bench_name) | \($cg.Ir | new_side) | \($cg.Ir | old_side // "—") | \($cg.Ir | marked(flag_pct_increase($ir_pct))) |"
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
                | (if $key == "TotalBlocks" then flag_abs_delta($blocks_abs)
                   else flag_pct_increase($peak_pct) end) as $flagged
                | "| \($bn) | \($key) | \(new_side) | \(old_side // "—") | \(marked($flagged)) |"))
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

# The verdict is a second pass over the same file with the same definitions,
# not a parse of the rendered markdown: the report is for people, the file is
# for the label workflow, and neither should have to stay grep-compatible
# with the other.
if [[ -n "$regressions_out" ]]; then
    has=$(jq -r -s \
        --argjson ir_pct "$IR_THRESHOLD_PCT" \
        --argjson blocks_abs "$BLOCKS_THRESHOLD_ABS" \
        --argjson peak_pct "$PEAK_THRESHOLD_PCT" \
        "$jq_defs"'
        [ .[]
          | (callgrind as $cg
              | if $cg == null or ($cg | has("Ir") | not) then false
                else ($cg.Ir | flag_pct_increase($ir_pct)) end),
            (dhat as $dh
              | if $dh == null then false
                else (($dh.TotalBlocks | if . == null then false else flag_abs_delta($blocks_abs) end),
                      ($dh.AtTGmaxBytes | if . == null then false else flag_pct_increase($peak_pct) end))
                end)
        ] | any
    ' "$summaries")
    printf '%s\n' "$has" >"$regressions_out"
fi

printf '%s\n' "$report"
