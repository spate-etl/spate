#!/usr/bin/env bash
# Render gungraun's machine-readable summaries as a Markdown report.
#
#   gungraun-report.sh [--regressions-out FILE] <summaries> [<baseline-label>]
#   gungraun-report.sh --self-test
#
# <summaries> is a concatenation of the `summary.json` files one or more CI
# runs wrote under `target/gungraun` (`GUNGRAUN_SAVE_SUMMARY=json`), each a
# single JSON object. Concatenation is the only merge operation.
#
# <baseline-label> names what the old column is for any line that does not name
# its own.
#
# ## Shard identity: the `spate_shard` stamp
#
# A summary carries neither the package nor the feature arm, so over a matrix
# of (package, feature arm) nothing tells two rows apart. Each job stamps its
# own summaries as it collects them, with one object this script reads and
# gungraun never writes:
#
#   {"spate_shard": {"package": "spate-json",
#                    "features": "simd",
#                    "baseline": "main @ 0123456789ab"}}
#
#   package   the cargo package built, for the Shard column.
#   features  the feature arm's label; the empty string (or an absent key)
#             means one unlabelled arm, rendered as the bare package name.
#   baseline  what this job's old column is. The empty string means this job
#             measured no baseline, and its rows read *no baseline* rather
#             than *new*: one job's merge-base leg can fail while another's
#             succeeds. An absent key falls back to <baseline-label>.
#
# Without the stamp, `package` falls back to the last segment of `package_dir`
# and the feature arm is left blank rather than guessed. Two rows that still
# land on one identity are rendered with a warning naming the key they share.
#
# The output is one table of callgrind instruction counts and, when any summary
# carries a DHAT profile, one of heap counts, with every other metric behind a
# <details> fold. `--regressions-out FILE` writes the bare string `true` or
# `false`, the two values `perf-label.yml` acts on. The script always exits 0 on
# a metric moving; the label is the only consequence.
set -euo pipefail

# Bumping the gungraun workspace dependency across a summary-format major
# version silently changes the field paths the jq below walks. Failing on the
# version string turns "the report went quietly blank" into a build error.
SCHEMA_VERSION="6"

# Advisory thresholds. Instructions and peak heap flag on percentage
# *increases* only; the block count flags on an absolute move in either
# direction. Rows with no baseline never flag.
IR_THRESHOLD_PCT=5
BLOCKS_THRESHOLD_ABS=1
PEAK_THRESHOLD_PCT=5

# The write side of the perf-label.yml contract, executable, and the only place
# it runs on a pull request: the flag file must be the bare `true` or `false`
# the label workflow's `case` accepts, and threshold crossings must mark rows.
# The fixtures pin schema v6, so bumping SCHEMA_VERSION fails at the version
# gate until they are rebuilt.
#
# GUNGRAUN_REPORT_UNDER_TEST points the fixtures at another copy of this
# script.
if [[ "${1:-}" == "--self-test" ]]; then
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    under_test="${GUNGRAUN_REPORT_UNDER_TEST:-$0}"
    fail_self() {
        echo "gungraun-report.sh --self-test: $1" >&2
        exit 1
    }
    # Asserts exactly $1 lines contain $2, by count: a merged report's failure
    # mode is a row appearing twice, which presence alone cannot see.
    count_is() {
        local want=$1 needle=$2 desc=$3 got
        got=$(grep -c -F -- "$needle" "$tmp/report.md" || true)
        [[ "$got" -eq "$want" ]] \
            || fail_self "$desc: expected $want line(s) matching '$needle', found $got"
    }
    printf '%s\n' \
        '{"version":"6","package_dir":"/w/crates/spate-json","module_path":"decode_gungraun::decode::decode_value","id":"flat_record","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Both":[{"Int":107000},{"Int":100000}]},"diffs":{"diff_pct":"7.0"}}}}}}},{"tool":"DHAT","summaries":{"total":{"summary":{"Dhat":{"TotalBlocks":{"metrics":{"Both":[{"Int":38},{"Int":40}]},"diffs":{"diff_pct":"-5.0"}},"AtTGmaxBytes":{"metrics":{"Both":[{"Int":4342},{"Int":4096}]},"diffs":{"diff_pct":"6.0"}}}}}}}]}' \
        >"$tmp/hot.jsonl"
    printf '%s\n' \
        '{"version":"6","package_dir":"/w/crates/spate-json","module_path":"decode_gungraun::decode::decode_value","id":"flat_record","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Both":[{"Int":100000},{"Int":99000}]},"diffs":{"diff_pct":"1.0"}}}}}}}]}' \
        >"$tmp/quiet.jsonl"
    # Three jobs of one matrix run. The first two are the same package and the
    # same bench built two ways, indistinguishable without the stamp; the third
    # measured no baseline.
    printf '%s\n' \
        '{"version":"6","spate_shard":{"package":"spate-json","features":"default","baseline":"main @ 0123456789ab"},"package_dir":"/w/crates/spate-json","module_path":"decode_gungraun::decode::decode_value","id":"flat_record","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Both":[{"Int":100000},{"Int":99000}]},"diffs":{"diff_pct":"1.0"}}}}}}}]}' \
        '{"version":"6","spate_shard":{"package":"spate-json","features":"default","baseline":"main @ 0123456789ab"},"package_dir":"/w/crates/spate-json","module_path":"decode_gungraun::decode::decode_value","id":"nested_record","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Left":{"Int":211000}}}}}}}}]}' \
        '{"version":"6","spate_shard":{"package":"spate-json","features":"simd","baseline":"main @ 0123456789ab"},"package_dir":"/w/crates/spate-json","module_path":"decode_gungraun::decode::decode_value","id":"flat_record","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Both":[{"Int":61000},{"Int":60000}]},"diffs":{"diff_pct":"1.67"}}}}}}}]}' \
        '{"version":"6","spate_shard":{"package":"spate-core","features":"default","baseline":""},"package_dir":"/w/crates/spate-core","module_path":"chain_gungraun::chain::forward","id":"one_stage","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Left":{"Int":50000}}}}}}}}]}' \
        >"$tmp/matrix.jsonl"
    # Two jobs that stamped themselves identically: an aggregation bug the
    # report has to name rather than average over.
    printf '%s\n' \
        '{"version":"6","spate_shard":{"package":"spate-json","features":"simd","baseline":"main @ 0123456789ab"},"package_dir":"/w/crates/spate-json","module_path":"decode_gungraun::decode::decode_value","id":"flat_record","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Both":[{"Int":61000},{"Int":60000}]},"diffs":{"diff_pct":"1.67"}}}}}}}]}' \
        '{"version":"6","spate_shard":{"package":"spate-json","features":"simd","baseline":"main @ 0123456789ab"},"package_dir":"/w/crates/spate-json","module_path":"decode_gungraun::decode::decode_value","id":"flat_record","profiles":[{"tool":"Callgrind","summaries":{"total":{"summary":{"Callgrind":{"Ir":{"metrics":{"Both":[{"Int":62000},{"Int":60000}]},"diffs":{"diff_pct":"3.33"}}}}}}}]}' \
        >"$tmp/collide.jsonl"

    "$under_test" --regressions-out "$tmp/flag" "$tmp/hot.jsonl" self-test >"$tmp/report.md"
    [[ "$(cat "$tmp/flag")" == "true" ]] \
        || fail_self "hot fixture: flag file holds '$(cat "$tmp/flag")', not the bare string 'true'"
    grep -q "(over threshold)" "$tmp/report.md" \
        || fail_self "hot fixture: no row carries the over-threshold marker"
    # The unstamped path a single-job run takes: the package still has to be
    # named, from `package_dir`, with no feature arm invented.
    count_is 1 "| spate-json | decode::decode_value flat_record | 107000 |" \
        "hot fixture"

    "$under_test" --regressions-out "$tmp/flag" "$tmp/quiet.jsonl" self-test >"$tmp/report.md"
    [[ "$(cat "$tmp/flag")" == "false" ]] \
        || fail_self "quiet fixture: flag file holds '$(cat "$tmp/flag")', not the bare string 'false'"
    if grep -q "(over threshold)" "$tmp/report.md"; then
        fail_self "quiet fixture: a row is marked over threshold"
    fi

    "$under_test" --regressions-out "$tmp/flag" "$tmp/matrix.jsonl" self-test >"$tmp/report.md"
    [[ "$(cat "$tmp/flag")" == "false" ]] \
        || fail_self "matrix fixture: flag file holds '$(cat "$tmp/flag")', not the bare string 'false'"
    # One bench, two feature arms, two rows that name which is which.
    count_is 1 "| spate-json (default) | decode::decode_value flat_record | 100000 |" \
        "matrix fixture"
    count_is 1 "| spate-json (simd) | decode::decode_value flat_record | 61000 |" \
        "matrix fixture"
    # A job whose merge-base leg failed must not read like a bench that is new,
    # and a new bench in a job that did measure one must not read like a
    # missing baseline.
    count_is 1 "| spate-core (default) | chain::forward one_stage | 50000 | — | *no baseline* |" \
        "matrix fixture"
    count_is 1 "| spate-json (default) | decode::decode_value nested_record | 211000 | — | *new* |" \
        "matrix fixture"
    grep -q "Baseline per shard" "$tmp/report.md" \
        || fail_self "matrix fixture: shards disagree about the baseline and no legend says so"
    if grep -q "Duplicate shard identity" "$tmp/report.md"; then
        fail_self "matrix fixture: distinct shards were reported as a collision"
    fi

    "$under_test" --regressions-out "$tmp/flag" "$tmp/collide.jsonl" self-test >"$tmp/report.md"
    grep -q "Duplicate shard identity" "$tmp/report.md" \
        || fail_self "collision fixture: two identically stamped jobs are reported as one shard, unremarked"

    echo "gungraun-report.sh: self-test ok: the flag file is the bare boolean perf-label.yml parses, markers track the thresholds, and merged jobs keep their shard identity"
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
    echo "::error::usage: $0 [--regressions-out FILE] <summaries> [<baseline-label>]. Needs a non-empty, readable summaries file" >&2
    exit 1
fi
summaries="$1"
baseline_label="${2:-baseline}"

drifted=$(jq -r --arg v "$SCHEMA_VERSION" 'select(.version != $v) | .version' "$summaries" | sort -u | tr '\n' ' ')
drifted="${drifted% }"
if [[ -n "$drifted" ]]; then
    echo "::error::gungraun summary schema is v${drifted}, this report is written against v${SCHEMA_VERSION}. Update scripts/gungraun-report.sh against the new schema before trusting its output" >&2
    exit 1
fi

# Field-path conventions (summary.v6.schema.json): `metrics` is Left/Both/Right
# with Left = the NEW metric and Right = the OLD one; `diffs` is present only
# when both sides are, carrying the percentage as a string, including "inf" and
# "-inf" (zero at the baseline) and "NaN" (zero on both sides), which must not
# reach `tonumber`.
#
# One definition set, two programs, so the flag a row shows and the flag the
# label workflow reads cannot disagree.
# shellcheck disable=SC2016
jq_defs='
    def val: if type == "object" then (.Int // .Float) else . end;
    def new_side: .metrics | (if .Both then .Both[0] else .Left end) | val;
    def old_side: .metrics | (if .Both then .Both[1] elif .Right then .Right else null end)
        | if . == null then null else val end;
    # $absent is what a metric with no comparison reads as, decided per row
    # by whether that row'\''s shard measured a baseline at all.
    def delta($absent):
        if .diffs == null then $absent
        else (.diffs.diff_pct
            | if test("inf") then (if startswith("-") then "-∞%" else "+∞%" end)
              elif . == "NaN" then "n/a"
              else (tonumber | (. * 100 | round) / 100
                    | if . > 0 then "+\(.)%" else "\(.)%" end)
              end)
        end;
    # An increase past $t percent, with the same inf/NaN guards as `delta`.
    # "+inf" counts as flagged; "-inf" and NaN do not; no diff means no baseline.
    def flag_pct_increase($t):
        if .diffs == null then false
        else (.diffs.diff_pct
            | if test("inf") then (startswith("-") | not)
              elif . == "NaN" then false
              else (tonumber >= $t)
              end)
        end;
    # An absolute move past $t in either direction, from the two sides rather
    # than the percentage: a one-block change on any baseline is the same fact.
    def flag_abs_delta($t):
        old_side as $old
        | if $old == null then false
          else ((new_side - $old) | if . < 0 then -. else . end) > $t
          end;
    def marked($flagged; $absent): delta($absent) as $d
        | if $flagged then "**\($d)** (over threshold)" else $d end;
    def bench_name:
        (.module_path | split("::") | .[1:] | join("::"))
        + (if .id != null and .id != "" then " \(.id)" else "" end);
    # Shard identity; see the header.
    def shard_package:
        .spate_shard.package
        // ((.package_dir // "") | split("/") | map(select(. != "")) | last)
        // "unknown";
    def shard_features: .spate_shard.features // "";
    def shard_name:
        shard_package + (shard_features | if . == "" then "" else " (\(.))" end);
    def shard_baseline($fallback): .spate_shard.baseline // $fallback;
    # A bench with no comparison is new if its shard measured a baseline, and
    # uncompared if that shard produced none.
    def absent_label($fallback):
        if shard_baseline($fallback) == "" then "*no baseline*" else "*new*" end;
    def row_key: "\(shard_name) — \(bench_name)";
    # null when the summary carries no callgrind profile at all: rendered as an
    # explicit row, never an error that truncates the report.
    def callgrind:
        [.profiles[] | select(.tool == "Callgrind")][0]
        | if . == null then null else .summaries.total.summary.Callgrind end;
    # null when the summary carries no DHAT profile: the heap table is absent,
    # so the report stays correct against summaries measured before DHAT.
    def dhat:
        [.profiles[] | select(.tool == "DHAT")][0]
        | if . == null then null else .summaries.total.summary.Dhat end;
'

# Captured whole and printed only on success: jq streams, so an error midway
# would leave a truncated table under a green `continue-on-error` step.
report=$(jq -r -s --arg base "$baseline_label" \
    --argjson ir_pct "$IR_THRESHOLD_PCT" \
    --argjson blocks_abs "$BLOCKS_THRESHOLD_ABS" \
    --argjson peak_pct "$PEAK_THRESHOLD_PCT" \
    "$jq_defs"'
    # Sorted once, up front: jobs are merged by concatenation and artifacts
    # download in no particular order, so input order is not an order.
    sort_by([shard_name, bench_name]) as $rows
    | ([$rows[] | shard_baseline($base)] | unique) as $bases
    # One column header can only name one baseline. When jobs disagree it goes
    # generic and a legend below carries the per-job labels.
    | (if ($bases | length) == 1
       then ($bases[0] | if . == "" then "no baseline" else . end)
       else "baseline" end) as $base_header
    | ([$rows[] | row_key] | group_by(.) | map(select(length > 1) | .[0])) as $dupes
    | "## Instruction counts",
    "",
    "Callgrind instructions (`Ir`) per bench: pull request vs \($base_header).",
    "Advisory: numbers never block a merge; a bench that stops running does.",
    "A **bold** delta crossed a provisional threshold and syncs the",
    "`affects-performance` label; nothing else happens.",
    (if ($dupes | length) > 0 then
        "",
        "**Duplicate shard identity**: \($dupes | map("`\(.)`") | join(", ")) "
            + "appears more than once. Either two jobs stamped themselves alike, "
            + "or one package has two bench files whose group, bench and case names "
            + "coincide (the bench-file stem is not part of the name). Either way "
            + "the rows below cannot be told apart."
     else empty end),
    (if ($bases | length) > 1 then
        "",
        "Baseline per shard:",
        ($rows | map({s: shard_name, b: shard_baseline($base)}) | unique | .[]
            | "- `\(.s)` — \(.b | if . == "" then "*none measured*" else . end)")
     else empty end),
    "",
    "| Shard | Bench | PR | \($base_header) | Δ |",
    "| --- | --- | ---: | ---: | ---: |",
    ($rows[] | absent_label($base) as $absent | shard_name as $sh | callgrind as $cg
        | if $cg == null or ($cg | has("Ir") | not)
          then "| \($sh) | \(bench_name) | — | — | *no callgrind profile* |"
          else "| \($sh) | \(bench_name) | \($cg.Ir | new_side) | \($cg.Ir | old_side // "—") | \($cg.Ir | marked(flag_pct_increase($ir_pct); $absent)) |"
          end),
    (if any($rows[]; dhat != null) then
        "",
        "## Heap (DHAT)",
        "",
        "DHAT heap blocks and peak bytes per bench: pull request vs \($base_header).",
        "",
        "| Shard | Bench | Metric | PR | \($base_header) | Δ |",
        "| --- | --- | --- | ---: | ---: | ---: |",
        ($rows[] | dhat as $dh
            | select($dh != null)
            | absent_label($base) as $absent
            | shard_name as $sh
            | bench_name as $bn
            | (["TotalBlocks", "AtTGmaxBytes"][] as $key
                | $dh[$key]
                | select(. != null)
                | (if $key == "TotalBlocks" then flag_abs_delta($blocks_abs)
                   else flag_pct_increase($peak_pct) end) as $flagged
                | "| \($sh) | \($bn) | \($key) | \(new_side) | \(old_side // "—") | \(marked($flagged; $absent)) |"))
    else empty end),
    "",
    "<details><summary>All metrics</summary>",
    "",
    ($rows[] | absent_label($base) as $absent | shard_name as $sh | callgrind as $cg
        | select($cg != null)
        | "**\($sh) — \(bench_name)** — callgrind",
          "",
          "| Metric | PR | \($base_header) | Δ |",
          "| --- | ---: | ---: | ---: |",
          ($cg | to_entries[] | "| \(.key) | \(.value | new_side) | \(.value | old_side // "—") | \(.value | delta($absent)) |"),
          ""),
    ($rows[] | absent_label($base) as $absent | shard_name as $sh | dhat as $dh
        | select($dh != null)
        | "**\($sh) — \(bench_name)** — DHAT",
          "",
          "| Metric | PR | \($base_header) | Δ |",
          "| --- | ---: | ---: | ---: |",
          ($dh | to_entries[] | "| \(.key) | \(.value | new_side) | \(.value | old_side // "—") | \(.value | delta($absent)) |"),
          ""),
    "</details>"
' "$summaries")

# A second pass over the same file with the same definitions, not a parse of
# the rendered markdown.
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
