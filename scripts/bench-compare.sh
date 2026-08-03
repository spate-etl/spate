#!/usr/bin/env bash
# Compare two sets of benchmark records and render the difference as Markdown.
#
#   bench-compare.sh [--verdict-out FILE] <base.jsonl> <head.jsonl> \
#       [<base-label>] [<head-label>]
#   bench-compare.sh --self-test
#
# Both inputs are the JSONL `benchmarks/src/report.rs` emits: one record per
# line, one arm per record. The two legs are two builds of the same rigs, run
# alternately over one corpus — see `docs/benchmarks/methodology.mdx`.
#
# ## What it is for
#
# Wall-clock numbers cannot gate a pull request, so this never exits non-zero
# because a metric moved. `--verdict-out` writes the bare string `true` or
# `false` for a caller that wants to act on it. The exit status answers a
# different question: whether the comparison could be made at all.
#
# ## Pairing, which is where a comparator hides its worst failure
#
# A report can render a flawless table off a comparison that matched the wrong
# records. Four hazards, each measured against the committed archive rather than
# assumed:
#
#   - **Identity is the whole variant map.** One `bench` name carries different
#     variant key sets across the archive — `deser_formats` has four, with and
#     without `backend`, with and without `events`. Pairing on a subset merges
#     arms that differ; pairing on anything absent from the record is not
#     computable. So identity is `bench` plus every variant key and value, which
#     is also how the documentation site groups records.
#   - **A record with no partner is reported, never dropped.** A scenario the
#     head deleted and one it added are both findings. And if a change adds a
#     variant key, *every* identity becomes unpairable at once — which the report
#     says at the top, rather than rendering an empty table that reads as "no
#     change".
#   - **A metric may exist on one side only.** Five identities in the archive
#     already carry more than one metric key set, and a newly added metric is on
#     the head side alone by construction.
#   - **Repetitions pair by position.** Records append in run order, so within
#     one identity the k-th record of a leg is its k-th repetition. Pairing on
#     anything else silently compares one repetition against another.
#
# ## The verdict
#
# A row moved when BOTH hold: the paired mean difference is at least the
# metric's threshold in magnitude, and the 95% Student-t interval on the
# per-repetition differences excludes zero. Both inputs come from one
# comparison, so this needs no history — which matters, because there is none to
# learn a noise floor from.
#
# Paired rather than two-sample because the legs are interleaved: repetition i
# of each arm ran adjacent in time, so drift cancels within the pair.
set -euo pipefail

SCHEMA_VERSION=1

# Advisory thresholds, as a fraction of the base mean, applied per BENCH rather
# than per metric. Deliberately wider than a quiet machine needs: they are
# provisional, and a threshold set too tight turns an advisory signal into a
# rerun button. `docs/benchmarks/methodology.mdx` states these numbers, so the
# two move together.
DEFAULT_THRESHOLD='0.05'
# Per-bench overrides. Rebalancing is inherently noisier than a bounded backfill.
BENCH_THRESHOLDS='{"s3_backfill_coordinated": 0.10}'
# Rendered, never judged. Two kinds sit here for two reasons.
#
# `peak_rss_mb` is a high-water mark that moves for allocator reasons unrelated
# to the change — measured on the synthetic pipeline rig at roughly three times
# the coefficient of variation of that rig own throughput numbers.
#
# The rest are workload constants: how many records a bounded run processed, how
# many rows it wrote. They describe the job, not its speed, and a run that
# processed 7% more records is a different run rather than a slower one. Graded
# at the same 5% as a timing they would dominate the headline, because an
# archive record carries up to sixteen metrics and most of them are counts.
INFORMATIONAL='["peak_rss_mb","records_total","rows_written_total","produced_total","consumed","commits","records","sink_records","sink_rows_total","rows_in_clickhouse","ch_written_rows","tx_messages"]'

# Student-t critical values t(df, 0.975) for df 1..30, then t(30) beyond. The
# same table as `benchmarks::stats`, for the same reason: at five repetitions
# 1.96 is optimistic by a wide margin, and an interval that is too narrow is
# what turns noise into an apparent result.
T975='[12.706,4.303,3.182,2.776,2.571,2.447,2.365,2.306,2.262,2.228,2.201,2.179,2.160,2.145,2.131,2.120,2.110,2.101,2.093,2.086,2.080,2.074,2.069,2.064,2.060,2.056,2.052,2.048,2.045,2.042]'

# The comparison itself: two slurped files in, one JSON summary out. Rendering
# and the verdict are both read off this, so a row's marker and the flag a
# caller acts on cannot disagree.
#
# No literal single quote may appear in this program — it would close the shell
# string holding it and leave a syntax error that reads as the shell's fault.
# shellcheck disable=SC2016  # jq variables, deliberately unexpanded
ANALYSE='
def finite: type == "number" and (isinfinite | not) and (isnan | not);

# Identity is the bench plus the whole variant map, with keys sorted. Sorting
# matters: jq preserves insertion order, so two records describing the same arm
# with their keys written in a different order would otherwise be two
# identities and pair with nothing. Rust writes a BTreeMap and is already
# sorted; a hand-edited or foreign leg need not be.
def canonical_variant: (.variant // {}) | to_entries | sort_by(.key) | from_entries;
def identity: "\(.bench) \(canonical_variant | tojson)";
def pretty:
    (canonical_variant | to_entries | map("\(.key)=\(.value | tostring)") | join(" ")) as $v
    | if $v == "" then .bench else "\(.bench) · \($v)" end;

# (mean, half-width of the 95% Student-t interval, n) over $xs.
def interval($xs):
    ($xs | length) as $n
    | (($xs | add) / $n) as $m
    | if $n < 2 then {mean: $m, half: null, n: $n}
      else
        (($xs | map(pow(. - $m; 2)) | add) / ($n - 1)) as $var
        | (($var / $n) | sqrt) as $sem
        | (if ($n - 1) <= ($t975 | length) then $t975[$n - 2] else $t975[-1] end) as $crit
        | {mean: $m, half: ($crit * $sem), n: $n}
      end;

# Measurements of one schema version. A verdict record is not an arm — it has no
# metrics and its variant keys describe a conclusion — and one sitting in a leg
# would pair with itself, make the comparison look non-empty, and suppress the
# banner that says nothing could be paired. `clickhouse-native-format.jsonl`
# carries exactly such a record today.
def of_schema: map(select(type == "object" and .schema == $schema));
# A record with no `bench` has no identity worth pairing on — it would render a
# scenario literally named `null` and silently compare unrelated runs.
def keep: of_schema | map(select(.kind == "measurement" and (.bench | type) == "string"));

(($base | keep)  | group_by(identity)) as $b
| (($head | keep) | group_by(identity)) as $h
| ($b | map({key: (.[0] | identity), value: .}) | from_entries) as $bi
| ($h | map({key: (.[0] | identity), value: .}) | from_entries) as $hi
| (($bi | keys) + ($hi | keys) | unique) as $ids
| [ $ids[] as $id
    | ($bi[$id] // []) as $bg
    | ($hi[$id] // []) as $hg
    | (($hg[0] // $bg[0]) | pretty) as $name
    | (($hg[0] // $bg[0]) | .bench) as $bench
    | if ($bg | length) == 0 then
        {kind: "head_only", name: $name, bench: $bench}
      elif ($hg | length) == 0 then
        {kind: "base_only", name: $name, bench: $bench}
      else
        ((($bg | map(.metrics | keys)) + ($hg | map(.metrics | keys)))
            | add | unique) as $metrics
        | {kind: "paired", name: $name, bench: $bench,
           metrics: [ $metrics[] as $mk
             # Each usable sample keeps the index of the record it came from,
             # which IS its repetition number: records append in run order, so
             # the k-th record of a leg is its k-th repetition. Pairing then
             # joins on that index rather than on position in a filtered list.
             # Filtering first and indexing afterwards is what makes "the k-th
             # usable value" stop meaning "the k-th repetition" — one dropped
             # sample shifts every later one against its partner and fabricates
             # a difference that never happened.
             | [$bg | to_entries[] | select(.value.metrics[$mk].value | finite) | {i: .key, v: .value.metrics[$mk].value}] as $ball
             | [$hg | to_entries[] | select(.value.metrics[$mk].value | finite) | {i: .key, v: .value.metrics[$mk].value}] as $hall
             | ($ball | map(.i)) as $bidx
             | ($hall | map(.i)) as $hidx
             | [$ball[] | select(.i as $i | $hidx | index($i)) | .v] as $bv
             | [$hall[] | select(.i as $i | $bidx | index($i)) | .v] as $hv
             # The unit and direction come from any record carrying the metric,
             # not from record zero. A metric that first appears on a later
             # repetition — which happens in this archive — would otherwise take
             # its direction from a default, and a throughput gain would render
             # as a regression.
             | (([$bg[], $hg[]] | map(.metrics[$mk]) | map(select(. != null)))[0]) as $proto
             | if ($bv | length) == 0 then
                 {metric: $mk, state: "one_side_only",
                  side: (if ($ball | length) == 0 then "head" else "base" end),
                  unit: ($proto.unit),
                  value: (if ($ball | length) == 0
                          then (if ($hall | length) == 0 then null else (($hall | map(.v) | add) / ($hall | length)) end)
                          else (($ball | map(.v) | add) / ($ball | length)) end)}
               else
                 interval($bv) as $bs
                 | interval($hv) as $hs
                 | interval([range(0; $bv | length) | $hv[.] - $bv[.]]) as $d
                 | (if $bs.mean == 0 then null else $d.mean / ($bs.mean | fabs) end) as $rel
                 | (if ($informational | index($mk)) != null then null
                    else ($thresholds[$bench] // $default) end) as $thr
                 | {metric: $mk, state: "paired", unit: $proto.unit,
                    higher_is_better: ($proto.higher_is_better // false),
                    base: $bs, head: $hs, diff: $d, rel: $rel,
                    threshold: $thr,
                    # Repetitions either leg captured that the other did not.
                    # Counts alignment, not length: two legs can lose the same
                    # number of samples at different repetitions and pair
                    # nothing correctly while their lengths match.
                    discarded: ((($ball | length) - ($bv | length))
                                + (($hall | length) - ($hv | length))),
                    # A relative change needs a non-zero base. Without one the
                    # row cannot be judged — which is not the same as steady,
                    # and filing it as steady is how a metric that went from
                    # nothing to something reads as unchanged. 107 records in
                    # the archive carry `backpressure_pauses` at zero.
                    judgeable: (($bv | length) >= 2 and $bs.mean != 0),
                    moved: (
                      if $thr == null or $rel == null or $d.half == null then false
                      else (($rel | fabs) >= $thr) and (($d.mean | fabs) > $d.half)
                      end)}
               end ]}
      end ]
| {rows: .,
   base_records: (($base | keep) | length),
   head_records: (($head | keep) | length),
   base_foreign: (($base | length) - (($base | of_schema) | length)),
   head_foreign: (($head | length) - (($head | of_schema) | length)),
   base_nonmeasurement: ((($base | of_schema) | length) - (($base | keep) | length)),
   head_nonmeasurement: ((($head | of_schema) | length) - (($head | keep) | length)),
   paired: ([.[] | select(.kind == "paired")] | length),
   base_only: ([.[] | select(.kind == "base_only")] | length),
   head_only: ([.[] | select(.kind == "head_only")] | length)}
| .moved = ([.rows[] | select(.kind == "paired") | .metrics[] | select(.moved)] | length)
| .moved_scenarios = ([.rows[] | select(.kind == "paired")
                       | select([.metrics[] | select(.moved)] | length > 0)] | length)
'

analyse() { # base.jsonl head.jsonl
    jq -n --slurpfile base "$1" --slurpfile head "$2" \
        --argjson schema "$SCHEMA_VERSION" \
        --argjson t975 "$T975" \
        --argjson thresholds "$BENCH_THRESHOLDS" \
        --argjson informational "$INFORMATIONAL" \
        --argjson default "$DEFAULT_THRESHOLD" \
        "$ANALYSE"
}

# ---------------------------------------------------------------------------

if [[ "${1:-}" == "--self-test" ]]; then
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    fail_self() {
        printf 'bench-compare.sh --self-test: %s\n' "$1" >&2
        exit 1
    }
    # SPATE_BENCH_COMPARE_UNDER_TEST points the fixtures at another copy of this
    # script, which is how a reviewer confirms a fixture is load-bearing by
    # running it against a revision that should not pass it.
    under_test="${SPATE_BENCH_COMPARE_UNDER_TEST:-$0}"

    out=""
    err=""
    rc=0
    # Captured, never piped into grep: under `pipefail` a non-zero exit fails the
    # pipeline whether or not grep matched, so the assertion would pass for the
    # wrong reason and keep passing if the output were deleted.
    #
    # stdout and stderr are captured SEPARATELY, and the report assertions read
    # stdout alone. Merging them passes a script that writes its whole report to
    # stderr — which renders `make bench-compare > report.md` an empty file with
    # every fixture green.
    compare() { # dir [extra args...]
        local d=$1
        shift
        set +e
        # Inside $tmp, which the EXIT trap removes. A `mktemp` here would
        # leak one file per call into the system temp directory, because the
        # path is overwritten by the contents on the next line.
        out=$("$under_test" "$@" "$d/base.jsonl" "$d/head.jsonl" "main@abc1234" "pr@def5678" 2>"$tmp/stderr")
        rc=$?
        set -e
        err=$(cat "$tmp/stderr")
    }
    both() { printf '%s\n%s' "$out" "$err"; }
    says() { grep -qF -- "$1" <<<"$out" || fail_self "$2: wanted [$1] on stdout:
$out"; }
    row_says() { # row-needle mark description
        local line
        # `grep | head` would die 141 on a large report under `pipefail`, which
        # is the masking shape this repository bans. Take the first match with
        # grep -m1 and no pipe at all.
        line=$(grep -m1 -F -- "$1" <<<"$out" || true)
        [[ -n "$line" ]] || fail_self "$3: no row matching [$1] in:
$out"
        grep -qF -- "$2" <<<"$line" \
            || fail_self "$3: row [$1] lacks [$2]:
$line"
    }
    row_denies() { # row-needle mark description
        local line
        # `grep | head` would die 141 on a large report under `pipefail`, which
        # is the masking shape this repository bans. Take the first match with
        # grep -m1 and no pipe at all.
        line=$(grep -m1 -F -- "$1" <<<"$out" || true)
        [[ -n "$line" ]] || fail_self "$3: no row matching [$1] in:
$out"
        if grep -qF -- "$2" <<<"$line"; then
            fail_self "$3: row [$1] carries [$2]:
$line"
        fi
    }
    says_anywhere() { grep -qF -- "$1" <<<"$(both)" || fail_self "$2: wanted [$1] in output:
$(both)"; }
    denies() { ! grep -qF -- "$1" <<<"$out" || fail_self "$2: did not want [$1] in output:
$out"; }

    # value-only record helper: bench, variant JSON, metric name, value
    rec() {
        printf '{"schema":1,"bench":"%s","kind":"measurement","trigger":"dispatched","run":{"ts_ms":0,"host":"box","cpu":"x","cores":8,"os":"linux/aarch64","profile":"release"},"variant":%s,"metrics":{"%s":{"value":%s,"unit":"s","higher_is_better":false}}}\n' \
            "$1" "$2" "$3" "$4"
    }
    case_dir() { local d="$tmp/$1"; mkdir -p "$d"; printf '%s' "$d"; }

    # --- The wrong answers first. -----------------------------------------

    # 1. A variant key added on the head side makes every identity unpairable.
    #    The failure this guards is an empty table that reads as "no change".
    d=$(case_dir added-variant-key)
    for v in 10 11 12; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    for v in 10 11 12; do rec s3_backfill '{"codec":"none","threads":2}' wall_s "$v" >>"$d/head.jsonl"; done
    compare "$d"
    [[ "$rc" -eq 0 ]] || fail_self "an unpairable comparison must still render (rc=$rc)"
    says "Nothing could be paired" "added variant key"
    says "codec=none threads=2" "added variant key: the head identity is named"

    # 2. A metric present on one side only — guaranteed by any newly added
    #    metric, and true of `peak_rss_mb` against every committed record.
    d=$(case_dir metric-one-side)
    for v in 10 11 12; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    for v in 10 11 12; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/head.jsonl"; done
    printf '{"schema":1,"bench":"s3_backfill","kind":"measurement","trigger":"dispatched","run":{"ts_ms":0,"host":"box","cpu":"x","cores":8,"os":"linux/aarch64","profile":"release"},"variant":{"codec":"none"},"metrics":{"peak_rss_mb":{"value":120,"unit":"MB","higher_is_better":false}}}\n' >>"$d/head.jsonl"
    compare "$d"
    says "peak_rss_mb" "metric on one side only is named"
    says "**head only**" "metric on one side only is labelled"

    # 3. A scenario deleted, and a scenario added.
    d=$(case_dir scenario-added-and-removed)
    for v in 10 11; do rec s3_backfill '{"codec":"gzip"}' wall_s "$v" >>"$d/base.jsonl"; done
    for v in 10 11; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    for v in 10 11; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/head.jsonl"; done
    for v in 10 11; do rec s3_backfill '{"codec":"zstd"}' wall_s "$v" >>"$d/head.jsonl"; done
    compare "$d"
    says "codec=gzip" "a deleted scenario is reported"
    says "codec=zstd" "an added scenario is reported"

    # 4. Unequal repetition counts: pair what can be paired, say what was not.
    d=$(case_dir unequal-reps)
    for v in 10 11 12 13 14; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    for v in 10 11 12; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/head.jsonl"; done
    compare "$d"
    says "discarded 2 repetition" "unequal counts are disclosed"

    # 5. A leg with no records at all.
    d=$(case_dir empty-leg)
    for v in 10 11; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    : >"$d/head.jsonl"
    compare "$d"
    [[ "$rc" -ne 0 ]] || fail_self "an empty leg must fail rather than render an empty comparison"
    says_anywhere "is missing, unreadable or empty" "an empty file names which leg it was"

    # A file with content, none of it usable. The empty-file check above cannot
    # see this one, and rendering it as a comparison of nothing would read as
    # "no change".
    d=$(case_dir leg-with-no-usable-records)
    for v in 10 11; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    printf '{"schema":99,"bench":"x","kind":"measurement","variant":{},"metrics":{}}\n' >>"$d/head.jsonl"
    compare "$d"
    [[ "$rc" -ne 0 ]] || fail_self "a leg whose records are all unusable must fail"
    says_anywhere "holds no records" "a leg with no usable records says why"

    # 6. Non-finite values must not reach the arithmetic.
    d=$(case_dir non-finite)
    for v in 10 11; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    rec s3_backfill '{"codec":"none"}' wall_s 10 >>"$d/head.jsonl"
    printf '{"schema":1,"bench":"s3_backfill","kind":"measurement","trigger":"dispatched","run":{"ts_ms":0,"host":"box","cpu":"x","cores":8,"os":"linux/aarch64","profile":"release"},"variant":{"codec":"none"},"metrics":{"wall_s":{"value":null,"unit":"s","higher_is_better":false}}}\n' >>"$d/head.jsonl"
    compare "$d"
    [[ "$rc" -eq 0 ]] || fail_self "a non-finite value must be skipped, not fatal (rc=$rc)"
    denies "null" "a non-finite value never reaches the table"

    # 7. A record from another schema version is dropped and counted.
    d=$(case_dir foreign-schema)
    for v in 10 11; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    for v in 10 11; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/head.jsonl"; done
    printf '{"schema":99,"bench":"s3_backfill","kind":"measurement","variant":{},"metrics":{}}\n' >>"$d/head.jsonl"
    compare "$d"
    says "1 record(s) skipped as another schema" "a foreign-schema record is counted"

    # 8. Misalignment. Every repetition the two legs BOTH captured is
    #    identical, and only base repetition 1 is missing its value. Pairing on
    #    position in a filtered list would compare head rep 1 against base rep
    #    2 and report a large, confident, entirely fabricated move.
    d=$(case_dir misaligned-hole)
    rec_null() { printf '{"schema":1,"bench":"s3_backfill","kind":"measurement","trigger":"dispatched","run":{"ts_ms":0,"host":"box","cpu":"x","cores":8,"os":"linux/aarch64","profile":"release"},"variant":{"codec":"none"},"metrics":{"wall_s":{"value":null,"unit":"s","higher_is_better":false}}}\n'; }
    rec_null >>"$d/base.jsonl"
    for v in 10 20 30; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    for v in 5 10 20 30; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/head.jsonl"; done
    compare "$d" --verdict-out "$d/flag"
    [[ "$(cat "$d/flag")" == "false" ]] \
        || fail_self "a hole in one leg must not fabricate a move: repetitions 2-4 are identical"
    says "discarded 1 repetition" "the unpaired repetition is disclosed"

    # 9. Equal-length legs that captured DIFFERENT repetitions. Nothing pairs
    #    correctly, and length equality hides it — so the disclosure must key on
    #    alignment, not on counts matching.
    d=$(case_dir misaligned-shifted)
    for v in 10 11 12; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    rec_null >>"$d/base.jsonl"; rec_null >>"$d/base.jsonl"
    rec_null >>"$d/head.jsonl"; rec_null >>"$d/head.jsonl"
    for v in 12 13 14; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/head.jsonl"; done
    compare "$d" --verdict-out "$d/flag"
    says "one repetition" "no repetition pairs, so nothing is judgeable"
    [[ "$(cat "$d/flag")" == "false" ]] \
        || fail_self "legs that share no repetition must not report a move"

    # 10. A verdict record is not an arm. One sitting in a leg used to pair with
    #     itself, make the comparison look non-empty and suppress the banner.
    d=$(case_dir verdict-record)
    for v in 10 11 12; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    for v in 10 11 12; do rec s3_backfill '{"codec":"none","threads":2}' wall_s "$v" >>"$d/head.jsonl"; done
    for leg in base head; do
        printf '{"schema":1,"bench":"ch_native_format","kind":"verdict","trigger":"dispatched","run":{"ts_ms":0,"host":"box","cpu":"x","cores":8,"os":"linux/aarch64","profile":"release"},"variant":{"client_ok":false},"metrics":{}}\n' >>"$d/$leg.jsonl"
    done
    compare "$d"
    says "Nothing could be paired" "a verdict record must not suppress the banner"

    # 11. Direction is read from the record. A throughput gain is an
    #     improvement even though the number went up.
    d=$(case_dir direction)
    up() { printf '{"schema":1,"bench":"s3_backfill","kind":"measurement","trigger":"dispatched","run":{"ts_ms":0,"host":"box","cpu":"x","cores":8,"os":"linux/aarch64","profile":"release"},"variant":{"codec":"none"},"metrics":{"records_per_s":{"value":%s,"unit":"records/s","higher_is_better":true}}}\n' "$1"; }
    for v in 100 101 99 100.5 99.5; do up "$v" >>"$d/base.jsonl"; done
    for v in 150 151 149 150.5 149.5; do up "$v" >>"$d/head.jsonl"; done
    compare "$d"
    # Asserted on the row, not on the whole report: the explanation block names
    # both marks, so a document-wide ban would match its own legend.
    row_says "records_per_s" "✅" "a throughput gain renders as an improvement"
    row_denies "records_per_s" "❌" "a throughput gain must not render as a regression"

    # 12. The per-bench threshold override is live. 7% moves the default rig
    #     and does not move the one whose floor is 10%.
    for bench in s3_backfill s3_backfill_coordinated; do
        d=$(case_dir "threshold-$bench")
        for v in 10.00 10.01 9.99 10.005 9.995; do rec "$bench" '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
        for v in 10.70 10.71 10.69 10.705 10.695; do rec "$bench" '{"codec":"none"}' wall_s "$v" >>"$d/head.jsonl"; done
        compare "$d" --verdict-out "$d/flag"
        want=true; [[ "$bench" == s3_backfill_coordinated ]] && want=false
        [[ "$(cat "$d/flag")" == "$want" ]] \
            || fail_self "a 7% shift on $bench: wanted verdict $want, got $(cat "$d/flag")"
    done

    # 13. One repetition cannot be judged, and must not be filed as steady — a
    #     doubled wall clock reading as "within resolution" is the worst
    #     possible way to be wrong. `REPS` defaults to 1, so this is the common
    #     case rather than an edge one.
    d=$(case_dir single-repetition)
    rec s3_backfill '{"codec":"none"}' wall_s 10 >>"$d/base.jsonl"
    rec s3_backfill '{"codec":"none"}' wall_s 20 >>"$d/head.jsonl"
    compare "$d"
    says "Not judgeable" "a single repetition is reported as unjudgeable"
    denies "Within resolution" "a single repetition must not read as steady"

    # 14. The report goes to stdout. A script that wrote it to stderr would
    #     leave `make bench-compare > report.md` empty with CI green.
    d=$(case_dir stdout)
    for v in 10 11 12; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    for v in 10 11 12; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/head.jsonl"; done
    compare "$d"
    says "spate-wallclock-report" "the report marker is on stdout"
    [[ "$rc" -eq 0 ]] || fail_self "a clean comparison must exit 0 (rc=$rc)"

    # 15. `--verdict-out` is only recognised before the filenames. Accepting it
    #     silently as a label wrote no flag and put a filename in the header.
    d=$(case_dir verdict-out-misplaced)
    for v in 10 11; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    for v in 10 11; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/head.jsonl"; done
    set +e
    "$under_test" "$d/base.jsonl" "$d/head.jsonl" --verdict-out "$d/flag" >/dev/null 2>&1
    misplaced_rc=$?
    set -e
    [[ "$misplaced_rc" -ne 0 ]] \
        || fail_self "--verdict-out after the filenames must be refused, not read as a label"

    # 16. Reordering either leg before pairing must change the answer. The
    #      mean of the differences is invariant under sorting, so only the
    #      interval can catch it: paired, these differences average +3 and
    #      scatter far wider; sorted, every difference is exactly +3 and the
    #      interval collapses to nothing.
    d=$(case_dir order-sensitive)
    for v in 10 20 30; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    for v in 33 13 23; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/head.jsonl"; done
    compare "$d" --verdict-out "$d/flag"
    [[ "$(cat "$d/flag")" == "false" ]] \
        || fail_self "differences that scatter must not move, however they would sort"

    # 17. The same arm written with its variant keys in a different order is
    #     the same arm. jq preserves insertion order, so without canonicalising
    #     these become two identities and pair with nothing.
    d=$(case_dir variant-key-order)
    for v in 10 11 12; do rec s3_backfill '{"codec":"none","threads":2}' wall_s "$v" >>"$d/base.jsonl"; done
    for v in 10 11 12; do rec s3_backfill '{"threads":2,"codec":"none"}' wall_s "$v" >>"$d/head.jsonl"; done
    compare "$d"
    denies "Nothing could be paired" "key order must not split one arm into two identities"
    says "1 scenario(s) paired" "the reordered arm pairs with its partner"

    # 18. A metric that first appears on a later repetition still takes its
    #     direction from the record. Reading record zero alone would default to
    #     lower-is-better and render a throughput gain as a regression — and
    #     `ch_sink_saturation` has exactly this shape in the archive, with
    #     `ch_async_flushes` absent from the first record.
    d=$(case_dir direction-late-metric)
    for leg in base head; do rec s3_backfill '{"codec":"none"}' wall_s 10 >>"$d/$leg.jsonl"; done
    late() { printf '{"schema":1,"bench":"s3_backfill","kind":"measurement","trigger":"dispatched","run":{"ts_ms":0,"host":"box","cpu":"x","cores":8,"os":"linux/aarch64","profile":"release"},"variant":{"codec":"none"},"metrics":{"records_per_s":{"value":%s,"unit":"records/s","higher_is_better":true}}}\n' "$1"; }
    for v in 100 101 99 100.5; do late "$v" >>"$d/base.jsonl"; done
    for v in 150 151 149 150.5; do late "$v" >>"$d/head.jsonl"; done
    compare "$d"
    row_denies "records_per_s" "❌" "direction must come from a record that carries the metric"

    # 19. A metric whose base is zero cannot yield a relative change, and must
    #     not be filed as steady. `backpressure_pauses` reads zero in a healthy
    #     run and non-zero when something regresses — 107 archive records carry
    #     it at zero — so "0 became 41000" reading as "within resolution" is the
    #     single worst answer this tool could give.
    d=$(case_dir zero-base)
    zero() { printf '{"schema":1,"bench":"s3_backfill","kind":"measurement","trigger":"dispatched","run":{"ts_ms":0,"host":"box","cpu":"x","cores":8,"os":"linux/aarch64","profile":"release"},"variant":{"codec":"none"},"metrics":{"backpressure_pauses":{"value":%s,"unit":"events","higher_is_better":false}}}\n' "$1"; }
    for v in 0 0 0; do zero "$v" >>"$d/base.jsonl"; done
    for v in 41000 41100 40900; do zero "$v" >>"$d/head.jsonl"; done
    compare "$d"
    denies "Within resolution" "a rise from zero must not be filed as steady"
    says "Not judgeable" "a rise from zero is reported as unjudgeable"

    # 20. When nothing can be judged, the headline must not claim nothing moved.
    #     Every committed deser_formats identity holds one record, so comparing
    #     two such legs judges nothing at all.
    d=$(case_dir nothing-judgeable)
    rec s3_backfill '{"codec":"none"}' wall_s 10 >>"$d/base.jsonl"
    rec s3_backfill '{"codec":"none"}' wall_s 20 >>"$d/head.jsonl"
    compare "$d"
    says "Nothing could be judged" "an unjudgeable comparison says so in the headline"
    denies "No scenario moved" "an unjudgeable comparison must not claim nothing moved"

    # 21. Disclosure keys on alignment. Both legs lose two repetitions here, so
    #     lengths match and only an alignment-based count sees it.
    d=$(case_dir discarded-counts-alignment)
    for v in 10 11 12; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    rec_null >>"$d/base.jsonl"; rec_null >>"$d/base.jsonl"
    rec_null >>"$d/head.jsonl"; rec_null >>"$d/head.jsonl"
    for v in 12 13 14; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/head.jsonl"; done
    compare "$d"
    says "discarded 4 repetition" "the count must be of unaligned samples, not of length difference"

    # 22. The headline distinguishes scenarios from metric rows. One scenario
    #     with three moved metrics is one scenario, not three.
    d=$(case_dir headline-counts)
    three() { printf '{"schema":1,"bench":"s3_backfill","kind":"measurement","trigger":"dispatched","run":{"ts_ms":0,"host":"box","cpu":"x","cores":8,"os":"linux/aarch64","profile":"release"},"variant":{"codec":"none"},"metrics":{"wall_s":{"value":%s,"unit":"s","higher_is_better":false},"decoded_mb_per_s":{"value":%s,"unit":"MB/s","higher_is_better":true},"stored_mb_per_s":{"value":%s,"unit":"MB/s","higher_is_better":true}}}\n' "$1" "$2" "$3"; }
    for v in 10.0 10.1 9.9; do three "$v" 100 100 >>"$d/base.jsonl"; done
    for v in 13.0 13.1 12.9; do three "$v" 70 70 >>"$d/head.jsonl"; done
    compare "$d"
    says "1 scenario(s) moved" "the headline counts scenarios"
    says "3 metric(s)" "the headline also reports how many metric rows moved"

    # 23. A verdict record is same-schema, so calling it a schema mismatch sends
    #     a reader to check a version that is not the problem.
    d=$(case_dir verdict-not-schema)
    for v in 10 11; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    for v in 10 11; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/head.jsonl"; done
    printf '{"schema":1,"bench":"ch_native_format","kind":"verdict","trigger":"dispatched","run":{"ts_ms":0,"host":"box","cpu":"x","cores":8,"os":"linux/aarch64","profile":"release"},"variant":{"client_ok":false},"metrics":{}}\n' >>"$d/head.jsonl"
    compare "$d"
    says "1 verdict record(s) skipped" "a verdict record is named as one"
    denies "1 record(s) skipped as another schema" "a verdict record is not a schema mismatch"

    # 24. The steady section exists. Deleting it entirely left every fixture
    #     green, because nothing asserted a within-resolution row is rendered.
    d=$(case_dir steady-rendered)
    for v in 10.00 10.01 9.99; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    for v in 10.02 10.03 9.98; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/head.jsonl"; done
    compare "$d"
    says "Within resolution" "a judged row that did not move is still rendered"

    # --- Then the happy path. ---------------------------------------------

    # 25. A real move: 20% slower, well outside the interval.
    d=$(case_dir moved)
    for v in 10.0 10.1 9.9 10.05 9.95; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    for v in 12.0 12.1 11.9 12.05 11.95; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/head.jsonl"; done
    compare "$d" --verdict-out "$d/flag"
    says "moved past what this rig can resolve" "a real move is called out"
    [[ "$(cat "$d/flag")" == "true" ]] \
        || fail_self "the verdict file holds [$(cat "$d/flag")], not the bare string true"

    # 26. No move: within noise, and the flag says so.
    d=$(case_dir quiet)
    for v in 10.0 10.1 9.9 10.05 9.95; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    for v in 10.02 10.08 9.92 10.03 9.97; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/head.jsonl"; done
    compare "$d" --verdict-out "$d/flag"
    says "No scenario moved past" "a quiet comparison says so"
    [[ "$(cat "$d/flag")" == "false" ]] \
        || fail_self "the verdict file holds [$(cat "$d/flag")], not the bare string false"

    # 27. A consistent shift smaller than the threshold does NOT move, even
    #     though its interval excludes zero. Both conditions are required, and
    #     this is the fixture that proves the effect-size half is live.
    d=$(case_dir small-but-certain)
    for v in 10.000 10.001 9.999 10.0005 9.9995; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    for v in 10.100 10.101 10.099 10.1005 10.0995; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/head.jsonl"; done
    compare "$d" --verdict-out "$d/flag"
    [[ "$(cat "$d/flag")" == "false" ]] \
        || fail_self "a 1% shift is below the 5% threshold and must not count as moved"

    # 28. A large mean shift with an interval that straddles zero must NOT
    #     move. This is the fixture that proves the interval half is live:
    #     the differences average +2 on a base of 10 — four times the
    #     threshold — but scatter far wider than their own mean.
    d=$(case_dir large-but-noisy)
    for v in 10 5 15 8 12; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/base.jsonl"; done
    for v in 12 20 4 18 6; do rec s3_backfill '{"codec":"none"}' wall_s "$v" >>"$d/head.jsonl"; done
    compare "$d" --verdict-out "$d/flag"
    [[ "$(cat "$d/flag")" == "false" ]] \
        || fail_self "a 20% shift whose interval straddles zero must not count as moved"

    # 29. Informational metrics are rendered and never judged.
    d=$(case_dir informational)
    for v in 100 101 99; do
        printf '{"schema":1,"bench":"s3_backfill","kind":"measurement","trigger":"dispatched","run":{"ts_ms":0,"host":"box","cpu":"x","cores":8,"os":"linux/aarch64","profile":"release"},"variant":{"codec":"none"},"metrics":{"peak_rss_mb":{"value":%s,"unit":"MB","higher_is_better":false}}}\n' "$v" >>"$d/base.jsonl"
    done
    for v in 200 201 199; do
        printf '{"schema":1,"bench":"s3_backfill","kind":"measurement","trigger":"dispatched","run":{"ts_ms":0,"host":"box","cpu":"x","cores":8,"os":"linux/aarch64","profile":"release"},"variant":{"codec":"none"},"metrics":{"peak_rss_mb":{"value":%s,"unit":"MB","higher_is_better":false}}}\n' "$v" >>"$d/head.jsonl"
    done
    compare "$d" --verdict-out "$d/flag"
    [[ "$(cat "$d/flag")" == "false" ]] \
        || fail_self "a doubled informational metric must not set the verdict"
    says "peak_rss_mb" "an informational metric is still rendered"

    echo "bench-compare.sh: self-test ok — unpairable, one-sided, added, removed, unequal, empty, non-finite and foreign-schema inputs are all reported; the verdict needs both an effect size and an interval clear of zero"
    exit 0
fi

# ---------------------------------------------------------------------------

verdict_out=""
if [[ "${1:-}" == "--verdict-out" ]]; then
    if [[ $# -lt 2 || -z "${2:-}" ]]; then
        echo "::error::--verdict-out needs a file argument" >&2
        exit 1
    fi
    verdict_out="$2"
    shift 2
fi

if [[ $# -lt 2 ]]; then
    echo "usage: $0 [--verdict-out FILE] <base.jsonl> <head.jsonl> [<base-label>] [<head-label>]" >&2
    echo "       $0 --self-test" >&2
    exit 1
fi
# An option after the filenames is a mistake, not a label. Accepting
# `--verdict-out` there wrote no flag, put a filename in the table header, and
# exited 0 — a caller would see a green run and an empty verdict.
for arg in "$@"; do
    case "$arg" in
        --*)
            echo "::error::$arg must come before the two result files" >&2
            exit 1
            ;;
    esac
done

base_file="$1"
head_file="$2"
base_label="${3:-base}"
head_label="${4:-head}"

check_leg() { # label path
    if [[ -z "$2" ]]; then
        echo "::error::no $1 result file given — pass one as the $1 argument" >&2
        return 1
    fi
    if [[ -d "$2" ]]; then
        echo "::error::the $1 result file $2 is a directory" >&2
        return 1
    fi
    if [[ ! -r "$2" || ! -s "$2" ]]; then
        echo "::error::the $1 result file $2 is missing, unreadable or empty" >&2
        return 1
    fi
}
check_leg base "$base_file" || exit 1
check_leg head "$head_file" || exit 1

if ! summary=$(analyse "$base_file" "$head_file"); then
    echo "::error::the comparison failed — the inputs are not one JSON record per line, or the analysis errored on their contents" >&2
    exit 1
fi

# A leg with nothing in it is not a comparison. Reported as a failure rather
# than rendered as "no change", which is what an empty table would read as.
for leg in base head; do
    n=$(jq -r ".${leg}_records" <<<"$summary")
    if [[ "$n" -eq 0 ]]; then
        echo "::error::the $leg leg holds no records of schema $SCHEMA_VERSION — there is nothing to compare" >&2
        exit 1
    fi
done

if [[ -n "$verdict_out" ]]; then
    jq -r 'if .moved > 0 then "true" else "false" end' <<<"$summary" >"$verdict_out"
fi

# shellcheck disable=SC2016  # jq variables, deliberately unexpanded
RENDER='
def n3($x): if $x == null then "—" else "\(($x * 1000 | round) / 1000)" end;
def pct($x): if $x == null then "—" else "\(($x * 1000 | round) / 10)%" end;
def spread($s): if $s.half == null then n3($s.mean) else "\(n3($s.mean)) ±\(n3($s.half))" end;
# `higher_is_better` travels with the number, so the arrow never has to be
# guessed from the metric name.
def better($m): if $m.higher_is_better then ($m.rel > 0) else ($m.rel < 0) end;
def mark($m): (if $m.moved then "**\(pct($m.rel))**" else pct($m.rel) end)
            + (if $m.moved then (if better($m) then " ✅" else " ❌" end) else "" end);
def esc: tostring | gsub("\\|"; "\\|");

[.rows[] | select(.kind == "paired") as $r | $r.metrics[] | select(.state == "paired") | . + {row: $r.name}] as $judged
| [$judged[] | select(.threshold != null) | select(.judgeable)] as $graded
| [$graded[] | select(.moved)] as $moved
| [$graded[] | select(.moved | not)] as $steady
| [$judged[] | select(.threshold != null) | select(.judgeable | not)] as $unjudgeable
| [$judged[] | select(.threshold == null)] as $info
| [.rows[] | select(.kind == "paired") as $r | $r.metrics[] | select(.state == "one_side_only") | . + {row: $r.name}] as $sided
| [.rows[] | select(.kind == "base_only")] as $gone
| [.rows[] | select(.kind == "head_only")] as $new
| [$judged[] | select(.discarded > 0)] as $ragged
| (
"<!-- spate-wallclock-report -->",
"## Wall-clock A/B",
"",
(if .paired == 0 then
   "**Nothing could be paired.** Every scenario appears on one side only. That usually means a variant key was added or removed, so the two legs describe the same runs under different identities — no comparison is possible until that is reconciled."
 elif ($graded | length) == 0 then
   "**Nothing could be judged.** Every paired row either has a single repetition, so no interval, or a base of zero, so no relative change. The numbers are below; none of them is a verdict."
 elif ($moved | length) == 0 then
   "**No scenario moved past what this rig can resolve.**"
 else
   "**\(.moved_scenarios) scenario(s) moved past what this rig can resolve** — \($moved | length) metric(s)."
 end),
"",
("`\($head)` vs `\($base)` · \(.paired) scenario(s) paired"
  + (if (.base_only + .head_only) > 0 then " · \(.base_only + .head_only) unpaired" else "" end)
  + (if (.base_foreign + .head_foreign) > 0 then " · \(.base_foreign + .head_foreign) record(s) skipped as another schema" else "" end)
  + (if (.base_nonmeasurement + .head_nonmeasurement) > 0 then " · \(.base_nonmeasurement + .head_nonmeasurement) verdict record(s) skipped" else "" end)),
"",
(if ($moved | length) > 0 then
   ("| Scenario | Metric | \($base) | \($head) | Δ | Threshold | n |",
    "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ($moved[] | "| \(.row | esc) | `\(.metric | esc)` | \(spread(.base)) | \(spread(.head)) | \(mark(.)) | \(pct(.threshold)) | \(.diff.n) |"),
    "")
 else empty end),
(if ($steady | length) > 0 then
   ("<details><summary>Within resolution (\($steady | length))</summary>",
    "",
    "| Scenario | Metric | \($base) | \($head) | Δ | Threshold | n |",
    "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ($steady[] | "| \(.row | esc) | `\(.metric | esc)` | \(spread(.base)) | \(spread(.head)) | \(pct(.rel)) | \(pct(.threshold)) | \(.diff.n) |"),
    "",
    "</details>",
    "")
 else empty end),
(if ($unjudgeable | length) > 0 then
   ("<details><summary>Not judgeable — one repetition, so no interval (\($unjudgeable | length))</summary>",
    "",
    "A single repetition has no spread, so nothing here can be called moved or steady. Run more repetitions to judge these.",
    "",
    "| Scenario | Metric | \($base) | \($head) | Δ | n |",
    "| --- | --- | ---: | ---: | ---: | ---: |",
    ($unjudgeable[] | "| \(.row | esc) | `\(.metric | esc)` | \(spread(.base)) | \(spread(.head)) | \(pct(.rel)) | \(.diff.n) |"),
    "",
    "</details>",
    "")
 else empty end),
(if ($info | length) > 0 then
   ("<details><summary>Informational — reported, never judged (\($info | length))</summary>",
    "",
    "| Scenario | Metric | \($base) | \($head) | Δ | n |",
    "| --- | --- | ---: | ---: | ---: | ---: |",
    ($info[] | "| \(.row | esc) | `\(.metric | esc)` | \(spread(.base)) | \(spread(.head)) | \(pct(.rel)) | \(.diff.n) |"),
    "",
    "</details>",
    "")
 else empty end),
(if ($sided | length) + ($gone | length) + ($new | length) > 0 then
   ("<details><summary>Unpaired — present on one side only (\(($sided | length) + ($gone | length) + ($new | length)))</summary>",
    "",
    "These are findings, not omissions. A metric or scenario on one side alone is a change to what is measured.",
    "",
    ($sided[] | "- `\(.metric | esc)` on \(.row | esc): **\(.side) only**, \(n3(.value)) \(.unit // "" | esc)"),
    ($gone[] | "- scenario `\(.name | esc)`: **\($base) only** — deleted, renamed, or its variant identity changed"),
    ($new[] | "- scenario `\(.name | esc)`: **\($head) only** — added, renamed, or its variant identity changed"),
    "",
    "</details>",
    "")
 else empty end),
(if ($ragged | length) > 0 then
   ("<details><summary>Unequal repetition counts (\($ragged | length))</summary>",
    "",
    ($ragged[] | "- `\(.metric | esc)` on \(.row | esc): paired \(.diff.n), discarded \(.discarded) repetition(s) with no partner"),
    "",
    "</details>",
    "")
 else empty end),
"<details><summary>How a row is judged</summary>",
"",
"A row **moved** when both hold: the paired mean difference is at least the threshold for that bench in magnitude, and the 95% Student-t interval on the per-repetition differences excludes zero. Requiring both is what stops a tiny but very consistent shift being reported as a regression, and a large but noisy one too.",
"",
"The legs are interleaved, so repetition *i* of each ran adjacent in time and the difference is paired rather than between-groups. Repetitions pair by position within a scenario.",
"",
"Direction comes from the record: each metric carries `higher_is_better`, so ✅ and ❌ are read from the data rather than guessed from the metric name.",
"",
"Thresholds are fixed because this tier keeps no result history. They are provisional — wide enough that an advisory signal does not become a rerun button. Informational metrics are rendered and never judged.",
"",
"</details>"
)
'

jq -r --arg base "$base_label" --arg head "$head_label" "$RENDER" <<<"$summary"
